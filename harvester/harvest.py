# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import datetime
import shutil
import pandas
import os
import tempfile
import time
import json
import dask.dataframe
import requests

from . import settings
from .parse.exceptions import UnsupportedFileTypeError
from .parse.ivium_input_file import IviumInputFile
from .parse.biologic_input_file import BiologicMprInputFile
from .parse.maccor_input_file import (
    MaccorInputFile,
    MaccorExcelInputFile,
    MaccorRawInputFile,
)
from .parse.delimited_input_file import DelimitedInputFile

from .settings import get_logger, get_standard_units, get_standard_columns, VERSION
from .api import report_harvest_result

logger = get_logger(__file__)

class HarvestProcessor:

    registered_input_files = [
        BiologicMprInputFile,
        IviumInputFile,
        MaccorInputFile,
        MaccorExcelInputFile,
        MaccorRawInputFile,
        DelimitedInputFile  # Should be last because it processes files line by line and accepts anything table-like
    ]

    def __init__(self, file_path: str, monitored_path: dict):
        self.file_path = file_path
        self.monitored_path = monitored_path
        for input_file_cls in self.registered_input_files:
            try:
                logger.debug('Tried input reader {}'.format(input_file_cls))
                input_file = input_file_cls(
                    file_path=file_path,
                    standard_units=get_standard_units(),
                    standard_columns=get_standard_columns()
                )
            except Exception as e:
                logger.debug('...failed with: ', type(e), e)
                continue
            logger.debug('...succeeded...')
            self.input_file = input_file
            self.parser = input_file_cls
            return
        raise UnsupportedFileTypeError

    @staticmethod
    def serialize_datetime(v):
        """
        Recursively search for date[time] classes and convert
        dates to iso format strings and datetimes to timestamps
        """
        if isinstance(v, datetime.datetime):
            return v.timestamp()
        if isinstance(v, datetime.date):
            return v.isoformat()
        if isinstance(v, dict):
            return {k: HarvestProcessor.serialize_datetime(x) for k, x in v.items()}
        if isinstance(v, list):
            return [HarvestProcessor.serialize_datetime(x) for x in v]
        return v

    @staticmethod
    def get_test_date(metadata):
        """
        Get the test date from the metadata
        """
        return HarvestProcessor.serialize_datetime(metadata.get('Date of Test'))

    def harvest(self):
        """
        Report the file metadata, column metadata, and upload the data to the server
        """
        metadata_time = time.time()
        self._report_file_metadata()
        column_time = time.time()
        logger.info(f"Metadata reported in {column_time - metadata_time:.2f} seconds")
        self._report_column_metadata()
        data_prep_time = time.time()
        logger.info(f"Column metadata reported in {data_prep_time - column_time:.2f} seconds")
        self._prepare_data()
        upload_time = time.time()
        logger.info(f"Data prepared in {upload_time - data_prep_time:.2f} seconds")
        self._upload_data()
        logger.info(f"Data uploaded in {time.time() - upload_time:.2f} seconds")
        self._delete_temp_files()

    def _report_file_metadata(self):
        """
        Report a file's metadata, and save the server response to self.server_metadata
        """
        core_metadata, extra_metadata = self.input_file.load_metadata()
        report = report_harvest_result(
            path=self.file_path,
            monitored_path_uuid=self.monitored_path.get('uuid'),
            content={
                'task': settings.HARVESTER_TASK_IMPORT,
                'stage': settings.HARVEST_STAGE_FILE_METADATA,
                'data': {
                    'core_metadata': HarvestProcessor.serialize_datetime(core_metadata),
                    'extra_metadata': HarvestProcessor.serialize_datetime(extra_metadata),
                    'test_date': HarvestProcessor.get_test_date(core_metadata),
                    'parser': self.input_file.__class__.__name__
                }
            }
        )
        if report is None:
            logger.error(f"Report Metadata - API Error: no response from server")
            raise RuntimeError("API Error: no response from server")
        if not report.ok:
            try:
                logger.error(f"Report Metadata - API responded with Error: {report.json()['error']}")
            except BaseException:
                logger.error(f"Report Metadata - API Error: {report.status_code}")
            raise RuntimeError("API Error: server responded with error")
        self.server_metadata = report.json()['upload_info']

    def _report_column_metadata(self):
        """
        Report the column metadata to the server.
        Data include the column names, types, units, and whether they relate to recognised standard columns.
        """
        default_units = get_standard_units()
        columns = self.server_metadata.get('columns')
        if len(columns):
            mapping = {c.get('name'): c.get('id') for c in columns}
        else:
            mapping = self.input_file.get_file_column_to_standard_column_mapping()

        # Use first row to determine column data types
        columns_with_data = [c for c in self.input_file.column_info.keys() if self.input_file.column_info[c].get('has_data')]
        first_row = next(self.input_file.load_data(self.input_file.file_path, columns_with_data))
        column_data = {}

        for k, v in first_row.items():
            column_data[k] = {'data_type': type(v).__name__}
            if k in mapping:
                column_data[k]['column_id'] = mapping[k]
            else:
                column_data[k]['column_name'] = k
                if 'unit' in self.input_file.column_info[k]:
                    column_data[k]['unit_symbol'] = self.input_file.column_info[k].get('unit')

        # Upload results
        report = report_harvest_result(
            path=self.file_path,
            monitored_path_uuid=self.monitored_path.get('uuid'),
            content={
                'task': settings.HARVESTER_TASK_IMPORT,
                'stage': settings.HARVEST_STAGE_COLUMN_METADATA,
                'data': column_data
            }
        )
        if report is None:
            logger.error(f"Report Column Metadata - API Error: no response from server")
            raise RuntimeError("API Error: no response from server")
        if not report.ok:
            try:
                logger.error(f"Report Column Metadata - API responded with Error: {report.json()['error']}")
            except BaseException:
                logger.error(f"Report Column Metadata - API Error: {report.status_code}")
            raise RuntimeError("API Error: server responded with error")

    def _prepare_data(self, partition_line_count=100_000_000):
        """
        Read the data from the file and save it as a temporary .parquet file self.data_file
        """
        def partition_generator(generator, partition_line_count=100_000_000):
            def to_df(rows):
                return pandas.DataFrame(rows)

            stopping = False
            while not stopping:
                rows = []
                try:
                    for _ in range(partition_line_count):
                        rows.append(next(generator))
                except StopIteration:
                    stopping = True
                yield to_df(rows)

        reader = self.input_file.load_data(
            self.file_path,
            [c for c in self.input_file.column_info.keys() if self.input_file.column_info[c].get('has_data')]
        )

        data = dask.dataframe.from_map(
            pandas.DataFrame,
            partition_generator(reader, partition_line_count=partition_line_count)
        )

        # Save the data as parquet
        self.data_file_name = os.path.join(tempfile.gettempdir(), f"{os.path.basename(self.file_path)}.parquet")
        data.to_parquet(
            self.data_file_name,
            write_index=False,
            compute=True,
            custom_metadata={
                'galv-harvester-version': VERSION
            }
        )
        self.row_count = data.shape[0].compute()
        self.partition_count = data.npartitions

    def _upload_data(self):
        """
        Upload the data to the server
        """
        upload_params = report_harvest_result(
            path=self.file_path,
            monitored_path_uuid=self.monitored_path.get('uuid'),
            content={
                'task': settings.HARVESTER_TASK_IMPORT,
                'stage': settings.HARVEST_STAGE_GET_UPLOAD_URLS,
                'data': {
                    'row_count': self.row_count,
                    'partition_count': self.partition_count
                }
            }
        )
        # The server should respond with a list of presigned URLs for each partition in the format {url, fields}
        if upload_params is None:
            logger.error(f"Get Upload Params - API Error: no response from server")
            raise RuntimeError("API Error: no response from server")
        if not upload_params.ok:
            try:
                logger.error(f"Get Upload Params - API responded with Error: {upload_params.json()['error']}")
            except BaseException:
                logger.error(f"Get Upload Params - API Error: {upload_params.status_code}")
            raise RuntimeError("API Error: server responded with error")

        self.upload_params = upload_params.json()

        successes = 0
        errors = []
        for i in range(self.partition_count):
            files = {'file': open(os.path.join(self.data_file_name, f"part.{i}.parquet"), 'rb')}
            if self.upload_params['storage_urls'][i].get('error'):
                errors.append((i, f"Skipped file {i}: {self.upload_params['storage_urls'][i]['error']}"))
                continue
            url = self.upload_params['storage_urls'][i]['url']
            fields = self.upload_params['storage_urls'][i]['fields']
            response = requests.post(url, data=fields, files=files)
            if response.status_code != 204:
                try:
                    errors.append((i, f"POST to {url} failed: {response.json()['error']}"))
                except BaseException:
                    errors.append((i, f"POST to {url} failed: {response.status_code}"))
            else:
                successes += 1

        if successes != self.partition_count:
            logger.error(f"Data Upload - {successes} of {self.partition_count} partitions uploaded successfully")
            for i, error in errors:
                logger.error(f"Data Upload - Partition {i} failed with error: {error}")
        else:
            logger.info(f"Data Upload - {successes} partitions uploaded successfully")

        report_harvest_result(
            path=self.file_path,
            monitored_path_uuid=self.monitored_path.get('uuid'),
            content={
                'task': settings.HARVESTER_TASK_IMPORT,
                'stage': settings.HARVEST_STAGE_UPLOAD_COMPLETE,
                'data': {
                    'successes': successes,
                    'errors': errors
                }
            }
        )

    def _delete_temp_files(self):
        """
        Delete temporary files created during the process
        """
        if hasattr(self, 'data_file_name') and os.path.exists(self.data_file_name):
            shutil.rmtree(self.data_file_name)

    def __del__(self):
        self._delete_temp_files()


if False:
    # My debugger won't connect, so running this in the console can figure out dask issues
    import os
    import pandas
    import dask.dataframe
    import shutil
    from harvester.settings import get_standard_units, get_standard_columns
    from harvester.parse.biologic_input_file import BiologicMprInputFile
    os.system('cp .harvester/.harvester.json /harvester_files')
    standard_units = get_standard_units()
    standard_columns = get_standard_columns()
    file_path = '.test-data/test-suite-small/adam_3_C05.mpr'
    input_file = BiologicMprInputFile(file_path, standard_units=standard_units, standard_columns=standard_columns)
    def partition_generator(generator, partition_line_count = 100_000_000):
        def to_df(rows):
            return pandas.DataFrame(rows)
        stopping = False
        while not stopping:
            rows = []
            try:
                for _ in range(partition_line_count):
                    rows.append(next(generator))
            except StopIteration:
                stopping = True
            yield to_df(rows)

    generator = input_file.load_data(
        file_path,
        [c for c in input_file.column_info.keys() if input_file.column_info[c].get('has_data')]
    )

    data = dask.dataframe.from_map(pandas.DataFrame, partition_generator(generator, partition_line_count=100000))
    data.compute()
    print(f"Partitions: {data.npartitions}")
    data.to_parquet(
        "test.tmp.parquet",
        write_index=False,
        compute=True,
        custom_metadata={
            'galv-harvester-version': '0.1.0'
        }
    )
    print(f"Rows: {data.shape[0].compute()}")
    # Then we would upload the data by getting presigned URLs for each partition
    shutil.rmtree("test.tmp.parquet")
    print('done')

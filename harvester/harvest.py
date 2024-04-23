# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import datetime
import shutil

import math
import pandas
import os
import tempfile
import time
import json
import dask.dataframe
import requests
import fastnumbers

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

from .settings import get_logger, get_standard_units, get_standard_columns, VERSION, get_setting
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
        self.mapping = None
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
        self._report_summary()
        if self.mapping is not None:
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
            monitored_path_id=self.monitored_path.get('id'),
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

    def _report_summary(self):
        """
        Report the column metadata to the server.
        Data include the column names, types, units, and whether they relate to recognised standard columns.
        """
        summary_row_count = 10
        summary_data = []
        iterator = self.input_file.load_data(
            self.file_path,
            [c for c in self.input_file.column_info.keys() if self.input_file.column_info[c].get('has_data')]
        )
        for row in iterator:
            summary_data.append(row)
            if len(summary_data) >= summary_row_count:
                break

        summary = pandas.DataFrame(summary_data)

        # Upload results
        report = report_harvest_result(
            path=self.file_path,
            monitored_path_id=self.monitored_path.get('id'),
            content={
                'task': settings.HARVESTER_TASK_IMPORT,
                'stage': settings.HARVEST_STAGE_DATA_SUMMARY,
                'data': summary.to_json()
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

        mapping_url = report.json()['mapping']
        if mapping_url is None:
            logger.info("Mapping could not be automatically determined. Will revisit when user determines mapping.")
            return
        mapping_request = requests.get(mapping_url, headers={'Authorization': f"Harvester {get_setting('api_key')}"})
        if mapping_request is None:
            logger.error(f"Report Column Metadata - API Error: no response from server")
            raise RuntimeError("API Error: no response from server")
        if not mapping_request.ok:
            try:
                logger.error(f"Report Column Metadata - API responded with Error: {mapping_request.json()['error']}")
            except BaseException:
                logger.error(f"Report Column Metadata - API Error: {mapping_request.status_code}")
            raise RuntimeError("API Error: server responded with error")
        self.mapping = mapping_request.json().get('rendered_map')
        if not isinstance(self.mapping, dict):
            if mapping_request:
                logger.error(f"Server returned mapping request but no mapping was found")
            else:
                logger.info("Mapping could not be automatically determined")

    def _prepare_data(self):
        """
        Read the data from the file and save it as a temporary .parquet file self.data_file
        """
        def remap(df, mapping):
            """
            Remap the columns in the dataframe according to the mapping.
            """
            columns = list(df.columns)
            for col_name, mapping in mapping.items():
                new_name = mapping.get('new_name')
                if new_name in df.columns and new_name != col_name:
                    raise ValueError(f"New name '{new_name}' already exists in the dataframe")
                if mapping['data_type'] in ["bool", "str"]:
                    df[col_name] = df[col_name].astype(mapping["data_type"])
                elif mapping['data_type'] == 'datetime64[ns]':
                    df[col_name] = pandas.to_datetime(df[col_name])
                else:
                    if mapping['data_type'] == 'int':
                        df[col_name] = fastnumbers.try_forceint(df[col_name], map=list, on_fail=math.nan)
                    else:
                        df[col_name] = fastnumbers.try_float(df[col_name], map=list, on_fail=math.nan)

                    addition = mapping.get('addition', 0)
                    multiplier = mapping.get('multiplier', 1)
                    df[col_name] = df[col_name] + addition
                    df[col_name] = df[col_name] * multiplier
                df.rename(columns={col_name: new_name}, inplace=True)
                columns.pop(columns.index(col_name))
            # If there are any columns left, they are not in the mapping and should be converted to floats
            for col_name in columns:
                df[col_name] = fastnumbers.try_float(df[col_name], map=list, on_fail=math.nan)
            return df

        def partition_generator(generator, partition_line_count=100_000):
            def to_df(rows):
                return remap(pandas.DataFrame(rows), mapping=self.mapping)

            stopping = False
            while not stopping:
                rows = []
                try:
                    for _ in range(partition_line_count):
                        rows.append(next(generator))
                except StopIteration:
                    stopping = True
                yield to_df(rows)

        partition_line_count = self.monitored_path.get("max_partition_line_count", 100_000)

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

        def pad0(n, width=math.floor(self.partition_count/10) + 1):
            return f"{n:0{width}d}"

        successes = 0
        errors = {}

        for i in range(self.partition_count):
            filename = f"{os.path.splitext(os.path.basename(self.file_path))[0]}.part_{pad0(i)}.parquet"
            files = {'parquet_file': (filename, open(os.path.join(self.data_file_name, f"part.{i}.parquet"), 'rb'))}
            report = report_harvest_result(
                path=self.file_path,
                monitored_path_id=self.monitored_path.get('id'),
                # send data in a flat format to accompany file upload protocol.
                # Kinda hacky because it overwrites much of report_harvest_result's functionality
                data={
                    'status': settings.HARVESTER_STATUS_SUCCESS,
                    'path': self.file_path,
                    'monitored_path_id': self.monitored_path.get('id'),
                    'task': settings.HARVESTER_TASK_IMPORT,
                    'stage': settings.HARVEST_STAGE_UPLOAD_PARQUET,
                    'total_row_count': self.row_count,
                    'partition_number': i,
                    'partition_count': self.partition_count,
                    'filename': filename
                },
                files=files
            )
            if report is None:
                errors[i] = (f"Failed to upload {filename} - API Error: no response from server")
            elif not report.ok:
                try:
                    errors[i] = (f"Failed to upload {filename} - API responded with Error: {report.json()['error']}")
                except BaseException:
                    errors[i] = f"Failed to upload {filename}. Received HTTP {report.status_code}"
            else:
                successes += 1

        if successes == 0 and self.partition_count > 0:
            raise RuntimeError("API Error: failed to upload all partitions to server")
        if successes != self.partition_count:
            logger.error(f"Data Upload - {successes} of {self.partition_count} partitions uploaded successfully")
            for filename, error in errors.items():
                logger.error(f"Data Upload - Partition {filename} failed with error: {error}")
        else:
            logger.info(f"Data Upload - {successes} partitions uploaded successfully")

        report_harvest_result(
            path=self.file_path,
            monitored_path_id=self.monitored_path.get('id'),
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
    import fastnumbers
    import math
    from harvester.settings import get_standard_units, get_standard_columns
    from harvester.parse.maccor_input_file import MaccorInputFile
    os.system('cp .harvester/.harvester.json /harvester_files')
    standard_units = get_standard_units()
    standard_columns = get_standard_columns()
    file_path = '.test-data/test-suite-small/TPG1+-+Cell+15+-+002.txt'
    input_file = MaccorInputFile(file_path, standard_units=standard_units, standard_columns=standard_columns)

    mapping = {
        "Amps": {
            "new_name": "Current_A",
            "data_type": "float",
            "multiplier": 0.001,
            "addition": 0
        },
        "Rec#": {
            "new_name": "Sample_number",
            "data_type": "int",
            "multiplier": 1,
            "addition": 0
        },
        "Step": {
            "new_name": "Step_number",
            "data_type": "int",
            "multiplier": 1,
            "addition": 0
        },
        "State": {
            "new_name": "State",
            "data_type": "str"
        },
        "Volts": {
            "new_name": "Voltage_V",
            "data_type": "float",
            "multiplier": 1,
            "addition": 0
        },
        "Temp 1": {
            "new_name": "Temperature_K",
            "data_type": "float",
            "multiplier": 1,
            "addition": 273.15
        },
        "DPt Time": {
            "new_name": "Datetime",
            "data_type": "datetime64[ns]"
        },
        "StepTime": {
            "new_name": "Step_time_s",
            "data_type": "float",
            "multiplier": 1,
            "addition": 0
        },
        "TestTime": {
            "new_name": "Elapsed_time_s",
            "data_type": "float",
            "multiplier": 1,
            "addition": 0
        }
    }

    def remap(df, mapping):
        columns = list(df.columns)
        for col_name, mapping in mapping.items():
            new_name = mapping['new_name']
            if mapping['data_type'] in ["bool", "str"]:
                df[new_name] = df[col_name].astype(mapping["data_type"])
            elif mapping['data_type'] == 'datetime64[ns]':
                df[new_name] = pandas.to_datetime(df[col_name])
            else:
                if mapping['data_type'] == 'int':
                    df[new_name] = fastnumbers.try_forceint(df[col_name], map=list, on_fail=math.nan)
                else:
                    df[new_name] = fastnumbers.try_float(df[col_name], map=list, on_fail=math.nan)

                addition = mapping.get('addition', 0)
                multiplier = mapping.get('multiplier', 1)
                df[new_name] = df[new_name] + addition
                df[new_name] = df[new_name] * multiplier
            df.drop(columns=[col_name], inplace=True)
            columns.pop(columns.index(col_name))
        # If there are any columns left, they are not in the mapping and should be converted to floats
        for col_name in columns:
            df[col_name] = fastnumbers.try_float(df[col_name], map=list, on_fail=math.nan)
        return df

    def partition_generator(generator, partition_line_count=100_000):
        def to_df(rows):
            return remap(pandas.DataFrame(rows), mapping=mapping)

        stopping = False
        while not stopping:
            rows = []
            try:
                for _ in range(partition_line_count):
                    rows.append(next(generator))
            except StopIteration:
                stopping = True
            yield to_df(rows)

    partition_line_count = 10_000

    reader = input_file.load_data(
        file_path,
        [c for c in input_file.column_info.keys() if input_file.column_info[c].get('has_data')]
    )

    data = dask.dataframe.from_map(
        pandas.DataFrame,
        partition_generator(reader, partition_line_count=partition_line_count)
    )

    data.compute()
    print(f"Rows: {data.shape[0].compute()}")
    # Then we would upload the data by getting presigned URLs for each partition
    shutil.rmtree("test.tmp.parquet")
    print('done')

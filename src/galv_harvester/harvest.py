# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import datetime

import tempfile
import time
import dask.dataframe
import pandas
import fastnumbers
import math
import os
import shutil
import requests
import holoviews as hv
import holoviews.operation.datashader as hd

from . import settings
from .parse.arbin import ArbinCSVFile
from .parse.exceptions import UnsupportedFileTypeError
from .parse.ivium_input_file import IviumInputFile
from .parse.biologic_input_file import BiologicMprInputFile
from .parse.maccor_input_file import (
    MaccorInputFile,
    MaccorExcelInputFile,
    MaccorRawInputFile,
)
from .parse.delimited_input_file import DelimitedInputFile

from .api import report_harvest_result, StorageError

from .__about__ import VERSION

logger = settings.get_logger(__file__)


class HarvestProcessor:
    registered_input_files = [
        BiologicMprInputFile,
        IviumInputFile,
        MaccorInputFile,
        MaccorExcelInputFile,
        MaccorRawInputFile,
        ArbinCSVFile,
        DelimitedInputFile  # Should be last because it processes files line by line and accepts anything table-like
    ]

    @staticmethod
    def check_response(step: str, response):
        if response is None:
            logger.error(f"{step} failed: no response from server")
            raise RuntimeError(f"{step} failed: no response from server")
        if not response.ok:
            try:
                logger.error(f"{step} failed: {response.json()['error']}")
            except BaseException:
                logger.error(f"{step} failed: received HTTP {response.status_code}")
            raise RuntimeError("{step}: failed server responded with error")

    def __init__(self, file_path: str, monitored_path: dict):
        self.mapping = None
        self.file_path = file_path
        self.monitored_path = monitored_path
        for input_file_cls in self.registered_input_files:
            try:
                logger.debug('Tried input reader {}'.format(input_file_cls))
                input_file = input_file_cls(file_path=file_path)
            except UnsupportedFileTypeError as e:
                logger.debug('...failed with: ', type(e), e)
                continue
            except Exception as e:
                logger.error((
                    f"{input_file_cls.__name__} failed to import"
                    f" {file_path} with non-UnsupportedFileTypeError: {e}"
                ))
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
        try:
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
        except StorageError as e:
            logger.error(f"Skipping file due to StorageError: {e}")

    def _report_file_metadata(self):
        """
        Report a file's metadata
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
        HarvestProcessor.check_response("Report Metadata", report)

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
        HarvestProcessor.check_response("Report Column Metadata", report)

        mapping_url = report.json()['mapping']
        if mapping_url is None:
            logger.info("Mapping could not be automatically determined. Will revisit when user determines mapping.")
            return
        mapping_request = requests.get(
            mapping_url,
            headers={'Authorization': f"Harvester {settings.get_setting('api_key')}"}
        )
        HarvestProcessor.check_response("Get Mapping", mapping_request)
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

        # Create a plot of key data columns for identification purposes
        self._plot_png(data)

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

    def _plot_png(self, data):
        """
        Create a plot of key data columns for identification purposes
        """
        try:
            self.png_file_name = os.path.join(tempfile.gettempdir(), f"{os.path.basename(self.file_path)}.png")
            hd.shade.cmap = ["lightblue", "darkblue"]
            hv.extension("matplotlib")
            hv.output(fig='png', backend="matplotlib")
            dataset = hv.Dataset(data, 'ElapsedTime_s', ['Voltage_V', 'Current_A'])
            layout = (
                    dataset.to(hv.Curve, 'ElapsedTime_s', 'Voltage_V') +
                    dataset.to(hv.Curve, 'ElapsedTime_s', 'Current_A')
            )
            layout.opts(hv.opts.Curve(framewise=True, aspect=4, sublabel_format=''))
            hv.save(layout, self.png_file_name, fmt='png', dpi=300)
            self.png_ok = True
        except Exception as e:
            logger.warning(f"Failed to create plot: {e}")
            self.png_ok = False

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
            with open(os.path.join(self.data_file_name, f"part.{i}.parquet"), 'rb') as f:
                files = {'parquet_file': (filename, f)}
                report = report_harvest_result(
                    path=self.file_path,
                    monitored_path_id=self.monitored_path.get('id'),
                    # send data in a flat format to accompany file upload protocol.
                    # Kinda hacky because it overwrites much of report_harvest_result's functionality
                    data={
                        'format': 'flat',
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

        if self.png_ok:
            with open(self.png_file_name, 'rb') as f:
                files = {'png_file': f}
                report = report_harvest_result(
                    path=self.file_path,
                    monitored_path_id=self.monitored_path.get('id'),
                    # send data in a flat format to accompany file upload protocol.
                    # Kinda hacky because it overwrites much of report_harvest_result's functionality
                    data={
                        'format': 'flat',
                        'status': settings.HARVESTER_STATUS_SUCCESS,
                        'path': self.file_path,
                        'monitored_path_id': self.monitored_path.get('id'),
                        'task': settings.HARVESTER_TASK_IMPORT,
                        'stage': settings.HARVEST_STAGE_UPLOAD_PNG,
                        'filename': os.path.basename(self.png_file_name)
                    },
                    files=files
                )
            try:
                HarvestProcessor.check_response("Upload PNG", report)
            except BaseException as e:
                logger.warning(f"Failed to upload PNG: {e}")

    def _delete_temp_files(self):
        """
        Delete temporary files created during the process
        """
        for attribute in ['data_file_name', 'png_file_name']:
            if hasattr(self, attribute):
                filename = getattr(self, attribute)
                if os.path.exists(filename):
                    try:
                        if os.path.isdir(filename):
                            shutil.rmtree(filename)
                        else:
                            os.remove(filename)
                    except PermissionError:
                        logger.warning(f"Failed to delete {filename}. This will have to be manually deleted.")

    def __del__(self):
        self._delete_temp_files()

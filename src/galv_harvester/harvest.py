# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import datetime

import tempfile
import time
from typing import Optional

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

from .plugins import get_parsers

logger = settings.get_logger(__file__)


class HarvestProcessor:
    default_parsers = [
        BiologicMprInputFile,
        IviumInputFile,
        MaccorInputFile,
        MaccorExcelInputFile,
        MaccorRawInputFile,
        ArbinCSVFile,
        DelimitedInputFile,  # Should be last because it processes files line by line and accepts anything table-like
    ]
    summary_row_count = 10
    no_upload = False

    mapping = None
    png_file_name = None
    data_file_name = None
    tmp_dir = None
    zip_file = None
    row_count = None
    partition_count = None
    parser_errors = {}

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

    def __init__(self, file_path: str, monitored_path: Optional[dict]):
        self.mapping = None
        self.file_path = file_path
        self.tmp_dir = tempfile.mkdtemp(prefix="galv_h_")
        self.monitored_path = monitored_path
        self.parser_classes = [*get_parsers(), *self.default_parsers]
        for input_file_cls in self.default_parsers:
            try:
                logger.debug("Tried input reader {}".format(input_file_cls))
                input_file = input_file_cls(file_path=file_path)
            except UnsupportedFileTypeError as e:
                self.parser_errors[input_file_cls.__name__] = e
                logger.debug(f"...failed with {e.__class__.__name__}: {e}")
                continue
            except Exception as e:
                logger.error(
                    (
                        f"{input_file_cls.__name__} failed to import"
                        f" {file_path} with non-UnsupportedFileTypeError: {e}"
                    )
                )
                continue
            logger.debug("...succeeded...")
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
        return HarvestProcessor.serialize_datetime(metadata.get("Date of Test"))

    def harvest(self):
        """
        Report the file metadata, column metadata, and upload the data to the server
        """
        try:
            metadata_time = time.time()
            self._report_file_metadata()
            column_time = time.time()
            logger.info(
                f"Metadata reported in {column_time - metadata_time:.2f} seconds"
            )
            self._report_summary()
            if self.mapping is not None:
                data_prep_time = time.time()
                logger.info(
                    f"Column metadata reported in {data_prep_time - column_time:.2f} seconds"
                )
                self._prepare_data()
                upload_time = time.time()
                logger.info(
                    f"Data prepared in {upload_time - data_prep_time:.2f} seconds"
                )
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
            monitored_path_id=self.monitored_path.get("id"),
            content={
                "task": settings.HARVESTER_TASK_IMPORT,
                "stage": settings.HARVEST_STAGE_FILE_METADATA,
                "data": {
                    "core_metadata": HarvestProcessor.serialize_datetime(core_metadata),
                    "extra_metadata": HarvestProcessor.serialize_datetime(
                        extra_metadata
                    ),
                    "test_date": HarvestProcessor.get_test_date(core_metadata),
                    "parser": self.input_file.__class__.__name__,
                },
            },
        )
        HarvestProcessor.check_response("Report Metadata", report)

    def summarise_columns(self):
        """
        Column summary is the first few rows of the data file.

        Returns a pandas DataFrame of the first self.summary_row_count rows of all columns in the file.
        """
        summary_data = []
        iterator = self.input_file.load_data(
            self.file_path,
            [
                c
                for c in self.input_file.column_info.keys()
                if self.input_file.column_info[c].get("has_data")
            ],
        )
        for row in iterator:
            summary_data.append(row)
            if len(summary_data) >= self.summary_row_count:
                break

        return pandas.DataFrame(summary_data)

    def _report_summary(self):
        """
        Report the column metadata to the server.
        Data include the column names, types, units, and whether they relate to recognised standard columns.
        """
        summary = self.summarise_columns()

        # Upload results
        report = report_harvest_result(
            path=self.file_path,
            monitored_path_id=self.monitored_path.get("id"),
            content={
                "task": settings.HARVESTER_TASK_IMPORT,
                "stage": settings.HARVEST_STAGE_DATA_SUMMARY,
                "data": summary.to_json(),
            },
        )
        HarvestProcessor.check_response("Report Column Metadata", report)

        mapping_url = report.json()["mapping"]
        if mapping_url is None:
            logger.info(
                "Mapping could not be automatically determined. Will revisit when user determines mapping."
            )
            return
        mapping_request = requests.get(
            mapping_url,
            headers={"Authorization": f"Harvester {settings.get_setting('api_key')}"},
        )
        HarvestProcessor.check_response("Get Mapping", mapping_request)
        self.mapping = mapping_request.json().get("rendered_map")
        if not isinstance(self.mapping, dict):
            if mapping_request:
                logger.error("Server returned mapping request but no mapping was found")
            else:
                logger.info("Mapping could not be automatically determined")

    def _prepare_data(self):
        """
        Read the data from the file and save it as a temporary .csv file self.data_file
        """
        if self.mapping is None:
            raise RuntimeError(
                "Cannot process data without a mapping. Set `self.mapping` first."
            )

        def remap(df, mapping):
            """
            Remap the columns in the dataframe according to the mapping.
            """
            columns = list(df.columns)
            for col_name, mapping in mapping.items():
                new_name = mapping.get("new_name")
                if new_name in df.columns and new_name != col_name:
                    raise ValueError(
                        f"New name '{new_name}' already exists in the dataframe"
                    )
                if mapping["data_type"] in ["bool", "str"]:
                    df[col_name] = df[col_name].astype(mapping["data_type"])
                elif mapping["data_type"] == "datetime64[ns]":
                    df[col_name] = pandas.to_datetime(df[col_name])
                else:
                    if mapping["data_type"] == "int":
                        df[col_name] = fastnumbers.try_forceint(
                            df[col_name], map=list, on_fail=math.nan
                        )
                    else:
                        df[col_name] = fastnumbers.try_float(
                            df[col_name], map=list, on_fail=math.nan
                        )

                    addition = mapping.get("addition", 0)
                    multiplier = mapping.get("multiplier", 1)
                    df[col_name] = df[col_name] + addition
                    df[col_name] = df[col_name] * multiplier
                df.rename(columns={col_name: new_name}, inplace=True)
                columns.pop(columns.index(col_name))
            # If there are any columns left, they are not in the mapping and should be converted to floats
            for col_name in columns:
                df[col_name] = fastnumbers.try_float(
                    df[col_name], map=list, on_fail=math.nan
                )
            return df

        # Excel maximum rows is just over 1 million
        def partition_generator(generator, partition_line_count=1_000_000):
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

        partition_line_count = (
            self.monitored_path.get("max_partition_line_count", 1_000_000)
            if self.monitored_path
            else 1_000_000
        )

        reader = self.input_file.load_data(
            self.file_path,
            [
                c
                for c in self.input_file.column_info.keys()
                if self.input_file.column_info[c].get("has_data")
            ],
        )

        data = dask.dataframe.from_map(
            pandas.DataFrame,
            partition_generator(reader, partition_line_count=partition_line_count),
        )

        # Create a plot of key data columns for identification purposes
        self._plot_png(data)

        # Save the data as csv
        self.data_file_name = os.path.join(
            self.tmp_dir,
            f"{os.path.splitext(os.path.basename(self.file_path))[0]}",
        )
        data.to_csv(self.data_file_name, index=False)
        self.row_count = data.shape[0].compute()
        self.partition_count = data.npartitions

        # Rename part files to match the expected format
        if self.partition_count == 1:
            shutil.move(
                os.path.join(self.data_file_name, "0.part"),
                os.path.join(
                    self.data_file_name,
                    f"{os.path.splitext(os.path.basename(self.data_file_name))[0]}.csv",
                ),
            )
        else:
            for i in range(self.partition_count):
                part_file = os.path.join(self.data_file_name, f"{self.pad0(i)}.part")
                new_part_file = os.path.join(
                    self.data_file_name, f"{self.pad0(i)}.part_{self.pad0(i)}.csv"
                )
                shutil.move(part_file, new_part_file)
                logger.debug(f"Renamed {part_file} to {new_part_file}")

        # Zip data to reduce upload size
        self.zip_file = shutil.make_archive(
            self.data_file_name,
            "zip",
            self.data_file_name,
            logger=logger,
        )

    def process_data(self):
        """
        Process the data in the file.

        Will set self.data_file_name, self.row_count, and self.partition_count.
        """
        # Public interface for processing data
        self._prepare_data()

    def _plot_png(self, data):
        """
        Create a plot of key data columns for identification purposes
        """
        try:
            self.png_file_name = os.path.join(
                self.tmp_dir,
                f"{os.path.splitext(os.path.basename(self.file_path))[0]}.png",
            )
            hd.shade.cmap = ["lightblue", "darkblue"]
            hv.extension("matplotlib")
            hv.output(fig="png", backend="matplotlib")
            dataset = hv.Dataset(data, "ElapsedTime_s", ["Voltage_V", "Current_A"])
            layout = dataset.to(hv.Curve, "ElapsedTime_s", "Voltage_V") + dataset.to(
                hv.Curve, "ElapsedTime_s", "Current_A"
            )
            layout.opts(hv.opts.Curve(framewise=True, aspect=4, sublabel_format=""))
            hv.save(layout, self.png_file_name, fmt="png", dpi=300)
            self.png_ok = True
        except Exception as e:
            logger.warning(f"Failed to create plot: {e}")
            self.png_ok = False

    def pad0(self, n, width: int = None):
        if width is None:
            width = math.floor(self.partition_count / 10) + 1
        return f"{n:0{width}d}"

    def _upload_data(self):
        """
        Upload the data to the server
        """
        with open(self.zip_file, "rb") as f:
            report = report_harvest_result(
                path=self.file_path,
                monitored_path_id=self.monitored_path.get("id"),
                # send data in a flat format to accompany file upload protocol.
                # Kinda hacky because it overwrites much of report_harvest_result's functionality
                data={
                    "format": "flat",
                    "status": settings.HARVESTER_STATUS_SUCCESS,
                    "path": self.file_path,
                    "monitored_path_id": self.monitored_path.get("id"),
                    "task": settings.HARVESTER_TASK_IMPORT,
                    "stage": settings.HARVEST_STAGE_UPLOAD_DATA,
                    "total_row_count": self.row_count,
                    "filename": self.zip_file,
                },
                files={"zip_file": f},
            )
            if report is None:
                raise RuntimeError("API Error: no response from server")
            logger.info("Data Upload - success")

        if self.png_ok:
            with open(self.png_file_name, "rb") as f:
                report = report_harvest_result(
                    path=self.file_path,
                    monitored_path_id=self.monitored_path.get("id"),
                    # send data in a flat format to accompany file upload protocol.
                    # Kinda hacky because it overwrites much of report_harvest_result's functionality
                    data={
                        "format": "flat",
                        "status": settings.HARVESTER_STATUS_SUCCESS,
                        "path": self.file_path,
                        "monitored_path_id": self.monitored_path.get("id"),
                        "task": settings.HARVESTER_TASK_IMPORT,
                        "stage": settings.HARVEST_STAGE_UPLOAD_PNG,
                        "filename": os.path.basename(self.png_file_name),
                    },
                    files={"png_file": f},
                )
            try:
                HarvestProcessor.check_response("Upload PNG", report)
            except BaseException as e:
                logger.warning(f"Failed to upload PNG: {e}")

    def _delete_temp_files(self):
        """
        Delete temporary files created during the process
        """
        for attribute in ["data_file_name", "png_file_name", "tmp_dir"]:
            if hasattr(self, attribute):
                filename = getattr(self, attribute)
                if filename is not None and os.path.exists(filename):
                    try:
                        if os.path.isdir(filename):
                            shutil.rmtree(filename)
                        else:
                            os.remove(filename)
                    except PermissionError:
                        logger.warning(
                            f"Failed to delete {filename}. This will have to be manually deleted."
                        )

    def __del__(self):
        self._delete_temp_files()


class InternalHarvestProcessor(HarvestProcessor):
    """
    The internal HarvesterProcessor is designed to run within an instance of the Galv Django backend.
    It does not upload data to the server (because the data are already on the server),
    but does perform parsing, summarising, conversion, and plotting.

    This class does not make API requests. Consequently, none of its methods access settings.
    """

    no_upload = True

    def __init__(self, file_path: str):
        super().__init__(file_path, None)

    def harvest(self):
        raise NotImplementedError(
            "Do not use the InternalHarvestProcessor harvest method. "
            "Instead use the specific methods you require, e.g. summarise_data, and handle their return values."
        )

    @property
    def partition_names(self):
        if self.partition_count is None:
            raise RuntimeError(
                "Data has not been processed. Run self.process_data() first."
            )
        return [
            f"{os.path.splitext(os.path.basename(self.file_path))[0]}.part_{self.pad0(i)}.csv"
            for i in range(self.partition_count)
        ]

# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.
import json
import unittest
from unittest.mock import patch
import os
from pathlib import Path

import harvester.run
import harvester.harvest
from harvester import settings


def get_test_file_path():
    return os.getenv('TEST_DIR', "/usr/test_data")

class ConfigResponse:
    status_code = 200

    def json(self):
        return {
            "url": "http://app/harvesters/1/",
            "id": 1,
            "api_key": "galv_hrv_x",
            "name": "Test Harvester",
            "sleep_time": 0,
            "monitored_paths": [
                {
                    "uuid": "1f6852da-3d2d-46ce-a6c6-70b602fd0e84",
                    "path": get_test_file_path(),
                    "stable_time": 0,
                    "regex": "^(?!.*\\.skip$).*$",
                }
            ],
            "standard_units": [
                {
                    "id": 1,
                    "name": "Unitless"
                },
                {
                    "id": 2,
                    "name": "Time"
                },
                {
                    "id": 3,
                    "name": "Volts"
                },
                {
                    "id": 4,
                    "name": "Amps"
                },
                {
                    "id": 5,
                    "name": "Energy"
                },
                {
                    "id": 6,
                    "name": "Charge"
                },
                {
                    "id": 7,
                    "name": "Temperature"
                },
                {
                    "id": 8,
                    "name": "Power"
                },
                {
                    "id": 9,
                    "name": "Ohm"
                },
                {
                    "id": 10,
                    "name": "Degrees"
                },
                {
                    "id": 11,
                    "name": "Frequency"
                },
                {
                    "id": 12,
                    "name": "Unknown"
                }
            ],
            "standard_columns": [
                {
                    "id": 1,
                    "name": "Unknown",
                    "unit": 12
                },
                {
                    "id": 2,
                    "name": "Sample Number",
                    "unit": "http://app/units/1/"
                },
                {
                    "id": 3,
                    "name": "Time",
                    "unit": "http://app/units/2/"
                },
                {
                    "id": 4,
                    "name": "Volts",
                    "unit": "http://app/units/3/"
                },
                {
                    "id": 5,
                    "name": "Amps",
                    "unit": "http://app/units/4/"
                },
                {
                    "id": 6,
                    "name": "Energy Capacity",
                    "unit": "http://app/units/5/"
                },
                {
                    "id": 7,
                    "name": "Charge Capacity",
                    "unit": "http://app/units/6/"
                },
                {
                    "id": 8,
                    "name": "Temperature",
                    "unit": "http://app/units/7/"
                },
                {
                    "id": 9,
                    "name": "Step Time",
                    "unit": "http://app/units/8/"
                },
                {
                    "id": 10,
                    "name": "Impedence Magnitude",
                    "unit": "http://app/units/9/"
                },
                {
                    "id": 11,
                    "name": "Impedence Phase",
                    "unit": "http://app/units/10/"
                },
                {
                    "id": 12,
                    "name": "Frequency",
                    "unit": "http://app/units/11/"
                }
            ],
            "max_upload_bytes": 2621440
        }


class JSONResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self.ok = 200 <= self.status_code < 400
        self.json_data = json_data if json_data else {}

    def json(self):
        return self.json_data


def fail(e, *kwargs):
    raise Exception(e)


class TestHarvester(unittest.TestCase):
    @patch('requests.get')
    @patch('harvester.api.logger')
    @patch('harvester.run.logger')
    @patch('harvester.api.get_settings_file')
    @patch('harvester.settings.get_settings_file')
    @patch('harvester.settings.get_logfile')
    def test_config_update(
            self,
            mock_settings_log,
            mock_settings_file,
            mock_api_settings_file,
            mock_run_logger,
            mock_api_logger,
            mock_get
    ):
        mock_settings_log.return_value = '/tmp/harvester.log'
        mock_settings_file.return_value = '/tmp/harvester.json'
        mock_api_settings_file.return_value = '/tmp/harvester.json'
        mock_api_logger.error = fail
        mock_run_logger.error = fail
        mock_get.return_value = ConfigResponse()
        harvester.run.update_config()
        if not os.path.isfile(mock_settings_file()):
            raise AssertionError(f"Expected JSON file '{mock_settings_file()}' not found")

        os.remove(mock_settings_file())

    @patch('harvester.run.report_harvest_result')
    @patch('harvester.run.HarvestProcessor', autospec=True)
    @patch('harvester.run.logger')
    @patch('harvester.settings.get_settings')
    def test_harvest_path(self, mock_settings, mock_logger, mock_processor, mock_report):
        mock_settings.return_value = ConfigResponse().json()
        # Create an unparsable file in the test set
        Path(os.path.join(get_test_file_path(), 'unparsable.foo')).touch(exist_ok=True)
        Path(os.path.join(get_test_file_path(), 'skipped_by_regex.skip')).touch(exist_ok=True)
        mock_logger.error = fail
        mock_report.return_value = JSONResponse(200, {'state': 'STABLE'})
        harvester.run.harvest_path(ConfigResponse().json()['monitored_paths'][0])
        files = []
        expected_file_count = 9
        for c in mock_processor.call_args_list:
            f = c.args[0]
            if f not in files:
                files.append(c.args)
        if len(files) != expected_file_count:
            raise AssertionError(f"Found {len(files)} instead of {expected_file_count} files in path {get_test_file_path()}")
        for f in files:
            for task in ['file_size', 'import']:
                ok = False
                for c in mock_report.call_args_list:
                    if c.kwargs['content']['task'] == task:
                        if (not task == settings.HARVESTER_TASK_IMPORT or
                                c.kwargs['content']['stage'] == settings.HARVEST_STAGE_COMPLETE):
                            ok = True
                            break
                if not ok:
                    raise AssertionError(f"{f} did not make call with 'task'={task}")

    @patch('requests.post')
    @patch('requests.get')
    @patch('harvester.harvest.report_harvest_result')
    @patch('harvester.harvest.logger')
    @patch('harvester.settings.get_settings')
    def import_file(self, filename, mock_settings, mock_logger, mock_report, mock_get, mock_post, additional_checks=None):
        mock_settings.return_value = ConfigResponse().json()
        mock_logger.error = fail
        mock_report.return_value = JSONResponse(
            200,
            # An amalgam of the expected report content for different calls
            {
                'upload_info': {'last_record_number': 0, 'columns': []},
                'mapping': 'http://localhost'
            }
        )
        mock_get.return_value = JSONResponse(200, {'rendered_map': {}})
        mock_post.return_value = JSONResponse(204, {})
        harvester.harvest.HarvestProcessor(
                os.path.join(get_test_file_path(), filename), ConfigResponse().json()["monitored_paths"][0]
        ).harvest()
        self.validate_report_calls(mock_report.call_args_list)
        if additional_checks:
            additional_checks(mock_report.call_args_list)

    def validate_report_calls(self, calls):
        stages = ['file metadata', 'data summary', 'upload parquet partitions', 'upload complete']
        upload_fired = False
        for c in calls:
            if not 'content' in c.kwargs:
                if 'files' in c.kwargs and not upload_fired:
                    upload_fired = True
                    data = c.kwargs.get('data')
                    if 'partition_number' not in data:
                        raise AssertionError(f"Expected upload parquet partitions report to contain row count")
                    if 'partition_count' not in data:
                        raise AssertionError(f"Expected upload parquet partitions report to contain partition count")
                    if 'total_row_count' not in data:
                        raise AssertionError(f"Expected upload parquet partitions report to contain total row count")
                    if 'filename' not in data:
                        raise AssertionError(f"Expected upload parquet partitions report to contain filename")
                    return
                else:
                    if upload_fired:
                        raise AssertionError(f"Received multiple upload calls")
                    raise AssertionError(f"Report made with no content")

            if c.kwargs['content']['task'] == settings.HARVESTER_TASK_IMPORT:
                stage = stages.pop(0)
                s = c.kwargs['content']['stage']
                if s != stage:
                    raise AssertionError(f"Expected import report to have stage {stage}, received {s}")

                data = c.kwargs['content'].get('data')
                try:
                    if s == settings.HARVEST_STAGE_FILE_METADATA:
                        for k in ['core_metadata', 'extra_metadata', 'test_date', 'parser']:
                            if k not in data:
                                raise AssertionError(f"Expected file_metadata report to contain {k}")
                    elif s == settings.HARVEST_STAGE_DATA_SUMMARY:
                        if not isinstance(data, str) or not isinstance(json.loads(data), dict):
                            raise AssertionError(f"Expected data summary to be JSON representation of a dictionary")
                    elif s == settings.HARVEST_STAGE_UPLOAD_COMPLETE:
                        if 'successes' not in data:
                            raise AssertionError(f"Expected upload completion report to contain success count")
                        if 'errors' not in data:
                            raise AssertionError(f"Expected upload completion report to contain errors list")
                except AssertionError as e:
                    print(data)
                    raise e

    def test_import_mpr(self):
        self.import_file('adam_3_C05.mpr')

    def test_import_idf(self):
        self.import_file('Ivium_Cell+1.idf')

    # def test_import_txt(self):
    #     self.import_file('TPG1+-+Cell+15+-+002.txt')

    def test_import_csv(self):
        self.import_file('headered.csv')
        self.import_file('headerless.csv')

        def validate_preamble(calls):
            for c in calls:
                if (c.kwargs['content']['task'] == settings.HARVESTER_TASK_IMPORT and
                        c.kwargs['content']['stage'] == settings.HARVEST_STAGE_FILE_METADATA):
                    if c.kwargs['content'].get('data', {}).get('core_metadata', {}).get('preamble') is None:
                        raise AssertionError(f"Expected import report to contain 'preamble'")
                    else:
                        return
            raise AssertionError(f"Could not find import report with 'preamble'")

        self.import_file('preamble.csv', additional_checks=validate_preamble)


if __name__ == '__main__':
    unittest.main()

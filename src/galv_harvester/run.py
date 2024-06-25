# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import os.path
import re
import time
import traceback

from .parse.exceptions import UnsupportedFileTypeError
from .settings import (
    get_logger,
    get_setting,
    HARVESTER_TASK_FILE_SIZE,
    HARVESTER_TASK_IMPORT,
    HARVEST_STAGE_COMPLETE,
    HARVESTER_TASK_IMPORT,
    HARVEST_STAGE_FAILED
)
from .api import report_harvest_result, update_config
from .harvest import HarvestProcessor

logger = get_logger(__file__)


def split_path(core_path, path) -> (os.PathLike, os.PathLike):
    """
    Split a path into the base path property and the rest
    """
    path = os.path.abspath(path)
    core_path_abs = os.path.abspath(core_path)
    return core_path, os.path.relpath(path, core_path_abs)


def harvest():
    logger.info("Beginning harvest cycle")
    paths = get_setting('monitored_paths')
    if not paths:
        logger.info("No paths are being monitored.")
        return

    logger.debug(paths)

    for path in paths:
        if path.get('active'):
            harvest_path(path)
        else:
            logger.info(f"Skipping inactive path {path.get('path')} {path.get('regex')}")


def harvest_file(file_path, monitored_path: dict, compiled_regex: re.Pattern = None):
    if os.path.basename(file_path).startswith('.'):
        logger.debug(f"Skipping hidden file {file_path}")
        return

    if compiled_regex is None:
        regex_str = monitored_path.get('regex')
        if regex_str is not None:
            regex = re.compile(regex_str)
        else:
            regex = re.compile(r".*")
    else:
        regex = compiled_regex

    path = monitored_path.get('path')

    _, rel_path = split_path(path, file_path)
    if regex is not None and not regex.search(rel_path):
        logger.debug(f"Skipping {rel_path} as it does not match regex {regex}")
        return
    try:
        file = HarvestProcessor(file_path, monitored_path)
    except UnsupportedFileTypeError:
        logger.debug(f"Skipping unsupported file {rel_path}")
        return
    try:
        logger.info(f"Reporting stats for {rel_path}")
        result = report_harvest_result(
            path=file_path,
            monitored_path_id=monitored_path.get('id'),
            content={
                'task': HARVESTER_TASK_FILE_SIZE,
                'size': os.stat(file_path).st_size
            }
        )
        if result is not None:
            result = result.json()
            status = result['state']
            logger.info(f"Server assigned status '{status}'")
            if status in ['STABLE', 'RETRY IMPORT', 'MAP ASSIGNED', 'AWAITING STORAGE']:
                logger.info(f"Parsing file {rel_path}")
                try:
                    file.harvest()
                    report_harvest_result(
                        path=file_path,
                        monitored_path_id=monitored_path.get('id'),
                        content={
                            'task': HARVESTER_TASK_IMPORT,
                            'stage': HARVEST_STAGE_COMPLETE
                        }
                    )
                    logger.info(f"Successfully parsed file {rel_path}")
                except BaseException as e:
                    logger.warning(f"FAILED parsing file {rel_path}: {e.__class__.__name__}: {e}")
                    if e.__traceback__ is not None:
                        logger.warning(traceback.format_exc())
                    report_harvest_result(
                        path=file_path,
                        monitored_path_id=monitored_path.get('id'),
                        content={
                            'task': HARVESTER_TASK_IMPORT,
                            'stage': HARVEST_STAGE_FAILED,
                            'error': f"Error in Harvester. {e.__class__.__name__}: {e}. [See harvester logs for more details]"
                        }
                    )
    except BaseException as e:
        logger.error(f"{e.__class__.__name__}: {e}")
        report_harvest_result(
            path=file_path,
            monitored_path_id=monitored_path.get('id'),
            error=e
        )



def harvest_path(monitored_path: dict):
    path = monitored_path.get('path')
    regex_str = monitored_path.get('regex')
    if regex_str is not None:
        logger.info(f"Harvesting from {path} with regex {regex_str}")
    else:
        logger.info(f"Harvesting from {path}")
    try:
        regex = re.compile(regex_str) if regex_str is not None else None
        for (dir_path, dir_names, filenames) in os.walk(path):
            for filename in filenames:
                harvest_file(os.path.join(dir_path, filename), monitored_path, compiled_regex=regex)
        logger.info(f"Completed directory walking of {path}")
    except BaseException as e:
        logger.error(f"{e.__class__.__name__}: {e}")
        report_harvest_result(
            monitored_path_id=monitored_path.get('id'),
            error=e,
            path=path
        )


def run():
    update_config()
    harvest()


def run_cycle():
    sleep_time = 10
    while True:
        try:
            run()
        except BaseException as e:
            logger.error(f"{e.__class__.__name__}: {e}")
        try:
            sleep_time = get_setting('sleep_time')
        except BaseException as e:
            logger.error(f"{e.__class__.__name__}: {e}")
        time.sleep(sleep_time)


if __name__ == "__main__":
    run_cycle()

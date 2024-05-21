# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import json

from . import settings
from .utils import NpEncoder
import requests
from .settings import get_setting, get_settings, get_settings_file, get_logger, update_envvars
import time

logger = get_logger(__file__)


def report_harvest_result(
        path,
        monitored_path_id: str,
        content=None,
        error: BaseException = None,
        **kwargs  # passed to requests.post
):
    start = time.time()
    try:
        if error is not None:
            data = {'status': settings.HARVESTER_STATUS_ERROR, 'error': f"{error.__class__.__name__}: {error}"}
        else:
            data = {'status': settings.HARVESTER_STATUS_SUCCESS, 'content': content}
        data['path'] = path
        data['monitored_path_id'] = monitored_path_id
        logger.debug(f"{get_setting('url')}report/; {json.dumps(data, cls=NpEncoder)}")
        out = requests.post(
            f"{get_setting('url')}report/",
            headers={
                'Authorization': f"Harvester {get_setting('api_key')}"
            },
            # encode then decode to ensure np values are converted to standard types and null bytes are removed
            json=json.loads(json.dumps(data, cls=NpEncoder).replace('\\u0000', '')),
            **kwargs
        )
        try:
            out.json()
        except json.JSONDecodeError:
            error_text = out.text[:100].replace("\n", "\\n")
            if len(out.text) > 100:
                error_text += "..."
            logger.error(f"Server returned invalid JSON (HTTP {out.status_code}): {error_text}")
            return None
        if not out.ok:
            logger.error(f"Server returned error (HTTP {out.status_code}): {out.json()}")
            return None
    except BaseException as e:
        logger.error(f"{e.__class__.__name__}: {e}")
        out = None
    logger.info(f"API call finished in {round(time.time() - start, 2)}s")
    return out


def update_config():
    logger.info("Updating configuration from API")
    try:
        url = get_setting('url')
        key = get_setting('api_key')
        result = requests.get(f"{url}config/", headers={'Authorization': f"Harvester {key}"})
        if result.status_code == 200:
            dirty = False
            new = result.json()
            old = get_settings()
            if old is None:
                old = {}
            all_keys = [*new.keys(), *old.keys()]
            for key in all_keys:
                if key in old.keys() and key in new.keys():
                    if json.dumps(old[key], cls=NpEncoder) == json.dumps(new[key], cls=NpEncoder):
                        continue
                    logger.info(f"Updating value for setting '{key}'")
                    logger.info(f"Old value: {json.dumps(old[key], cls=NpEncoder)}")
                    logger.info(f"New value: {json.dumps(new[key], cls=NpEncoder)}")
                    dirty = True
                if key in old.keys():
                    logger.info(f"Updating value for setting '{key}'")
                    logger.info(f"Old value: {json.dumps(old[key], cls=NpEncoder)}")
                    logger.info(f"New value: [not set]")
                    dirty = True
                if key in new.keys():
                    logger.info(f"Updating value for setting '{key}'")
                    logger.info(f"Old value: [not set]")
                    logger.info(f"New value: {json.dumps(new[key], cls=NpEncoder)}")
                    dirty = True

            if dirty:
                with open(get_settings_file(), 'w+') as f:
                    json.dump(result.json(), f)
                update_envvars()
        else:
            logger.error(f"Unable to fetch {url}config/ -- received HTTP {result.status_code}")
    except BaseException as e:
        logger.error(f"{e.__class__.__name__}: {e}")

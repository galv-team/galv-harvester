import importlib
import pkgutil

from src.galv_harvester.parse.input_file import InputFile
from .settings import get_logger


logger = get_logger(__file__)
_cached_parsers = None
_cached_plugins = {}


def _get_parsers(plugin, check=True):
    parsers = getattr(plugin, "parsers", None)
    ok_parsers = []
    if not isinstance(parsers, list):
        if check:
            logger.error(f"Plugin {plugin.__name__} does not have a 'parsers' list")
    else:
        for p in parsers:
            if not isinstance(p, InputFile):
                if check:
                    logger.error(
                        f"Plugin {plugin.__name__} has a non-InputFile parser: {p}"
                    )
            else:
                ok_parsers.append(p)
    return ok_parsers


def get_parsers(from_cache=True):
    global _cached_parsers
    global _cached_plugins

    if not from_cache or not _cached_parsers:
        logger.info("Searching for new plugins")
        parsers = []
        discovered_plugins = {
            name: importlib.import_module(name)
            for finder, name, ispkg in pkgutil.iter_modules()
            if name.startswith("galv_harvester_")
        }
        for name, plugin in discovered_plugins.items():
            if name not in _cached_plugins:
                _cached_plugins[name] = plugin
                plugin_parsers = _get_parsers(plugin)
                logger.info(
                    f"Discovered plugin: {name} with {len(plugin_parsers)} parsers"
                )
                parsers.extend(plugin_parsers)

        _cached_parsers = parsers

    return _cached_parsers

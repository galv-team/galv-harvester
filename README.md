# Galv Harvester (Python program)
> A metadata secretary for battery science

[![PyPI - Version](https://img.shields.io/pypi/v/galv-harvester)](https://pypi.org/project/galv-harvester/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/galv-harvester)](https://pypi.org/project/galv-harvester/)

[![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)
[![Test, Build, and Publish](https://github.com/galv-team/galv-harvester/actions/workflows/publish.yml/badge.svg)](https://github.com/galv-team/galv-harvester/actions/workflows/publish.yml)

## Galv Project
- [Backend](https://github.com/galv-team/galv-backend)
- [Frontend](https://github.com/galv-team/galv-frontend)
- [**Harvester**](https://github.com/galv-team/galv-harvester)

## Installation

The Galv Harvester can be installed from the [Python Package Index](https://pypi.org/project/galv-harvester/).

```bash
pip install galv-harvester
```

This will install the harvester and its dependencies, and make the `galv-harvester` command available.

## Usage

The first time you use the harvester, it will have to register itself with the Galv server.
To set up the harvester, using the following command:

```bash
galv-harvester setup
```

The harvester will prompt you for the necessary settings to connect to the Galv server (see [Initial Setup](#initial-setup)).

Alternatively, you can specify the settings as environment variables 
(see [Using Environment Variables](#using-environment-variables))
or as [command line arguments](#using-command-line-arguments) to the `galv-harvester` program.

## Initial Setup

There are three ways to set up the harvester: using the [setup wizard](#using-the-setup-wizard), 
using [command line arguments](#using-command-line-arguments),
or by specifying [environment variables](#using-environment-variables).
You can use a combination of both methods, specifying some settings in the environment and others in the wizard.

If you launch the program using the commands above, you will be prompted to enter the necessary settings by the wizard.

### Using the setup wizard

First, you'll be asked for the [Galv server](https://github.com/galv-team/galv-backend) URL.
This should be the URL of the Galv server you have set up.
Providing a frontend URL will not work, as the harvester needs to communicate with the backend.

Next, you'll be asked for your API token. 
This can be generated in either the Galv frontend or backend.
The token should be for a User who administers the Lab the Harvester will belong to.

Next, you'll be asked to specify a name for the new Harvester. 

Finally, you'll be asked if you want to monitor a directory.
If you answer 'yes', you'll be asked for the path to the directory you want to monitor,
and the Team that the monitored path will belong to.

The Harvester will register itself with the Galv server and begin to monitor for data files.

The `--foreground` flag is optional, and will keep the harvester running in the foreground.

### Using environment variables

You can specify harvester properties as environment variables.
If you are using docker-compose, you can specify these in the `docker-compose.yml` file (see below),
or you can specify them in your shell environment before running the harvester if you are running it as a standalone Python program.
Any environment variables can be omitted, and the harvester will prompt you for them when you start it if they are necessary.
For details on the variables you can set, and whether they are necessary, see the [variable details](#variable-details) section.

```yaml
# .env
GALV_HARVESTER_SERVER_URL=<your_server_url>
GALV_HARVESTER_NAME=<your_harvester_name>
GALV_HARVESTER_API_TOKEN=<your_api_token>
GALV_HARVESTER_LAB_ID=<your_lab_id>
GALV_HARVESTER_TEAM_ID=<your_team_id>
GALV_HARVESTER_MONITOR_PATH=<your_monitor_path>
GALV_HARVESTER_MONITOR_PATH_REGEX=<your_monitor_path_regex>
GALV_HARVESTER_SKIP_WIZARD=<true_or_omit>
GALV_HARVESTER_FOREGROUND=<true_or_omit>
```

If you don't want to have to specify the path to the data directory every time you start the harvester,
you can also edit the `docker-compose.yml` file to include the path as a volume.

### Using command line arguments

You can also specify harvester properties as command line arguments:

```text
Usage: galv-harvester setup [OPTIONS]

Options:
  --version                  Show the version and exit.
  --url TEXT                 API URL to register harvester with.
  --name TEXT                Name for the harvester.
  --api_token TEXT           Your API token. You must have admin access to at
                             least one Lab.
  --lab_id INTEGER           Id of the Lab to assign the Harvester to. Only
                             required if you administrate multiple Labs.
  --team_id INTEGER          Id of the Team to create a Monitored Path for.
                             Only required if you administrate multiple Teams
                             and wish to create a monitored path.
  --monitor_path TEXT        Path to harvest files from.
  --monitor_path_regex TEXT  Regex to match files to harvest. Other options
                             can be specified using the frontend.
  --foreground           On completion, run the harvester in the
                             foreground (will not close the thread, useful for
                             Dockerized application).
  --restart                  Ignore other options and run harvester if config
                             file already exists.
  --help                     Show this message and exit.
```

For details on the variables you can set, and when they are necessary, see the [variable details](#variable-details) section.

## Variable details

If not restarting from a previous configuration, the following variables are required, 
and will be prompted for by the wizard if not set (unless `GALV_HARVESTER_SKIP_WIZARD` is set to `true`).
If `GALV_HARVESTER_SKIP_WIZARD` is set to `true`, you must provide these variables in the environment or the docker-compose file:

- `GALV_HARVESTER_SERVER_URL`: The URL of the Galv server.
- `GALV_HARVESTER_NAME`: The name of the harvester.
- `GALV_HARVEST_API_TOKEN`: The API token for a User who administers the Lab the Harvester will be associated with.
- `GALV_HARVESTER_LAB_ID`: The ID of the lab the harvester belongs to. Only required if the User administers multiple Labs.

If you want to set up a monitored path, the following variables are required:
-  `GALV_HARVESTER_TEAM_ID`: The ID of the team the monitored path will belong to. Only required if the User has multiple Teams.
- `GALV_HARVESTER_MONITOR_PATH`: The path to the directory you want to monitor.

You may also optionally specify the following variables:
- `GALV_HARVESTER_MONITOR_PATH_REGEX`: A regex pattern to match files in the monitored path. Only files that match this pattern will be uploaded to the Galv server.
- `GALV_HARVESTER_SKIP_WIZARD`: If set to `true`, the harvester will not prompt you for any missing variables and setup will fail if necessary variables are not set.
- `GALV_HARVESTER_FOREGROUND`: If set to `true`, the harvester will run in the foreground.

## Further setup

Further setup can be done in the web frontend.

When you log into the frontend as a User who belongs to the same Lab as the Harvester,
you'll see the Harvester listed in the 'Harvesters' tab.

You can add new monitored paths to the Harvester, or change the Harvester's settings (if you have appropriate permissions).
Monitored Paths can only be created and edited by Team administrators, as a security measure.

## Starting the harvester

When the harvester is set up, you can start it by running the following command:

```bash
galv-harvester start
```

This will start the harvester using the previously-configured settings.

## Harvesting specific files/directories

If you want to harvest specific files or directories, or run the entire harvest cycle manually, 
you can do so with the command `galv-harvester harvest`.

With no arguments, this will harvest each monitored path in turn.

You can also specify paths to harvest:

```bash
galv-harvester harvest /path/to/directory /path/to/another/directory/file.csv /path/to/somewhere/else
```

This will harvest the specified paths, and only those paths. 
**Note**: The paths must be included in the monitored paths for the harvester.
This includes the regex pattern, if specified. 

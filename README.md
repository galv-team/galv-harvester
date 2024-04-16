# Galv Harvester (Python program)
> A metadata secretary for battery science

## Galv Project
- [Specification](https://github.com/Battery-Intelligence-Lab/galv-spec)
- [Backend](https://github.com/Battery-Intelligence-Lab/galv-backend)
- [Frontend](https://github.com/Battery-Intelligence-Lab/galv-frontend)
- [**Harvester**](https://github.com/Battery-Intelligence-Lab/galv-harvester)

## Usage

This section describes how to set up the system for the first time.
It is assumed you have already set up the [Galv server](https://github.com/Battery-Intelligence-Lab/galv-backend).
The application has been dockerised, so can in theory be used on
any major operating system with minimal modification.

The first step is to clone the repository and navigate to the project directory:

```bash
git clone https://github.com/Battery-Intelligence-Lab/galv-harvester.git
cd galv-harvester
```

### Running with `docker-compose`

The harvester will need access to the directories you want to monitor for data files.
This is provided by mounting the directories as volumes in the docker-compose command.

```shell
docker-compose up -v /data/directory/path:/data_dir harvester python start.py --run_foreground
```

### Running as a standalone Python program

You can run the harvester as a standalone Python program.
First, you'll need to install the required dependencies:

```bash
pip install -r requirements.txt
```

Next, you can launch the harvester with the following command:

```bash
python start.py --run_foreground
```

## Initial Setup

There are two ways to set up the harvester: using the [setup wizard](#using-the-setup-wizard), 
or by specifying [environment variables](#using-environment-variables).
You can use a combination of both methods, specifying some settings in the environment and others in the wizard.

If you launch the program using the commands above, you will be prompted to enter the necessary settings by the wizard.

### Using the setup wizard

First, you'll be asked for the [Galv server](https://github.com/Battery-Intelligence-Lab/galv-backend) URL.
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

The `--run_foreground` flag is optional, and will keep the harvester running in the foreground.

### Using environment variables

You can specify harvester properties as environment variables.
If you are using docker-compose, you can specify these in the `docker-compose.yml` file (see below),
or you can specify them in your shell environment before running the harvester if you are running it as a standalone Python program.
Any environment variables can be omitted, and the harvester will prompt you for them when you start it if they are necessary.
For details on the variables you can set, and whether they are necessary, see the [variable details](#variable-details) section.

```yaml
services:
  harvester:
    #...
    environment:
      - GALV_HARVESTER_RESTART=1
      - GALV_HARVESTER_SERVER_URL=<your_server_url>
      - GALV_HARVESTER_NAME=<your_harvester_name>
      - GALV_HARVESTER_API_TOKEN=<your_api_token>
      - GALV_HARVESTER_LAB_ID=<your_lab_id>
      - GALV_HARVESTER_TEAM_ID=<your_team_id>
      - GALV_HARVESTER_MONITOR_PATH=<your_monitor_path>
      - GALV_HARVESTER_MONITOR_PATH_REGEX=<your_monitor_path_regex>
      - GALV_HARVESTER_SKIP_WIZARD=<true_or_omit>
      - GALV_HARVESTER_RUN_FOREGROUND=<true_or_omit>
```

If you don't want to have to specify the path to the data directory every time you start the harvester,
you can also edit the `docker-compose.yml` file to include the path as a volume.

## Variable details

- `GALV_HARVESTER_RESTART`: If set to 1, the harvester will attempt to resume from a previous configuration file.

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
- `GALV_HARVESTER_RUN_FOREGROUND`: If set to `true`, the harvester will run in the foreground.

## Further setup

Further setup can be done in the web frontend.

When you log into the frontend as a User who belongs to the same Lab as the Harvester,
you'll see the Harvester listed in the 'Harvesters' tab.

You can add new monitored paths to the Harvester, or change the Harvester's settings (if you have appropriate permissions).
Monitored Paths can only be created and edited by Team administrators, as a security measure.

## Restarting the harvester

If you need to restart the harvester, you can do so by running the following command:

```bash
docker-compose up -v /data/directory/path:/data_dir harvester python start.py --restart
```

This will restart the harvester using the previously-configured settings.

# dagster_and_r

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

1. Clone the repository: `git clone https://github.com/philiporlando/dagster-and-r.git`
2. Navigate to the repository directory: `cd dagster-and-r`
3. Install the package and its dependencies with [poetry](https://python-poetry.org/): `poetry install`

Then, start the Dagster UI web server:

```bash
poetry run dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dagster_and_r/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development


### Adding new Python dependencies

```bash
poetry add <pkg-name>
```

### Unit testing

Tests are in the `dagster_and_r_tests` directory and you can run tests using `pytest`:

```bash
poetry run pytest dagster_and_r_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `poetry run dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

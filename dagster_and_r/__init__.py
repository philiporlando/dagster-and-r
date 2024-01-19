from dagster import (
    Definitions,
    PipesSubprocessClient,
    )
from . jobs import docker_container_op_r
from . asset_checks import (
    # no_missing_sepal_length_check_r,
    no_missing_sepal_length_check_py,
    )

# python_assets = load_assets_from_modules([assets])
from . assets import (
    hello_world_r,
    iris_r,
    iris_py,
    )

defs = Definitions(
    assets=[
        hello_world_r,
        iris_r,
        iris_py,
        ],
    asset_checks=[
        # no_missing_sepal_length_check_r,
        no_missing_sepal_length_check_py,
        ],
    jobs=[docker_container_op_r],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

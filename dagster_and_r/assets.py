import base64
import requests
import json
import os
import shutil
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
    AssetCheckSpec,
    MaterializeResult,
    PipesSubprocessClient,
    file_relative_path,
    Field, 
    String,
    )

@asset
def hello_world_r(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/hello_world.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_materialize_result()


@asset(
    config_schema={"output_dir": Field(String, default_value="./data")},
    check_specs=[
        AssetCheckSpec(name="no_missing_sepal_length_check_r", asset="iris_r"),
        AssetCheckSpec(name="no_missing_sepal_width_check_r", asset="iris_r"),
        AssetCheckSpec(name="no_missing_petal_length_check_r", asset="iris_r"),
        AssetCheckSpec(name="no_missing_petal_width_check_r", asset="iris_r"),
        AssetCheckSpec(name="species_name_check_r", asset="iris_r"),
        ],
    )
def iris_r(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    output_dir = context.op_config["output_dir"]
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/iris.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "This is an environment variable passed from Dagster to R!",
            "OUTPUT_DIR": output_dir,
        },
    ).get_materialize_result()


@asset(deps=[iris_r])
def iris_py(context):
    # TODO replace hardcoded output_dir with resource key
    iris = pd.read_csv(f"data/iris.csv")
    context.log.info(type(iris))
    context.log.info(iris.head())
    return iris

import shutil
import pandas as pd

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetExecutionContext,
    PipesSubprocessClient,
    file_relative_path,
)

# TODO figure out how to use this syntax with pipes subprocesses (instead of CheckSpecs)?
@asset_check(asset="iris_r")
def no_missing_sepal_length_check_r(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> AssetCheckResult:
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/iris.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_asset_check_result()
    
    
@asset_check(asset="iris_py")
def no_missing_sepal_length_check_py() -> AssetCheckResult:
    iris = pd.read_csv(f"data/iris.csv")
    num_missing_sepal_length = iris["Sepal.Length"].isna().sum()
    return AssetCheckResult(
        passed=bool(num_missing_sepal_length == 0),
        metadata={
            "num_missing_sepal_length": int(num_missing_sepal_length),
        },
    )

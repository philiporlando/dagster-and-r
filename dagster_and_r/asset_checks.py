import shutil
import pandas as pd
import dagster as dg

# TODO figure out how to use this syntax with pipes subprocesses (instead of CheckSpecs)?
@dg.asset_check(asset="iris_r")
def no_missing_sepal_length_check_r(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.AssetCheckResult:
    cmd = [shutil.which("Rscript"), dg.file_relative_path(__file__, "./R/iris.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_asset_check_result()
    
    
@dg.asset_check(asset="iris_py")
def no_missing_sepal_length_check_py() -> dg.AssetCheckResult:
    iris = pd.read_csv(f"data/iris.csv")
    num_missing_sepal_length = iris["Sepal.Length"].isna().sum()
    return dg.AssetCheckResult(
        passed=bool(num_missing_sepal_length == 0),
        metadata={
            "num_missing_sepal_length": int(num_missing_sepal_length),
        },
    )

import shutil

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetExecutionContext,
    PipesSubprocessClient,
    file_relative_path,
)

@asset_check(asset="hello_world_r")
def no_missing_sepal_length_check(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> AssetCheckResult:
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/hello_world.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_asset_check_result()
library(reticulate)
library(readr)
library(glue)
library(magrittr)

# Ensure that RETICULATE_PYTHON is set to the poetry path
reticulate::py_config()
stopifnot(reticulate::py_module_available("dagster_pipes"))

# Import Python modules
# R doesn't support selective imports like Python, so you have to do this
# to avoid typing the full namespace path repeatedly...
os <- reticulate::import("os")
dagster_pipes <- reticulate::import("dagster_pipes")
open_dagster_pipes <- dagster_pipes$open_dagster_pipes
PipesContext <- dagster_pipes$PipesContext

# https://rstudio.github.io/reticulate/articles/calling_python.html#with-contexts
with(open_dagster_pipes() %as% pipes, {
    context <- PipesContext$get()
    context$log$info("This is a log from R!")
    data(iris)
    context$log$info(head(iris))
    context$log$info(os$environ["MY_ENV_VAR_IN_SUBPROCESS"])
    output_dir <- Sys.getenv("OUTPUT_DIR")
    context$log$info(glue::glue("output_dir: {output_dir}"))
    # TODO troubleshoot asset checks
    # context$report_asset_materialization() # method not available?
    # context$report_asset_check(
    #     asset_key="my_r_asset_with_context",
    #     passed=all(!is.na(iris$Sepal.Length)),
    #     check_name="no_missing_sepal_length"
    # )
    # Write iris data to csv so it can be read by a downstream Python asset
    readr::write_csv(iris, glue::glue("{output_dir}/iris.csv"))
})

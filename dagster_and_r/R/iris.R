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
    context$report_asset_materialization() 

    # Ensure that Sepal.Length field does not contain any NAs
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(!is.na(iris$Sepal.Length))),
        check_name="no_missing_sepal_length_check_r",
    )

    # Ensure that Species field contains expected set of values
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(iris$Species %in% c("setosa", "versicolor", "virginica"))),
        check_name="species_name_check_r",
    )

    # Write iris data to csv so it can be read by a downstream Python asset
    readr::write_csv(iris, glue::glue("{output_dir}/iris.csv"))
})

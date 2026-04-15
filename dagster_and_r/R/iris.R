# make sure these packages are installed
library(reticulate)
library(readr)
library(glue)
library(magrittr)

# Ensure that RETICULATE_PYTHON is set to the poetry path
reticulate::py_config()
stopifnot(reticulate::py_module_available("dagster_pipes"))

# Function to convert R types to Python types
convert_r_to_python_types <- function(df) {
    # Get R types
    r_types <- sapply(df, class)
    
    # Define type mapping
    type_mapping <- list(
        "numeric" = "float",
        "integer" = "int",
        "character" = "str",
        "factor" = "str",
        "logical" = "bool",
        "Date" = "datetime.date",
        "POSIXct" = "datetime.datetime",
        "POSIXlt" = "datetime.datetime"
    )
    
    # Convert types
    python_types <- sapply(r_types, function(x) {
        if (x %in% names(type_mapping)) {
            type_mapping[[x]]
        } else {
            "object"  # default type
        }
    })
    
    return(reticulate::r_to_py(as.list(python_types)))
}


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
    iris_head <- head(iris)
    context$log$info(glue::glue("output_dir: {output_dir}"))
    #python function to report back the materialization and metadata
    context$report_custom_message(
        payload = reticulate::r_to_py(list(
        "dagster/row_count" = nrow(iris),
         # if using report_asset_materialization 
         #list( type = "md", "raw_value" = paste(knitr::kable(iris_head, format = "pipe"), collapse = "\n") ),
        "preview" = paste(knitr::kable(iris_head, format = "pipe"), collapse = "\n"),
        "iris_head_df" = reticulate::r_to_py(jsonlite::toJSON(x = iris_head, dataframe = "columns")),
        "column_types" = convert_r_to_python_types(iris_head)
    ))
    )
    context$log$info(glue::glue("got here!"))
    # Ensure that Sepal.Length field does not contain any NAs
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(!is.na(iris$Sepal.Length))),
        check_name="no_missing_sepal_length_check_r",
    )

    # Ensure that Sepal.Width field does not contain any NAs
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(!is.na(iris$Sepal.Width))),
        check_name="no_missing_sepal_width_check_r",
    )

    # Ensure that Sepal.Length field does not contain any NAs
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(!is.na(iris$Petal.Length))),
        check_name="no_missing_petal_length_check_r",
    )

    # Ensure that Sepal.Width field does not contain any NAs
    context$report_asset_check(
        asset_key="iris_r",
        passed=reticulate::r_to_py(all(!is.na(iris$Petal.Width))),
        check_name="no_missing_petal_width_check_r",
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

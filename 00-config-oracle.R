# Enter the location of your oracle JDBC driver (ojdbc7.jar or ojdbc8.jar)
drv <- JDBC("oracle.jdbc.OracleDriver",
            "[location of driver here]")

# Enter your connection string and username here. You will be prompted for
# your password upon running the scripts.
conn <- dbConnect(drv, "[connection string]",
                  "[username]", password = getPass::getPass())

# Enter the schema name and version of your database
schema <- "schema_name"
version <- "3.1 or 4.1"

# dbplyr v1.2 does not provide dplyr->sql verb translations for median, quantile,
# or conditional counts (SUM(CASE WHEN)). Below modifies the base_odbc_agg object
# from dbplyr to add these custom translations.
sql_translate_env.Oracle <- function(con) {
  sql_variant(
    sql_translator(.parent = base_odbc_scalar,
                   as.character = sql_cast("VARCHAR(255)"),
                   as.numeric = sql_cast("NUMBER"),
                   as.double = sql_cast("NUMBER")
    ),
    sql_translator(.parent = base_odbc_agg,
                   count = function(...) {
                     vars <- sql_vector(list(...), parens = FALSE, collapse = ", ")
                     build_sql("COUNT(", vars, ")")
                   },
                   median = function(...) {
                     vars <- sql_vector(list(...), parens = FALSE, collapse = ", ")
                     build_sql("MEDIAN(", vars, ")")
                   },
                   quantile = function(varlist, perc) {
                     vars <- sql_vector(list(varlist), parens = FALSE, collapse = ", ")
                     build_sql("PERCENTILE_DISC(", (1 - perc), ") WITHIN GROUP (ORDER BY ", vars, " DESC)")
                   },
                   cond_count = function(varlist, cond) {
                     vars <- sql_vector(list(varlist), parens = FALSE, collapse = ", ")
                     build_sql("SUM(CASE WHEN (", vars, ") = ", cond, " THEN 1 ELSE 0 END)")
                   }
    ),
    base_odbc_win
  )
}

# Stores Oracle translations to the appropriate JDBCConnection objects
sql_translate_env.JDBCConnection <- sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle
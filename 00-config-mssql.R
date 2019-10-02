# Enter the location of your mssql JDBC driver (jtds-1.3.1.jar)
drv <- JDBC("net.sourceforge.jtds.jdbc.Driver", 
            "enter location of jdbc driver here")

# Enter your server address, domain, database name, and CDM version below
# if you do not connect via Windows Authentication, you may not need domain.
# Be sure to edit the connection string accordingly.
mssql_addr <- "server address"
mssql_port <- "1433"
domain <- "domain"
db <- "db_name"
version <- "3.1 or 4.1"

connection_string <- paste0("jdbc:jtds:sqlserver://", mssql_addr, ":", mssql_port, 
                            ";domain=", domain, ";databaseName=", db)

# Edit username below. You will be prompted for your password upon running scripts.
conn <- dbConnect(drv, 
                  connection_string, 
                  user = 'user_name', 
                  password = getPass::getPass())

# Edit CDM configuration objects below
version <- "5.1"
USE_LOOKUP_TBL <- "N" # Y if looking up values against db, N if looking up against the parseable csv
ref_table <- "ref table name" # fill in if USE_LOOKUP_TBL = Y, otherwise make NULL


# dbplyr v1.2 does not provide dplyr->sql verb translations for median, quantile,
# or conditional counts (SUM(CASE WHEN)). Below modifies the base_odbc_agg object
# from dbplyr to add these custom translations.
`sql_translate_env.Microsoft SQL Server` <- function(con) {
  sql_variant(
    sql_translator(.parent = base_odbc_scalar,
                   
                   `!`           = dbplyr:::mssql_not_sql_prefix(),
                   
                   `!=`           = dbplyr:::mssql_logical_infix("!="),
                   `==`           = dbplyr:::mssql_logical_infix("="),
                   `<`            = dbplyr:::mssql_logical_infix("<"),
                   `<=`           = dbplyr:::mssql_logical_infix("<="),
                   `>`            = dbplyr:::mssql_logical_infix(">"),
                   `>=`           = dbplyr:::mssql_logical_infix(">="),
                   
                   `&`            = dbplyr:::mssql_generic_infix("&", "AND"),
                   `&&`           = dbplyr:::mssql_generic_infix("&", "AND"),
                   `|`            = dbplyr:::mssql_generic_infix("|", "OR"),
                   `||`           = dbplyr:::mssql_generic_infix("|", "OR"),
                   
                   `if`           = dbplyr:::mssql_sql_if,
                   if_else        = function(condition, true, false) dbplyr:::mssql_sql_if(condition, true, false),
                   ifelse         = function(test, yes, no) dbplyr:::mssql_sql_if(test, yes, no),
                   
                   as.numeric    = sql_cast("NUMERIC"),
                   as.double     = sql_cast("NUMERIC"),
                   as.character  = sql_cast("VARCHAR(MAX)"),
                   log           = sql_prefix("LOG"),
                   nchar         = sql_prefix("LEN"),
                   atan2         = sql_prefix("ATN2"),
                   ceil          = sql_prefix("CEILING"),
                   ceiling       = sql_prefix("CEILING"),
                   substr        = function(x, start, stop){
                     len <- stop - start + 1
                     build_sql(
                       "SUBSTRING(", x, ", ", start, ", ", len, ")"
                     )},
                   is.null       = function(x) dbplyr:::mssql_is_null(x, dbplyr:::sql_current_context()),
                   is.na         = function(x) dbplyr:::mssql_is_null(x, dbplyr:::sql_current_context()),
                   # TRIM is not supported on MS SQL versions under 2017
                   # https://docs.microsoft.com/en-us/sql/t-sql/functions/trim-transact-sql
                   # Best solution was to nest a left and right trims.
                   trimws        = function(x){
                     build_sql(
                       "LTRIM(RTRIM(", x ,"))"
                     )},
                   # MSSQL supports CONCAT_WS in the CTP version of 2016
                   paste         = sql_not_supported("paste()"),
                   
                   # stringr functions
                   
                   str_length      = sql_prefix("LEN"),
                   str_locate      = function(string, pattern){
                     build_sql(
                       "CHARINDEX(", pattern, ", ", string, ")"
                     )},
                   str_detect      = function(string, pattern){
                     build_sql(
                       "CHARINDEX(", pattern, ", ", string, ") > 0"
                     )}
    ),
    sql_translator(.parent = base_odbc_agg,
                   sd            = sql_aggregate("STDEV"),
                   var           = sql_aggregate("VAR"),
                   count = function(...) {
                     vars <- sql_vector(list(...), parens = FALSE, collapse = ", ")
                     build_sql("COUNT(", vars, ")")
                   },
                   quantile = function(varlist, perc) {
                     vars <- sql_vector(list(varlist), parens = FALSE, collapse = ", ")
                     build_sql("PERCENTILE_CONT(", (1 - perc), ") WITHIN GROUP (ORDER BY ", vars, " DESC) OVER ()")
                   },
                   cond_count = function(varlist, cond) {
                     vars <- sql_vector(list(varlist), parens = FALSE, collapse = ", ")
                     build_sql("SUM(CASE WHEN (", vars, ") = ", cond, " THEN 1 ELSE 0 END)")
                   },
                   # MSSQL does not have function for: cor and cov
                   cor           = sql_not_supported("cor()"),
                   cov           = sql_not_supported("cov()")
    ),
    sql_translator(.parent = base_odbc_win,
                   sd            = win_aggregate("STDEV"),
                   var           = win_aggregate("VAR"),
                   # MSSQL does not have function for: cor and cov
                   cor           = win_absent("cor"),
                   cov           = win_absent("cov")
    )
    
  )}

# Stores mssql translations to the appropriate JDBCConnection objects
sql_translate_env.JDBCConnection <- `sql_translate_env.Microsoft SQL Server`
sql_select.JDBCConnection <- dbplyr:::`sql_select.Microsoft SQL Server`
backend <- "Oracle" # or mssql
addr <- "hostname"
port <- 1521 # or 1433 for mssql, or whichever port your rdbms is located
db <- "db_name"
user <- "username"
pass <- "password"
jdbc_conn <- T # T if using jdbc, F if odbc
path_to_driver <- "file_path" # if using jdbc, fill in path to jdbc driver, otherwise set to NULL
odbc_driver <- "driver_name" # if using odbc, fill in name of odbc driver from odbcinst.ini, otherwise set to NULL
cdm_schema <- "schema_name" # if using oracle, provide CDM schema name
USE_LOOKUP_TBL <- "Y" # Y if using ref table for value validation, N if checking against parseable CSV
ref_schema <- "schema_name" # if USE_LOOKUP_TBL=Y and using Oracle, provide schema name for ref table, otherwise set to NULL
ref_table <- "PATH_REF" # if USE_LOOKUP_TBL=Y, provide ref table name
version <- "5.1"

if (jdbc_conn == T) {
  library(RJDBC)
  if (backend == "Oracle") {
    drv <- JDBC("oracle.jdbc.OracleDriver",
                path_to_driver)
    connection_string <- paste0("jdbc:oracle:thin:@", addr, ":", port, ":", db)
    sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
    sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
    sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle
  } else if (backend == "mssql") {
    drv <- JDBC("net.sourceforge.jtds.jdbc.Driver", 
                path_to_driver)
    connection_string <- paste0("jdbc:jtds:sqlserver://", addr, ":", port, ";databaseName=", db)
    sql_translate_env.JDBCConnection <- dbplyr:::`sql_translate_env.Microsoft SQL Server`
    sql_select.JDBCConnection <- dbplyr:::`sql_select.Microsoft SQL Server`
  }
  conn <- dbConnect(drv, connection_string, user, pass)
} else {
  conn <- DBI::dbConnect(odbc::odbc(),
                         Driver = odbc_driver,
                         Server = addr,
                         Database = db,
                         uid = user,
                         pwd = pass)
}
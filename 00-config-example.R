### Config file
### edit the following lines according to your SQL config

# Oracle driver class: oracle.jdbc.OracleDriver
# SQL server driver class: com.microsoft.sqlserver.jdbc.SQLServerDriver
# location: wherever the jar file is locally stored (ojdbc7/ojdbc8.jar for Oracle,
# and sqljdbc4.jar for SQL server)
drv <- JDBC("[class of driver]", "[location of driver]")

# Enter your connection string and username here.
conn <- dbConnect(drv, "[connection string]",
                  "[username]", password = pwd)

# dplyr does not have default translations for JDBCConnections, so you must declare 
# them for both Oracle and SQL Server. Choose the set of translations which correspond 
# to your setup.

# Oracle translations
sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle

# SQL Server translations
# sql_translate_env.JDBCConnection <- dbplyr:::`sql_translate_env.Microsoft SQL Server`
# sql_select.JDBCConnection <- dbplyr:::`sql_select.Microsoft SQL Server`

# Declare the name/path of the database
db_prefix <- "PCORI_ETL_31."
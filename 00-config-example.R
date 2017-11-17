### Config file
### edit the following lines according to your SQL config

# Oracle driver class: oracle.jdbc.OracleDriver
# SQL server driver class: com.microsoft.sqlserver.jdbc.SQLServerDriver or 
# net.sourceforge.jtds.jdbc.Driver (if using jTDS driver)
# MySQL driver class: com.mysql.jdbc.Driver
# location: wherever the jar file is locally stored (ojdbc7/ojdbc8.jar for Oracle,
# sqljdbc4.jar or jtds-1.3.1.jar for SQL server)
drv <- JDBC("[class of driver]", "[location of driver]")

# Enter your connection string and username here.
conn <- dbConnect(drv, "[connection string]",
                  "[username]", password = pwd)

# dplyr does not have default SQL translations for JDBCConnections, so you must declare 
# them. Choose the set of translations which correspond to your setup.

# Oracle translations
sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle

# SQL Server translations
# sql_translate_env.JDBCConnection <- dbplyr:::`sql_translate_env.Microsoft SQL Server`
# sql_select.JDBCConnection <- dbplyr:::`sql_select.Microsoft SQL Server`

# MySQL translation
# sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.MySQLConnection

# Declare the name/path of the database.
db_prefix <- "PCORI_ETL_31."
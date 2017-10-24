# increase Java heap size to 16 gb
options(java.parameters = "-Xmx16G")
source('01-functions.R')

# get field names and db password from wrapper script
args = commandArgs(trailingOnly = TRUE)
pwd <- args[2]

# load required packages and functions
require(RJDBC)
require(dbplyr)

# establish connection to db
drv <- JDBC("oracle.jdbc.OracleDriver", "/home/pmo14/sql_jar/ojdbc7.jar")
conn <- dbConnect(drv, "jdbc:oracle:thin:@dbmi-db-dev-01.dbmi.pitt.edu:1521:dbmi02",
                  "pmo14", password = pwd)

# oracle verb translations to dplyr
sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle

# generate summary for given loinc code (args[1])
generate_filtered_summary('LAB_RESULT_CM', 'LAB_LOINC', args[1], conn, drv)

# disconnect from db when finished
dbDisconnect(conn)
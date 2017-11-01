# increase Java heap size to 16 gb
options(java.parameters = "-Xmx16G")

# source in functions and required packages
source('01-functions.R')
require(RJDBC)
require(dbplyr)

# get table names and db password from wrapper script
args = commandArgs(trailingOnly = TRUE)
pwd <- args[2]
table <- args[1]

# source in SQL connection config
source('00-config.R')

# generate summary for given table in args[1]
generate_summary(db, table, conn, drv)

# disconnect from db when finished
dbDisconnect(conn)

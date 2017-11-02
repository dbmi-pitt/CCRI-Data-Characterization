# increase Java heap size to 16 gb
options(java.parameters = "-Xmx16G")

# source in functions and required packages
source('01-functions.R')
require(RJDBC)
require(dbplyr)

# get field names and db password from wrapper script
args = commandArgs(trailingOnly = TRUE)
pwd <- args[2]

# source in SQL config 
source('00-config.R')

# generate summary for given loinc code (args[1])
generate_filtered_summary(db_prefix, 'LAB_RESULT_CM', 'LAB_LOINC', args[1], conn, drv)

# disconnect from db when finished
dbDisconnect(conn)
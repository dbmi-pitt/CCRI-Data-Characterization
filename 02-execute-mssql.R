# Load libraries
library(RJDBC)
library(dplyr)
library(dbplyr)
library(tidyr)

# Source in config and function objects
source('00-config-mssql.R')
source('01-functions.R')

# Declare list of tables to characterize
table_list <- c("CONDITION", "DEATH", "DEATH_CAUSE", "DEMOGRAPHIC", "DIAGNOSIS", 
                "DISPENSING", "ENCOUNTER", "ENROLLMENT", "PCORNET_TRIAL",
                "PRESCRIBING", "PROCEDURES", "PRO_CM", "VITAL")

# Create directory structure to store reports
dir.create('./summaries/CSV', recursive = TRUE)
dir.create('./summaries/HTML')

# Loop through list of tables and run data characterization
for (i in table_list) {
  generate_summary(conn, backend = "mssql", table = i)
}
# Load libraries
library(RJDBC)
library(dplyr)
library(dbplyr)
library(tidyr)

# Source in config and function objects
source('00-config-oracle.R')
source('01-functions.R')

# Declare list of tables to characterize
table_list <- c("CONDITION", "DEATH", "DEATH_CAUSE", "DEMOGRAPHIC", "DIAGNOSIS", 
                "DISPENSING", "ENCOUNTER", "ENROLLMENT", "MED_ADMIN", "OBS_CLIN",
                "OBS_GEN", "PCORNET_TRIAL", "PRESCRIBING", "PROCEDURES", "PRO_CM",
                "PROVIDER", "VITAL")

# Create directory structure to store reports
dir.create('./summaries/CSV', recursive = TRUE)
dir.create('./summaries/HTML')

# Create time elapsed df
time_elapsed <- tibble::tibble(table_name = NA, elapsed = NA)

# Loop through list of tables and run data characterization
for (i in table_list) {
  start_timer <- proc.time()
  generate_summary(conn, backend = "Oracle", version = version, schema = schema, table = i)
  elapsed_time <- proc.time() - start_timer
  time_elapsed %>%
    add_row(table_name = i, elapsed = as.numeric(elapsed_time[3])) -> time_elapsed
}

# export time elapsed for each table to a CSV
time_elapsed %>%
  filter(!is.na(table_name)) %>%
  readr::write_csv('./time_elapsed.csv')
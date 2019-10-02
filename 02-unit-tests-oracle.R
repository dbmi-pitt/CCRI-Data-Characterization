# Load libraries
library(RJDBC)
library(dplyr)
library(dbplyr)
library(tidyr)

# Source in config and function objects
source('00-config-oracle.R')
source('01-functions.R')

# Run unit tests and save results to unit_tests subdirectory
dir.create('./unit_tests/')
dir.create('./unit_tests/invalid_values/')

{if (version == "3.1") readr::read_csv('./inst/unit_tests_31.csv')
  else if (version == "4.1") readr::read_csv('./inst/unit_tests_41.csv')
  else if (version == "5.1") readr::read_csv('./inst/unit_tests_51.csv') } %>%
  mutate(schema = cdm_schema, ref_schema = ref_schema, ref_table = ref_table, backend = "Oracle", version = version) %>%
  purrr::pmap_df(perform_unit_tests) %>%
  readr::write_csv(., paste0('./unit_tests/unit_tests_', format(Sys.time(), "%m%d%Y"), '.csv'))

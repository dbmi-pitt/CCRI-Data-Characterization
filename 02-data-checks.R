# Load libraries
library(tidyverse)
library(magrittr)
library(dbplyr)

source('00-config-conn.R')
source('01-functions.R')
# Run unit tests and save results to unit_tests subdirectory
dir.create('./unit_tests/')
dir.create('./unit_tests/invalid_values/')

readr::read_csv('./inst/unit_tests_51.csv') %>% 
  mutate(schema = cdm_schema, ref_schema = ref_schema, ref_table = ref_table, backend = backend, version = version) %>%
  purrr::pmap_df(perform_unit_tests) %>%
  readr::write_csv(., paste0('./unit_tests/unit_tests_', format(Sys.time(), "%m%d%Y"), '.csv'))

# Overview

The R code in this repository contain functions which generate summary reports 
for PCORI CDM tables.

# Prerequisites 

* r-base (>=3.6) or RStudio
* The following R packages:
    * tidyverse, glue, tidyselect for data wrangling
    * DT, htmltools, htmlwidgets for data visualization
    * if using a JDBC connection: rJava and RJDBC
    * if using a ODBC connection: odbc
    * if using PostgreSQL: RPostgreSQL
    * if using MySQL: RMySQL
* Appropriate SQL driver stored locally
    * for Oracle JDBC connections, ojdbc7.jar or ojdbc8.jar
    * for MSSQL JDBC connections, jtds-1.3.1.jar

# Usage

* 00-config-conn.R set up the connection to your database.
* 01-functions.R contains all functions required to generate summary reports.
* 02-data-checks.R run a prespecified list of data validation tests located in `inst/unit_tests.csv`. These tests are replications of selected required checks in the PCORnet data characterization SAS file.
* 03-table-summaries.R loop over lists of tables/fields to run data characterization.

## 1 Config ##

Edit the config file 00-config-conn.R. The file is commented with instructions. **For Oracle systems, please be sure to declare the schema name, otherwise the scripts will fail.**

If summarizing the CDM tables, provide your 03-table-summaries.R file with a list of tables to analyze:

```r
# Declare list of tables to characterize
table_list <- c("CONDITION", "DEATH", "DEATH_CAUSE", "DEMOGRAPHIC", "DIAGNOSIS",
                "DISPENSING", "ENCOUNTER", "ENROLLMENT", "MED_ADMIN", "OBS_CLIN",
                "OBS_GEN", "PCORNET_TRIAL", "PRESCRIBING", "PROCEDURES", "PRO_CM",
                "PROVIDER", "VITAL", "IMMUNIZATION")
```

Running the data characterization scripts on a LAB_RESULT table could run into resource limitations. Therefore, it is advised to run data characterization on-demand for a set of lab results. To run an on-demand summary for a given LOINC code, add the following to the end of the table summary file:

```r
# Run an on-demand DC for a LOINC code in the LAB_RESULT_CM table.
generate_summary(conn, backend = [either "Oracle" or "mssql"], schema = [required if backend is Oracle], table = "LAB_RESULT_CM", filtered = TRUE, field = "LAB_LOINC", value = ["LOINC code of choice"])
```

## 2 Execution ##

To run data validation tests, open an R session and issue `source('02-data-checks.R')`. Results are saved to the subdirectory `/unit_tests/` in a datestamped CSV file.

To generate the data characterization summary for each table, open an R session and issue `source('03-table-summaries.R')`. Completed reports are saved to the subdirectories `summaries/{CSV, HTML}` and are created during execution. 

# Known Issues / Caveats

* Please submit an issue or email pmo14@pitt.edu with any bug reports.


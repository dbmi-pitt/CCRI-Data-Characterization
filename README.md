# Overview

The R code in this repository contain functions which generate summary reports 
for PCORI CDM tables.

# Prerequisites 

* RStudio
* The following R packages:
    * dplyr, dbplyr, stringr, tidyr, purrr for data wrangling
    * formattable, htmltools, htmlwidgets, sparkline for data visualization
    * RJDBC and getPass for connecting to the Oracle database
* The Oracle JDBC driver (ojdbc8.jar) stored locally

# Usage

The following code block shows how to generate a report for the VITAL table given 
a random sample of 1000 PATIDs.

```r
# load required modules
require(RJDBC)
require(dbplyr)
require(getPass)

# source gen_tables functions
source('gen_table.R')

# establish connection to Oracle db
drv <- JDBC("oracle.jdbc.OracleDriver",
            <location of JDBC driver>)
conn <- dbConnect(drv, "jdbc:oracle:thin:@dbmi-db-dev-01.dbmi.pitt.edu:1521:dbmi02",
                  <username>, password = getPass())

# oracle translations to dplyr
sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle

# generate random subset of patients
patids <- conn %>%
  tbl(sql("SELECT PATID FROM PCORI_ETL_31.DEMOGRAPHIC")) %>%
  collect() %>%
  sample_n(1000)
  
# generate report
generate_report('VITAL', conn, drv, samp = patids)
```
# Overview

The R code in this repository contain functions which generate summary reports 
for PCORI CDM tables.

# Prerequisites 

* r-base (>=3.4.2) or RStudio
* The following R packages:
    * dplyr, dbplyr, stringr, tidyr, purrr for data wrangling
    * DT, htmltools, htmlwidgets for data visualization
    * rJava, RJDBC, and getPass for connecting to the database
* Appropriate SQL driver stored locally
    * for Oracle JDBC connections, ojdbc7.jar or ojdbc8.jar
    * for MSSQL JDBC connections, jtds-1.3.1.jar

# Usage

* 00-config-{oracle, mssql}.R set up the connection to your database.
* 01-functions.R contains all functions required to generate summary reports.
* 02-execute-{oracle, mssql}.R loop over lists of tables/fields to run data characterization.

## 1 Set up connection information ##

Edit the config file 00-config-oracle.R or 00-config-mssql.R depending on your 
RDBMS. **For Oracle systems, please be sure to declare the schema name, otherwise the
scripts will fail.**

## 2 Schema config ##

Provide your execute file with a list of tables to analyze:

```r
# Declare list of tables to characterize
table_list <- c("CONDITION", "DEATH", "DEATH_CAUSE", "DEMOGRAPHIC", "DIAGNOSIS", 
                "DISPENSING", "ENCOUNTER", "ENROLLMENT", "PCORNET_TRIAL",
                "PRESCRIBING", "PROCEDURES", "PRO_CM", "VITAL")
```

If generating filtered summaries, provide the execute file with the field and filters of interest. (the execution scripts do this for LOINC codes, but below is a canonical example):

```r
# Get field values for filtered characterization
loinc_codes <- conn %>%
      tbl(sql("SELECT [field] FROM [schema if required][.][table]")) %>%
      distinct([field]) %>%
      collect()
    
field_values <- field_values$FIELD
```

## 3 Execute! ##

To run this example, open an R session and issue `source('02-execute-oracle.R')`
if your RDBMS is Oracle, or `source('02-execute-mssql.R')` if your RDBMS is SQL
server. You will be prompted for your db password. Your queries will start after successful authentication. Completed reports are saved to the subdirectories `summaries/{CSV, HTML}` and are created during execution. 

# Known Issues / Caveats

* Please submit an issue or email pmo14@pitt.edu with any bug reports.


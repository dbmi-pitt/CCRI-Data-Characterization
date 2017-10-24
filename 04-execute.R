#!/usr/bin/Rscript --vanilla
# increase Java heap size to 16gb
options(java.parameters="-Xmx16G")

# load required libraries
require(RJDBC)
require(dplyr)
require(dbplyr)
require(getPass)

# ask for password to connect to db
pwd <- getPass()

# list of all tables to be summarized
table_list <- c("CONDITION", "DEATH", "DEMOGRAPHIC",
                "DIAGNOSIS", "DISPENSING", "ENCOUNTER", "ENROLLMENT", 
                "PRESCRIBING", "PRO_CM", "PROCEDURES", "VITAL",
                "LAB_RESULT_CM")

# generate report for each table in list
print("generating reports...")

for(i in table_list) {
  print(i)
  if (i == "LAB_RESULT_CM") {
    # connection parameters
    pwd <- getPass()
    drv <- JDBC("oracle.jdbc.OracleDriver",
                "/home/pmo14/sql_jar/ojdbc7.jar")
    conn <- dbConnect(drv, "jdbc:oracle:thin:@dbmi-db-dev-01.dbmi.pitt.edu:1521:dbmi02",
                      "pmo14", password = pwd)
    
    # oracle translations to dplyr
    sql_translate_env.JDBCConnection <- dbplyr:::sql_translate_env.Oracle
    sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
    sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle
    
    print("generating lab reports for each LOINC code...")
    
    loinc_codes <- conn %>%
      tbl(sql("SELECT LAB_LOINC FROM PCORI_ETL_31.LAB_RESULT_CM")) %>%
      distinct(LAB_LOINC) %>%
      collect()
    
    loinc_codes <- loinc_codes$LAB_LOINC
    gc()
    
  for (j in loinc_codes) {
    print(j)
    system(paste("Rscript 03-process-labs.R", j, pwd))
	  }
  } else {
    print('not a lab')
    system(paste("Rscript 02-process.R", i, pwd), intern = TRUE)
  }
}

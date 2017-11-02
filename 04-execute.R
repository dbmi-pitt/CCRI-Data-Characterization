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
    # source in SQL config
    source('00-config.R')
    
    print("generating lab reports for each LOINC code...")
    
    loinc_codes <- conn %>%
      tbl(sql(paste0("SELECT LAB_LOINC FROM ", db_prefix, "LAB_RESULT_CM"))) %>%
      distinct(LAB_LOINC) %>%
      collect()
    
    loinc_codes <- loinc_codes$LAB_LOINC
    gc()
    
  for (j in loinc_codes) {
    print(j)
    system(paste("Rscript 03-process-labs.R", j, pwd))
	  }
  } else {
    system(paste("Rscript 02-process.R", i, pwd), intern = TRUE)
  }
}

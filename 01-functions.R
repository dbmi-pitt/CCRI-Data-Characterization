numeric_query_oracle <- function(conn, schema, table, colinfo, filtered = FALSE,
                                 field = NULL, value = NULL) {
  tbl(conn, dbplyr::in_schema(schema, table)) %>%
    {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
    rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
    summarize_if(is.numeric, funs(count, nd = n_distinct, min, p10 = quantile(., 0.1),
                                  p25 = quantile(., 0.25), median, mean,
                                  p75 = quantile(., 0.75), p90 = quantile(., 0.9),
                                  max)) %>%
    collect() %>%
    mutate_all(funs(round(., 3))) %>%
    purrr::when(
      sum(1*(colinfo$data.type=="numeric"))==1
      ~ setNames(., paste0(gsub("\\_", "", colinfo$field.name[colinfo$data.type=="numeric"]), "_", names(.))),
      ~ .
    )
}

numeric_query_mssql <- function(conn, table, colinfo, filtered = FALSE,
                                field = NULL, value = NULL) {
  tbl(conn, table) %>%
    {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
    rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
    summarize_if(is.numeric, funs(count, nd = n_distinct, min, mean, max)) %>%
    collect() %>%
    cbind(
      tbl(conn, table) %>%  
        {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
        rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
        summarize_if(is.numeric, funs(p10 = quantile(., 0.1), p25 = quantile(., 0.25),
                                      median = quantile(., 0.5), p75 = quantile(., 0.75),
                                      p90 = quantile(., 0.9))) %>%
        summarize_all(min) %>%
        collect()
    ) %>%
    mutate_all(funs(round(., 3))) %>%
    purrr::when(
      sum(1*(colinfo$data.type=="numeric"))==1
      ~ setNames(., paste0(gsub("\\_", "", colinfo$field.name[colinfo$data.type=="numeric"]), "_", names(.))),
      ~ .
    )
}

generate_summary <- function(conn, backend = NULL, schema = NULL, table = NULL,
                             filtered = FALSE, field = NULL, value = NULL) {
  #' generate_summary()
  #' Arguments:
  #' conn = DBI connection object,
  #' backend: name of sql backend (either Oracle or MSSQL)
  #' schema: name of db/schema, required argument if using Oracle
  #' table: name of table to analyze
  #' filtered: set flag to TRUE if running data characterization over a subset of data,
  #'           (for instance over a subset of LOINC codes)
  #' field: name of column to filter over, required if filtered = TRUE
  #' value: name of value to filter in given field, required if filtered = TRUE

  # get column info to decide what queries to run
  rs <- DBI::dbSendQuery(conn, paste0("SELECT * FROM ", ifelse(backend=="Oracle", paste0(schema, ".", table), table)))
  colinfo <- DBI::dbColumnInfo(rs)
  
  # initiate queries on given table in schema
  {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>%
  {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
    rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
    summarize_if(is.character, funs(count, nd = n_distinct, min, max)) %>%
    collect() %>%
    cbind(
      {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table)) else tbl(conn, table)} %>%
      {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
        rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
        select(., -contains("DATE"), -contains("TIME")) %>%
        summarize_if(is.character, funs(nNI = cond_count(., 'NI'))) %>%
        collect()
    ) %>%
    # dbplyr complains if a query returns no columns, so check if any columns are numeric
    # if there exist any numeric columns, then calculate numeric summary stats
    purrr::when(sum(1*(colinfo$data.type=="numeric"))>0
                ~ cbind(., 
                        if(backend == "Oracle") {
                          if(filtered == TRUE) 
                            numeric_query_oracle(conn, schema, table, colinfo, filtered = TRUE,
                                                 field = field, value = value)
                          else numeric_query_oracle(conn, schema, table, colinfo)
                        }
                        else {
                          if(filtered == TRUE) 
                            numeric_query_mssql(conn, table, colinfo, filtered = TRUE, 
                                                field = field, value = value)
                          else numeric_query_mssql(conn, table, colinfo)
                        }
                ),
                ~ .) %>%
    gather(var, val) %>% 
    separate(var, c('key', 'var')) %>% 
    spread(var, val) %>%
    mutate(n = {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table))
      else tbl(conn, table)} %>%
        tally %>%
        collect %>%
        as.numeric,
      n_null = n - as.numeric(count),
      pct_null = round(n_null / n, 3),
      pct_dist = round(as.numeric(nd) / n, 3),
      pct_NI = round(as.numeric(nNI) / n, 3)
    ) %>%
    arrange(key) %T>%
    write.csv(file = paste0('./summaries/CSV/', table, '.csv'), row.names = FALSE) %>%
    purrr::when(sum(1*(colinfo$data.type=="numeric"))>0 
                  ~ select(., key, count, nd, pct_dist, n_null, pct_null, nNI, pct_NI, 
                             min, p10, p25, median, mean, p75, p90, max) %>%
                    tibble::column_to_rownames(var = "key") %>%
                    DT::datatable(options = list(dom = 't', displayLength = -1),
                                  colnames = c("N", "Distinct N", "Distinct %", "Null N", "Null %",
                                               "No Information N", "No Information %", "Min", "10th Percentile",
                                               "25th Percentile", "Median", "Mean", "75th Percentile",
                                               "90th Percentile", "Max")
                                  ),
                  ~ select(., key, count, nd, pct_dist, n_null, pct_null, nNI, pct_NI, min, max) %>%
                    tibble::column_to_rownames(var = "key") %>%
                    DT::datatable(options = list(dom = 't', displayLength = -1),
                                  colnames = c("N", "Distinct N", "Distinct %", "Null N", "Null %",
                                               "No Information N", "No Information %", "Min", "Max")
                                  )
                  ) %>%
        htmlwidgets::saveWidget(., paste0(normalizePath('./summaries/HTML'), '/', table, '.html'), selfcontained = TRUE)
    }
 

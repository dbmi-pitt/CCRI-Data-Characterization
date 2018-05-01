### Unit test / data validation functions

events_per_encounter <- function(table, field, vals, desc, test, schema = NULL, backend = NULL) {
  n_field <- glue::glue("N_", field)
  field_type <- glue::glue(field, "_TYPE")
  sql <- glue::glue_sql("
                        SELECT a.ENC_TYPE, a.{`n_field`}, b.N_ENC, (a.{`n_field`} / b.N_ENC) as RATIO FROM
                        (SELECT ENC_TYPE, COUNT(*) as {`n_field`} ",
                        ifelse(backend == "Oracle", 
                               "FROM {`schema`}.{`table`} ",
                               "FROM {`table`} "),
                        "WHERE {`field_type`} IN ({vals*})
                        GROUP BY ENC_TYPE) a
                        FULL OUTER JOIN (
                        SELECT ENC_TYPE, COUNT(*) as N_ENC ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.ENCOUNTER ",
                               "FROM ENCOUNTER "),
                        "GROUP BY ENC_TYPE) b
                        ON a.ENC_TYPE = b.ENC_TYPE",
                        vals = vals, .con = conn)
  
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  return(result %>% 
           filter(!is.na(ENC_TYPE)) %>% 
           mutate(RATIO = round(RATIO, 2), 
                  text = glue::glue("Encounter type {ENC_TYPE} has {RATIO} {desc} per encounter."),
                  test = test) %>% 
           rename(result = RATIO) %>% 
           select(text, test, result) %>% 
           as_tibble())
}

extreme_values <- function(table, field, test, schema = NULL, backend = NULL) {
  bounds <- readr::read_csv('./inst/CDM_31_parseable.csv') %>%
    filter(TABLE_NAME == table & FIELD_NAME == field) %>%
    select(VALUESET_ITEM) %>%
    pull() %>%
    as.numeric
  
  sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                        ifelse(backend == "Oracle", "{`schema`}.{`table`} ",
                               "{`table`} "),
                        "WHERE {`field`} NOT BETWEEN {low} AND {high}",
                        low = bounds[1], high = bounds[2], .con = conn)
  
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Field {field} from {table} has {result} extreme records.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result)
  )
  )
}

get_valueset <- function(table, field, version = NULL) {
  if(version == "3.1") {
    parseable <- readr::read_csv('./inst/CDM_31_parseable.csv')
  }
  if(version == "4.0") {
    parseable <- readr::read_csv('./inst/CDM_40_parseable.csv')
  }
  return(parseable %>%
           filter(TABLE_NAME == table & FIELD_NAME == field) %>%
           select(VALUESET_ITEM) %>%
           pull())
}


orphans <- function(child, parent, key, test, schema = NULL, backend = NULL) {
  sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                        ifelse(backend == "Oracle", "{`schema`}.{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM {`schema`}.{`parent`} p ",
                               "{`child`} c
                               WHERE NOT EXISTS (SELECT {`key`} FROM {`parent`} p "),
                        "WHERE p.{`key`} = c.{`key`})",
                        .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Table {child} has {result} orphan {key}s.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result)
  )
  )
}

replication_error <- function(original, replication, key, field, test, schema = NULL, backend = NULL) {
  sql <- glue::glue_sql("
                        SELECT COUNT(*) FROM ",
                        ifelse(backend == "Oracle", "{`schema`}.{`original`} a 
                               INNER JOIN {`schema`}.{`replication`} b ",
                               "{`original`} a 
                               INNER JOIN {`replication`} b "),
                        "on a.{`key`} = b.{`key`}
                        WHERE a.{`field`} != b.{`field`}",
                        .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Field {field} from {replication} has {result} replication errors.")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result)
  )
  )
}

value_validation <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  valueset <- get_valueset(table, field, version = version)
  sql <- glue::glue_sql("
                        SELECT SUM(CASE WHEN {`field`} NOT IN ({vals*}) THEN 1 ELSE 0 END) ",
                        ifelse(backend == "Oracle",
                               "FROM {`schema`}.{`table`}",
                               "FROM {`table`}"),
                        vals = valueset, .con = conn)
  query <- DBI::dbSendQuery(conn, sql)
  result <- DBI::dbFetch(query)
  DBI::dbClearResult(query)
  txt <- glue::glue("Field {field} from {table} has {result} invalid records")
  return(tibble::tibble(text = txt,
                        test = test,
                        result = as.numeric(result)
  )
  )
}

perform_unit_tests <- function(table, field, test, schema = NULL, backend = NULL, version = NULL) {
  if (test == "DC 1.06") {
    value_validation(table, field, test = test, schema = schema, backend = backend, version = version)
  } else if (test == "DC 1.08") {
    orphans(table, "DEMOGRAPHIC", "PATID", test = test, schema = schema, backend = backend)
  } else if (test == "DC 1.09") {
    orphans(table, "ENCOUNTER", "ENCOUNTERID", test = test, schema = schema, backend = backend)
  } else if (test == "DC 1.10") {
    replication_error("ENCOUNTER", table, "ENCOUNTERID", field, test = test, schema = schema, backend = backend)
  } else if (test == "DC 2.02") {
    extreme_values(table, field, test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.01") {
    events_per_encounter(table, field, c('09', '10', '11', 'SM'), "diagnosis records with known DX_TYPE", test = test, schema = schema, backend = backend)
  } else if (test == "DC 3.02") {
    events_per_encounter(table, field, c('09', '10', '11', 'CH', 'LC', 'ND', 'RE'), "procedure records with known PX_TYPE", test = test, schema = schema, backend = backend)
  }
}

### Data Characterization summary functions

numeric_query_oracle <- function(conn, schema, table, colinfo, filtered = FALSE,
                                 field = NULL, value = NULL) {
  tbl(conn, dbplyr::in_schema(schema, table)) %>%
    {if(filtered == TRUE) filter(., rlang::sym(field) == value) else .} %>%
    rename_at(.vars = vars(contains("_")), .funs = funs(gsub("\\_", "", .))) %>%
    summarize_if(is.numeric, funs(count, nd = n_distinct, min, p05 = quantile(., 0.05),
                                  p25 = quantile(., 0.25), median, mean,
                                  p75 = quantile(., 0.75), p95 = quantile(., 0.95),
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
        summarize_if(is.numeric, funs(p05 = quantile(., 0.05), p25 = quantile(., 0.25),
                                      median = quantile(., 0.5), p75 = quantile(., 0.75),
                                      p95 = quantile(., 0.95))) %>%
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
  DBI::dbClearResult(rs)
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
        summarize_if(is.character, funs(nNI = cond_count(., 'NI'), nUN = cond_count(., 'UN'),
                                        nOT = cond_count(., 'OT'))) %>%
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
    purrr::when(filtered == TRUE ~ 
                  mutate(., n = {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table))
                    else tbl(conn, table)} %>%
                      filter(., rlang::sym(field) == value) %>%
                      tally %>%
                      collect %>%
                      as.numeric,
                    n_null = n - as.numeric(count),
                    pct_null = round(n_null / n, 3) * 100,
                    pct_dist = round(as.numeric(nd) / n, 3) * 100,
                    pct_NI = round(as.numeric(nNI) / n, 3) * 100,
                    pct_UN = round(as.numeric(nUN) / n, 3) * 100,
                    pct_OT = round(as.numeric(nOT) / n, 3) * 100,
                    n_missing = as.numeric(n_null) + as.numeric(nNI) + as.numeric(nUN) + as.numeric(nOT),
                    pct_missing = pct_null + pct_NI + pct_UN + pct_OT
                    ),
                ~ mutate(., n = {if(backend == "Oracle") tbl(conn, dbplyr::in_schema(schema, table))
                  else tbl(conn, table)} %>%
                    tally %>%
                    collect %>%
                    as.numeric,
                  n_null = n - as.numeric(count),
                  pct_null = round(n_null / n, 3) * 100,
                  pct_dist = round(as.numeric(nd) / n, 3) * 100,
                  pct_NI = round(as.numeric(nNI) / n, 3) * 100,
                  pct_UN = round(as.numeric(nUN) / n, 3) * 100,
                  pct_OT = round(as.numeric(nOT) / n, 3) * 100,
                  n_missing = as.numeric(n_null) + as.numeric(nNI) + as.numeric(nUN) + as.numeric(nOT),
                  pct_missing = pct_null + pct_NI + pct_UN + pct_OT
                )
                ) %>%
    left_join(., readr::read_csv('./inst/CDM_31_field_names.csv') %>%
                filter(Table == table) %>%
                select(key, Field:Required),
              by = 'key') %>%
    arrange(Order) %>%
    select(-key, -Order) %>%
    select(Field, Required, everything()) %T>%
    purrr::when(filtered == TRUE ~ write.csv(.,
      file = paste0('./summaries/CSV/', table, "_", field, "_", value, '.csv'),
      row.names = FALSE),
      ~ write.csv(., file = paste0('./summaries/CSV/', table, '.csv'), row.names = FALSE)
      ) %>%
    purrr::when(sum(1*(colinfo$data.type=="numeric"))>0 
                  ~ select(., Field, Required, count, nd, pct_dist, n_null, pct_null, n_missing, pct_missing, 
                             min, p05, p25, median, mean, p75, p95, max) %>%
                    tibble::column_to_rownames(var = "Field") %>%
                    DT::datatable(options = list(dom = 't', displayLength = -1),
                                  colnames = c("Required", "N", "Distinct N", "Distinct %", "Null N", "Null %",
                                               "Missing, NI, UN, or OT N", "Missing, NI, UN, or OT %", "Min", "5th Percentile",
                                               "25th Percentile", "Median", "Mean", "75th Percentile",
                                               "95th Percentile", "Max")
                                  ),
                  ~ select(., Field, Required, count, nd, pct_dist, n_null, pct_null, n_missing, pct_missing, min, max) %>%
                    tibble::column_to_rownames(var = "Field") %>%
                    DT::datatable(options = list(dom = 't', displayLength = -1),
                                  colnames = c("Required", "N", "Distinct N", "Distinct %", "Null N", "Null %",
                                               "Missing, NI, UN, or OT N", "Missing, NI, UN, or OT %", "Min", "Max")
                                  )
                  ) %>%
    purrr::when(filtered == TRUE ~ htmlwidgets::saveWidget(., 
                                                           paste0(normalizePath('./summaries/HTML'), '/', table, '_', field, '_', value, '.html'),
                                                           selfcontained = TRUE),
                ~ htmlwidgets::saveWidget(., paste0(normalizePath('./summaries/HTML'), '/', table, '.html'),
                                          selfcontained = TRUE)
    )
  }
 

require(dplyr)
require(DT)
require(formattable)
require(htmltools)
require(htmlwidgets)
require(purrr)
require(stringr)
require(sparkline)
require(tibble)
require(tidyr)

add_sparklines <- function(df) {
  table <- df
  df <- df %>% describe()
  for (i in 1:nrow(df)) {
    var <- df$var[i]
    text <- paste0('table$', var)
    if(typeof(eval(parse(text=text)))=='character') {
      df$sl[i] <- as.character(htmltools::as.tags(sparkline(sort(table(eval(parse(text=text))), decreasing = TRUE), type='bar')))
    } else {
      df$sl[i] <- as.character(htmltools::as.tags(sparkline(sort(eval(parse(text=text))), type='box')))
    }
  }
  df
}

describe <- function(df,...)
{
  if (nargs() > 1) df = dplyr::select(df, ...)
  df %>%
    gather(var, value) %>%
    mutate(not_na = 1 - is.na(value)) %>%
    group_by(var) %>%
    summarize(
      N = sum(not_na),
      unique = n_distinct(value, na.rm = TRUE),
      unique_pct = unique / nrow(df),
      null = nrow(df) - N,
      null_pct = null / nrow(df),
      no_info = (sum(1*(value %in% c('NI')))),
      no_info_pct = no_info / nrow(df),
      min = min(as.numeric(value), na.rm = TRUE),
      min_date = min(value, na.rm = TRUE),
      perc25 = quantile(as.numeric(value), probs = 0.25, na.rm = TRUE),
      mean = mean(as.numeric(value), na.rm = TRUE),
      perc75 = quantile(as.numeric(value), probs = 0.75, na.rm = TRUE),
      max = max(as.numeric(value), na.rm = TRUE),
      max_date = max(value, na.rm = TRUE),
      t10 = list(top_categories(value))
    ) %>%
    select(var, N, everything()) %>%
    mutate_if(is.numeric, ~ round(.x , digits = 3) ) %>%
    mutate_cond(unique_pct == 1, min = NA, perc25 = NA, mean = NA, perc75 = NA, max = NA, t10 = NA) %>%
    mutate_cond(grepl("DATE", var), min = min_date, max = max_date) %>%
    mutate(min = replace(min, which(min == Inf), NA), mean = replace(mean, which(is.na(mean)), NA), max = replace(max, which(max == -Inf), NA)) %>%
    select(c(-min_date, -max_date)) %>%
    as.data.frame()
}

describe_field <- function(table, field, con, drv) {
  des <- conn %>%
    tbl(sql(paste0("SELECT ", field, " FROM PCORI_ETL_31.", table))) %>%
    collect() %>%
    describe()
  gc()
  des
}

mutate_cond <- function(.data, condition, ..., envir = parent.frame()) {
  condition <- eval(substitute(condition), .data, envir)
  .data[condition, ] <- .data[condition, ] %>% mutate(...)
  .data
}

substrRight <- function(x, n) {
  substr(x, nchar(x)-n+1, nchar(x))
}

trunchr <- function(x) {
  if(nchar(x)>8){
    paste0(substr(x, 1, 4), "...", substrRight(x, 4))
  } else {
    x
  }
}

top_categories <- function(x) {
  counts <- as.data.frame(table(x))
  ncats <- nrow(counts)
  counts <- counts[order(-counts$Freq),]
  if (ncats == 0) {
    return(NA)
  }
  if (ncats < 10){
    counts <- counts[1:ncats, ]
  } else {
    counts <- counts[1:10, ]
  }
  top_list <- as.character(counts$x)
  top_list <- sapply(top_list, FUN = trunchr)
  return(top_list)
}

generate_report <- function(table, con, drv, samp = NULL, full = FALSE) {
  
  if (is.null(samp)) {
    patids <- con %>%
      tbl(sql("SELECT PATID FROM PCORI_ETL_31.DEMOGRAPHIC")) %>%
      collect() %>%
      sample_n(1000)
  } else {
    patids <- samp
  }
  
  if (full != FALSE) {
    df <- con %>%
      tbl(sql(paste0('SELECT * FROM PCORI_ETL_31.', table))) %>%
      collect()
  } else {
    if (is.null(samp)) {
      patids <- con %>%
        tbl(sql("SELECT PATID FROM PCORI_ETL_31.DEMOGRAPHIC")) %>%
        collect() %>%
        sample_n(1000)
    } else {
      patids <- samp
    }
    
    df <- con %>%
      tbl(sql(paste0('SELECT * FROM PCORI_ETL_31.', table))) %>%
      filter(PATID %in% patids$PATID) %>%
      collect()
  }
  
  df <- add_sparklines(df)
  
  html_table <- df %>%
    select(sl, everything()) %>%
    formattable(align='l') %>%
    formattable::as.htmlwidget()
  
  html_table$dependencies <- c(
    html_table$dependencies,
    htmlwidgets:::widget_dependencies("sparkline","sparkline")
  )
  html_table
}

generate_summary <- function(table, con, drv, chunked = FALSE, verbose = FALSE) {
  df <- data.frame()
  
  if (chunked == "TRUE") {
    field_list <- con %>% dbListFields(., table)

    # dbListFields lists column names twice, so subset extras out
    field_list <- field_list[1:(length(field_list)/2)]
    
    for (i in field_list) {
      if (verbose == "TRUE") { print(paste0("Field: ", i))}
      des <- describe_field(table, i, con, drv)
      df <- rbind(df, des)
      rm(des)
      gc()
    }
  } else {
    df <- con %>%
      tbl(sql(paste0("SELECT * FROM PCORI_ETL_31.", table))) %>%
      collect() %>%
      describe()
  }
  
  df %>%
    column_to_rownames(var = "var") %>%
    datatable(options = list(dom = 't',
                             displayLength = -1),
              colnames = c("N", "Distinct N", "Distinct %", "Null N", "Null %", 
                           "No Information N", "No Information %", "Min", "25th Percentile", 
                           "Mean", "75th Percentile", "Max", "Top 10")) %>%
    saveWidget(., paste0(table, '.html'), selfcontained = FALSE)
}

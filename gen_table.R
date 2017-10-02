require(dplyr)
require(formattable)
require(htmltools)
require(htmlwidgets)
require(purrr)
require(stringr)
require(sparkline)
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
      t10 = list(top_categories(value))
    ) %>%
    bind_cols( data_frame(type =map(df, function(x)stringr::str_c(type_sum(x))) )) %>%
    select(var, type, N, everything()) %>%
    mutate_if(is.numeric, ~ round(.x , digits = 3) ) %>%
    as.data.frame()
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

generate_report <- function(table, con, drv, samp = NULL) {
  
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

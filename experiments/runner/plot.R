library(ggplot2)
library(readr)
library(dplyr)

and_plot_1 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(monitor, time, fill=monitor)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_wrap(facets = vars(nc)) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

and_plot_2 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(monitor, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_grid(rows = vars(n1), cols = vars(n2)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

and_common <- read_csv("and_common_data.csv")
# Missing good data where sizes vary
nc_data <- and_common %>% filter(lsize == 150, rsize == 300, n1 == 10, n2 == 10)
n1_n2_data <- and_common %>% filter(nc == 1, lsize == 150, rsize == 300)
and_plot_1(nc_data)
and_plot_2(n1_n2_data)

or_plot_1 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(order, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_grid(rows = vars(lsize), cols = vars(rsize)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

or_plot_2 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(order, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_wrap(facets = vars(nvars)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

or_data <- read_csv("or_data.csv")
or_size_data <- or_data %>% filter(nvars == 10)
or_nvars_data <- or_data %>% filter(lsize == 1000, rsize == 1000)
or_plot_1(or_size_data)
or_plot_2(or_nvars_data)

exists_plot_1 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(monitor, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_wrap(facets = vars(size)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

exists_data <- read_csv("exists_data.csv")
exists_size_data <- exists_data %>% filter(exvars == 1, pvars == 5)
exists_plot_1(exists_size_data)
exists_plot_2(exists_pv_ex_data)

once_plot_1 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(monitor, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_wrap(facets = vars(nvars)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

once_plot_2 <- function(data) {
  dodge_text = position_dodge(width = 0.9)
  p <- ggplot(data, aes(monitor, time, fill=monitor)) +
    geom_col(width = 0.8, position = dodge_text) +
    facet_wrap(facets = vars(lbound)) +
    geom_text(
      aes(label = time),
      size = 2.5,
      vjust = -0.2,
      position = dodge_text,
    ) +
    ylab("Duration (s)") +
    xlab("Monitor")
  return(p)
}

once_data <- read_csv("once_data.csv")
once_nvars_data <- once_data %>% filter(lbound == 0, ubound == 10, evr == 1000)
once_ubound_data <- once_data %>% filter(ubound == 10, evr == 1000, nvars == 1)
once_plot_1(once_nvars_data)
once_plot_2(once_ubound_data)

library(tidyverse) |> suppressPackageStartupMessages()
library(readxl)
library(fs)
library(stringr)

# scope out the files
excel_files <-
  dir_ls(
    path_wd("data"),
    glob = "*.xlsx"
  )

meta <-
  as_tibble_col(excel_files, "path") |>
  mutate(
    file_name = path_file(path),
    concept = str_to_lower(file_name) |> path_ext_remove()
  )

tibbles <-
  map(meta$path, read_xlsx)

tibbles <-
  map(
    tibbles,
    \(tbl) tbl |> rename_all(str_to_lower)
  ) |>
  set_names(meta$concept)


unique_col_names <- map(tibbles, colnames) |> flatten_chr() |> unique()
unique_col_names

# quick and dirty
lookup <-
  tibble(
    source_name = c(
      "datayear",
      "weightedpercent",
      "for_percent",
      "uppercl_forn",
      "upperci",
      "lowercl_forn",
      "lowerci",
      "coeffvar_form",
      "cv",
      "rounded_wfreq",
      "weightedn"
    ),
    name = c(
      "year",
      "percent",
      "percent",
      "ucl",
      "ucl",
      "lcl",
      "lcl",
      "coefficient_variance",
      "coefficient_variance",
      "n",
      "n"
    )
  )

pivot_rename <- function(x, y = lookup) {
  x |>
    mutate(across(everything(), as.character)) |>
    rowid_to_column("id") |>
    pivot_longer(
      cols = where(is.character),
      names_to = "source_name",
      values_to = "values"
    ) |>
    left_join({{ y }}, by = "source_name") |>
    mutate(name = if_else(is.na(name), source_name, name)) |>
    select(-source_name) |>
    pivot_wider(
      id_cols = id,
      names_from = name,
      values_from = values
    ) |>
    select(-id)
}

new_names <-
  map(tibbles, pivot_rename)

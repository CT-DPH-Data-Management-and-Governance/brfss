library(tidyverse) |> suppressPackageStartupMessages()
library(readxl)
library(fs)
library(stringr)
library(nanoparquet)

# scope out the files
excel_files <-
  dir_ls(
    path_wd("data"),
    glob = "*.xlsx"
  )

# collect some info about the files
meta <-
  as_tibble_col(excel_files, "path") |>
  mutate(
    file_name = path_file(path),
    concept = str_remove(file_name, "_.*$"),
  )

# clean up the names
new_concept <-
  meta |>
  pull(concept) |>
  str_split("(?<=[a-z])(?=[A-Z])") |>
  map(\(x) paste(x, collapse = "_")) |>
  as.character() |>
  str_to_lower() |>
  as_tibble_col("concept")

# place names back into meta
meta <-
  meta |>
  select(-concept) |>
  bind_cols(new_concept)

# read and lowercase all the column name variants
tibbles <-
  map(meta$path, read_xlsx) |>
  map(\(tbl) rename_with(tbl, str_to_lower)) |>
  set_names(meta$concept)

# grab all the unique names so we can map to new names
unique_col_names <- map(tibbles, colnames) |> flatten_chr() |> unique()

# quick crosswalk
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
  map(tibbles, pivot_rename) |>
  imap(\(x, idx) mutate(x, concept = idx)) |>
  list_rbind()

path = path_wd("output", "combo", ext = "parquet")
write_parquet(new_names, path)
write_csv(new_names, path_ext_set(path, ext = "csv"))

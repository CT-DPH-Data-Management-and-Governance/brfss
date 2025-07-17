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
  map(meta$path, read_xlsx)


tibbles <-
  tibbles |>
  map(
    \(tbl) {
      tbl |> 
        rename_with(str_to_lower) |> 
        rename_with(
          \(x) {
            stringr::str_replace_all(x, " ", "_")
          }
        )

    } 
  ) |>
  set_names(
    meta |>
      pull(concept)
  )

one_off <- 
  pluck(tibbles,1) |> 
  mutate(
    across(
      ends_with("_date"),
      lubridate::mdy
    )
  )

glimpse(one_off)


path = path_wd("output", "one-off", ext = "parquet")
write_parquet(one_off, path)
write_csv(one_off, path_ext_set(path, ext = "csv"))

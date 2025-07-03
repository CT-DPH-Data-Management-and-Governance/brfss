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

other_tibbles <-
  tibbles |>
  keep(\(tbl) ncol(tbl) > 7)

tibbles <-
  tibbles |>
  keep(\(tbl) ncol(tbl) < 8) |>
  map(\(tbl) rename_with(tbl, str_to_lower)) |>
  set_names(
    meta |>
      filter(!path %in% names(other_tibbles)) |>
      pull(concept)
  )

other_tibbles <-
  other_tibbles |>
  set_names(
    meta |>
      filter(path %in% names(other_tibbles)) |>
      pull(concept)
  )


# grab all the unique names so we can map to new names
unique_col_names <- map(tibbles, colnames) |> flatten_chr() |> unique()


collapse_names <- function(.data) {
  .data |>
    rename_with(
      \(col) {
        case_when(
          col == "datayear" ~ "year",
          col == "for_percent" ~ "percent",
          col == "weightedpercent" ~ "percent",
          col == "uppercl_forn" ~ "upper_confidence_interval",
          col == "upperci" ~ "upper_confidence_interval",
          col == "lowerci" ~ "lower_confidence_interval",
          col == "lowercl_forn" ~ "lower_confidence_interval",
          col == "cv" ~ "coefficient_variance",
          col == "coeffvar_form" ~ "coefficient_variance",
          col == "rounded_wfreq" ~ "count",
          col == "weightedn" ~ "count",
          .default = col
        )
      }
    )
}

new_names <-
  map(tibbles, collapse_names) |>
  imap(\(x, idx) mutate(x, measure = idx)) |>
  list_rbind()

mods <- "\\#|\\^|\\*"

# TODO mark the ones where the CI is for n so when
# CI gets divided by 100 for percents those are skipped

final_wide <-
  new_names |>
  mutate(
    percent_modifier = str_extract(percent, mods),
    percent = str_remove(percent, mods),
    percent = if_else(percent == "", NA_character_, percent),
    across(
      c(
        year,
        percent,
        lower_confidence_interval,
        upper_confidence_interval,
        coefficient_variance,
        count
      ),
      as.numeric
    ),
    across(
      c(
        percent,
        lower_confidence_interval,
        upper_confidence_interval
      ),
      \(col) {
        round(
          col / 100,
          digits = 3
        )
      }
    )
  )

final_long <- final_wide

path = path_wd("output", "combo", ext = "parquet")
write_parquet(final_wide, path)
write_csv(final_wide, path_ext_set(path, ext = "csv"))

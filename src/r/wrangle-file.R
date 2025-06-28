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
      "uppercl_forn", # need to mark these somehow
      "upperci",
      "lowercl_forn", # need to mark these somehow
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
  x <-
    x |>
    mutate(
      across(everything(), as.character),
      ci_target = "percent",
      cv_target = "percent"
    ) |>
    rowid_to_column("id")

  # TODO: Test this - are coev and ci for n always together? - they should be...
  is_freq <- FALSE
  if (any(str_detect(names(x), "forn|form"))) {
    is_freq <- TRUE
  }

  if (is_freq) {
    x <- x |>
      mutate(
        ci_target = "n",
        cv_target = "n"
      )
  }

  x |>
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

mods <- "\\#|\\^|\\*"

# TODO mark the ones where the CI is for n so when
# CI gets divided by 100 for percents those are skipped

new_names <-
  new_names |>
  mutate(
    percent_modifier = str_extract(percent, mods),
    percent = str_remove(percent, mods),
    percent = if_else(percent == "", NA_character_, percent),
    across(
      c(
        year,
        percent,
        lcl,
        ucl,
        coefficient_variance,
        n
      ),
      as.numeric
    ),
    percent = percent / 100,
    # TODO: test this out - talk to sme
    across(c(lcl, ucl, coefficient_variance), \(var) {
      if_else(ci_target == "percent", var / 100, var)
    })
  )

path = path_wd("output", "combo", ext = "parquet")
write_parquet(new_names, path)
write_csv(new_names, path_ext_set(path, ext = "csv"))

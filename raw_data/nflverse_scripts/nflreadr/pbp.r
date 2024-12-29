# Step 1: Install and load packages
if(!require(arrow)) install.packages("arrow")
if(!require(nflreadr)) install.packages("nflreadr")
if(!require(tidyverse)) install.packages("tidyverse")

library(nflreadr)
library(tidyverse)
library(arrow)

# Step 2: Test arrow functionality
 <- normalizePath(file.path("C:", "Users", "Wiltse Family", "Documents", 
                                   "databricks", "dfl_raw_scripts_output", 
                                   "nflverse", "pbp"), mustWork = FALSE)
dir.create(test_dir, recursive = TRUE, showWarnings = FALSE)
setwd(test_dir)

# Step 3: Process years and weeks
for(year in 2022:2023) {
  # Create year directory
  year_dir <- file.path(test_dir, paste0("year_", year))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Load data for year
  message(sprintf("Processing year %d", year))
  pbp_data <- nflreadr::load_pbp(year)
  
  # Process each week
  weeks <- sort(unique(pbp_data$week))
  for(week in weeks) {
    week_data <- pbp_data %>% filter(week == !!week)
    file_name <- file.path(year_dir, paste0("pbp_data_year_", year, "_week_", week, ".parquet"))
    arrow::write_parquet(week_data, file_name)
    message(sprintf("Saved year %d week %d", year, week))
  }
}

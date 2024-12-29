# Load required packages
library(nflreadr)
library(tidyverse)
library(arrow)

# Check if the function load_pbp is available
if ("load_pbp" %in% ls("package:nflreadr")) {
  message("load_pbp function is available")
} else {
  stop("load_pbp function is not available")
}

# Check and set working directory
current_dir <- getwd()
message("Current working directory: ", current_dir)

# Optionally set a new working directory
setwd("C:/Users/Wiltse Family/Documents/databricks/dfl_raw_scripts_output/nflverse/pbp")
message("New working directory: ", getwd())

# Function to save data for each week to a Parquet file
save_weekly_data <- function(year) {
  for (week in 1:17) {
    tryCatch({
      message(paste("Loading data for week", week))
      pbp_data <- nflreadr::load_pbp(year, weeks = week)
      
      file_name <- paste0("pbp_data_week_", week, ".parquet")
      message(paste("Saving data to", file_name))
      write_parquet(pbp_data, file_name)
      message(paste("Data for week", week, "saved successfully to", file_name))
    }, error = function(e) {
      message(paste("Error loading data for week", week, ":", e$message))
    })
  }
}

# Load and save play-by-play data for each week of the 2019 season
save_weekly_data(2019)
# Install and load required packages
if(!require(arrow)) install.packages("arrow")
if(!require(nflreadr)) install.packages("nflreadr")
if(!require(tidyverse)) install.packages("tidyverse")

library(nflreadr)
library(tidyverse)
library(arrow)

# Set output directory with full path
teams_dir <- normalizePath(file.path("C:", "Users", "Wiltse Family", "Documents", 
                                   "databricks", "dfl_raw_scripts_output", 
                                   "nflverse", "teams"), mustWork = FALSE)
dir.create(teams_dir, recursive = TRUE, showWarnings = FALSE)

# Load and save teams data with error handling
tryCatch({
    teams_data <- nflreadr::load_teams()
    output_file <- file.path(teams_dir, "nfl_teams.parquet")
    message("Attempting to save to: ", output_file)
    arrow::write_parquet(teams_data, output_file)
    
    if(file.exists(output_file)) {
        message("File saved successfully at: ", output_file)
        message("File size: ", file.size(output_file), " bytes")
    }
}, error = function(e) {
    message("Error: ", e$message)
})

# Install and load required packages
if(!require(arrow)) install.packages("arrow")
if(!require(nflreadr)) install.packages("nflreadr")
if(!require(tidyverse)) install.packages("tidyverse")

library(nflreadr)
library(tidyverse)
library(arrow)

# Set base directory for output
base_dir <- normalizePath(file.path("C:", "Users", "Wiltse Family", "Documents", 
                                   "databricks", "dfl_raw_scripts_output", 
                                   "nflverse", "rosters"), 
                         mustWork = FALSE)

# Process seasons
for(season in 2022:2024) {
    # Create season directory
    season_dir <- file.path(base_dir, paste0("season_", season))
    dir.create(season_dir, recursive = TRUE, showWarnings = FALSE)
    
    tryCatch({
        # Load season data
        message(sprintf("Processing season %d", season))
        rosters <- nflreadr::load_rosters(seasons = season)
        
        # Save roster data
        file_name <- file.path(season_dir, paste0("rosters_season_", season, ".parquet"))
        arrow::write_parquet(rosters, file_name)
        message(sprintf("Saved rosters for season %d", season))
    }, error = function(e) {
        message(sprintf("Error processing season %d: %s", season, e$message))
    })
}
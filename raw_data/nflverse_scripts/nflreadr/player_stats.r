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
                                   "nflverse", "player_stats"), 
                         mustWork = FALSE)

# Process seasons and weeks
for(season in 2022:2024) {
    # Create season directory
    season_dir <- file.path(base_dir, paste0("season_", season))
    dir.create(season_dir, recursive = TRUE, showWarnings = FALSE)
    
    tryCatch({
        # Load season data
        message(sprintf("Processing season %d", season))
        player_stats <- nflreadr::load_player_stats(
            seasons = season,
            stat_type = c("offense", "defense", "kicking")
        )
        
        # Process each week
        weeks <- sort(unique(player_stats$week))
        for(week in weeks) {
            week_data <- player_stats %>% filter(week == !!week)
            file_name <- file.path(season_dir, 
                                 paste0("player_stats_season_", season, 
                                      "_week_", week, ".parquet"))
            arrow::write_parquet(week_data, file_name)
            message(sprintf("Saved season %d week %d", season, week))
        }
    }, error = function(e) {
        message(sprintf("Error processing season %d: %s", season, e$message))
    })
}


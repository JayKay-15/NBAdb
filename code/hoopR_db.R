### hoopR DB ------------------------------
library(tidyverse)
library(lubridate)
library(hoopR)
library(RSQLite)
library(DBI)


options(dplyr.summarise.inform = FALSE)
Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

rm(list=ls())


# Update hoopR db
hoopR::update_nba_db(
    dbdir = "../nba_sql_db/",
    dbname = "hoopR_db",
    tblname = "hoopR_nba_pbp",
    force_rebuild = TRUE,
    db_connection = NULL
)



# Collect nba betting odds
box_scores <- hoopR::load_nba_team_box(seasons = c(2014:2022)) %>%
    arrange(desc(season), game_id)
ids <- unique(box_scores$game_id)

odds_df <- data.frame()

for (i in unique(ids)) {
    
    odds_list <- espn_nba_betting(i)
    
    odds_picks <- odds_list$pickcenter
    
    odds <- odds_picks %>%
        mutate(game_id = i) %>%
        select(game_id, provider_name, spread, over_under,
               away_team_odds_team_id, home_team_odds_team_id,
               away_team_odds_favorite, home_team_odds_favorite,
               away_team_odds_money_line, home_team_odds_money_line)
    
    odds_df <- bind_rows(odds_df, odds)
    
    print(paste0("Collected odds for ", i))
    
}

saveRDS(odds_df)







######## DATABASE ######## --------------------------------------------------

library(tidyverse)
library(lubridate)
library(nbastatR)
library(RSQLite)
library(DBI)
library(tictoc)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

rm(list=ls())

# tic()

# TeamShots missing: 21201214_1610612754 & 21201214_1610612738

# team_logs <- game_logs(seasons = 2022, result_types = "team")
# 
# games <- team_logs %>%
#     mutate(slugTeamHome = ifelse(locationGame == "H", slugTeam, slugOpponent),
#            slugTeamAway = ifelse(locationGame == "A", slugTeam, slugOpponent)) %>%
#     distinct(idGame, slugTeamHome, slugTeamAway)
# 
# games <- games[1:1230,]


### Database Builder ---------------------------------------------------

## GameLogsTeam & GameLogsPlayer & PlayerDictionary
# game_logs(seasons = 2022, result_types = c("team","players"))


## PlayerAdvanced & PlayerTotals & PlayerPerGame
# bref_players_stats(seasons = c(2022), tables = c("advanced", "totals", "per game"))


## TeamShots
# TeamShots <- teams_shots(team_ids = unique(team_logs$idTeam),
#                          seasons = 2022, season_types = "Regular Season", all_active_teams = T)

## PlayerProfiles - Update Later
# players <- player_profiles(player_ids = unique(TeamShots$idPlayer))


## play by play
# play_logs_all <- play_by_play_v2(game_ids = unique(games$idGame))


## box scores
# box_scores(
#     game_ids = 22100655,
#     league = "NBA",
#     box_score_types = c("Traditional", "Advanced", "Scoring", "Misc", "Usage",
#                         "Four Factors", "hustle", "tracking"),
#     result_types = c("player", "team"),
#     join_data = TRUE,
#     assign_to_environment = TRUE,
#     return_message = TRUE
# )




## connect to SQL database--------------------------------------------------------

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), 
                        "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite")
# NBAdb
# DBI::dbListTables(NBAdb)


# DBI::dbWriteTable(NBAdb, "GameLogsTeam", dataGameLogsTeam, append = T)
# DBI::dbWriteTable(NBAdb, "GameLogsPlayer", dataGameLogsPlayer, append = T)
# DBI::dbWriteTable(NBAdb, "PlayerDictionary", df_nba_player_dict, overwrite = T)
# DBI::dbWriteTable(NBAdb, "PlayerAdvanced", dataBREFPlayerAdvanced, append = T)
# DBI::dbWriteTable(NBAdb, "PlayerTotals", dataBREFPlayerTotals, append = T)
# DBI::dbWriteTable(NBAdb, "PlayerPerGame", dataBREFPlayerPerGame, append = T)
# DBI::dbWriteTable(NBAdb, "TeamShots", TeamShots, append = T)
# DBI::dbWriteTable(NBAdb, "PlayerProfiles", players, append = T) # Update Later
# DBI::dbWriteTable(NBAdb, "PlayByPlay", play_logs_all, append = T)
# DBI::dbWriteTable(NBAdb, "BoxScorePlayer", dataBoxScorePlayerNBA, append = T)
# DBI::dbWriteTable(NBAdb, "BoxScoreTeam", dataBoxScoreTeamNBA, append = T)
# DBI::dbWriteTable(NBAdb, "TeamDictionary", team_dict)
# DBI::dbWriteTable(NBAdb, "BoxScoreBREF", master_vorp, append = T)

## fix dates: df$dateGame <- as.Date(df$dateGame, origin ="1970-01-01")


DBI::dbDisconnect(NBAdb)

# toc()

## how to query
df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                   "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                   "BoxScoreBREF")

df <- df %>%
    collect()


## checking for missing games
# df2 <- unique(df$idGame)
# df3 <- unique(team_logs$idGame)
# # df3 <- unique(games$idGame)
# df4 <- df3[!df3 %in% df2]
# 
# df5 <- unique(df[c("idGame","idTeam")])
# df6 <- unique(team_logs[c("idGame","idTeam")])
# df7 <- df5[!df5 %in% df6]


## check for duplicates
# sum(duplicated(df))

# play_logs_all <- unique(df)


## filtering for specific season
# tm <- df %>%
#     filter(substr(idGame, 1,3) == 221)


## concatenate to see what else is missing from TeamShots - *** all active teams?? try this
# df_check <- df %>%
#     mutate(checker = paste(idGame,idTeam, sep="_"))
# team_logs_check <- team_logs %>%
#     mutate(checker = paste(idGame,idTeam, sep="_"))
# 
# df10 <- unique(df_check$checker)
# df11 <- unique(team_logs_check$checker)
# 
# df12 <- df11[!df11 %in% df10]
# df12


## rename tables
## write new data with "_tmp" suffix
# DBI::dbWriteTable(NBAdb, "BoxScoreBREF", df)
# DBI::dbListTables(NBAdb)

## remove old data
# DBI::dbRemoveTable(NBAdb, "BoxScoreBRED")

## rename new data by remove "_tmp"
# DBI::dbExecute(con, "ALTER TABLE flights_tmp RENAME TO flights;")






    







    
    
    
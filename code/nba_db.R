######## DATABASE ######## --------------------------------------------------

library(tidyverse)
library(lubridate)
library(nbastatR)
library(RSQLite)
library(DBI)
library(tictoc)

# Updated 7/15/2023

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

# TeamShots missing: 21201214_1610612754 & 21201214_1610612738

team_logs <- game_logs(seasons = 2023, result_types = "team")

games <- team_logs %>%
    mutate(slugTeamHome = ifelse(locationGame == "H", slugTeam, slugOpponent),
           slugTeamAway = ifelse(locationGame == "A", slugTeam, slugOpponent)) %>%
    distinct(idGame, slugTeamHome, slugTeamAway)

# games <- games[1:1230,]


### Database Builder ---------------------------------------------------
NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite")

# GameLogsTeam & GameLogsPlayer & PlayerDictionary
game_logs(seasons = c(2010:2023), result_types = c("team","players"))

dataGameLogsTeam_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite"),"GameLogsTeam") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

dataGameLogsPlayer_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite"),"GameLogsPlayer") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

dataGameLogsTeam <- dataGameLogsTeam %>% filter(!idGame %in% dataGameLogsTeam_db$idGame)

dataGameLogsPlayer <- dataGameLogsPlayer %>% filter(!idGame %in% dataGameLogsPlayer_db$idGame)

# PlayerAdvanced & PlayerTotals & PlayerPerGame
dataBREFPlayerAdvanced_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite"),"PlayerAdvanced") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPlayerTotals_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"PlayerTotals") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPlayerPerGame_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"PlayerPerGame") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_players_stats(seasons = c(2023), tables = c("advanced", "totals", "per game"))

dataBREFPlayerAdvanced <- dataBREFPlayerAdvanced %>% bind_rows(dataBREFPlayerAdvanced_db) %>% arrange(yearSeason,slugPlayerBREF)
dataBREFPlayerTotals <- dataBREFPlayerTotals %>% bind_rows(dataBREFPlayerTotals_db) %>% arrange(yearSeason,slugPlayerBREF)
dataBREFPlayerPerGame <- dataBREFPlayerPerGame %>% bind_rows(dataBREFPlayerPerGame_db) %>% arrange(yearSeason,slugPlayerBREF)


## TeamShots
TeamShots_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"TeamShots") %>% collect()

TeamShots_new <- teams_shots(team_ids = unique(team_logs$idTeam),
                         seasons = 2023, season_types = "Regular Season", all_active_teams = T)

TeamShots <- TeamShots_new %>% filter(!idGame %in% TeamShots_db$idGame)

# ## PlayerProfiles
players <- player_profiles(player_ids = unique(TeamShots_db$idPlayer))


## play by play
PlayByPlay_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"PlayByPlay") %>% collect()

games_pbp <- games %>% filter(!idGame %in% PlayByPlay_db$idGame)

play_logs_all_new <- play_by_play_v2(game_ids = unique(games_pbp$idGame))

play_logs_all <- play_logs_all_new %>% filter(!idGame %in% PlayByPlay_db$idGame)


## box scores
BoxScorePlayer_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"BoxScorePlayer") %>%
    collect()

BoxScoreTeam_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"./nba_db_sqlite"),"BoxScoreTeam") %>%
    collect()

games_bs <- games %>% filter(!idGame %in% BoxScorePlayer_db$idGame)
# games_bs2 <- games %>% filter(!idGame %in% BoxScoreTeam_db$idGame)

box_scores(
    game_ids = unique(games_bs$idGame),
    league = "NBA",
    box_score_types = c("Traditional", "Advanced", "Scoring", "Misc", "Usage",
                        "Four Factors", "hustle", "tracking"),
    result_types = c("player", "team"),
    join_data = TRUE,
    assign_to_environment = TRUE,
    return_message = TRUE
)

dataBoxScorePlayerNBA <- dataBoxScorePlayerNBA %>% filter(!idGame %in% BoxScorePlayer_db$idGame)
dataBoxScoreTeamNBA <- dataBoxScoreTeamNBA %>% filter(!idGame %in% BoxScoreTeam_db$idGame)


teams <- nbastatR::nba_teams(league = "NBA")


## BREF team stats - ADD TO DATABASE *********
nbastatR::bref_teams_stats(seasons = 2023)

## connect to SQL database--------------------------------------------------------

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite")
# NBAdb
DBI::dbListTables(NBAdb)


# DBI::dbWriteTable(NBAdb, "GameLogsTeam", dataGameLogsTeam, overwrite = T)           # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "GameLogsPlayer", dataGameLogsPlayer, overwrite = T)       # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "PlayerDictionary", df_nba_player_dict, overwrite = T)     # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "PlayerAdvanced", dataBREFPlayerAdvanced, overwrite = T)   # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "PlayerTotals", dataBREFPlayerTotals, overwrite = T)       # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "PlayerPerGame", dataBREFPlayerPerGame, overwrite = T)     # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "TeamShots", TeamShots, append = T)                        # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "PlayerProfiles", players, overwrite = T)                  # automated ---
# DBI::dbWriteTable(NBAdb, "PlayByPlay", play_logs_all, append = T)                   # automated --- 7/15
# DBI::dbWriteTable(NBAdb, "BoxScorePlayer", dataBoxScorePlayerNBA, append = T)       # automated --- slow scrape
# DBI::dbWriteTable(NBAdb, "BoxScoreTeam", dataBoxScoreTeamNBA, append = T)           # automated --- slow scrape
# DBI::dbWriteTable(NBAdb, "TeamDictionary", team_dict, overwrite = T)                # as needed ---
# DBI::dbWriteTable(NBAdb, "BasicBoxScoreBREF", master_fic)                           # Box_Scores_BREF - error in scrape
# DBI::dbWriteTable(NBAdb, "AdvancedBoxScoreBREF", master_vorp)                       # Box_Scores_BREF - error in scrape
# DBI::dbWriteTable(NBAdb, "GamesBREF", game_df, append = T)                          # Box_Scores_BREF - error in scrape
# DBI::dbWriteTable(NBAdb, "GameLogsAdj", final_db)                                   # dbRefresh --- 7/15
# DBI::dbWriteTable(NBAdb, "ResultsBook", df)                                         # model ---
# DBI::dbWriteTable(NBAdb, "Plays", df)                                               # model ---
# DBI::dbWriteTable(NBAdb, "Odds", df)                                                # model ---
# DBI::dbWriteTable(NBAdb, "AllShots", shots)                                         # model ---

### FIX DATES ### 
# df$dateGame <- as.Date(df$dateGame, origin ="1970-01-01")

DBI::dbDisconnect(NBAdb)

## how to query
df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "./nba_db_sqlite"), "GameLogsAdj") %>% 
    collect() %>%
    mutate(date = as_date(date, origin ="1970-01-01"))


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


# # rename tables
# # write new data with "_tmp" suffix
# DBI::dbWriteTable(NBAdb, "Odds_tmp", odds)
# DBI::dbListTables(NBAdb)
# 
# # remove old data
# DBI::dbRemoveTable(NBAdb, "Odds")
# 
# # rename new data by remove "_tmp"
# DBI::dbExecute(con, "ALTER TABLE flights_tmp RENAME TO flights;")



######## DATABASE ######## --------------------------------------------------

library(tidyverse)
library(janitor)
library(nbastatR)
library(RSQLite)
library(DBI)
library(tictoc)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

# team_shots missing: 21201214_1610612754 & 21201214_1610612738
team_logs <- game_logs(seasons = c(2010:2023), result_types = "team")

games <- team_logs %>%
    mutate(slugTeamHome = ifelse(locationGame == "H", slugTeam, slugOpponent),
           slugTeamAway = ifelse(locationGame == "A", slugTeam, slugOpponent)) %>%
    distinct(idGame, slugTeamHome, slugTeamAway)

# games <- games[1:1230,]


#### Database Builder ----
NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")

## game_logs_team & game_logs_player & player_dictionary
game_logs(seasons = c(2024), result_types = c("team","players"))

game_logs_team_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                               "../nba_sql_db/nba_db"),"game_logs_team") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

game_logs_player_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                 "../nba_sql_db/nba_db"),"game_logs_player") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

game_logs_team <- dataGameLogsTeam %>% filter(!idGame %in% game_logs_team_db$idGame)

game_logs_player <- dataGameLogsPlayer %>% filter(!idGame %in% game_logs_player_db$idGame)


## team_shots
team_shots_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                           "../nba_sql_db/nba_db"),"team_shots") %>% collect()

team_shots_new <- teams_shots(team_ids = unique(team_logs$idTeam),
                              seasons = 2024, season_types = "Regular Season", all_active_teams = T)

team_shots <- team_shots_new %>% filter(!idGame %in% team_shots_db$idGame)


## pbp_nba
pbp_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                    "../nba_sql_db/nba_db"),"pbp_nba") %>% collect()

games_pbp <- games %>% filter(!idGame %in% pbp_db$idGame)

pbp_new <- play_by_play_v2(game_ids = unique(games_pbp$idGame))

pbp_nba <- pbp_new %>% filter(!idGame %in% pbp_db$idGame)


## player_profiles
players <- player_profiles(player_ids = unique(TeamShots_db$idPlayer))


## box scores team & box_scores_player
team_list <- read_rds("./team_stats_list.rds")
player_list <- read_rds("./player_stats_list.rds")

team_base <- as.data.frame(team_list["Base"]) %>% clean_names()
team_advanced <- as.data.frame(team_list["Advanced"]) %>% clean_names()
team_four_factors <- as.data.frame(team_list["Four Factors"]) %>% clean_names()
team_misc <- as.data.frame(team_list["Misc"]) %>% clean_names()
team_scoring <- as.data.frame(team_list["Scoring"]) %>% clean_names()

box_scores_team <- bind_cols(team_base,team_advanced,team_four_factors,
                             team_misc,team_scoring)

player_base <- as.data.frame(player_list["Base"]) %>% clean_names()
player_advanced <- as.data.frame(player_list["Advanced"]) %>% clean_names()
player_usage <- as.data.frame(player_list["Usage"]) %>% clean_names()
player_misc <- as.data.frame(player_list["Misc"]) %>% clean_names()
player_scoring <- as.data.frame(player_list["Scoring"]) %>% clean_names()

box_scores_player <- bind_cols(player_base,player_advanced,player_usage,
                               player_misc,player_scoring)


#### basketball reference ---- re-code team and player stats
## player advanced & player totals & player per game
bref_player_advanced_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                     "../nba_sql_db/nba_db"),"bref_player_advanced") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_player_totals_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                   "../nba_sql_db/nba_db"),"bref_player_totals") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_player_game_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                 "../nba_sql_db/nba_db"),"bref_player_game") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_players_stats(seasons = c(1997:2009), tables = c("advanced", "totals", "per game"))

bref_player_advanced <- bref_player_advanced_db %>%
    bind_rows(dataBREFPlayerAdvanced) %>%
    arrange(yearSeason,slugPlayerBREF) %>%
    distinct()

bref_player_totals <- bref_player_totals_db %>%
    bind_rows(dataBREFPlayerTotals) %>%
    arrange(yearSeason,slugPlayerBREF) %>%
    distinct()

bref_player_game <- bref_player_game_db %>%
    bind_rows(dataBREFPlayerPerGame) %>%
    arrange(yearSeason,slugPlayerBREF) %>%
    distinct()

## team per game & team per poss & team shooting & team totals
dataBREFPerGameTeams_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db"),"TeamPerGame") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPerPossTeams_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"../nba_sql_db/nba_db"),"TeamPerPoss") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFShootingTeams_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"../nba_sql_db/nba_db"),"TeamShooting") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFTotalsTeams_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),"../nba_sql_db/nba_db"),"TeamTotals") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_teams_stats(seasons = c(1997:2003))

dataBREFPerGameTeams <- dataBREFPerGameTeams %>% bind_rows(dataBREFPerGameTeams_db)
dataBREFPerPossTeams <- dataBREFPerPossTeams %>% bind_rows(dataBREFPerPossTeams_db)
dataBREFShootingTeams <- dataBREFShootingTeams %>% bind_rows(dataBREFShootingTeams_db)
dataBREFTotalsTeams <- dataBREFTotalsTeams %>% bind_rows(dataBREFTotalsTeams_db)


#### connect to SQL database ----

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")

DBI::dbListTables(NBAdb)

# DBI::dbWriteTable(NBAdb, "game_logs_team", game_logs_team, append = T)              # automated --- 2023
# DBI::dbWriteTable(NBAdb, "game_logs_player", game_logs_player, append = T)          # automated --- 2023
# DBI::dbWriteTable(NBAdb, "team_shots", team_shots, append = T)                      # automated --- 2023
# DBI::dbWriteTable(NBAdb, "pbp_nba", pbp_nba, append = T)                            # automated --- 2023
# DBI::dbWriteTable(NBAdb, "box_scores_team", box_scores_team, append = T)            # automated --- 10/24
# DBI::dbWriteTable(NBAdb, "box_scores_player", box_scores_player, append = T)        # automated --- 10/24

# DBI::dbWriteTable(NBAdb, "game_logs_adj", nba_final, append = T)                    # nba_db_refresh
# DBI::dbWriteTable(NBAdb, "box_scores_gbg", box_scores_gbg, append = T)              # nba_db_refresh
# DBI::dbWriteTable(NBAdb, "nba_league_avg", nba_league_avg, append = T)              # nba_db_refresh

# DBI::dbWriteTable(NBAdb, "results_book", df)                                        # model ---
# DBI::dbWriteTable(NBAdb, "plays", df)                                               # model ---
# DBI::dbWriteTable(NBAdb, "odds", df)                                                # model ---
# DBI::dbWriteTable(NBAdb, "all_shots", df)                                           # model ---

# DBI::dbWriteTable(NBAdb, "player_dictionary", df_nba_player_dict, overwrite = T)    # automated --- 2023
# DBI::dbWriteTable(NBAdb, "team_dictionary", team_dict, overwrite = T)               # as needed ---
# DBI::dbWriteTable(NBAdb, "player_profiles", players, overwrite = T)                 # automated ---

DBI::dbDisconnect(NBAdb)

## query db
df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db"),
                 "box_scores_team") %>%
    collect() %>%
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))






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


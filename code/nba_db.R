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
NBAdb <- dbConnect(SQLite(), "../nba_sql_db/nba_db")

## game_logs_team & game_logs_player & player_dictionary
game_logs(seasons = c(2024), result_types = c("team","players"))

game_logs_team_db <- tbl(dbConnect(SQLite(),
                                   "../nba_sql_db/nba_db"),"game_logs_team") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

game_logs_player_db <- tbl(dbConnect(SQLite(),
                                     "../nba_sql_db/nba_db"),"game_logs_player") %>%
    collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

game_logs_team <- dataGameLogsTeam %>% filter(!idGame %in% game_logs_team_db$idGame)

game_logs_player <- dataGameLogsPlayer %>% filter(!idGame %in% game_logs_player_db$idGame)


## team_shots
team_shots_db <- tbl(dbConnect(SQLite(),
                               "../nba_sql_db/nba_db"),"team_shots") %>% collect()

team_shots_new <- teams_shots(team_ids = unique(team_logs$idTeam),
                              seasons = 2024,
                              season_types = "Regular Season",
                              all_active_teams = T)

team_shots <- team_shots_new %>% filter(!idGame %in% team_shots_db$idGame)


## pbp_nba
pbp_db <- tbl(dbConnect(SQLite(),
                        "../nba_sql_db/nba_db"),"pbp_nba") %>% collect()

games_pbp <- games %>% filter(!idGame %in% pbp_db$idGame)

pbp_new <- play_by_play_v2(game_ids = unique(games_pbp$idGame))

pbp_nba <- pbp_new %>% filter(!idGame %in% pbp_db$idGame)


## player_profiles
players <- player_profiles(player_ids = unique(TeamShots_db$idPlayer))


## box scores team & box_scores_player
team_list <- read_rds("./team_stats_list.rds")

cleaned_team_list <- lapply(team_list, function(df) {
    df <- clean_names(df)
    # cols_to_remove <- grep("^(e_|opp_|.+_rank$)", names(df))
    # df <- df[, -cols_to_remove]
    return(df)
})

combined_df <- cleaned_team_list[[1]]

# Iterate through the remaining data frames and join them while removing existing columns
for (i in 2:length(cleaned_team_list)) {
    df_to_join <- cleaned_team_list[[i]]
    existing_cols <- intersect(names(combined_df), names(df_to_join))
    existing_cols <- setdiff(existing_cols, c("game_id", "team_name"))
    df_to_join <- df_to_join %>% select(-any_of(existing_cols))
    combined_df <- left_join(combined_df, df_to_join, by = c("game_id", "team_name"))
}

team_all_stats <- combined_df %>%
    select(-starts_with(c("e_","opp_")), -ends_with(c("_rank","_flag"))) %>%
    arrange(game_date, game_id) %>%
    mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
           game_date = as_date(game_date)) %>%
    select(season_year:matchup, location, wl:pct_uast_fgm)

opp_all_stats <- team_all_stats %>%
    select(game_id, team_name, fgm:pct_uast_fgm) %>%
    rename_with(~paste0("opp_", .), -c(game_id)) %>%
    select(-opp_plus_minus)
    
all_stats <- team_all_stats %>%
    inner_join(opp_all_stats, by = c("game_id"), relationship = "many-to-many") %>%
    filter(team_name != opp_team_name) %>%
    select(season_year:team_name, opp_team_name,
           game_id:min, pts, opp_pts, plus_minus, fgm:opp_pct_uast_fgm) %>%
    group_by(season_year, team_id, location) %>%
    mutate(across(c(fgm:opp_pct_uast_fgm),
                  \(x) pracma::movavg(x, n = 10, type = 'e'))) %>%
    ungroup()

base_stats <- all_stats %>%
    filter(location == "away") %>%
    select(season_year:plus_minus)

away_stats <- all_stats %>%
    filter(location == "away") %>%
    select(game_id, team_name, fgm:opp_pct_uast_fgm) %>%
    rename_with(~paste0("away_", .), -c(game_id, team_name)) %>%
    group_by(team_name) %>%
    mutate(across(away_fgm:away_opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
    ungroup()

home_stats <- all_stats %>%
    filter(location == "home") %>%
    select(game_id, team_name, fgm:opp_pct_uast_fgm) %>%
    rename_with(~paste0("home_", .), -c(game_id, team_name)) %>%
    group_by(team_name) %>%
    mutate(across(home_fgm:home_opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
    ungroup()

nba_final <- base_stats %>%
    left_join(away_stats, by = c("game_id" = "game_id",
                                   "team_name" = "team_name")) %>%
    left_join(home_stats, by = c("game_id" = "game_id",
                                   "opp_team_name" = "team_name")) %>%
    na.exclude() %>%
    arrange(game_date, game_id) # add game count and b2b


saveRDS(nba_final, "./nba_final.rds")


player_list <- read_rds("./player_stats_list.rds")

player_base <- as.data.frame(player_list["Base"]) %>% clean_names()
player_advanced <- as.data.frame(player_list["Advanced"]) %>% clean_names()
player_usage <- as.data.frame(player_list["Usage"]) %>% clean_names()
player_misc <- as.data.frame(player_list["Misc"]) %>% clean_names()
player_scoring <- as.data.frame(player_list["Scoring"]) %>% clean_names()

box_scores_player <- bind_cols(player_base,player_advanced,player_usage,
                               player_misc,player_scoring)


#### basketball reference ---- re-code team and player stats
## player advanced & player totals & player per game
bref_player_advanced_db <- tbl(dbConnect(SQLite(),
                                         "../nba_sql_db/nba_db"),"bref_player_advanced") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_player_totals_db <- tbl(dbConnect(RSQLite(),
                                       "../nba_sql_db/nba_db"),"bref_player_totals") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_player_game_db <- tbl(dbConnect(SQLite(),
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
dataBREFPerGameTeams_db <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),"TeamPerGame") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPerPossTeams_db <- tbl(DBI::dbConnect(SQLite(),"../nba_sql_db/nba_db"),"TeamPerPoss") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFShootingTeams_db <- tbl(dbConnect(SQLite(),"../nba_sql_db/nba_db"),"TeamShooting") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFTotalsTeams_db <- tbl(dbConnect(SQLite(),"../nba_sql_db/nba_db"),"TeamTotals") %>%
    collect() %>%
    filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

bref_teams_stats(seasons = c(1997:2003))

dataBREFPerGameTeams <- dataBREFPerGameTeams %>% bind_rows(dataBREFPerGameTeams_db)
dataBREFPerPossTeams <- dataBREFPerPossTeams %>% bind_rows(dataBREFPerPossTeams_db)
dataBREFShootingTeams <- dataBREFShootingTeams %>% bind_rows(dataBREFShootingTeams_db)
dataBREFTotalsTeams <- dataBREFTotalsTeams %>% bind_rows(dataBREFTotalsTeams_db)


#### connect to SQL database ----

NBAdb <- dbConnect(SQLite(), "../nba_sql_db/nba_db")

dbListTables(NBAdb)

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

dbDisconnect(NBAdb)

## query db
df <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "box_scores_team") %>%
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

box_scores_home <- team_all_stats %>%
    filter(location == "home") %>%
    select(game_id, team_name, fgm:pct_uast_fgm) %>%
    rename_with(~paste0("opp_", .),
                -c(game_id))

box_scores <- team_all_stats %>%
    filter(location == "away") %>%
    select(season_year:matchup, location, wl:pct_uast_fgm) %>%
    left_join(box_scores_home, by = "game_id") %>%
    select(season_year:team_name, opp_team_name, game_id:pct_uast_fgm, opp_fgm:opp_pct_uast_fgm)

box_scores_wt <- box_scores %>%
    group_by(season_year, team_id) %>%
    mutate(across(c(min:opp_pct_uast_fgm),
                  \(x) pracma::movavg(x, n = 10, type = 'e'))) %>%
    ungroup()

box_scores_base <- box_scores_wt %>%
    group_by(team_id) %>%
    mutate(across(fgm:opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
    na.exclude()


















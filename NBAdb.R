######## DATABASE ######## --------------------------------------------------

library(tidyverse)
library(lubridate)
library(nbastatR)
library(RSQLite)
library(DBI)
library(tictoc)

# Updated 7/15/2023

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

# tic()

# TeamShots missing: 21201214_1610612754 & 21201214_1610612738

team_logs <- game_logs(seasons = 2023, result_types = "team")

games <- team_logs %>%
    mutate(slugTeamHome = ifelse(locationGame == "H", slugTeam, slugOpponent),
           slugTeamAway = ifelse(locationGame == "A", slugTeam, slugOpponent)) %>%
    distinct(idGame, slugTeamHome, slugTeamAway)

# games <- games[1:1230,]


### Database Builder ---------------------------------------------------
NBAdb <- DBI::dbConnect(RSQLite::SQLite(), 
                        "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite")

# GameLogsTeam & GameLogsPlayer & PlayerDictionary
game_logs(seasons = c(2010:2023), result_types = c("team","players"))

dataGameLogsTeam_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                       "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                        "GameLogsTeam") %>% collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

dataGameLogsPlayer_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                                 "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                  "GameLogsPlayer") %>% collect() %>% 
    mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

dataGameLogsTeam <- dataGameLogsTeam %>% filter(!idGame %in% dataGameLogsTeam_db$idGame)

dataGameLogsPlayer <- dataGameLogsPlayer %>% filter(!idGame %in% dataGameLogsPlayer_db$idGame)

# PlayerAdvanced & PlayerTotals & PlayerPerGame
dataBREFPlayerAdvanced_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                "PlayerAdvanced") %>% collect() %>% filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPlayerTotals_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                "PlayerTotals") %>% collect() %>% filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)

dataBREFPlayerPerGame_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                "PlayerPerGame") %>% collect() %>% filter(isSeasonCurrent == FALSE) %>% 
    mutate(isSeasonCurrent = FALSE)


bref_players_stats(seasons = c(2023), tables = c("advanced", "totals", "per game"))


dataBREFPlayerAdvanced <- dataBREFPlayerAdvanced %>% bind_rows(dataBREFPlayerAdvanced_db) %>% arrange(yearSeason,slugPlayerBREF)
dataBREFPlayerTotals <- dataBREFPlayerTotals %>% bind_rows(dataBREFPlayerTotals_db) %>% arrange(yearSeason,slugPlayerBREF)
dataBREFPlayerPerGame <- dataBREFPlayerPerGame %>% bind_rows(dataBREFPlayerPerGame_db) %>% arrange(yearSeason,slugPlayerBREF)


## TeamShots
TeamShots_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                 "TeamShots") %>% collect()

TeamShots_new <- teams_shots(team_ids = unique(team_logs$idTeam),
                         seasons = 2023, season_types = "Regular Season", all_active_teams = T)

TeamShots <- TeamShots_new %>% filter(!idGame %in% TeamShots_db$idGame)

# ## PlayerProfiles
players <- player_profiles(player_ids = unique(TeamShots_db$idPlayer))


## play by play
PlayByPlay_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                          "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                           "PlayByPlay") %>% collect()

games_pbp <- games %>% filter(!idGame %in% PlayByPlay_db$idGame)

play_logs_all_new <- play_by_play_v2(game_ids = unique(games_pbp$idGame))

play_logs_all <- play_logs_all_new %>% filter(!idGame %in% PlayByPlay_db$idGame)


## box scores
BoxScorePlayer_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                           "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                            "BoxScorePlayer") %>% collect()

BoxScoreTeam_db <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                           "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                            "BoxScoreTeam") %>% collect()

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

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), 
                        "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite")
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

# toc()

## how to query
df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                   "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),
                                   "GameLogsAdj") %>% 
    collect() %>% mutate(date = as_date(date, origin ="1970-01-01"))


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




### DB Refresh ------------------------------
library(tidyverse)
library(lubridate)
library(nbastatR)
library(RSQLite)
library(DBI)


options(dplyr.summarise.inform = FALSE)
Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

rm(list=ls())
setwd("/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/")

final_db <- data.frame()

dataGameLogsTeam  <- game_logs(seasons = c(2014:2023), result_types = "team")

dataGameLogsTeam <- dataGameLogsTeam %>% mutate(dateGame = as_date(dateGame)) %>% arrange(dateGame,idGame)

yr_list <- unique(dataGameLogsTeam$yearSeason)

games_df <- data.frame()

for (i in yr_list) {
    holder <- dataGameLogsTeam %>%
        filter(yearSeason == i) %>%
        distinct(dateGame, yearSeason) %>%
        mutate(stat_start = min(dateGame)) %>%
        mutate(stat_end = dateGame - 1) %>%
        mutate(keep = if_else(dateGame >= (min(as_date(dateGame)) + 14) & dateGame <= as_date("2023-04-09") - 14, 
                              1, 0)) %>%
        mutate(adj = if_else(dateGame >= min(as_date(dateGame)) + 21, 1, 0)) %>%
        filter(keep == 1) %>%
        select(yearSeason,dateGame,stat_start,stat_end,adj)
    
    games_df <- rbind(games_df, holder)
    
}

NBAdb <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                   "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite"),"GameLogsAdj")

nba_gl <- NBAdb %>% collect() %>% mutate(date = as_date(date, origin ="1970-01-01")) %>% arrange(date)
nba_gl$date %>% tail(1)

last_date <- as_date(max(nba_gl$date))

games_df <- games_df %>% filter(dateGame > last_date)

# for testing
# games_df <- games_df %>% filter(dateGame > "2022-03-26")

dates_no_adj <- games_df %>%
    filter(adj == 0)

dates_adj <- games_df %>%
    filter(adj == 1)

#### No ADJ + Weights ####

b <- 1
h <- nrow(dates_no_adj)

for (b in b:h) {
    
    stats_start_gxg <- dates_no_adj[b,3]
    stats_end_gxg <- dates_no_adj[b,4]
    
    gm_day_gxg <- dates_no_adj[b,2]
    
    stat_range <- dataGameLogsTeam %>% filter(dateGame >= stats_start_gxg & dateGame <= stats_end_gxg)
    
    game_range <- left_join(dataGameLogsTeam, dataGameLogsTeam, 
                            by = c("idGame" = "idGame", "slugTeam" = "slugOpponent"))
    
    ### Attach game logs to itself to get all stats for each game in one row
    
    gl <- left_join(stat_range, stat_range, by = c("idGame" = "idGame", "slugTeam" = "slugOpponent"))
    
    gl <- gl %>%
        select(5,13,8,54,21,
               45,90,34,79,
               23,24,26,27,35,36,37,38,39,40,43,41,42,44,
               68,69,71,72,80,81,82,83,84,85,88,86,87,89)
    
    colnames(gl) <- c("Date", "teamLoc", "teamName", "opptName", "teamRslt", 
                      "teamPTS", "opptPTS", "teamMin", "opptMin", 
                      "teamFGM", "teamFGA", "team3PM", "team3PA", "teamFTM",
                      "teamFTA", "teamORB", "teamDRB", "teamTRB", "teamAST",
                      "teamTOV", "teamSTL", "teamBLK", "teamPF", 
                      "opptFGM", "opptFGA", "oppt3PM", "oppt3PA", "opptFTM", 
                      "opptFTA", "opptORB", "opptDRB", "opptTRB", "opptAST", 
                      "opptTOV", "opptSTL", "opptBLK", "opptPF")
    
    # Filter for home/away 
    
    home <- gl %>%
        filter(teamLoc == "H")
    
    away <- gl %>%
        filter(teamLoc == "A")
    
    ##### Games count - Season #####
    
    gl <- gl %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### Games count - Away #####
    
    away <- away %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### Games count - Home #####
    
    home <- home %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### SEASON TOTALS #####
    
    season_grouped <- gl %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ##### SEASON ADVANCED STATS #####
    
    season_adv <- season_grouped
    
    # season_adv$Poss <- with(season_adv, 0.5 * ((teamFGA + 0.4 * teamFTA - 1.07 * (teamORB / (teamORB + opptDRB)) * (teamFGA - teamFGM) + teamTOV) + 
    #                                                (opptFGA + 0.4 * opptFTA - 1.07 * (opptORB / (opptORB + teamDRB)) * (opptFGA - opptFGM) + opptTOV)))
    # season_adv$oPoss <- with(season_adv, 0.5 * ((opptFGA + 0.4 * opptFTA - 1.07 * (opptORB / (opptORB + teamDRB)) * (opptFGA - opptFGM) + opptTOV) +
    #                                                 (teamFGA + 0.4 * teamFTA - 1.07 * (teamORB / (teamORB + opptDRB)) * (teamFGA - teamFGM) + teamTOV)))
    season_adv$Poss <- with(season_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    season_adv$oPoss <- with(season_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    season_adv$Pace <- with(season_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    season_adv$oPace <- with(season_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    season_adv$ORtg <- with(season_adv, (teamPTS / Poss) * 100)
    season_adv$DRtg <- with(season_adv, (opptPTS / oPoss) * 100)
    
    season_adv$FG <- with(season_adv, teamFGM / teamFGA)
    season_adv$SR2 <- with(season_adv, (teamFGA - team3PA) / teamFGA)
    season_adv$FG3 <- with(season_adv, team3PM / team3PA)
    season_adv$SR3 <- with(season_adv, team3PA / teamFGA)
    season_adv$FT <- with(season_adv, teamFTM / teamFTA)
    season_adv$FTR <- with(season_adv, teamFTM / teamFGA)
    season_adv$ORB <- with(season_adv, teamORB / (teamORB + opptDRB))
    season_adv$DRB <- with(season_adv, teamDRB / (teamDRB + opptORB))
    season_adv$TRB <- with(season_adv, teamTRB / (teamTRB + opptTRB))
    season_adv$AST <- with(season_adv, teamAST / teamFGM)
    season_adv$TOV <- with(season_adv, teamTOV / Poss)
    season_adv$STL <- with(season_adv, teamSTL / oPoss)
    season_adv$BLK <- with(season_adv, teamBLK / (opptFGA - oppt3PA))
    season_adv$PF <- with(season_adv, teamPF / oPoss)
    season_adv$eFG <- with(season_adv, (teamFGM + .5 * team3PM) / teamFGA)
    season_adv$TS <- with(season_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    season_adv$oFG <- with(season_adv, opptFGM / opptFGA)
    season_adv$oSR2 <- with(season_adv, (opptFGA - oppt3PA) / opptFGA)
    season_adv$oFG3 <- with(season_adv, oppt3PM / oppt3PA)
    season_adv$oSR3 <- with(season_adv, oppt3PA / opptFGA)
    season_adv$oFT <- with(season_adv, opptFTM / opptFTA)
    season_adv$oFTR <- with(season_adv, opptFTM / opptFGA)
    season_adv$oORB <- with(season_adv, opptORB / (opptORB + teamDRB))
    season_adv$oDRB <- with(season_adv, opptDRB / (opptDRB + teamORB))
    season_adv$oTRB <- with(season_adv, opptTRB / (teamTRB + opptTRB))
    season_adv$oAST <- with(season_adv, opptAST / opptFGM)
    season_adv$oTOV <- with(season_adv, opptTOV / oPoss)
    season_adv$oSTL <- with(season_adv, opptSTL / Poss)
    season_adv$oBLK <- with(season_adv, opptBLK / (teamFGA - team3PA))
    season_adv$oPF <- with(season_adv, opptPF / Poss)
    season_adv$oeFG <- with(season_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    season_adv$oTS <- with(season_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    season_final <- season_adv %>%
        select(1,41:72,39,40,37)
    
    ##### WEIGHTING - SEASON ####
    
    wt_holder_season <- data.frame()
    
    a <- 1
    g <- nrow(season_final)
    
    for (a in a:g) {
        
        act_id <- as.character(season_final[a,1])
        
        adj_gxg <- season_final %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2 * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3 * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3 * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2 * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3 * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3 * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_season <- bind_rows(wt_holder_season,wt_df)
        
    }
    
    season_final_wt <- wt_holder_season
    
    colnames(season_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                   "AST","TOV","STL","BLK","PF","eFG","TS",
                                   "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                   "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                   "ORtg","DRtg","Pace")
    
    ### GROUPING HOME GAMES 
    
    home_grouped <- home %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ###### HOME ADVANCED STATS ######
    
    home_adv <- home_grouped
    
    home_adv$Poss <- with(home_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    home_adv$oPoss <- with(home_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    home_adv$Pace <- with(home_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    home_adv$oPace <- with(home_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    home_adv$ORtg <- with(home_adv, (teamPTS / Poss) * 100)
    home_adv$DRtg <- with(home_adv, (opptPTS / oPoss) * 100)
    
    home_adv$FG <- with(home_adv, teamFGM / teamFGA)
    home_adv$SR2 <- with(home_adv, (teamFGA - team3PA) / teamFGA)
    home_adv$FG3 <- with(home_adv, team3PM / team3PA)
    home_adv$SR3 <- with(home_adv, team3PA / teamFGA)
    home_adv$FT <- with(home_adv, teamFTM / teamFTA)
    home_adv$FTR <- with(home_adv, teamFTM / teamFGA)
    home_adv$ORB <- with(home_adv, teamORB / (teamORB + opptDRB))
    home_adv$DRB <- with(home_adv, teamDRB / (teamDRB + opptORB))
    home_adv$TRB <- with(home_adv, teamTRB / (teamTRB + opptTRB))
    home_adv$AST <- with(home_adv, teamAST / teamFGM)
    home_adv$TOV <- with(home_adv, teamTOV / Poss)
    home_adv$STL <- with(home_adv, teamSTL / oPoss)
    home_adv$BLK <- with(home_adv, teamBLK / (opptFGA - oppt3PA))
    home_adv$PF <- with(home_adv, teamPF / oPoss)
    home_adv$eFG <- with(home_adv, (teamFGM + .5 * team3PM) / teamFGA)
    home_adv$TS <- with(home_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    home_adv$oFG <- with(home_adv, opptFGM / opptFGA)
    home_adv$oSR2 <- with(home_adv, (opptFGA - oppt3PA) / opptFGA)
    home_adv$oFG3 <- with(home_adv, oppt3PM / oppt3PA)
    home_adv$oSR3 <- with(home_adv, oppt3PA / opptFGA)
    home_adv$oFT <- with(home_adv, opptFTM / opptFTA)
    home_adv$oFTR <- with(home_adv, opptFTM / opptFGA)
    home_adv$oORB <- with(home_adv, opptORB / (opptORB + teamDRB))
    home_adv$oDRB <- with(home_adv, opptDRB / (opptDRB + teamORB))
    home_adv$oTRB <- with(home_adv, opptTRB / (teamTRB + opptTRB))
    home_adv$oAST <- with(home_adv, opptAST / opptFGM)
    home_adv$oTOV <- with(home_adv, opptTOV / oPoss)
    home_adv$oSTL <- with(home_adv, opptSTL / Poss)
    home_adv$oBLK <- with(home_adv, opptBLK / (teamFGA - team3PA))
    home_adv$oPF <- with(home_adv, opptPF / Poss)
    home_adv$oeFG <- with(home_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    home_adv$oTS <- with(home_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    home_final <- home_adv %>%
        select(1,41:72,39,40,37)
    
    ##### WEIGHTING - home ####
    
    wt_holder_home <- data.frame()
    
    a <- 1
    g <- nrow(home_final)
    
    for (a in a:g) {
        
        act_id <- as.character(home_final[a,1])
        
        adj_gxg <- home_final %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2 * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3 * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3 * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2 * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3 * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3 * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_home <- bind_rows(wt_holder_home,wt_df)
        
    }
    
    home_final_wt <- wt_holder_home
    
    colnames(home_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                 "AST","TOV","STL","BLK","PF","eFG","TS",
                                 "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                 "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                 "ORtg","DRtg","Pace")
    
    ### GROUPING AWAY GAMES
    
    away_grouped <- away %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ###### AWAY ADVANCED STATS #####
    
    away_adv <- away_grouped
    
    away_adv$Poss <- with(away_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    away_adv$oPoss <- with(away_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    away_adv$Pace <- with(away_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    away_adv$oPace <- with(away_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    away_adv$ORtg <- with(away_adv, (teamPTS / Poss) * 100)
    away_adv$DRtg <- with(away_adv, (opptPTS / oPoss) * 100)
    
    away_adv$FG <- with(away_adv, teamFGM / teamFGA)
    away_adv$SR2 <- with(away_adv, (teamFGA - team3PA) / teamFGA)
    away_adv$FG3 <- with(away_adv, team3PM / team3PA)
    away_adv$SR3 <- with(away_adv, team3PA / teamFGA)
    away_adv$FT <- with(away_adv, teamFTM / teamFTA)
    away_adv$FTR <- with(away_adv, teamFTM / teamFGA)
    away_adv$ORB <- with(away_adv, teamORB / (teamORB + opptDRB))
    away_adv$DRB <- with(away_adv, teamDRB / (teamDRB + opptORB))
    away_adv$TRB <- with(away_adv, teamTRB / (teamTRB + opptTRB))
    away_adv$AST <- with(away_adv, teamAST / teamFGM)
    away_adv$TOV <- with(away_adv, teamTOV / Poss)
    away_adv$STL <- with(away_adv, teamSTL / oPoss)
    away_adv$BLK <- with(away_adv, teamBLK / (opptFGA - oppt3PA))
    away_adv$PF <- with(away_adv, teamPF / oPoss)
    away_adv$eFG <- with(away_adv, (teamFGM + .5 * team3PM) / teamFGA)
    away_adv$TS <- with(away_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    away_adv$oFG <- with(away_adv, opptFGM / opptFGA)
    away_adv$oSR2 <- with(away_adv, (opptFGA - oppt3PA) / opptFGA)
    away_adv$oFG3 <- with(away_adv, oppt3PM / oppt3PA)
    away_adv$oSR3 <- with(away_adv, oppt3PA / opptFGA)
    away_adv$oFT <- with(away_adv, opptFTM / opptFTA)
    away_adv$oFTR <- with(away_adv, opptFTM / opptFGA)
    away_adv$oORB <- with(away_adv, opptORB / (opptORB + teamDRB))
    away_adv$oDRB <- with(away_adv, opptDRB / (opptDRB + teamORB))
    away_adv$oTRB <- with(away_adv, opptTRB / (teamTRB + opptTRB))
    away_adv$oAST <- with(away_adv, opptAST / opptFGM)
    away_adv$oTOV <- with(away_adv, opptTOV / oPoss)
    away_adv$oSTL <- with(away_adv, opptSTL / Poss)
    away_adv$oBLK <- with(away_adv, opptBLK / (teamFGA - team3PA))
    away_adv$oPF <- with(away_adv, opptPF / Poss)
    away_adv$oeFG <- with(away_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    away_adv$oTS <- with(away_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    away_final <- away_adv %>%
        select(1,41:72,39,40,37)
    
    ##### WEIGHTING - away ####
    
    wt_holder_away <- data.frame()
    
    a <- 1
    g <- nrow(away_final)
    
    for (a in a:g) {
        
        act_id <- as.character(away_final[a,1])
        
        adj_gxg <- away_final %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2 * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3 * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3 * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2 * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3 * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3 * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_away <- bind_rows(wt_holder_away,wt_df)
        
    }
    
    away_final_wt <- wt_holder_away
    
    colnames(away_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                 "AST","TOV","STL","BLK","PF","eFG","TS",
                                 "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                 "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                 "ORtg","DRtg","Pace")
    
    ##### Scores Only ####
    
    scores <- game_range %>%
        select(1,5,13,54,8,90,45)
    
    colnames(scores) <- c("season", "date", "loc", "away", "home", "as", "hs")
    
    scores <- scores %>%
        filter(loc == "H" & date == gm_day_gxg)
    
    scores <- left_join(scores, away_final_wt, by = c("away" = "team")) %>%
        left_join(., home_final_wt, by = c("home" = "team")) 
    
    colnames(scores)[8:77] <- c("FG_away","SR2_away","FG3_away","SR3_away","FT_away","FTR_away",
                                "ORB_away","DRB_away","TRB_away","AST_away","TOV_away","STL_away",
                                "BLK_away","PF_away","eFG_away","TS_away",
                                "oFG_away","oSR2_away","oFG3_away","oSR3_away","oFT_away","oFTR_away",
                                "oORB_away","oDRB_away","oTRB_away","oAST_away","oTOV_away","oSTL_away",
                                "oBLK_away","oPF_away","oeFG_away","oTS_away",
                                "ORtg_away","DRtg_away","Pace_away",
                                "FG_home","SR2_home","FG3_home","SR3_home","FT_home","FTR_home",
                                "ORB_home","DRB_home","TRB_home","AST_home","TOV_home","STL_home",
                                "BLK_home","PF_home","eFG_home","TS_home",
                                "oFG_home","oSR2_home","oFG3_home","oSR3_home","oFT_home","oFTR_home",
                                "oORB_home","oDRB_home","oTRB_home","oAST_home","oTOV_home","oSTL_home",
                                "oBLK_home","oPF_home","oeFG_home","oTS_home",
                                "ORtg_home","DRtg_home","Pace_home")
    
    final_db <- bind_rows(final_db,scores)
    
    print(paste(tail(final_db$date, n = 1), "Complete"))
    
}


#### ADJ + Weights ####

b <- 1
h <- nrow(dates_adj)

for (b in b:h) {
    
    stats_start_gxg <- dates_adj[b,3]
    stats_end_gxg <- dates_adj[b,4]
    
    gm_day_gxg <- dates_adj[b,2]
    
    stat_range <- dataGameLogsTeam %>% filter(dateGame >= stats_start_gxg & dateGame <= stats_end_gxg)
    
    game_range <- left_join(dataGameLogsTeam, dataGameLogsTeam, 
                            by = c("idGame" = "idGame", "slugTeam" = "slugOpponent"))
    
    ### Attach game logs to itself to get all stats for each game in one row
    
    gl <- left_join(stat_range, stat_range, by = c("idGame" = "idGame", "slugTeam" = "slugOpponent"))
    
    gl <- gl %>%
        select(5,13,8,54,21,
               45,90,34,79,
               23,24,26,27,35,36,37,38,39,40,43,41,42,44,
               68,69,71,72,80,81,82,83,84,85,88,86,87,89)
    
    colnames(gl) <- c("Date", "teamLoc", "teamName", "opptName", "teamRslt", 
                      "teamPTS", "opptPTS", "teamMin", "opptMin", 
                      "teamFGM", "teamFGA", "team3PM", "team3PA", "teamFTM",
                      "teamFTA", "teamORB", "teamDRB", "teamTRB", "teamAST",
                      "teamTOV", "teamSTL", "teamBLK", "teamPF", 
                      "opptFGM", "opptFGA", "oppt3PM", "oppt3PA", "opptFTM", 
                      "opptFTA", "opptORB", "opptDRB", "opptTRB", "opptAST", 
                      "opptTOV", "opptSTL", "opptBLK", "opptPF")
    
    # Filter for home/away 
    
    home <- gl %>%
        filter(teamLoc == "H")
    
    away <- gl %>%
        filter(teamLoc == "A")
    
    ##### Games count - Season #####
    
    gl <- gl %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### Games count - Away #####
    
    away <- away %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### Games count - Home #####
    
    home <- home %>%
        add_count(teamName, name = "teamGameCount") %>%
        add_count(opptName, name = "opptGameCount")
    
    ##### SEASON TOTALS #####
    
    season_grouped <- gl %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ##### SEASON ADVANCED STATS #####
    
    season_adv <- season_grouped
    
    season_adv$Poss <- with(season_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    season_adv$oPoss <- with(season_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    season_adv$Pace <- with(season_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    season_adv$oPace <- with(season_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    season_adv$ORtg <- with(season_adv, (teamPTS / Poss) * 100)
    season_adv$DRtg <- with(season_adv, (opptPTS / oPoss) * 100)
    
    season_adv$FG <- with(season_adv, teamFGM / teamFGA)
    season_adv$SR2 <- with(season_adv, (teamFGA - team3PA) / teamFGA)
    season_adv$FG3 <- with(season_adv, team3PM / team3PA)
    season_adv$SR3 <- with(season_adv, team3PA / teamFGA)
    season_adv$FT <- with(season_adv, teamFTM / teamFTA)
    season_adv$FTR <- with(season_adv, teamFTM / teamFGA)
    season_adv$ORB <- with(season_adv, teamORB / (teamORB + opptDRB))
    season_adv$DRB <- with(season_adv, teamDRB / (teamDRB + opptORB))
    season_adv$TRB <- with(season_adv, teamTRB / (teamTRB + opptTRB))
    season_adv$AST <- with(season_adv, teamAST / teamFGM)
    season_adv$TOV <- with(season_adv, teamTOV / Poss)
    season_adv$STL <- with(season_adv, teamSTL / oPoss)
    season_adv$BLK <- with(season_adv, teamBLK / (opptFGA - oppt3PA))
    season_adv$PF <- with(season_adv, teamPF / oPoss)
    season_adv$eFG <- with(season_adv, (teamFGM + .5 * team3PM) / teamFGA)
    season_adv$TS <- with(season_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    season_adv$oFG <- with(season_adv, opptFGM / opptFGA)
    season_adv$oSR2 <- with(season_adv, (opptFGA - oppt3PA) / opptFGA)
    season_adv$oFG3 <- with(season_adv, oppt3PM / oppt3PA)
    season_adv$oSR3 <- with(season_adv, oppt3PA / opptFGA)
    season_adv$oFT <- with(season_adv, opptFTM / opptFTA)
    season_adv$oFTR <- with(season_adv, opptFTM / opptFGA)
    season_adv$oORB <- with(season_adv, opptORB / (opptORB + teamDRB))
    season_adv$oDRB <- with(season_adv, opptDRB / (opptDRB + teamORB))
    season_adv$oTRB <- with(season_adv, opptTRB / (teamTRB + opptTRB))
    season_adv$oAST <- with(season_adv, opptAST / opptFGM)
    season_adv$oTOV <- with(season_adv, opptTOV / oPoss)
    season_adv$oSTL <- with(season_adv, opptSTL / Poss)
    season_adv$oBLK <- with(season_adv, opptBLK / (teamFGA - team3PA))
    season_adv$oPF <- with(season_adv, opptPF / Poss)
    season_adv$oeFG <- with(season_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    season_adv$oTS <- with(season_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    season_final <- season_adv %>%
        select(1,41:72,39,40,37)
    
    ### GROUPING HOME GAMES 
    
    home_grouped <- home %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ###### HOME ADVANCED STATS ######
    
    home_adv <- home_grouped
    
    home_adv$Poss <- with(home_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    home_adv$oPoss <- with(home_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    home_adv$Pace <- with(home_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    home_adv$oPace <- with(home_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    home_adv$ORtg <- with(home_adv, (teamPTS / Poss) * 100)
    home_adv$DRtg <- with(home_adv, (opptPTS / oPoss) * 100)
    
    home_adv$FG <- with(home_adv, teamFGM / teamFGA)
    home_adv$SR2 <- with(home_adv, (teamFGA - team3PA) / teamFGA)
    home_adv$FG3 <- with(home_adv, team3PM / team3PA)
    home_adv$SR3 <- with(home_adv, team3PA / teamFGA)
    home_adv$FT <- with(home_adv, teamFTM / teamFTA)
    home_adv$FTR <- with(home_adv, teamFTM / teamFGA)
    home_adv$ORB <- with(home_adv, teamORB / (teamORB + opptDRB))
    home_adv$DRB <- with(home_adv, teamDRB / (teamDRB + opptORB))
    home_adv$TRB <- with(home_adv, teamTRB / (teamTRB + opptTRB))
    home_adv$AST <- with(home_adv, teamAST / teamFGM)
    home_adv$TOV <- with(home_adv, teamTOV / Poss)
    home_adv$STL <- with(home_adv, teamSTL / oPoss)
    home_adv$BLK <- with(home_adv, teamBLK / (opptFGA - oppt3PA))
    home_adv$PF <- with(home_adv, teamPF / oPoss)
    home_adv$eFG <- with(home_adv, (teamFGM + .5 * team3PM) / teamFGA)
    home_adv$TS <- with(home_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    home_adv$oFG <- with(home_adv, opptFGM / opptFGA)
    home_adv$oSR2 <- with(home_adv, (opptFGA - oppt3PA) / opptFGA)
    home_adv$oFG3 <- with(home_adv, oppt3PM / oppt3PA)
    home_adv$oSR3 <- with(home_adv, oppt3PA / opptFGA)
    home_adv$oFT <- with(home_adv, opptFTM / opptFTA)
    home_adv$oFTR <- with(home_adv, opptFTM / opptFGA)
    home_adv$oORB <- with(home_adv, opptORB / (opptORB + teamDRB))
    home_adv$oDRB <- with(home_adv, opptDRB / (opptDRB + teamORB))
    home_adv$oTRB <- with(home_adv, opptTRB / (teamTRB + opptTRB))
    home_adv$oAST <- with(home_adv, opptAST / opptFGM)
    home_adv$oTOV <- with(home_adv, opptTOV / oPoss)
    home_adv$oSTL <- with(home_adv, opptSTL / Poss)
    home_adv$oBLK <- with(home_adv, opptBLK / (teamFGA - team3PA))
    home_adv$oPF <- with(home_adv, opptPF / Poss)
    home_adv$oeFG <- with(home_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    home_adv$oTS <- with(home_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    home_final <- home_adv %>%
        select(1,41:72,39,40,37)
    
    ### GROUPING AWAY GAMES
    
    away_grouped <- away %>%
        select(3,6:38) %>%
        group_by(teamName,teamGameCount) %>%
        summarise(across(c(teamPTS:opptPF), sum))
    
    ###### AWAY ADVANCED STATS #####
    
    away_adv <- away_grouped
    
    away_adv$Poss <- with(away_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    away_adv$oPoss <- with(away_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    away_adv$Pace <- with(away_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    away_adv$oPace <- with(away_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    away_adv$ORtg <- with(away_adv, (teamPTS / Poss) * 100)
    away_adv$DRtg <- with(away_adv, (opptPTS / oPoss) * 100)
    
    away_adv$FG <- with(away_adv, teamFGM / teamFGA)
    away_adv$SR2 <- with(away_adv, (teamFGA - team3PA) / teamFGA)
    away_adv$FG3 <- with(away_adv, team3PM / team3PA)
    away_adv$SR3 <- with(away_adv, team3PA / teamFGA)
    away_adv$FT <- with(away_adv, teamFTM / teamFTA)
    away_adv$FTR <- with(away_adv, teamFTM / teamFGA)
    away_adv$ORB <- with(away_adv, teamORB / (teamORB + opptDRB))
    away_adv$DRB <- with(away_adv, teamDRB / (teamDRB + opptORB))
    away_adv$TRB <- with(away_adv, teamTRB / (teamTRB + opptTRB))
    away_adv$AST <- with(away_adv, teamAST / teamFGM)
    away_adv$TOV <- with(away_adv, teamTOV / Poss)
    away_adv$STL <- with(away_adv, teamSTL / oPoss)
    away_adv$BLK <- with(away_adv, teamBLK / (opptFGA - oppt3PA))
    away_adv$PF <- with(away_adv, teamPF / oPoss)
    away_adv$eFG <- with(away_adv, (teamFGM + .5 * team3PM) / teamFGA)
    away_adv$TS <- with(away_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    away_adv$oFG <- with(away_adv, opptFGM / opptFGA)
    away_adv$oSR2 <- with(away_adv, (opptFGA - oppt3PA) / opptFGA)
    away_adv$oFG3 <- with(away_adv, oppt3PM / oppt3PA)
    away_adv$oSR3 <- with(away_adv, oppt3PA / opptFGA)
    away_adv$oFT <- with(away_adv, opptFTM / opptFTA)
    away_adv$oFTR <- with(away_adv, opptFTM / opptFGA)
    away_adv$oORB <- with(away_adv, opptORB / (opptORB + teamDRB))
    away_adv$oDRB <- with(away_adv, opptDRB / (opptDRB + teamORB))
    away_adv$oTRB <- with(away_adv, opptTRB / (teamTRB + opptTRB))
    away_adv$oAST <- with(away_adv, opptAST / opptFGM)
    away_adv$oTOV <- with(away_adv, opptTOV / oPoss)
    away_adv$oSTL <- with(away_adv, opptSTL / Poss)
    away_adv$oBLK <- with(away_adv, opptBLK / (teamFGA - team3PA))
    away_adv$oPF <- with(away_adv, opptPF / Poss)
    away_adv$oeFG <- with(away_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    away_adv$oTS <- with(away_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    away_final <- away_adv %>%
        select(1,41:72,39,40,37)
    
    ### HOME LEAGUE AVG STATS
    
    home_lg_avg <- home_final %>%
        group_by() %>%
        summarise(across(where(is.numeric), mean))
    
    home_lg_avg$Lg_Avg <- "Home"
    home_lg_avg <- home_lg_avg %>%
        select(36,1:35)
    
    ### AWAY LEAGUE AVG STATS
    
    away_lg_avg <- away_final %>%
        group_by() %>%
        summarise(across(where(is.numeric), mean))
    
    away_lg_avg$Lg_Avg <- "Away"
    away_lg_avg <- away_lg_avg %>%
        select(36,1:35)
    
    ### SEASON LEAGUE AVG STATS
    
    season_lg_avg <- season_final %>%
        group_by() %>%
        summarise(across(where(is.numeric), mean))
    
    season_lg_avg$Lg_Avg <- "Season"
    season_lg_avg <- season_lg_avg %>%
        select(36,1:35)
    
    # COMBINE LEAGUE AVERAGE TABLES
    
    league_avg <- bind_rows(season_lg_avg, home_lg_avg, away_lg_avg)
    
    ##### RAW SCHEDULE AND RESULTS ######
    
    raw_adv <- gl
    
    raw_adv$Poss <- with(raw_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
    raw_adv$oPoss <- with(raw_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
    raw_adv$Pace <- with(raw_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
    raw_adv$oPace <- with(raw_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
    raw_adv$ORtg <- with(raw_adv, (teamPTS / Poss) * 100)
    raw_adv$DRtg <- with(raw_adv, (opptPTS / oPoss) * 100)
    
    raw_adv$FG <- with(raw_adv, teamFGM / teamFGA)
    raw_adv$SR2 <- with(raw_adv, (teamFGA - team3PA) / teamFGA)
    raw_adv$FG3 <- with(raw_adv, team3PM / raw_adv$team3PA)
    raw_adv$SR3 <- with(raw_adv, team3PA / teamFGA)
    raw_adv$FT <- with(raw_adv, teamFTM / teamFTA)
    raw_adv$FTR <- with(raw_adv, teamFTM / teamFGA)
    raw_adv$ORB <- with(raw_adv, teamORB / (teamORB + opptDRB))
    raw_adv$DRB <- with(raw_adv, teamDRB / (teamDRB + opptORB))
    raw_adv$TRB <- with(raw_adv, teamTRB / (teamTRB + opptTRB))
    raw_adv$AST <- with(raw_adv, teamAST / teamFGM)
    raw_adv$TOV <- with(raw_adv, teamTOV / Poss)
    raw_adv$STL <- with(raw_adv, teamSTL / oPoss)
    raw_adv$BLK <- with(raw_adv, teamBLK / (opptFGA - oppt3PA))
    raw_adv$PF <- with(raw_adv, teamPF / oPoss)
    raw_adv$eFG <- with(raw_adv, (teamFGM + .5 * team3PM) / teamFGA)
    raw_adv$TS <- with(raw_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))
    
    raw_adv$oFG <- with(raw_adv, opptFGM / opptFGA)
    raw_adv$oSR2 <- with(raw_adv, (opptFGA - oppt3PA) / opptFGA)
    raw_adv$oFG3 <- with(raw_adv, oppt3PM / oppt3PA)
    raw_adv$oSR3 <- with(raw_adv, oppt3PA / opptFGA)
    raw_adv$oFT <- with(raw_adv, opptFTM / opptFTA)
    raw_adv$oFTR <- with(raw_adv, opptFTM / opptFGA)
    raw_adv$oORB <- with(raw_adv, opptORB / (opptORB + teamDRB))
    raw_adv$oDRB <- with(raw_adv, opptDRB / (opptDRB + teamORB))
    raw_adv$oTRB <- with(raw_adv, opptTRB / (teamTRB + opptTRB))
    raw_adv$oAST <- with(raw_adv, opptAST / opptFGM)
    raw_adv$oTOV <- with(raw_adv, opptTOV / oPoss)
    raw_adv$oSTL <- with(raw_adv, opptSTL / Poss)
    raw_adv$oBLK <- with(raw_adv, opptBLK / (teamFGA - team3PA))
    raw_adv$oPF <- with(raw_adv, opptPF / Poss)
    raw_adv$oeFG <- with(raw_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
    raw_adv$oTS <- with(raw_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))
    
    raw_final <- raw_adv %>%
        select(2:4,46:77,44,45,42)
    
    ######### ROUND 1 ADJUSTMENTS ########
    
    ## join each team's average stats on to raw_adj
    ## split by home/away then add averages
    ## bring file back together
    
    raw_adj_home <- raw_final %>%
        left_join(away_final, by = c("opptName" = "teamName")) %>%
        left_join(., home_final, by = c("teamName" = "teamName")) %>%
        filter(teamLoc == "H")
    
    raw_adj_away <- raw_final %>%
        left_join(home_final, by = c("opptName" = "teamName")) %>%
        left_join(., away_final, by = c("teamName" = "teamName")) %>%
        filter(teamLoc == "A")
    
    raw_adj <- bind_rows(raw_adj_home,raw_adj_away)
    
    # opptavg = .y, team avg. = no tail, team actual for that game = .x
    
    raw_adj$FG_adj <- (raw_adj$FG.x - (raw_adj$oFG.y - season_lg_avg$FG))
    raw_adj$SR2_adj <- (raw_adj$SR2.x - (raw_adj$oSR2.y - season_lg_avg$SR2))
    raw_adj$FG3_adj <- (raw_adj$FG3.x - (raw_adj$oFG3.y - season_lg_avg$FG3 ))
    raw_adj$SR3_adj <- (raw_adj$SR3.x - (raw_adj$oSR3.y - season_lg_avg$SR3))
    raw_adj$FT_adj <- (raw_adj$FT.x - (raw_adj$oFT.y - season_lg_avg$FT))
    raw_adj$FTR_adj <- (raw_adj$FTR.x - (raw_adj$oFTR.y - season_lg_avg$FTR))
    raw_adj$ORB_adj <- (raw_adj$ORB.x + (raw_adj$oDRB.y - season_lg_avg$DRB))
    raw_adj$DRB_adj <- (raw_adj$DRB.x - (raw_adj$oORB.y - season_lg_avg$ORB))
    raw_adj$TRB_adj <- (raw_adj$TRB.x + (raw_adj$oTRB.y - season_lg_avg$TRB))
    raw_adj$AST_adj <- (raw_adj$AST.x - (raw_adj$oAST.y - season_lg_avg$AST))
    raw_adj$TOV_adj <- (raw_adj$TOV.x - (raw_adj$oTOV.y - season_lg_avg$TOV))
    raw_adj$STL_adj <- (raw_adj$STL.x - (raw_adj$oSTL.y - season_lg_avg$STL))
    raw_adj$BLK_adj <- (raw_adj$BLK.x - (raw_adj$oBLK.y - season_lg_avg$BLK))
    raw_adj$PF_adj <- (raw_adj$PF.x - (raw_adj$oPF.y - season_lg_avg$PF)) 
    raw_adj$eFG_adj <- (raw_adj$eFG.x - (raw_adj$oeFG.y - season_lg_avg$eFG))
    raw_adj$TS_adj <- (raw_adj$TS.x - (raw_adj$oTS.y - season_lg_avg$TS))
    raw_adj$ExpPace <- (season_lg_avg$Pace + (raw_adj$Pace - season_lg_avg$Pace) + 
                            (raw_adj$Pace.y - season_lg_avg$Pace))
    raw_adj$PaceDiff <- (raw_adj$Pace.x - raw_adj$ExpPace)
    raw_adj$PaceR <- (raw_adj$Pace / (raw_adj$Pace + raw_adj$Pace.y))
    raw_adj$oPaceR <- (raw_adj$Pace.y / (raw_adj$Pace + raw_adj$Pace.y))
    raw_adj$Pace_adj <- (raw_adj$Pace + (raw_adj$PaceDiff * raw_adj$PaceR))
    raw_adj$ORtg_adj <- (raw_adj$ORtg.x - (raw_adj$DRtg.y - season_lg_avg$DRtg))
    raw_adj$DRtg_adj <- (raw_adj$DRtg.x - (raw_adj$ORtg.y - season_lg_avg$ORtg))
    
    raw_adj$oFG_adj <- (raw_adj$oFG.x - (raw_adj$FG - season_lg_avg$FG))
    raw_adj$oSR2_adj <- (raw_adj$oSR2.x - (raw_adj$SR2 - season_lg_avg$SR2))
    raw_adj$oFG3_adj <- (raw_adj$oFG3.x - (raw_adj$FG3 - season_lg_avg$FG3 ))
    raw_adj$oSR3_adj <- (raw_adj$oSR3.x - (raw_adj$SR3 - season_lg_avg$SR3))
    raw_adj$oFT_adj <- (raw_adj$oFT.x - (raw_adj$FT - season_lg_avg$FT))
    raw_adj$oFTR_adj <- (raw_adj$oFTR.x - (raw_adj$FTR - season_lg_avg$FTR))
    raw_adj$oORB_adj <- (raw_adj$oORB.x + (raw_adj$DRB - season_lg_avg$DRB))
    raw_adj$oDRB_adj <- (raw_adj$oDRB.x - (raw_adj$ORB - season_lg_avg$ORB))
    raw_adj$oTRB_adj <- (raw_adj$oTRB.x + (raw_adj$TRB - season_lg_avg$TRB))
    raw_adj$oAST_adj <- (raw_adj$oAST.x - (raw_adj$AST - season_lg_avg$AST))
    raw_adj$oTOV_adj <- (raw_adj$oTOV.x - (raw_adj$TOV - season_lg_avg$TOV))
    raw_adj$oSTL_adj <- (raw_adj$oSTL.x - (raw_adj$STL - season_lg_avg$STL))
    raw_adj$oBLK_adj <- (raw_adj$oBLK.x - (raw_adj$BLK - season_lg_avg$BLK))
    raw_adj$oPF_adj <- (raw_adj$oPF.x - (raw_adj$PF - season_lg_avg$PF)) 
    raw_adj$oeFG_adj <- (raw_adj$oeFG.x - (raw_adj$eFG - season_lg_avg$eFG))
    raw_adj$oTS_adj <- (raw_adj$oTS.x - (raw_adj$TS - season_lg_avg$TS))
    
    ### GROUP ROUND 1 ADJUSTMENTS
    
    season_adj_round_1 <- raw_adj %>%
        select(2,109:124,132:147,130,131,129) %>%
        group_by(teamName) %>%
        summarise(across(where(is.numeric), mean))
    
    home_adj_round_1 <- raw_adj %>%
        select(1,2,109:124,132:147,130,131,129) %>%
        filter(teamLoc == "H") %>%
        group_by(teamName) %>%
        summarise(across(where(is.numeric), mean))
    
    away_adj_round_1 <- raw_adj %>%
        select(1,2,109:124,132:147,130,131,129) %>%
        filter(teamLoc == "A") %>%
        group_by(teamName) %>%
        summarise(across(where(is.numeric), mean))
    
    ### Weighting Data frames ###
    
    season_uw <- raw_adj %>%
        select(2,109:124,132:147,130,131,129)
    
    home_uw <- raw_adj %>%
        select(1,2,109:124,132:147,130,131,129) %>%
        filter(teamLoc == "H")
    
    away_uw <- raw_adj %>%
        select(1,2,109:124,132:147,130,131,129) %>%
        filter(teamLoc == "A")
    
    ##### WEIGHTING - AWAY ####
    
    wt_holder_away <- data.frame()
    
    a <- 1
    g <- nrow(away_final)
    
    for (a in a:g) {
        
        act_id <- as.character(away_final[a,1])
        
        adj_gxg <- away_uw %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG_adj * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2_adj * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3_adj * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3_adj * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT_adj * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR_adj * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB_adj * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB_adj * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB_adj * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST_adj * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV_adj * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL_adj * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK_adj * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF_adj * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG_adj * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS_adj * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace_adj * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg_adj * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg_adj * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG_adj * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2_adj * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3_adj * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3_adj * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT_adj * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR_adj * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB_adj * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB_adj * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB_adj * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST_adj * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV_adj * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL_adj * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK_adj * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF_adj * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG_adj * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS_adj * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_away <- bind_rows(wt_holder_away,wt_df)
        
    }
    
    away_final_wt <- wt_holder_away
    
    colnames(away_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                 "AST","TOV","STL","BLK","PF","eFG","TS",
                                 "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                 "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                 "ORtg","DRtg","Pace")
    
    
    ##### WEIGHTING - HOME ####
    
    wt_holder_home <- data.frame()
    
    a <- 1
    g <- nrow(home_final)
    
    for (a in a:g) {
        
        act_id <- as.character(home_final[a,1])
        
        adj_gxg <- home_uw %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG_adj * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2_adj * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3_adj * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3_adj * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT_adj * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR_adj * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB_adj * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB_adj * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB_adj * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST_adj * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV_adj * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL_adj * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK_adj * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF_adj * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG_adj * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS_adj * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace_adj * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg_adj * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg_adj * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG_adj * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2_adj * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3_adj * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3_adj * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT_adj * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR_adj * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB_adj * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB_adj * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB_adj * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST_adj * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV_adj * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL_adj * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK_adj * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF_adj * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG_adj * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS_adj * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_home <- bind_rows(wt_holder_home,wt_df)
        
    }
    
    home_final_wt <- wt_holder_home
    
    colnames(home_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                 "AST","TOV","STL","BLK","PF","eFG","TS",
                                 "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                 "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                 "ORtg","DRtg","Pace")
    
    ##### WEIGHTING - SEASON ####
    
    wt_holder_season <- data.frame()
    
    a <- 1
    g <- nrow(season_final)
    
    for (a in a:g) {
        
        act_id <- as.character(season_final[a,1])
        
        adj_gxg <- season_uw %>%
            filter(teamName == act_id)
        
        ngames <- nrow(adj_gxg)
        
        if (ngames > 20) { weightmax <- 4 } else { weightmax <- 1 + ((ngames - 1) * .157894737)  }
        weightmin <- 1
        weightdist <- (weightmax - weightmin) / (ngames - 1)
        if (ngames < 2) { weightdist <- 0 }
        
        weightcurve <- matrix(0, nrow = ngames, ncol = 1)
        c <- 1
        i <- nrow(weightcurve)
        
        for (c in c:i) {
            
            weightcurve[c] <- weightmin + ((c - 1) * weightdist)
            
        }
        
        weight_sums <- sum(weightcurve)
        weight_avg <- mean(weightcurve)
        
        FG_wt <- (adj_gxg$FG_adj * weightcurve) / weight_sums
        SR2_wt <- (adj_gxg$SR2_adj * weightcurve) / weight_sums
        FG3_wt <- (adj_gxg$FG3_adj * weightcurve) / weight_sums
        SR3_wt <- (adj_gxg$SR3_adj * weightcurve) / weight_sums
        FT_wt <- (adj_gxg$FT_adj * weightcurve) / weight_sums
        FTR_wt <- (adj_gxg$FTR_adj * weightcurve) / weight_sums
        ORB_wt <- (adj_gxg$ORB_adj * weightcurve) / weight_sums
        DRB_wt <- (adj_gxg$DRB_adj * weightcurve) / weight_sums
        TRB_wt <- (adj_gxg$TRB_adj * weightcurve) / weight_sums
        AST_wt <- (adj_gxg$AST_adj * weightcurve) / weight_sums
        TOV_wt <- (adj_gxg$TOV_adj * weightcurve) / weight_sums
        STL_wt <- (adj_gxg$STL_adj * weightcurve) / weight_sums
        BLK_wt <- (adj_gxg$BLK_adj * weightcurve) / weight_sums
        PF_wt <- (adj_gxg$PF_adj * weightcurve) / weight_sums 
        eFG_wt <- (adj_gxg$eFG_adj * weightcurve) / weight_sums
        TS_wt <- (adj_gxg$TS_adj * weightcurve) / weight_sums
        Pace_wt <- (adj_gxg$Pace_adj * weightcurve) / weight_sums
        ORtg_wt <- (adj_gxg$ORtg_adj * weightcurve) / weight_sums
        DRtg_wt <- (adj_gxg$DRtg_adj * weightcurve) / weight_sums
        
        oFG_wt <- (adj_gxg$oFG_adj * weightcurve) / weight_sums
        oSR2_wt <- (adj_gxg$oSR2_adj * weightcurve) / weight_sums
        oFG3_wt <- (adj_gxg$oFG3_adj * weightcurve) / weight_sums
        oSR3_wt <- (adj_gxg$oSR3_adj * weightcurve) / weight_sums
        oFT_wt <- (adj_gxg$oFT_adj * weightcurve) / weight_sums
        oFTR_wt <- (adj_gxg$oFTR_adj * weightcurve) / weight_sums
        oORB_wt <- (adj_gxg$oORB_adj * weightcurve) / weight_sums
        oDRB_wt <- (adj_gxg$oDRB_adj * weightcurve) / weight_sums
        oTRB_wt <- (adj_gxg$oTRB_adj * weightcurve) / weight_sums
        oAST_wt <- (adj_gxg$oAST_adj * weightcurve) / weight_sums
        oTOV_wt <- (adj_gxg$oTOV_adj * weightcurve) / weight_sums
        oSTL_wt <- (adj_gxg$oSTL_adj * weightcurve) / weight_sums
        oBLK_wt <- (adj_gxg$oBLK_adj * weightcurve) / weight_sums
        oPF_wt <- (adj_gxg$oPF_adj * weightcurve) / weight_sums
        oeFG_wt <- (adj_gxg$oeFG_adj * weightcurve) / weight_sums
        oTS_wt <- (adj_gxg$oTS_adj * weightcurve) / weight_sums
        
        FG_wt <- sum(FG_wt)
        SR2_wt <- sum(SR2_wt)
        FG3_wt <- sum(FG3_wt)
        SR3_wt <- sum(SR3_wt)
        FT_wt <- sum(FT_wt)
        FTR_wt <- sum(FTR_wt)
        ORB_wt <- sum(ORB_wt)
        DRB_wt <- sum(DRB_wt)
        TRB_wt <- sum(TRB_wt)
        AST_wt <- sum(AST_wt)
        TOV_wt <- sum(TOV_wt)
        STL_wt <- sum(STL_wt)
        BLK_wt <- sum(BLK_wt)
        PF_wt <- sum(PF_wt)
        eFG_wt <- sum(eFG_wt)
        TS_wt <- sum(TS_wt)
        Pace_wt <- sum(Pace_wt)
        ORtg_wt <- sum(ORtg_wt)
        DRtg_wt <- sum(DRtg_wt)
        
        oFG_wt <- sum(oFG_wt)
        oSR2_wt <- sum(oSR2_wt)
        oFG3_wt <- sum(oFG3_wt)
        oSR3_wt <- sum(oSR3_wt)
        oFT_wt <- sum(oFT_wt)
        oFTR_wt <- sum(oFTR_wt)
        oORB_wt <- sum(oORB_wt)
        oDRB_wt <- sum(oDRB_wt)
        oTRB_wt <- sum(oTRB_wt)
        oAST_wt <- sum(oAST_wt)
        oTOV_wt <- sum(oTOV_wt)
        oSTL_wt <- sum(oSTL_wt)
        oBLK_wt <- sum(oBLK_wt)
        oPF_wt <- sum(oPF_wt)
        oeFG_wt <- sum(oeFG_wt)
        oTS_wt <- sum(oTS_wt)
        
        wt_df <- data.frame(act_id,FG_wt,SR2_wt,FG3_wt,SR3_wt,FT_wt,
                            FTR_wt,ORB_wt,DRB_wt,TRB_wt,AST_wt,TOV_wt,
                            STL_wt,BLK_wt,PF_wt,eFG_wt,TS_wt,oFG_wt,oSR2_wt,oFG3_wt,oSR3_wt,
                            oFT_wt,oFTR_wt,oORB_wt,oDRB_wt,oTRB_wt,oAST_wt,
                            oTOV_wt,oSTL_wt,oBLK_wt,oPF_wt,oeFG_wt,oTS_wt,
                            ORtg_wt,DRtg_wt,Pace_wt)
        
        wt_holder_season <- bind_rows(wt_holder_season,wt_df)
        
    }
    
    season_final_wt <- wt_holder_season
    
    colnames(season_final_wt) <- c("team","FG","SR2","FG3","SR3","FT","FTR","ORB","DRB","TRB",
                                   "AST","TOV","STL","BLK","PF","eFG","TS",
                                   "oFG","oSR2","oFG3","oSR3","oFT","oFTR","oORB","oDRB","oTRB",
                                   "oAST","oTOV","oSTL","oBLK","oPF","oeFG","oTS",
                                   "ORtg","DRtg","Pace")
    
    ##### Attach to game by game df ####
    
    ##### Scores Only ####
    
    scores <- game_range %>%
        select(1,5,13,54,8,90,45)
    
    colnames(scores) <- c("season", "date", "loc", "away", "home", "as", "hs")
    
    scores <- scores %>%
        filter(loc == "H" & date == gm_day_gxg)
    
    scores <- left_join(scores, away_final_wt, by = c("away" = "team")) %>%
        left_join(., home_final_wt, by = c("home" = "team"))
    
    colnames(scores)[8:77] <- c("FG_away","SR2_away","FG3_away","SR3_away","FT_away","FTR_away",
                                "ORB_away","DRB_away","TRB_away","AST_away","TOV_away","STL_away",
                                "BLK_away","PF_away","eFG_away","TS_away",
                                "oFG_away","oSR2_away","oFG3_away","oSR3_away","oFT_away","oFTR_away",
                                "oORB_away","oDRB_away","oTRB_away","oAST_away","oTOV_away","oSTL_away",
                                "oBLK_away","oPF_away","oeFG_away","oTS_away",
                                "ORtg_away","DRtg_away","Pace_away",
                                "FG_home","SR2_home","FG3_home","SR3_home","FT_home","FTR_home",
                                "ORB_home","DRB_home","TRB_home","AST_home","TOV_home","STL_home",
                                "BLK_home","PF_home","eFG_home","TS_home",
                                "oFG_home","oSR2_home","oFG3_home","oSR3_home","oFT_home","oFTR_home",
                                "oORB_home","oDRB_home","oTRB_home","oAST_home","oTOV_home","oSTL_home",
                                "oBLK_home","oPF_home","oeFG_home","oTS_home",
                                "ORtg_home","DRtg_home","Pace_home")
    
    final_db <- bind_rows(final_db,scores)
    
    print(paste(tail(final_db$date, n = 1), "Complete"))
    
}

final_db$margin <- final_db$as - final_db$hs
final_db$result <- if_else(final_db$as > final_db$hs, "W", "L")

final_db <- final_db %>%
    select(1:7,78,79,8:77) %>%
    arrange(date)




## connect to SQL database--------------------------------------------------------

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), 
                        "/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb.sqlite")

DBI::dbWriteTable(NBAdb, "GameLogsAdj", final_db, append = T)

DBI::dbDisconnect(NBAdb)

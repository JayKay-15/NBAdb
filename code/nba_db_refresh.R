### DB Refresh ------------------------------
library(tidyverse)
library(nbastatR)
library(RSQLite)
library(DBI)


Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

# pull game logs
game_logs(seasons = c(2024), result_types = c("team","players"))

# dataGameLogsTeam <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),
#                         "GameLogsTeam") %>%
#     collect() %>%
#     filter(yearSeason %in% c(2014:2023)) %>%
#     mutate(dateGame = as_date(dateGame, origin ="1970-01-01"))

# pull historical odds
odds_df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db"),
                      "odds_table") %>% 
    collect() %>%
    mutate(statr_id = paste0("00", statr_id))

# create box scores and additional stats
box_scores <- dataGameLogsTeam %>%
    mutate(
        outcomeGame = if_else(outcomeGame == "W", "win", "loss"),
        team_winner = factor(outcomeGame, levels = c("win","loss")),
        team_margin = plusminusTeam,
        team_loc = if_else(locationGame == "A", "away", "home"),
    ) %>%
    rename(
        game_id = idGame,
        season = yearSeason,
        season_type = typeSeason,
        game_date = dateGame,
        game_count = numberGameTeamSeason,
        team_id = idTeam,
        team_name = nameTeam,
        fg2m = fg2mTeam,
        fg2a = fg2aTeam,
        fg3m = fg3mTeam,
        fg3a = fg3aTeam,
        fgm = fgmTeam,
        fga = fgaTeam,
        ftm = ftmTeam,
        fta = ftaTeam,
        oreb = orebTeam,
        dreb = drebTeam,
        treb = trebTeam,
        ast = astTeam,
        tov = tovTeam,
        stl = stlTeam,
        blk = blkTeam,
        pf = pfTeam,
        team_score = ptsTeam,
        mins = minutesTeam,
        b2b_first = isB2BFirst,
        b2b_second = isB2BSecond
    ) %>%
    select(
        game_id, season, season_type, game_date, team_loc, team_id, team_name,
        team_winner, team_margin, b2b_first, b2b_second, game_count,
        mins, team_score,
        fg2m, fg2a, fg3m, fg3a, fgm, fga, ftm, fta,
        oreb, dreb, treb, ast, tov, stl, blk, pf, slugTeam, slugOpponent
    )

# create opponents box scores
box_scores_opp <- box_scores %>%
    select(-c(season:team_loc, mins)) %>%
    rename(opp_name = team_name,
           opp_id = team_id,
           opp_score = team_score) %>%
    rename_with(~paste0("opp_", .),
                -c(game_id, opp_name, opp_id, opp_score, slugTeam, slugOpponent))

# game by game box scores
box_scores_gbg <- box_scores %>%
    left_join(box_scores_opp, by = c("game_id" = "game_id",
                                     "slugTeam" = "slugOpponent")) %>%
    select(game_id:team_loc, team_id, opp_id, team_name, opp_name,
           team_winner, team_margin,
           b2b_first, opp_b2b_first, b2b_second, opp_b2b_second,
           game_count, opp_game_count, mins, team_score, opp_score,
           fg2m:pf, opp_fg2m:opp_pf)

# league averages
nba_league_avg <- box_scores_gbg %>%
    arrange(game_date, game_id) %>%
    group_by(season, team_loc) %>%
    summarize(across(mins:opp_pf, \(x) mean(x))) %>%
    ungroup() %>%
    mutate(
        poss = round(fga - oreb + tov + (0.44*fta), 0),
        opp_poss = round(opp_fga - opp_oreb + opp_tov + (0.44*opp_fta), 0),
        pace = round((poss + opp_poss)*48 / ((mins/5)*2), 0),
        off_rtg = round((team_score/poss)*100, 1),
        def_rtg = round((opp_score/opp_poss)*100, 1),
        net_rtg = off_rtg - def_rtg,
        fg2_pct = fg2m/fg2a,
        fg2_sr = (fga-fg2a)/fga,
        fg3_pct = fg3m/fg3a,
        fg3_sr = (fga-fg3a)/fga,
        fg_pct = fgm/fga,
        efg_pct = (fg3m*0.5 + fgm)/fga,
        ts_pct = team_score/(fga*2 + fta*0.44),
        ft_pct = ftm/fta,
        ftr = ftm/fga,
        oreb_pct = oreb/(oreb+opp_dreb),
        dreb_pct = dreb/(dreb+opp_oreb),
        treb_pct = treb/(treb+opp_treb),
        ast_pct = ast/fgm,
        tov_pct = tov/poss,
        ast_tov_pct = ast/tov,
        stl_pct = stl/opp_poss,
        blk_pct = blk/(opp_fga-opp_fg3a),
        pf_pct = pf/(poss+opp_poss),
        opp_fg2_pct = opp_fg2m/opp_fg2a,
        opp_fg2_sr = (opp_fga-opp_fg2a)/opp_fga,
        opp_fg3_pct = opp_fg3m/opp_fg3a,
        opp_fg3_sr = (opp_fga-opp_fg3a)/opp_fga,
        opp_fg_pct = opp_fgm/opp_fga,
        opp_efg_pct = (opp_fg3m*0.5 + opp_fgm)/opp_fga,
        opp_ts_pct = opp_score/(opp_fga*2 + opp_fta*0.44),
        opp_ft_pct = opp_ftm/opp_fta,
        opp_ftr = opp_ftm/opp_fga,
        opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
        opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
        opp_treb_pct = opp_treb/(treb+opp_treb),
        opp_ast_pct = opp_ast/opp_fgm,
        opp_tov_pct = opp_tov/opp_poss,
        opp_ast_tov_pct = opp_ast/opp_tov,
        opp_stl_pct = opp_stl/poss,
        opp_blk_pct = opp_blk/(fga-fg3a),
        opp_pf_pct = opp_pf/(poss+opp_poss)
    ) %>%
    select(
        season,team_loc,team_score,opp_score,
        fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,
        stl,stl_pct,blk,blk_pct,pf,pf_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,opp_pf,opp_pf_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    )

# straight average
nba_team_avg <- box_scores_gbg %>%
    arrange(game_date, game_id) %>%
    group_by(season, team_id, team_name, team_loc) %>%
    summarize(across(c(mins:opp_score, fg2m:opp_pf),
                     \(x) mean(x))) %>%
    ungroup() %>%
    mutate(
        poss = round(fga - oreb + tov + (0.44*fta), 0),
        opp_poss = round(opp_fga - opp_oreb + opp_tov + (0.44*opp_fta), 0),
        pace = round((poss + opp_poss)*48 / ((mins/5)*2), 0),
        off_rtg = round((team_score/poss)*100, 1),
        def_rtg = round((opp_score/opp_poss)*100, 1),
        net_rtg = off_rtg - def_rtg,
        fg2_pct = fg2m/fg2a,
        fg2_sr = (fga-fg2a)/fga,
        fg3_pct = fg3m/fg3a,
        fg3_sr = (fga-fg3a)/fga,
        fg_pct = fgm/fga,
        efg_pct = (fg3m*0.5 + fgm)/fga,
        ts_pct = team_score/(fga*2 + fta*0.44),
        ft_pct = ftm/fta,
        ftr = ftm/fga,
        oreb_pct = oreb/(oreb+opp_dreb),
        dreb_pct = dreb/(dreb+opp_oreb),
        treb_pct = treb/(treb+opp_treb),
        ast_pct = ast/fgm,
        tov_pct = tov/poss,
        ast_tov_pct = ast/tov,
        stl_pct = stl/opp_poss,
        blk_pct = blk/(opp_fga-opp_fg3a),
        pf_pct = pf/(poss+opp_poss),
        opp_fg2_pct = opp_fg2m/opp_fg2a,
        opp_fg2_sr = (opp_fga-opp_fg2a)/opp_fga,
        opp_fg3_pct = opp_fg3m/opp_fg3a,
        opp_fg3_sr = (opp_fga-opp_fg3a)/opp_fga,
        opp_fg_pct = opp_fgm/opp_fga,
        opp_efg_pct = (opp_fg3m*0.5 + opp_fgm)/opp_fga,
        opp_ts_pct = opp_score/(opp_fga*2 + opp_fta*0.44),
        opp_ft_pct = opp_ftm/opp_fta,
        opp_ftr = opp_ftm/opp_fga,
        opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
        opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
        opp_treb_pct = opp_treb/(treb+opp_treb),
        opp_ast_pct = opp_ast/opp_fgm,
        opp_tov_pct = opp_tov/opp_poss,
        opp_ast_tov_pct = opp_ast/opp_tov,
        opp_stl_pct = opp_stl/poss,
        opp_blk_pct = opp_blk/(fga-fg3a),
        opp_pf_pct = opp_pf/(poss+opp_poss),
    ) %>%
    select(
        season,team_id,team_name,team_loc,team_score,opp_score,
        fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,
        stl,stl_pct,blk,blk_pct,pf,pf_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,opp_pf,opp_pf_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    )

# weighted moving average
nba_wt_avg <- box_scores_gbg %>%
    arrange(game_date, game_id) %>%
    group_by(season, team_id, team_loc) %>%
    mutate(across(c(mins:opp_score, fg2m:opp_pf),
                  \(x) pracma::movavg(x, n = 10, type = 'e'))) %>%
    ungroup()

# base columns for final data frame 
nba_base <- box_scores_gbg %>%
    filter(team_loc == "away") %>%
    left_join(odds_df %>% select(-hoopr_id),
              by = c("game_id" = "statr_id")) %>%
    select(game_id:opp_score, away_spread:home_implied_prob)

# lagged away stats
nba_lag_away <- nba_wt_avg %>%
    filter(team_loc == "away") %>%
    select(game_id, team_id, mins:opp_score, fg2m:opp_pf) %>%
    mutate(
        poss = round(fga - oreb + tov + (0.44*fta), 0),
        opp_poss = round(opp_fga - opp_oreb + opp_tov + (0.44*opp_fta), 0),
        pace = round((poss + opp_poss)*48 / ((mins/5)*2), 0),
        off_rtg = round((team_score/poss)*100, 1),
        def_rtg = round((opp_score/opp_poss)*100, 1),
        net_rtg = off_rtg - def_rtg,
        fg2_pct = fg2m/fg2a,
        fg2_sr = (fga-fg2a)/fga,
        fg3_pct = fg3m/fg3a,
        fg3_sr = (fga-fg3a)/fga,
        fg_pct = fgm/fga,
        efg_pct = (fg3m*0.5 + fgm)/fga,
        ts_pct = team_score/(fga*2 + fta*0.44),
        ft_pct = ftm/fta,
        ftr = ftm/fga,
        oreb_pct = oreb/(oreb+opp_dreb),
        dreb_pct = dreb/(dreb+opp_oreb),
        treb_pct = treb/(treb+opp_treb),
        ast_pct = ast/fgm,
        tov_pct = tov/poss,
        ast_tov_pct = ast/tov,
        stl_pct = stl/opp_poss,
        blk_pct = blk/(opp_fga-opp_fg3a),
        pf_pct = pf/(poss+opp_poss),
        opp_fg2_pct = opp_fg2m/opp_fg2a,
        opp_fg2_sr = (opp_fga-opp_fg2a)/opp_fga,
        opp_fg3_pct = opp_fg3m/opp_fg3a,
        opp_fg3_sr = (opp_fga-opp_fg3a)/opp_fga,
        opp_fg_pct = opp_fgm/opp_fga,
        opp_efg_pct = (opp_fg3m*0.5 + opp_fgm)/opp_fga,
        opp_ts_pct = opp_score/(opp_fga*2 + opp_fta*0.44),
        opp_ft_pct = opp_ftm/opp_fta,
        opp_ftr = opp_ftm/opp_fga,
        opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
        opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
        opp_treb_pct = opp_treb/(treb+opp_treb),
        opp_ast_pct = opp_ast/opp_fgm,
        opp_tov_pct = opp_tov/opp_poss,
        opp_ast_tov_pct = opp_ast/opp_tov,
        opp_stl_pct = opp_stl/poss,
        opp_blk_pct = opp_blk/(fga-fg3a),
        opp_pf_pct = opp_pf/(poss+opp_poss),
    ) %>%
    select(
        game_id,team_id,fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,
        stl,stl_pct,blk,blk_pct,pf,pf_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,opp_pf,opp_pf_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    ) %>%
    rename_with(~paste0("away_", .), -c(game_id, team_id)) %>%
    group_by(team_id) %>%
    mutate(across(away_fg2m:away_net_rtg, \(x) lag(x, n = 1))) %>%
    ungroup()

# lagged home stats
nba_lag_home <- nba_wt_avg %>%
    filter(team_loc == "home") %>%
    select(game_id, team_id, mins:opp_score, fg2m:opp_pf) %>%
    mutate(
        poss = round(fga - oreb + tov + (0.44*fta), 0),
        opp_poss = round(opp_fga - opp_oreb + opp_tov + (0.44*opp_fta), 0),
        pace = round((poss + opp_poss)*48 / ((mins/5)*2), 0),
        off_rtg = round((team_score/poss)*100, 1),
        def_rtg = round((opp_score/opp_poss)*100, 1),
        net_rtg = off_rtg - def_rtg,
        fg2_pct = fg2m/fg2a,
        fg2_sr = (fga-fg2a)/fga,
        fg3_pct = fg3m/fg3a,
        fg3_sr = (fga-fg3a)/fga,
        fg_pct = fgm/fga,
        efg_pct = (fg3m*0.5 + fgm)/fga,
        ts_pct = team_score/(fga*2 + fta*0.44),
        ft_pct = ftm/fta,
        ftr = ftm/fga,
        oreb_pct = oreb/(oreb+opp_dreb),
        dreb_pct = dreb/(dreb+opp_oreb),
        treb_pct = treb/(treb+opp_treb),
        ast_pct = ast/fgm,
        tov_pct = tov/poss,
        ast_tov_pct = ast/tov,
        stl_pct = stl/opp_poss,
        blk_pct = blk/(opp_fga-opp_fg3a),
        pf_pct = pf/(poss+opp_poss),
        opp_fg2_pct = opp_fg2m/opp_fg2a,
        opp_fg2_sr = (opp_fga-opp_fg2a)/opp_fga,
        opp_fg3_pct = opp_fg3m/opp_fg3a,
        opp_fg3_sr = (opp_fga-opp_fg3a)/opp_fga,
        opp_fg_pct = opp_fgm/opp_fga,
        opp_efg_pct = (opp_fg3m*0.5 + opp_fgm)/opp_fga,
        opp_ts_pct = opp_score/(opp_fga*2 + opp_fta*0.44),
        opp_ft_pct = opp_ftm/opp_fta,
        opp_ftr = opp_ftm/opp_fga,
        opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
        opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
        opp_treb_pct = opp_treb/(treb+opp_treb),
        opp_ast_pct = opp_ast/opp_fgm,
        opp_tov_pct = opp_tov/opp_poss,
        opp_ast_tov_pct = opp_ast/opp_tov,
        opp_stl_pct = opp_stl/poss,
        opp_blk_pct = opp_blk/(fga-fg3a),
        opp_pf_pct = opp_pf/(poss+opp_poss),
    ) %>%
    select(
        game_id,team_id,fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,
        stl,stl_pct,blk,blk_pct,pf,pf_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,opp_pf,opp_pf_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    ) %>%
    rename_with(~paste0("home_", .), -c(game_id, team_id)) %>%
    group_by(team_id) %>%
    mutate(across(home_fg2m:home_net_rtg, \(x) lag(x, n = 1))) %>%
    ungroup()

# final data frame
nba_final <- nba_base %>%
    left_join(nba_lag_away, by = c("game_id" = "game_id",
                                   "team_id" = "team_id")) %>%
    left_join(nba_lag_home, by = c("game_id" = "game_id",
                                   "opp_id" = "team_id")) %>%
    na.exclude() %>%
    select(-c(game_count:mins)) %>%
    arrange(game_date, game_id)

# clear environment
rm(list=ls()[! ls() %in% c("box_scores_gbg", "nba_final",
                           "nba_league_avg", "nba_team_avg")])





## connect to SQL database-----------------------------------------------------

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")

DBI::dbWriteTable(NBAdb, "game_logs_adj", nba_final, overwrite = T)
DBI::dbWriteTable(NBAdb, "box_scores_gbg", box_scores_gbg, overwrite = T)
DBI::dbWriteTable(NBAdb, "nba_league_avg", nba_league_avg, overwrite = T)
DBI::dbWriteTable(NBAdb, "nba_team_avg", nba_team_avg, overwrite = T)

DBI::dbDisconnect(NBAdb)

## refresh odds ---------------------------------------------------------------
# update nba betting odds ----
nba_schedule <- as.data.frame(hoopR::load_nba_schedule(seasons = 2014:2023)) %>%
    filter(type_id == 1)
ids <- nba_schedule$id

# initialize an empty list to store odds data frames
odds_list <- list()

# define the function to collect odds for a single id
collect_odds <- function(i) {
    tryCatch({
        odds_list <- hoopR::espn_nba_betting(i)
        odds_picks <- odds_list$pickcenter
        
        odds <- odds_picks %>%
            mutate(game_id = i) %>%
            select(game_id, provider_name, spread, over_under,
                   away_team_odds_team_id, home_team_odds_team_id,
                   away_team_odds_favorite, home_team_odds_favorite,
                   away_team_odds_money_line, home_team_odds_money_line)
        
        print(paste0("Collected odds for ", i))
        
        return(odds)
    }, error = function(e) {
        cat("Error occurred for id:", i, "- Skipping.\n")
        return(NULL) # return NULL to indicate that there was an error
    })
}

# use lapply to collect odds for each id and store them in the odds_list
odds_list <- lapply(ids, collect_odds)

# filter out NULL elements which correspond to errors
odds_list <- odds_list[sapply(odds_list, function(x) !is.null(x))]

# combine the individual data frames into one using bind_rows
odds_df <- bind_rows(odds_list)

# save rds
# saveRDS(odds_df, "odds_v2")


odds_df <- readRDS("odds_v2")

odds_clean <- odds_df %>%
    filter(provider_name == "ESPN Bet") %>%
    pivot_longer(cols = c(away_team_odds_team_id, home_team_odds_team_id),
                 names_to = "id_type", values_to = "team_id") %>%
    pivot_longer(cols = c(away_team_odds_money_line, home_team_odds_money_line),
                 names_to = "line_type", values_to = "moneyline") %>%
    filter((id_type == "away_team_odds_team_id" &
                line_type == "away_team_odds_money_line") |
               (id_type == "home_team_odds_team_id" &
                    line_type == "home_team_odds_money_line")) %>%
    mutate(location = ifelse(grepl("away", id_type), "away", "home"),
           spread = if_else(location == "away", spread*-1, spread),
           team_id = as.numeric(team_id),
           implied_prob = if_else(moneyline < 0, -moneyline / (-moneyline + 100),
                                  100 / (moneyline + 100))) %>%
    select(game_id, location, team_id, spread, moneyline, over_under, implied_prob)


nba_schedule_odds <- nba_schedule %>%
    select(game_id, game_date, away_display_name, home_display_name) %>%
    pivot_longer(cols = away_display_name:home_display_name,
                 names_to = "team_loc",
                 values_to = "team_name") %>%
    mutate(team_loc = if_else(team_loc == "away_display_name", "away", "home")) %>%
    left_join(odds_clean, by = c("game_id" = "game_id",
                                 "team_loc" = "location")) %>%
    na.exclude() %>%
    arrange(game_date, game_id)

saveRDS(nba_schedule_odds, "odds_clean_v2")



odds_df <- readRDS("odds_final")
# DBI::dbWriteTable(NBAdb, "odds_table", odds_df, overwrite = T)

missing_games <- nba_schedule %>% filter(!id %in% odds_df$hoopr_id)
ids <- missing_games$id 


hoopR::espn_nba_betting(401584759)


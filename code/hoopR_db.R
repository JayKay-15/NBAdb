### hoopR DB ------------------------------
library(tidyverse)
library(lubridate)
library(nbastatR)
library(hoopR)
library(RSQLite)
library(DBI)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

# if (!requireNamespace('pacman', quietly = TRUE)){
#     install.packages('pacman')
# }
# pacman::p_load_current_gh("sportsdataverse/hoopR", dependencies = TRUE, update = TRUE)


# update hoopR db ----
hoopR::update_nba_db(
    dbdir = "../nba_sql_db/",
    dbname = "hoopR_db",
    tblname = "hoopR_nba_pbp",
    force_rebuild = TRUE,
    db_connection = NULL
)



# update nba cleanR stats
# cleanR - team box scores ----

# load box scores
box_scores <- hoopR::load_nba_team_box(seasons = c(2014:2023)) %>% # 2018 is broken
    filter(team_id <= 30 & season_type == 2) %>%
    arrange(desc(game_id))

# load schedule
nba_schedule <- load_nba_schedule(seasons = 2014:2023)
nba_schedule <- as.data.frame(load_nba_schedule(seasons = 2014:2023))

# create box score data frame
nba_team <- box_scores %>%
    left_join(nba_schedule %>% select(id, status_period),
              by = c("game_id" = "id")
    ) %>%
    mutate(
        team_winner = if_else(team_winner == TRUE, "win", "loss"),
        team_winner = factor(team_winner, levels = c("win","loss")),
        fast_break_points = as.numeric(fast_break_points),
        points_in_paint = as.numeric(points_in_paint),
        turnover_points = as.numeric(turnover_points),
        mins = if_else(status_period > 4, ((status_period - 4)*5*5)+240, 240),
        fg2m = field_goals_made - three_point_field_goals_made,
        fg2a = field_goals_attempted - three_point_field_goals_attempted
    ) %>%
    rename(
        team_name_short = team_name,
        team_loc = team_home_away,
        team_name = team_display_name,
        opp_id = opponent_team_id,
        opp_name = opponent_team_display_name,
        opp_score = opponent_team_score,
        fg3m = three_point_field_goals_made,
        fg3a = three_point_field_goals_attempted,
        fgm = field_goals_made,
        fga = field_goals_attempted,
        ftm = free_throws_made,
        fta = free_throws_attempted,
        oreb = offensive_rebounds,
        dreb = defensive_rebounds,
        treb = total_rebounds,
        ast = assists,
        tov = turnovers,
        stl = steals,
        blk = blocks,
        pf = fouls,
        fb_pts = fast_break_points,
        pip = points_in_paint,
        tov_pts = turnover_points
    ) %>%
    select(
        game_id, season, season_type, game_date, team_loc, team_id, team_name,
        opp_id, opp_name, team_winner, team_score, opp_score, mins,
        fg2m, fg2a, fg3m, fg3a, fgm, fga, ftm, fta,
        oreb, dreb, treb, ast, tov, stl, blk, pf, fb_pts, pip, tov_pts
    )

# create opponent data frame for defense stats
nba_opp <- nba_team %>%
    select(game_id, team_id, fg2m, fg2a, fg3m, fg3a, fgm, fga, ftm, fta,
           oreb, dreb, treb, ast, tov, stl, blk, pf, fb_pts, pip, tov_pts) %>%
    rename_with(~paste0("opp_", .), -c(game_id, team_id))

# join team and opponent data frames
nba_gxg <- nba_team %>%
    left_join(nba_opp, by = c("game_id" = "game_id", "opp_id" = "team_id"))

# calculate advanced stats
nba_adv <- nba_gxg %>%
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
        opp_blk_pct = opp_blk/(fga-fg3a)
    ) %>%
    select(
        game_id:mins,fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,tov_pts,
        stl,stl_pct,blk,blk_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_tov_pts,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    )

# league average
nba_league_avg <- nba_gxg %>%
    arrange(game_date, game_id) %>%
    group_by(season, team_loc) %>%
    summarize(across(team_score:opp_tov_pts, \(x) mean(x))) %>%
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
        opp_blk_pct = opp_blk/(fga-fg3a)
    ) %>%
    select(
        season,team_loc,fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
        fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
        dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,tov_pts,
        stl,stl_pct,blk,blk_pct,
        opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
        opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
        opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
        opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
        opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_tov_pts,opp_stl,opp_stl_pct,
        opp_blk,opp_blk_pct,
        poss,opp_poss,pace,off_rtg,def_rtg,net_rtg
    )



# model stats

# attach betting odds
odds_clean <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(),
                                        "../nba_sql_db/hoopR_db"), "nba_odds"
                         ) %>% collect()

odds_clean_wide <- odds_clean %>%
    pivot_wider(names_from = location,
                values_from = c(team_id, spread, moneyline, over_under, implied_prob),
                names_sep = "_") %>%
    rename(away_spread = spread_away,
           home_spread = spread_home,
           away_moneyline = moneyline_away,
           home_moneyline = moneyline_home,
           over_under = over_under_away,
           away_implied_prob = implied_prob_away,
           home_implied_prob = implied_prob_home) %>%
    select(-c(over_under_home,team_id_away,team_id_home))

odds_wpo <- odds_clean_wide %>%
    mutate(away_moneyline = odds.converter::odds.us2dec(away_moneyline),
           home_moneyline = odds.converter::odds.us2dec(home_moneyline)) %>%
    select(away_moneyline, home_moneyline)

odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')

odds_clean_wide <- odds_clean_wide %>%
    mutate(away_implied_prob = odds_wpo$probabilities[,1],
           home_implied_prob = odds_wpo$probabilities[,2])


# aggregate stats
nba_mov <- nba_gxg %>%
    arrange(game_date, game_id) %>%
    group_by(season, team_id, team_loc) %>%
    mutate(across(fg2m:opp_tov_pts, \(x) pracma::movavg(x, n = 10, type = 'e'))) %>%
    ungroup()

nba_model <- nba_mov %>%
    select(game_id:mins) %>%
    filter(team_loc == "away") %>%
    left_join(odds_clean_wide)

nba_model_away <- nba_mov %>%
    filter(team_loc == "away") %>%
    select(game_id, team_id, fg2m:opp_tov_pts) %>%
    rename_with(~paste0("away_", .), -c(game_id, team_id)) %>%
    group_by(team_id) %>%
    mutate(across(away_fg2m:away_opp_tov_pts, \(x) lag(x, n = 1))) %>%
    ungroup()

nba_model_home <- nba_mov %>%
    filter(team_loc == "home") %>%
    select(game_id, team_id, fg2m:opp_tov_pts) %>%
    rename_with(~paste0("home_", .), -c(game_id, team_id)) %>%
    group_by(team_id) %>%
    mutate(across(home_fg2m:home_opp_tov_pts, \(x) lag(x, n = 1))) %>%
    ungroup()

nba_model_final <- nba_model %>%
    left_join(nba_model_away, by = c("game_id" = "game_id", "team_id" = "team_id")) %>%
    left_join(nba_model_home, by = c("game_id" = "game_id", "opp_id" = "team_id")) %>%
    na.exclude()


hoop_db <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/hoopR_db")
DBI::dbListTables(hoop_db)

# DBI::dbWriteTable(hoop_db, "nba_schedule", nba_schedule, append = T)
# DBI::dbWriteTable(hoop_db, "nba_odds", odds_clean, append = T)
DBI::dbWriteTable(hoop_db, "nba_league_avg", nba_league_avg, overwrite = T)
DBI::dbWriteTable(hoop_db, "nba_model", nba_model, overwrite = T)

# DBI::dbRemoveTable(hoop_db, "hoop_db")
DBI::dbDisconnect(hoop_db)

# box <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/hoopR_db"),
#                   "hoopR_nba_pbp") %>% collect()

ugh <- nba_gxg %>%
    filter(season == 2018) # 2018 assist, steals, blocks, turnovers, pf - broken 



# cleanR - team box scores ----

# load box scores
box_scores <- hoopR::load_nba_player_box(seasons = c(2014:2023)) %>%
    filter(team_id <= 30 & season_type == 2) %>%
    arrange(desc(game_id))

nba_player <- box_scores %>%
    mutate(
        team_winner = if_else(team_winner == TRUE, "win", "loss"),
        team_winner = factor(team_winner, levels = c("win","loss")),
        fg2m = field_goals_made - three_point_field_goals_made,
        fg2a = field_goals_attempted - three_point_field_goals_attempted
    ) %>%
    rename(
        team_name_short = team_name,
        team_loc = home_away,
        team_name = team_display_name,
        player_name = athlete_display_name,
        player_id = athlete_id,
        opp_id = opponent_team_id,
        opp_name = opponent_team_display_name,
        opp_score = opponent_team_score,
        fg3m = three_point_field_goals_made,
        fg3a = three_point_field_goals_attempted,
        fgm = field_goals_made,
        fga = field_goals_attempted,
        ftm = free_throws_made,
        fta = free_throws_attempted,
        oreb = offensive_rebounds,
        dreb = defensive_rebounds,
        treb = rebounds,
        ast = assists,
        tov = turnovers,
        stl = steals,
        blk = blocks,
        pf = fouls,
        mins = minutes
    ) %>%
    select(
        game_id, season, season_type, game_date, team_loc, team_id, team_name,
        player_id, player_name, starter, did_not_play,
        opp_id, opp_name, team_winner, team_score, opp_score, mins,
        fg2m, fg2a, fg3m, fg3a, fgm, fga, ftm, fta,
        oreb, dreb, treb, ast, tov, stl, blk, pf, points
    )


nba_player_agg <- nba_player %>%
    group_by(season, team_name) %>%
    summarize(across(fg2m:points, \(x) sum(x, na.rm = T)))
    
nba_team_agg <- box_scores %>%
    group_by(season, team_display_name) %>%
    summarize(across(team_score, \(x) sum(x))) %>%
    left_join(nba_player_agg, by = c("season" = "season",
                                     "team_display_name" = "team_name")) %>%
    select(season, team_display_name, team_score, points) %>%
    mutate(check = team_score - points) %>%
    filter(check != 0)





# # bridge 

# hoop_schedule <- hoopR::load_nba_schedule(seasons = 2014:2023)
# statr_schedule <- nbastatR::game_logs(seasons = c(2014:2023),
#                                       result_types = "team")
# nba_schedule_odds <- readRDS("odds_clean_v2")
# 
# statr_schedule <- statr_schedule %>%
#     filter(locationGame == "A") %>%
#     select(idGame, dateGame, nameTeam) %>%
#     mutate(nameTeam = str_replace_all(nameTeam, 
#                                       "Los Angeles Clippers", "LA Clippers"))
# 
# bridge <- hoop_schedule %>%
#     select(id, date, away_display_name, home_display_name, season_type) %>%
#     mutate(date = ymd_hm(gsub("T|Z", " ", date), tz = "UTC"),
#            date = date(with_tz(date, tzone = "America/New_York"))) %>%
#     left_join(statr_schedule, by = c("date" = "dateGame",
#                                      "away_display_name" = "nameTeam")) 
# 
# 
# odds_clean_wide <- nba_schedule_odds %>%
#     select(game_id, team_loc, team_name, spread:over_under) %>%
#     pivot_wider(names_from = team_loc,
#                 values_from = c(team_name, spread, moneyline, over_under),
#                 names_sep = "_") %>%
#     rename(away_spread = spread_away,
#            home_spread = spread_home,
#            away_moneyline = moneyline_away,
#            home_moneyline = moneyline_home,
#            over_under = over_under_away) %>%
#     select(-c(over_under_home,team_name_away,team_name_home))
# 
# odds_wpo <- odds_clean_wide %>%
#     mutate(away_moneyline = odds.converter::odds.us2dec(away_moneyline),
#            home_moneyline = odds.converter::odds.us2dec(home_moneyline)) %>%
#     select(away_moneyline, home_moneyline)
# 
# odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')
# 
# odds_clean_bridge <- odds_clean_wide %>%
#     mutate(away_implied_prob = odds_wpo$probabilities[,1],
#            home_implied_prob = odds_wpo$probabilities[,2]) %>%
#     left_join(bridge %>% select(id, idGame), by = c("game_id" = "id")) %>%
#     select(idGame, game_id, away_spread:home_implied_prob) %>%
#     rename(hoopr_id = game_id,
#            statr_id = idGame)
# 
# # saveRDS(odds_clean_bridge, "odds_final")


# # long format
# odds_away <- odds_clean_bridge %>%
#     select(statr_id, away_spread, away_moneyline, over_under, away_implied_prob) %>%
#     mutate(team_loc = "away") %>%
#     rename(spread = away_spread,
#            moneyline = away_moneyline,
#            implied_prob = away_implied_prob)
# 
# odds_home <- odds_clean_bridge %>%
#     select(statr_id, home_spread, home_moneyline, over_under, home_implied_prob) %>%
#     mutate(team_loc = "home") %>%
#     rename(spread = home_spread,
#            moneyline = home_moneyline,
#            implied_prob = home_implied_prob)
# 
# odds_final <- bind_rows(odds_away, odds_home)



# # old select code before lag
# select(
#     game_id, team_id,
#     away_fg2m,away_fg2a,away_fg2_pct,away_fg2_sr,
#     away_fg3m,away_fg3a,away_fg3_pct,away_fg3_sr,
#     away_fgm,away_fga,away_fg_pct,away_efg_pct,away_ts_pct,
#     away_ftm,away_fta,away_ft_pct,away_ftr,
#     away_oreb,away_oreb_pct,away_dreb,away_dreb_pct,away_treb,away_treb_pct,
#     away_ast,away_ast_pct,away_tov,away_tov_pct,away_ast_tov_pct,
#     away_stl,away_stl_pct,away_blk,away_blk_pct,
#     away_opp_fg2m,away_opp_fg2a,away_opp_fg2_pct,away_opp_fg2_sr,
#     away_opp_fg3m,away_opp_fg3a,away_opp_fg3_pct,away_opp_fg3_sr,
#     away_opp_fgm,away_opp_fga,away_opp_fg_pct,away_opp_efg_pct,
#     away_opp_ts_pct,away_opp_ftm,away_opp_fta,away_opp_ft_pct,away_opp_ftr,
#     away_opp_oreb,away_opp_oreb_pct,
#     away_opp_dreb,away_opp_dreb_pct,away_opp_treb,away_opp_treb_pct,
#     away_opp_ast,away_opp_ast_pct,
#     away_opp_tov,away_opp_tov_pct,away_opp_ast_tov_pct,
#     away_opp_stl,away_opp_stl_pct,away_opp_blk,away_opp_blk_pct,
#     away_poss,away_opp_poss,away_pace,away_off_rtg,away_def_rtg,away_net_rtg
# ) %>%

# select(
#     game_id, team_id,
#     home_fg2m,home_fg2a,home_fg2_pct,home_fg2_sr,
#     home_fg3m,home_fg3a,home_fg3_pct,home_fg3_sr,
#     home_fgm,home_fga,home_fg_pct,home_efg_pct,home_ts_pct,
#     home_ftm,home_fta,home_ft_pct,home_ftr,
#     home_oreb,home_oreb_pct,home_dreb,home_dreb_pct,home_treb,home_treb_pct,
#     home_ast,home_ast_pct,home_tov,home_tov_pct,home_ast_tov_pct,
#     home_stl,home_stl_pct,home_blk,home_blk_pct,
#     home_opp_fg2m,home_opp_fg2a,home_opp_fg2_pct,home_opp_fg2_sr,
#     home_opp_fg3m,home_opp_fg3a,home_opp_fg3_pct,home_opp_fg3_sr,
#     home_opp_fgm,home_opp_fga,home_opp_fg_pct,home_opp_efg_pct,
#     home_opp_ts_pct,home_opp_ftm,home_opp_fta,home_opp_ft_pct,home_opp_ftr,
#     home_opp_oreb,home_opp_oreb_pct,
#     home_opp_dreb,home_opp_dreb_pct,home_opp_treb,home_opp_treb_pct,
#     home_opp_ast,home_opp_ast_pct,
#     home_opp_tov,home_opp_tov_pct,home_opp_ast_tov_pct,
#     home_opp_stl,home_opp_stl_pct,home_opp_blk,home_opp_blk_pct,
#     home_poss,home_opp_poss,home_pace,home_off_rtg,home_def_rtg,home_net_rtg
# ) %>%






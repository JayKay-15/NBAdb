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


hoop_db <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/hoopR_db")
DBI::dbListTables(hoop_db)

DBI::dbWriteTable(hoop_db, "nba_odds", df2, append = T)
# DBI::dbRemoveTable(hoop_db, "hoop_db")

DBI::dbDisconnect(hoop_db)


# Collect nba betting odds
box_scores <- hoopR::load_nba_team_box(seasons = c(2014:2022)) %>%
    filter(team_id <= 30) %>%
    arrange(desc(game_id))

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

df2 <- df %>%
    filter(provider_name == "teamrankings") %>%
    pivot_longer(cols = c(away_team_odds_team_id, home_team_odds_team_id),
                 names_to = "id_type", values_to = "team_id") %>%
    pivot_longer(cols = c(away_team_odds_money_line, home_team_odds_money_line),
                 names_to = "line_type", values_to = "moneyline") %>%
    filter((id_type == "away_team_odds_team_id" & line_type == "away_team_odds_money_line") |
               (id_type == "home_team_odds_team_id" & line_type == "home_team_odds_money_line")) %>%
    mutate(location = ifelse(grepl("away", id_type), "away", "home"),
           spread = if_else(location == "away", spread*-1, spread),
           team_id = as.numeric(team_id)) %>%
    select(game_id, location, team_id, spread, moneyline, over_under)


df2 <- df %>%
    mutate(implied_prob = if_else(moneyline < 0, -moneyline / (-moneyline + 100),
                                  100 / (moneyline + 100)))

saveRDS(df2, "odds_clean")
df <- readRDS("odds_full")



# cleanR

# Player box scores
box_scores <- hoopR::load_nba_player_box(seasons = c(2014:2022)) %>%
    filter(team_id <= 30) %>%
    arrange(desc(game_id))

schedule <- load_nba_schedule(seasons = 2014:2022)

df <- box_scores %>%
    filter(!is.na(minutes)) %>%
    select(game_id, season, season_type, game_date, home_away,
           team_id, team_display_name, opponent_team_id, opponent_team_display_name,
           team_winner, points, minutes,
           field_goals_made, field_goals_attempted,
           three_point_field_goals_made, three_point_field_goals_attempted,
           free_throws_made, free_throws_attempted,
           offensive_rebounds, defensive_rebounds, rebounds,
           assists, turnovers, steals, blocks, fouls) %>%
    group_by(game_id, season, season_type, game_date, home_away,
             team_id, team_display_name, opponent_team_id, opponent_team_display_name,
             team_winner) %>%
    summarise(across(minutes:fouls, \(x) sum(x, na.rm = TRUE))) %>%
    ungroup() %>%
    left_join(schedule %>% select(id, status_period),
              by = c("game_id" = "id")) %>%
    mutate(minutes = if_else(status_period > 4,
                             ((status_period - 4)*5*5)+240,
                             240)) %>%
    select(-status_period)



# Team box scores
box_scores <- hoopR::load_nba_team_box(seasons = c(2014:2022)) %>%
    filter(team_id <= 30) %>%
    arrange(desc(game_id))

schedule <- load_nba_schedule(seasons = 2014:2022)

df_team <- box_scores %>%
    mutate(fast_break_points = as.numeric(fast_break_points),
           points_in_paint = as.numeric(points_in_paint),
           turnover_points = as.numeric(turnover_points),
           fg2m = field_goals_made - three_point_field_goals_made,
           fg2a = field_goals_attempted - three_point_field_goals_attempted) %>%
    select(game_id, season, season_type, game_date, team_home_away,
           team_id, team_display_name, opponent_team_id, opponent_team_display_name,
           team_winner, team_score, opponent_team_score,
           fg2m, fg2a,
           three_point_field_goals_made, three_point_field_goals_attempted,
           field_goals_made, field_goals_attempted,
           free_throws_made, free_throws_attempted,
           offensive_rebounds, defensive_rebounds, total_rebounds,
           assists, turnovers, steals, blocks, fouls,
           fast_break_points, points_in_paint, turnover_points) %>%
    left_join(schedule %>% select(id, status_period),
              by = c("game_id" = "id")) %>%
    mutate(mins = if_else(status_period > 4,
                             ((status_period - 4)*5*5)+240,
                             240)) %>%
    select(-status_period) %>%
    rename(team_loc = team_home_away,
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
           tov_pts = turnover_points)

df_opp <- df_team %>%
    select(game_id, team_id,
           fg2m, fg2a, fg3m, fg3a, fgm, fga, ftm, fta, oreb, dreb, treb, ast, tov, stl,
           blk, pf, fb_pts, pip, tov_pts, mins) %>%
    rename_with(~paste0("opp_", .), -c(game_id, team_id))

df_full <- df_team %>%
    left_join(df_opp, by = c("game_id" = "game_id", "opp_id" = "team_id"))

df_agg <- df_full %>%
    arrange(game_date, game_id) %>%
    filter(season == 2022 & season_type == 2) %>%
    group_by(team_id, team_loc) %>%
    mutate(across(fg2m:opp_mins, \(x) pracma::movavg(x, n = 10, type = 'e')))

df_adv <- df_agg %>%
    mutate(
        poss = round(fga - oreb + tov + (0.44 * fta),0),
        opp_poss = round(opp_fga - opp_oreb + opp_tov + (0.44 * opp_fta),0),
        pace = round((poss + opp_poss) * 48 / ((mins/5) * 2),0),
        off_rtg = round((team_score/poss) * 100, 1),
        def_rtg = round((opp_score/opp_poss) * 100, 1),
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
    select(game_id:opp_score,fg2m,fg2a,fg2_pct,fg2_sr,fg3m,fg3a,fg3_pct,fg3_sr,
           fgm,fga,fg_pct,efg_pct,ts_pct,ftm,fta,ft_pct,ftr,oreb,oreb_pct,
           dreb,dreb_pct,treb,treb_pct,ast,ast_pct,tov,tov_pct,ast_tov_pct,tov_pts,
           stl,stl_pct,blk,blk_pct,poss,opp_poss,pace,off_rtg,def_rtg,
           opp_fg2m,opp_fg2a,opp_fg2_pct,opp_fg2_sr,opp_fg3m,opp_fg3a,
           opp_fg3_pct,opp_fg3_sr,opp_fgm,opp_fga,opp_fg_pct,opp_efg_pct,
           opp_ts_pct,opp_ftm,opp_fta,opp_ft_pct,opp_ftr,opp_oreb,opp_oreb_pct,
           opp_dreb,opp_dreb_pct,opp_treb,opp_treb_pct,opp_ast,opp_ast_pct,
           opp_tov,opp_tov_pct,opp_ast_tov_pct,opp_tov_pts,opp_stl,opp_stl_pct,
           opp_blk,opp_blk_pct)
    
    
# test if group by is working properly... filter home/away and run movavg
    
    
    
    
    
    
    



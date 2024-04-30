#### NBA DATABASE -----------------------------------------------------------------

library(tidyverse)
library(janitor)
library(data.table) 
library(RSQLite)
library(DBI)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)
options(scipen = 999999)

#### load database ----
NBAdb <- dbConnect(SQLite(), "../nba_sql_db/nba_db")

#### MAMBA DATABASE ####

#### nba schedule scraper function ----
scrape_nba_schedule <- function() {
    
    # Define the URL and headers
    url <- "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
    headers <- c(
        `Sec-Fetch-Site` = "same-site",
        `Accept` = "*/*",
        `Origin` = "https://www.nba.com",
        `Sec-Fetch-Dest` = "empty",
        `Accept-Language` = "en-US,en;q=0.9",
        `Sec-Fetch-Mode` = "cors",
        `Host` = "cdn.nba.com",
        `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        `Referer` = "https://www.nba.com/",
        `Accept-Encoding` = "gzip, deflate, br",
        `Connection` = "keep-alive"
    )
    
    # Send a GET request to the specified URL with the given headers
    res <- httr::GET(url = url, httr::add_headers(.headers = headers))
    data <- httr::content(res, "text")
    json_data <- jsonlite::fromJSON(data)
    
    games_list <- json_data[["leagueSchedule"]][["gameDates"]][["games"]]
    
    schedule_df <- list()
    
    for (game_info in games_list) {
        # Extract gameId and gameDateTimeEst
        gameId <- game_info$gameId
        gameDateEst <- game_info$gameDateEst
        gameSeriesText <- game_info$seriesText
        
        # Extract the identifiers for awayTeam and homeTeam
        awayTeamId <- game_info$awayTeam$id
        homeTeamId <- game_info$homeTeam$id
        
        # Extract the data frames
        away_schedule <- game_info$awayTeam
        home_schedule <- game_info$homeTeam
        
        # Add gameId, gameDateTimeEst, and identifiers to both data frames
        away_schedule$gameId <- gameId
        away_schedule$gameDateEst <- gameDateEst
        away_schedule$location <- "away"
        away_schedule$seriesText <- gameSeriesText
        
        home_schedule$gameId <- gameId
        home_schedule$gameDateEst <- gameDateEst
        home_schedule$location <- "home"
        home_schedule$seriesText <- gameSeriesText
        
        # Add the modified data frames to the list
        schedule_df <- c(schedule_df, list(away_schedule, home_schedule))
    }
    
    nba_schedule_base <- rbindlist(schedule_df) %>%
        clean_names() %>%
        filter(!series_text %in% c("Preseason","Championship","All-Star Game") & team_name != "") %>%
        mutate(game_date = as_date(game_date_est),
               team_name = paste0(team_city, " ", team_name)) %>%
        select(game_date, game_id, location, team_name) %>%
        arrange(game_date, game_id, location)
    
    nba_schedule <- nba_schedule_base %>%
        left_join(nba_schedule_base %>% select(game_id, team_name),
                  by = c("game_id" = "game_id"),
                  suffix = c("_team1", "_team2"),
                  relationship = "many-to-many") %>%
        rename(
            team_name = team_name_team1,
            opp_team_name = team_name_team2
        ) %>%
        filter(team_name != opp_team_name) %>%
        arrange(game_date, game_id, location) %>%
        group_by(team_name, location) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        group_by(opp_team_name, location) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            opp_is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            opp_is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_date:opp_team_name, is_b2b_first:opp_is_b2b_second)
    
    assign(x = "nba_schedule", nba_schedule, envir = .GlobalEnv)
}

scrape_nba_schedule()

#### scrape stats for mamba model ----
mamba_nba <- function(seasons) {
    
    generate_headers <- function() {
        headers <- c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type,
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Regular Season",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    headers <- generate_headers()
    all_data_list <- list()
    
    # Define available measure types
    available_measure_types <- c("Base", "Advanced", "Four Factors", "Misc", "Scoring")
    
    for (measure_type in available_measure_types) {
        all_data <- data.frame()
        
        for (year in seasons) {
            params <- generate_parameters(year, measure_type)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                             httr::add_headers(.headers = headers), query = params)
            data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
            column_names <- data$headers %>% as.character()
            dt <- rbindlist(data$rowSet) %>% setnames(column_names)
            
            all_data <- bind_rows(all_data, dt)
            
            print(paste0(params$Season, " ", params$MeasureType))
        }
        
        all_data_list[[measure_type]] <- all_data
    }
    
    # Data transformation code
    cleaned_team_list <- lapply(all_data_list, function(df) {
        df <- clean_names(df) %>%
            select(-starts_with(c("e_","opp_")), -ends_with(c("_rank","_flag")))
        return(df)
    })
    
    combined_df <- cleaned_team_list[[1]]
    for (i in 2:length(cleaned_team_list)) {
        df_to_join <- cleaned_team_list[[i]]
        existing_cols <- intersect(names(combined_df), names(df_to_join))
        existing_cols <- setdiff(existing_cols, c("game_id", "team_name"))
        df_to_join <- df_to_join %>% select(-any_of(existing_cols))
        combined_df <- left_join(combined_df, df_to_join, by = c("game_id", "team_name"))
    }
    
    team_all_stats <- combined_df %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:pct_uast_fgm)
    
    opp_all_stats <- team_all_stats %>%
        select(game_id, team_id, team_abbreviation, team_name,
               fgm:pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), -c(game_id)) %>%
        select(-opp_plus_minus)
    
    team_games <- team_all_stats %>%
        distinct(season_year, game_id, game_date, team_id, team_name) %>%
        group_by(season_year, team_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    opp_team_games <- team_games %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    raw_stats <- team_all_stats %>%
        inner_join(opp_all_stats, by = c("game_id"), relationship = "many-to-many") %>%
        filter(team_name != opp_team_name) %>%
        left_join(team_games, by = c("game_id", "team_name")) %>%
        left_join(opp_team_games, by = c("game_id", "opp_team_name")) %>%
        select(season_year, team_id, team_abbreviation, team_name,
               opp_team_id, opp_team_abbreviation, opp_team_name,
               game_id:min, pts, opp_pts, plus_minus,
               game_count_season, opp_game_count_season,
               is_b2b_first, is_b2b_second, opp_is_b2b_first, opp_is_b2b_second,
               fgm:opp_pct_uast_fgm) %>%
        arrange(game_date, game_id, location)
    
    # mamba_pace_adj <- raw_stats %>%
    #     select(season_year, game_date, pace) %>%
    #     group_by(game_date) %>%
    #     summarize(sum_pace = sum(pace),
    #               num_row = n()) %>%
    #     mutate(pace_adj = cumsum(sum_pace)/cumsum(num_row)) %>%
    #     ungroup() %>%
    #     select(game_date, pace_adj)
    
    stats_mov_avg <- raw_stats %>%
        mutate(pts_2pt_mr = round(pct_pts_2pt_mr*pts,0),
               ast_2pm = round(pct_ast_2pm*(fgm-fg3m),0),
               ast_3pm = round(pct_ast_3pm*fg3m,0),
               opp_pts_2pt_mr = round(opp_pct_pts_2pt_mr*opp_pts,0),
               opp_ast_2pm = round(opp_pct_ast_2pm*(opp_fgm-opp_fg3m),0),
               opp_ast_3pm = round(opp_pct_ast_3pm*opp_fg3m,0)
        ) %>%
        # left_join(mamba_pace_adj) %>%
        group_by(season_year, team_id, location) %>%
        mutate(across(c(fgm:opp_pct_uast_fgm),
                      \(x) pracma::movavg(x, n = 10, type = 'e'))
        # mutate(across(c(fgm:opp_pct_uast_fgm), ~ . * (pace_adj/pace)),
        #        across(c(fgm:opp_pct_uast_fgm),
        #               \(x) pracma::movavg(x, n = 10, type = 'e'))
        ) %>%
        ungroup() %>%
        mutate(fg_pct = fgm/fga,
               fg3_pct = fg3m/fg3a,
               ft_pct = ftm/fta,
               ast_pct = ast/fgm,
               oreb_pct = oreb/(oreb+opp_dreb),
               dreb_pct = dreb/(dreb+opp_oreb),
               reb_pct = reb/(reb+opp_reb),
               tm_tov_pct = tov/poss,
               efg_pct = (fgm+(0.5*fg3m))/fga,
               ts_pct = pts/(2*(fga+0.44*fta)),
               fta_rate = fta/fga,
               pct_fga_2pt = (fga-fg3a)/fga,
               pct_fga_3pt = 1-pct_fga_2pt,
               pct_pts_2pt = ((fgm-fg3m)*2)/pts,
               pct_pts_2pt_mr = pts_2pt_mr/pts,
               pct_pts_3pt = (fg3m*3)/pts,
               pct_pts_fb = pts_fb/pts,
               pct_pts_ft = ftm/pts,
               pct_pts_off_tov = pts_off_tov/pts,
               pct_pts_paint = pts_paint/pts,
               pct_ast_2pm = ast_2pm/(fgm-fg3m),
               pct_uast_2pm = 1-pct_ast_2pm,
               pct_ast_3pm = ast_3pm/fg3m,
               pct_uast_3pm = 1-pct_ast_3pm,
               pct_ast_fgm = ast_pct,
               pct_uast_fgm = 1-ast_pct,
               opp_fg_pct = opp_fgm/opp_fga,
               opp_fg3_pct = opp_fg3m/opp_fg3a,
               opp_ft_pct = opp_ftm/opp_fta,
               opp_ast_pct = opp_ast/opp_fgm,
               opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
               opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
               opp_reb_pct = opp_reb/(reb+opp_reb),
               opp_tm_tov_pct = opp_tov/opp_poss,
               opp_efg_pct = (opp_fgm+(0.5*opp_fg3m))/opp_fga,
               opp_ts_pct = opp_pts/(2*(opp_fga+0.44*opp_fta)),
               opp_fta_rate = opp_fta/opp_fga,
               opp_pct_fga_2pt = (opp_fga-opp_fg3a)/opp_fga,
               opp_pct_fga_3pt = 1-opp_pct_fga_2pt,
               opp_pct_pts_2pt = ((opp_fgm-opp_fg3m)*2)/opp_pts,
               opp_pct_pts_2pt_mr = opp_pts_2pt_mr/opp_pts,
               opp_pct_pts_3pt = (opp_fg3m*3)/opp_pts,
               opp_pct_pts_fb = opp_pts_fb/opp_pts,
               opp_pct_pts_ft = opp_ftm/opp_pts,
               opp_pct_pts_off_tov = opp_pts_off_tov/opp_pts,
               opp_pct_pts_paint = opp_pts_paint/opp_pts,
               opp_pct_ast_2pm = opp_ast_2pm/(opp_fgm-opp_fg3m),
               opp_pct_uast_2pm = 1-opp_pct_ast_2pm,
               opp_pct_ast_3pm = opp_ast_3pm/opp_fg3m,
               opp_pct_uast_3pm = 1-opp_pct_ast_3pm,
               opp_pct_ast_fgm = opp_ast_pct,
               opp_pct_uast_fgm = 1-opp_ast_pct) %>%
        select(-c(pts_2pt_mr, ast_2pm, ast_3pm,
                  opp_pts_2pt_mr, opp_ast_2pm, opp_ast_3pm))
    
    stats_lag <- stats_mov_avg %>%
        group_by(season_year, team_name, location) %>%
        mutate(across(fgm:opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
        ungroup() %>%
        na.exclude()
    
    base_stats_full <- stats_lag %>%
        select(season_year:opp_is_b2b_second)
    
    team_stats <- stats_lag %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("team_", .), fgm:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    opp_stats <- stats_lag %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), team_name:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    nba_final_full <- base_stats_full %>%
        left_join(team_stats, by = c("game_id" = "game_id",
                                     "team_name" = "team_name")) %>%
        left_join(opp_stats, by = c("game_id" = "game_id",
                                     "opp_team_name" = "opp_team_name")) %>%
        arrange(game_date, game_id, location) %>%
        na.exclude()
    
    
    base_stats <- stats_lag %>%
        filter(location == "away") %>%
        select(season_year:opp_is_b2b_second) %>%
        rename_with(~gsub("^opp_", "home_", .), starts_with("opp_")) %>%
        rename_with(~paste0("away_", .), c(team_id:team_name,
                                           pts, game_count_season, plus_minus,
                                           is_b2b_first, is_b2b_second))
    
    away_stats <- stats_lag %>%
        filter(location == "away") %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("away_", .), team_name:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    home_stats <- stats_lag %>%
        filter(location == "home") %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("home_", .), team_name:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    nba_final <- base_stats %>%
        left_join(away_stats, by = c("game_id" = "game_id",
                                     "away_team_name" = "away_team_name")) %>%
        left_join(home_stats, by = c("game_id" = "game_id",
                                     "home_team_name" = "home_team_name")) %>%
        arrange(game_date, game_id) %>%
        na.exclude()
    
    # game by game stats in mamba format
    assign(x = "mamba_raw_stats", raw_stats, envir = .GlobalEnv)
    
    # lagged stats in mamba format - long
    assign(x = "mamba_lag_long", nba_final_full, envir = .GlobalEnv)
    
    # lagged stats in mamba format - wide
    assign(x = "mamba_lag_wide", nba_final, envir = .GlobalEnv)
}

mamba_nba(2024)

mamba_db <- tbl(NBAdb, "mamba_lag_long") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))

missing_games <- mamba_lag_long %>%
    anti_join(mamba_db, by = "game_id")

DBI::dbWriteTable(NBAdb, "mamba_raw_stats", mamba_raw_stats, append = T)
DBI::dbWriteTable(NBAdb, "mamba_lag_long", mamba_lag_long, append = T)
DBI::dbWriteTable(NBAdb, "mamba_lag_wide", mamba_lag_wide, append = T)

df_check <- mamba_lag_long %>%
    group_by(season_year) %>% tally()

#### NBA odds scraper ----
nba_odds_db <- tbl(NBAdb, "nba_odds") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))

season_active <- tbl(NBAdb, "odds_season_active") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01")) %>%
    filter(game_date < Sys.Date())

missing_dates <- season_active %>%
    anti_join(nba_odds_db, by = "game_date")

scrape_nba_odds <- function(date_range) {
    
    odds_df <- data.frame()
    
    for (date_id in date_range) {
        
        date_id <- gsub("-", "", as_date(date_id))
        
        print(paste0("scraping ", date_id))
        
        headers = c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "application/json",
            `Origin` = "https://www.actionnetwork.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "api.actionnetwork.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
            `Referer` = "https://www.actionnetwork.com/nba/odds",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        
        res <- httr::GET(url = paste0("https://api.actionnetwork.com/web/v2/scoreboard/nba?bookIds=15,30,76,75,123,69,68,972,71,247,79&date=",date_id,"&periods=event"),
                         httr::add_headers(.headers=headers))
        
        json <- res$content %>%
            rawToChar() %>%
            jsonlite::fromJSON(simplifyVector = T)
        
        odds_game_away <- json$games %>%
            select(id, season, start_time, away_team_id, home_team_id) %>%
            rename(event_id = id,
                   team_id = away_team_id,
                   opp_team_id = home_team_id) %>%
            mutate(location = "away",
                   season = season + 1)
        
        odds_game_home <- json$games %>%
            select(id, season, start_time, away_team_id, home_team_id) %>%
            rename(event_id = id,
                   team_id = home_team_id,
                   opp_team_id = away_team_id) %>%
            mutate(location = "home",
                   season = season + 1)
        
        odds_game_info <- bind_rows(odds_game_away, odds_game_home) %>%
            mutate(
                start_time = as_date(format(with_tz(ymd_hms(start_time),
                                                    tzone = "America/Chicago"),
                                            "%Y-%m-%d"))
            ) %>%
            arrange(event_id, location)
        
        remove_standings <- function(team) {
            team[["standings"]] <- NULL
            return(team)
        }
        
        json[["games"]][["teams"]] <- lapply(json[["games"]][["teams"]], remove_standings)
        
        odds_team_info <- rbindlist(json[["games"]][["teams"]]) %>%
            select(id, full_name)
        
        all_spread <- rbindlist(json[["games"]][["markets"]][["69"]][["event"]][["spread"]])
        all_moneyline <- rbindlist(json[["games"]][["markets"]][["69"]][["event"]][["moneyline"]])
        all_over_under <- rbindlist(json[["games"]][["markets"]][["69"]][["event"]][["total"]])
        
        # all_spread <- rbindlist(json[["games"]][["markets"]][["69"]][["firstquarter"]][["spread"]])
        # all_moneyline <- rbindlist(json[["games"]][["markets"]][["69"]][["firstquarter"]][["moneyline"]])
        # all_over_under <- rbindlist(json[["games"]][["markets"]][["69"]][["firstquarter"]][["total"]])
        
        odds_spread <- all_spread %>%
            select(event_id, side, value) %>%
            rename(location = side,
                   spread = value)
        
        odds_spread <- odds_spread %>%
            left_join(odds_spread,
                      by = "event_id",
                      relationship = "many-to-many",
                      suffix = c("_team", "_opp")) %>%
            filter(location_team != location_opp) %>%
            select(-location_opp) %>%
            rename(location = location_team)
        
        odds_moneyline <- all_moneyline %>%
            select(event_id, side, odds) %>%
            rename(location = side,
                   moneyline = odds)
        
        odds_moneyline <- odds_moneyline %>%
            left_join(odds_moneyline,
                      by = "event_id",
                      relationship = "many-to-many",
                      suffix = c("_team", "_opp")) %>%
            filter(location_team != location_opp) %>%
            select(-location_opp) %>%
            rename(location = location_team)
        
        odds_totals <- all_over_under %>%
            select(event_id, value) %>%
            rename(over_under = value) %>%
            distinct()
        
        odds_clean <- odds_game_info %>%
            left_join(odds_team_info, by = c("team_id" = "id")) %>%
            left_join(odds_team_info, by = c("opp_team_id" = "id"),
                      suffix = c("_team", "_opp")) %>%
            left_join(odds_spread, by = c("event_id", "location")) %>%
            left_join(odds_moneyline, by = c("event_id", "location")) %>%
            left_join(odds_totals, by = c("event_id")) %>%
            select(-c(event_id, team_id, opp_team_id)) %>%
            rename(
                season_year = season,
                game_date = start_time,
                team_name = full_name_team,
                opp_team_name = full_name_opp,
                team_spread = spread_team,
                opp_spread = spread_opp,
                team_moneyline = moneyline_team,
                opp_moneyline = moneyline_opp
            )
        
        odds_wpo <- odds_clean %>%
            mutate(team_moneyline = odds.converter::odds.us2dec(team_moneyline),
                   opp_moneyline = odds.converter::odds.us2dec(opp_moneyline)) %>%
            select(team_moneyline, opp_moneyline)
        
        odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')
        
        odds_final <- odds_clean %>%
            mutate(team_implied_prob = odds_wpo$probabilities[,1],
                   opp_implied_prob = odds_wpo$probabilities[,2])
        
        odds_df <- bind_rows(odds_df, odds_final)
        
    }
    
    assign(x = "nba_odds", odds_df, envir = .GlobalEnv)
}

scrape_nba_odds(missing_dates$game_date)

#### attach odds to mamba ----
add_odds <- function() {
    
    mamba_raw_stats <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),
                           "mamba_raw_stats") %>%
        collect() %>%
        mutate(game_date = as_date(game_date, origin ="1970-01-01"))
    
    mamba_lag_long <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),
                          "mamba_lag_long") %>%
        collect() %>%
        mutate(game_date = as_date(game_date, origin ="1970-01-01"))
    
    mamba_lag_wide <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),
                          "mamba_lag_wide") %>%
        collect() %>%
        mutate(game_date = as_date(game_date, origin ="1970-01-01"))
    
    odds_db <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"),
                   "nba_odds") %>%
        collect() %>%
        mutate(game_date = as_date(game_date, origin ="1970-01-01"))
    
    mamba_raw_odds <- mamba_raw_stats %>%
        left_join(odds_db %>% select(game_date, team_name,
                                     team_spread:opp_implied_prob),
                  by = c("game_date", "team_name")) %>%
        select(season_year:opp_is_b2b_second,
               team_spread:opp_implied_prob,
               fgm:opp_pct_uast_fgm) %>%
        na.exclude()
    
    mamba_long_odds <- mamba_lag_long %>%
        left_join(odds_db %>% select(game_date, team_name,
                                     team_spread:opp_implied_prob),
                  by = c("game_date", "team_name")) %>%
        select(season_year:opp_is_b2b_second,
               team_spread:opp_implied_prob,
               team_fgm:opp_opp_pct_uast_fgm) %>%
        na.exclude()
    
    mamba_wide_odds <- mamba_lag_wide %>%
        left_join(odds_db %>% select(game_date, team_name,
                                     team_spread:opp_implied_prob),
                  by = c("game_date", "away_team_name" = "team_name")) %>%
        select(season_year:home_is_b2b_second,
               team_spread:opp_implied_prob,
               away_fgm:home_opp_pct_uast_fgm) %>%
        rename_with(~gsub("^team_", "away_", .), starts_with("team_")) %>%
        rename_with(~gsub("^opp_", "home_", .), starts_with("opp_")) %>%
        na.exclude()
    
    assign(x = "mamba_raw_odds", mamba_raw_odds, envir = .GlobalEnv)
    assign(x = "mamba_long_odds", mamba_long_odds, envir = .GlobalEnv)
    assign(x = "mamba_wide_odds", mamba_wide_odds, envir = .GlobalEnv)
    
}

add_odds()

## game details ----

#### scrape all nba scores ----
nba_scores <- function(seasons) {
    
    generate_headers <- function() {
        headers <- c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type,
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Regular Season",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    headers <- generate_headers()
    all_data <- data.frame()
    
    for (year in seasons) {
        params <- generate_parameters(year, "Base")
        
        res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                         httr::add_headers(.headers = headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()
        dt <- rbindlist(data$rowSet) %>% setnames(column_names) %>% clean_names()
        
        all_data <- bind_rows(all_data, dt)
        
        print(paste0(params$Season, " ", params$MeasureType))
    }
    
    team_all_stats <- all_data %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:min, pts, plus_minus)
    
    team_games <- team_all_stats %>%
        distinct(season_year, game_id, game_date, team_id, team_name) %>%
        group_by(season_year, team_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    opp_team_games <- team_games %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    opp_all_stats <- team_all_stats %>%
        select(game_id, team_id, team_abbreviation, team_name, pts) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    all_stats <- team_all_stats %>%
        inner_join(opp_all_stats, by = c("game_id"), relationship = "many-to-many") %>%
        filter(team_name != opp_team_name) %>%
        select(season_year, team_id, team_abbreviation, team_name,
               opp_team_id, opp_team_abbreviation, opp_team_name,
               game_id:min, pts, opp_pts, plus_minus)
    
    joined_stats <- all_stats %>%
        left_join(team_games, by = c("game_id", "team_name")) %>%
        left_join(opp_team_games, by = c("game_id", "opp_team_name")) %>%
        select(season_year:opp_pts, plus_minus, game_count_season, opp_game_count_season,
               is_b2b_first, is_b2b_second, opp_is_b2b_first, opp_is_b2b_second) %>%
        arrange(game_date, game_id, location) %>%
        mutate(min = if_else(min < 48, 48, min))
    
    assign(x = "all_nba_scores", joined_stats, envir = .GlobalEnv)
}

nba_scores(1997:2023)


#### NBA STATS DATABASE ####

## box scores team & box scores player ----

#### scrape all team stats ----
scrape_nba_team_stats <- function(seasons) {
    
    generate_headers <- function() {
        headers <- c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type,
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Regular Season",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    headers <- generate_headers()
    all_data_list <- list()
    
    # Define available measure types
    available_measure_types <- c("Base", "Advanced", "Four Factors", "Misc", "Scoring")
    
    for (measure_type in available_measure_types) {
        all_data <- data.frame()
        
        for (year in seasons) {
            params <- generate_parameters(year, measure_type)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                             httr::add_headers(.headers = headers), query = params)
            data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
            column_names <- data$headers %>% as.character()
            dt <- rbindlist(data$rowSet) %>% setnames(column_names)
            
            all_data <- bind_rows(all_data, dt)
            
            print(paste0(params$Season, " ", params$MeasureType))
        }
        
        all_data_list[[measure_type]] <- all_data
    }
    
    # Data transformation code
    cleaned_team_list <- lapply(all_data_list, function(df) {
        df <- clean_names(df) %>%
            select(-starts_with(c("e_")), -ends_with(c("_rank","_flag"))) %>%
            return(df)
    })
    
    combined_df <- cleaned_team_list[[1]]
    combined_df <- combined_df %>%
        rename_with(~paste0("base_", .), -c(season_year:min))
    
    for (i in 2:length(cleaned_team_list)) {
        df_to_join <- cleaned_team_list[[i]]
        existing_cols <- names(df_to_join %>% select(season_year:min))
        existing_cols <- setdiff(existing_cols, c("game_id", "team_name"))
        df_to_join <- df_to_join %>% select(-any_of(existing_cols))
        df_to_join <- df_to_join %>% rename_with(~paste0(names(cleaned_team_list)[[i]],"_", .),
                                                 -c(game_id, team_name)) %>% clean_names()
        combined_df <- left_join(combined_df, df_to_join,
                                 by = c("game_id", "team_name"))
    }
    
    team_all_stats <- combined_df %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:ncol(.))
    
    team_games <- team_all_stats %>%
        distinct(season_year, game_id, game_date, team_id, team_name) %>%
        group_by(season_year, team_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    nba_final <- team_all_stats %>%
        left_join(team_games, by = c("game_id", "team_name")) %>%
        arrange(game_date, game_id, location) %>%
        mutate(min = if_else(min < 48, 48, min))
    
    assign(x = "box_scores_team", nba_final, envir = .GlobalEnv)
}

scrape_nba_team_stats(seasons = c(1997:2023))

#### scrape all player stats ----
scrape_nba_player_stats <- function(seasons) {
    
    generate_headers <- function() {
        headers <- c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type, # "Base" "Advanced" "Usage" "Misc" "Scoring"
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Regular Season",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    headers <- generate_headers()
    all_data_list <- list()
    
    # Define available measure types
    available_measure_types <- c("Base", "Advanced", "Usage", "Misc", "Scoring")
    
    for (measure_type in available_measure_types) {
        all_data <- data.frame()
        
        for (year in seasons) {
            params <- generate_parameters(year, measure_type)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/playergamelogs",
                             httr::add_headers(.headers = headers), query = params)
            data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
            column_names <- data$headers %>% as.character()
            dt <- rbindlist(data$rowSet) %>% setnames(column_names)
            
            all_data <- bind_rows(all_data, dt)
            
            print(paste0(params$Season, " ", params$MeasureType))
        }
        
        all_data_list[[measure_type]] <- all_data
    }
    
    # Data transformation code
    cleaned_team_list <- lapply(all_data_list, function(df) {
        df <- clean_names(df) %>%
            select(-starts_with(c("e_", "sp_")),
                   -ends_with(c("_rank","_flag")),
                   -contains("fantasy")) %>%
            return(df)
    })
    
    combined_df <- cleaned_team_list[[1]]
    combined_df <- combined_df %>%
        rename_with(~paste0("base_", .), -c(season_year:min))
    
    for (i in 2:length(cleaned_team_list)) {
        df_to_join <- cleaned_team_list[[i]]
        existing_cols <- names(df_to_join %>% select(season_year:min))
        existing_cols <- setdiff(existing_cols, c("game_id", "player_name"))
        df_to_join <- df_to_join %>% select(-any_of(existing_cols))
        df_to_join <- df_to_join %>% rename_with(~paste0(names(cleaned_team_list)[[i]],"_", .),
                                                 -c(game_id, player_name)) %>% clean_names()
        combined_df <- left_join(combined_df, df_to_join,
                                 by = c("game_id", "player_name"))
    }
    
    player_all_stats <- combined_df %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:ncol(.))
    
    player_games <- player_all_stats %>%
        distinct(season_year, game_id, game_date, player_id, player_name) %>%
        group_by(season_year, player_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, player_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    nba_final <- player_all_stats %>%
        left_join(player_games, by = c("game_id", "player_name")) %>%
        arrange(game_date, game_id, location)
    
    assign(x = "box_scores_player", nba_final, envir = .GlobalEnv)
}

scrape_nba_player_stats(seasons = c(1997:2023))

## play by play & win probability & fanduel ----

#### scrape play by play & win probability ----
scrape_nba_play_by_play <- function(game_ids) {
    
    # loop that pauses 5 minutes between scrapes - 100 games at a time
    game_ids <- unique(game_ids)
    sleeper <- 300
    games_per_batch <- 100
    game_counter <- 0
    pbp_df <- data.frame()
    wp_df <- data.frame()
    
    
    for (game_id in game_ids) {
        
        if (game_counter >= games_per_batch) {
            Sys.sleep(sleeper)
            game_counter <- 0
        }
        
        game_counter <- game_counter + 1
        
        tryCatch({
            
            # print(game_id)
            
            headers <- c(
                `Host` = 'stats.nba.com',
                `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
                `Accept` = 'application/json, text/plain, */*',
                `Accept-Language` = 'en-US,en;q=0.5',
                `Accept-Encoding` = 'gzip, deflate, br',
                `x-nba-stats-origin` = 'stats',
                `x-nba-stats-token` = 'true',
                `Connection` = 'keep-alive',
                `Referer` = 'https =//stats.nba.com/',
                `Pragma` = 'no-cache',
                `Cache-Control` = 'no-cache'
            )
            
            res <- httr::GET(url = paste0("https://stats.nba.com/stats/playbyplayv2?GameID=",game_id,"&StartPeriod=0&EndPeriod=12"),
                             httr::add_headers(.headers=headers))
            
            json <- res$content %>% rawToChar() %>% jsonlite::fromJSON(simplifyVector = T)
            
            data <- json$resultSets$rowSet[[1]] %>%
                data.frame(stringsAsFactors = F) %>%
                as_tibble()
            
            json_names <- json$resultSets$headers[[1]]
            
            data <- data %>% set_names(json_names) %>% clean_names()
            
            pbp_df <- bind_rows(pbp_df, data)
            
            print(paste0("Getting Game ", game_id))
            
            
            
            headers <- c(
                `Host` = 'stats.nba.com',
                `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
                `Accept` = 'application/json, text/plain, */*',
                `Accept-Language` = 'en-US,en;q=0.5',
                `Accept-Encoding` = 'gzip, deflate, br',
                `x-nba-stats-origin` = 'stats',
                `x-nba-stats-token` = 'true',
                `Connection` = 'keep-alive',
                `Referer` = 'https =//stats.nba.com/',
                `Pragma` = 'no-cache',
                `Cache-Control` = 'no-cache'
            )
            
            res <- httr::GET(url = paste0("https://stats.nba.com/stats/winprobabilitypbp?GameID=",game_id,"&StartPeriod=0&EndPeriod=12&StartRange=0&EndRange=12&RangeType=1&Runtype=each%20second"),
                             httr::add_headers(.headers=headers))
            
            json <- res$content %>%
                rawToChar() %>%
                jsonlite::fromJSON(simplifyVector = T)
            
            data <- json$resultSets$rowSet[[1]] %>%
                data.frame(stringsAsFactors = F) %>%
                as_tibble()
            
            json_names <- json$resultSets$headers[[1]]
            
            df_metadata <- json$resultSets$rowSet[[2]] %>%
                data.frame(stringsAsFactors = F) %>%
                as_tibble()
            
            names_md <- json$resultSets$headers[[2]]
            
            df_metadata <- df_metadata %>%
                set_names(names_md) %>%
                clean_names() %>%
                mutate(game_date = game_date %>% lubridate::mdy()) %>%
                select(-dplyr::matches("pts"))
            
            names_md <- names(df_metadata)
            
            data <- data %>%
                set_names(json_names) %>%
                clean_names() %>%
                left_join(df_metadata, by = "game_id") %>%
                select(one_of(names_md), everything()) %>%
                suppressMessages()
            
            wp_df <- bind_rows(wp_df, data)
            
            print(paste0("Getting Game ", game_id))
            
        }, error = function(e) {
            # Print an error message
            cat("Error in processing game ", game_id, ": ",
                conditionMessage(e), "\n")
            
            return(NULL) # return NULL to indicate that there was an error
        })
        
    }
    
    assign(x = "play_by_play", pbp_df, envir = .GlobalEnv)
    assign(x = "win_probability", wp_df, envir = .GlobalEnv)
    
}

scrape_nba_play_by_play(game_ids)

#### scrape play by play ----
scrape_nba_play_by_play <- function(game_ids) {
    
    pbp_df <- data.frame()
    
    for (game_id in game_ids) {
        
        headers <- c(
            `Host` = 'stats.nba.com',
            `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
            `Accept` = 'application/json, text/plain, */*',
            `Accept-Language` = 'en-US,en;q=0.5',
            `Accept-Encoding` = 'gzip, deflate, br',
            `x-nba-stats-origin` = 'stats',
            `x-nba-stats-token` = 'true',
            `Connection` = 'keep-alive',
            `Referer` = 'https =//stats.nba.com/',
            `Pragma` = 'no-cache',
            `Cache-Control` = 'no-cache'
        )
        
        res <- httr::GET(url = paste0("https://stats.nba.com/stats/playbyplayv2?GameID=",game_id,"&StartPeriod=0&EndPeriod=12"),
                         httr::add_headers(.headers=headers))
        
        json <- res$content %>% rawToChar() %>% jsonlite::fromJSON(simplifyVector = T)
        
        data <- json$resultSets$rowSet[[1]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        json_names <- json$resultSets$headers[[1]]
        
        data <- data %>% set_names(json_names) %>% clean_names()
        
        pbp_df <- bind_rows(pbp_df, data)
        
        print(paste0("Getting Game ", game_id))
        
    }
    
    assign(x = "play_by_play", pbp_df, envir = .GlobalEnv)
}

scrape_nba_play_by_play(game_ids)

#### scrape win probability ----
scrape_nba_win_probability <- function(game_ids) {
    
    wp_df <- data.frame()
    
    for (game_id in game_ids) {
        
        headers <- c(
            `Host` = 'stats.nba.com',
            `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
            `Accept` = 'application/json, text/plain, */*',
            `Accept-Language` = 'en-US,en;q=0.5',
            `Accept-Encoding` = 'gzip, deflate, br',
            `x-nba-stats-origin` = 'stats',
            `x-nba-stats-token` = 'true',
            `Connection` = 'keep-alive',
            `Referer` = 'https =//stats.nba.com/',
            `Pragma` = 'no-cache',
            `Cache-Control` = 'no-cache'
        )
        
        res <- httr::GET(url = paste0("https://stats.nba.com/stats/winprobabilitypbp?GameID=",game_id,"&StartPeriod=0&EndPeriod=12&StartRange=0&EndRange=12&RangeType=1&Runtype=each%20second"),
                         httr::add_headers(.headers=headers))
        
        json <- res$content %>%
            rawToChar() %>%
            jsonlite::fromJSON(simplifyVector = T)
        
        data <- json$resultSets$rowSet[[1]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        json_names <- json$resultSets$headers[[1]]
        
        df_metadata <- json$resultSets$rowSet[[2]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        names_md <- json$resultSets$headers[[2]]
        
        df_metadata <- df_metadata %>%
            set_names(names_md) %>%
            clean_names() %>%
            mutate(game_date = game_date %>% lubridate::mdy()) %>%
            select(-dplyr::matches("pts"))
        
        names_md <- names(df_metadata)
        
        data <- data %>%
            set_names(json_names) %>%
            clean_names() %>%
            left_join(df_metadata, by = "game_id") %>%
            select(one_of(names_md), everything()) %>%
            suppressMessages()
        
        wp_df <- bind_rows(wp_df, data)
        
        print(paste0("Getting Game ", game_id))
        
    }
    
    assign(x = "win_probability", wp_df, envir = .GlobalEnv)
}

scrape_nba_win_probability(game_ids)

#### scrape fanduel ----
scrape_fanduel <- function(game_ids) {
    
    fd_df <- data.frame()
    
    for (game_id in game_ids) {
        
        headers <- c(
            `Host` = 'stats.nba.com',
            `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
            `Accept` = 'application/json, text/plain, */*',
            `Accept-Language` = 'en-US,en;q=0.5',
            `Accept-Encoding` = 'gzip, deflate, br',
            `x-nba-stats-origin` = 'stats',
            `x-nba-stats-token` = 'true',
            `Connection` = 'keep-alive',
            `Referer` = 'https =//stats.nba.com/',
            `Pragma` = 'no-cache',
            `Cache-Control` = 'no-cache'
        )
        
        res <- httr::GET(paste0(url = "https://stats.nba.com/stats/infographicfanduelplayer/?gameId=", game_id), httr::add_headers(.headers=headers))
        
        json <- res$content %>% rawToChar() %>% jsonlite::fromJSON(simplifyVector = T)
        
        data <- json$resultSets$rowSet[[1]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        json_names <- json$resultSets$headers[[1]]
        
        data <- data %>%
            set_names(json_names) %>%
            clean_names() %>%
            mutate(game_id = game_id)
        
        fd_df <- bind_rows(fd_df, data)
        
        print(paste0("Getting Game ", game_id))
        
    }
    
    assign(x = "fanduel", fd_df, envir = .GlobalEnv)
}

scrape_fanduel(game_ids)

## shots data ----

#### scrape shots data ----
scrape_nba_shots <- function(seasons) {
    
    generate_headers <- function() {
        headers = c(
            `Accept` = '*/*',
            `Origin` = 'https://www.nba.com',
            `Accept-Encoding` = 'gzip, deflate, br',
            `Host` = 'stats.nba.com',
            `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
            `Accept-Language` = 'en-US,en;q=0.9',
            `Referer` = 'https://www.nba.com/',
            `Connection` = 'keep-alive'
        )
        return(headers)
    }
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        
        params = list(
            `AheadBehind` = '',
            # `CFID` = '155',
            # `CFPARAMS` = '2021-22',
            `ClutchTime` = '',
            `Conference` = '',
            `ContextFilter` = '',
            `ContextMeasure` = 'FGA',
            `DateFrom` = '',
            `DateTo` = '',
            `Division` = '',
            `EndPeriod` = '10',
            `EndRange` = '28800',
            `GROUP_ID` = '',
            `GameEventID` = '',
            `GameID` = '',
            `GameSegment` = '',
            `GroupID` = '',
            `GroupMode` = '',
            `GroupQuantity` = '5',
            `LastNGames` = '0',
            `LeagueID` = '00',
            `Location` = '',
            `Month` = '0',
            `OnOff` = '',
            `OppPlayerID` = '',
            `OpponentTeamID` = '0',
            `Outcome` = '',
            `PORound` = '0',
            `Period` = '0',
            `PlayerID` = '0',
            `PlayerID1` = '',
            `PlayerID2` = '',
            `PlayerID3` = '',
            `PlayerID4` = '',
            `PlayerID5` = '',
            `PlayerPosition` = '',
            `PointDiff` = '',
            `Position` = '',
            `RangeType` = '0',
            `RookieYear` = '',
            `Season` = season,
            `SeasonSegment` = '',
            `SeasonType` = 'Regular Season',
            `ShotClockRange` = '',
            `StartPeriod` = '1',
            `StartRange` = '0',
            `StarterBench` = '',
            `TeamID` = '0',
            `VsConference` = '',
            `VsDivision` = '',
            `VsPlayerID1` = '',
            `VsPlayerID2` = '',
            `VsPlayerID3` = '',
            `VsPlayerID4` = '',
            `VsPlayerID5` = '',
            `VsTeamID` = ''
        )
        return(params)
    }
    
    headers <- generate_headers()
    
    shots <- data.frame()
    league_avg <- data.frame()
    
    for (year in seasons) {
        params <- generate_parameters(year)
        
        res <- httr::GET(url = 'https://stats.nba.com/stats/shotchartdetail',
                         httr::add_headers(.headers=headers), query = params)
        
        json <- res$content %>%
            rawToChar() %>%
            jsonlite::fromJSON(simplifyVector = T)
        
        shots_data <- json$resultSets$rowSet[[1]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        shots_names <- json$resultSets$headers[[1]]
        
        shots_data <- shots_data %>%
            set_names(shots_names) %>%
            clean_names() %>%
            mutate(season_year = year)
        
        shots <- bind_rows(shots, shots_data)
        
        
        league_data <- json$resultSets$rowSet[[2]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        league_names <- json$resultSets$headers[[2]]
        
        league_data <- league_data %>%
            set_names(league_names) %>%
            clean_names() %>%
            mutate(season_year = year)
        
        league_avg <- bind_rows(league_avg, league_data)
        
        print(paste0(params$Season, " Complete"))
        
    }
    
    assign(x = "all_shots", shots, envir = .GlobalEnv)
    assign(x = "league_avg", league_avg, envir = .GlobalEnv)
    
}

scrape_nba_shots(1997:2023)

#### process shots data ----
process_shots <- function(shots, league_avg) {
    
    shots_processed <- shots %>%
        mutate(loc_x = (as.numeric(loc_x) / 10),
               loc_y = (as.numeric(loc_y) / 10) + 5.25,
               shot_distance = as.numeric(shot_distance),
               shot_made_numeric = as.numeric(shot_made_flag),
               shot_made_flag = factor(shot_made_flag, levels = c("1", "0"),
                                       labels = c("made", "missed")),
               shot_attempted_flag = as.numeric(shot_attempted_flag),
               shot_value = if_else(shot_type == "3pt field goal", 3, 2),
               game_date = as_date(game_date, format = "%Y%m%d")
        )
    
    league_avg_processed <- league_avg %>%
        mutate(fga = as.numeric(fga),
               fgm = as.numeric(fgm),
               fg_pct = as.numeric(fg_pct),
               shot_value = if_else(shot_zone_basic %in% c("Above the Break 3", "Backcourt", "Left Corner 3", "Right Corner 3"), 3, 2)
        )
    
    assign(x = "all_shots_processed", shots_processed, envir = .GlobalEnv)
    assign(x = "league_avg_processed", league_avg_processed, envir = .GlobalEnv)
}

process_shots(shots, league_avg)

## dictionaries ----

#### scrape player dictionary ----
scrape_player_dictionary <- function() {
    
    headers = c(
        `Sec-Fetch-Site` = "same-site",
        `Accept` = "*/*",
        `Origin` = "https://www.nba.com",
        `Sec-Fetch-Dest` = "empty",
        `Accept-Language` = "en-US,en;q=0.9",
        `Sec-Fetch-Mode` = "cors",
        `Host` = "stats.nba.com",
        `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        `Referer` = "https://www.nba.com/",
        `Accept-Encoding` = "gzip, deflate, br",
        `Connection` = "keep-alive"
    )
    
    params = list(
        `College` = "",
        `Country` = "",
        `DraftPick` = "",
        `DraftRound` = "",
        `DraftYear` = "",
        `Height` = "",
        `Historical` = "1",
        `LeagueID` = "00",
        `Season` = "2023-24",
        `SeasonType` = "Regular Season",
        `TeamID` = "0",
        `Weight` = ""
    )
    
    res <- httr::GET(url = "https://stats.nba.com/stats/playerindex",
                     httr::add_headers(.headers=headers), query = params)
    
    data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
    
    column_names <- data$headers %>%
        as.character()
    
    dt <- rbindlist(data$rowSet) %>%
        setnames(column_names) %>%
        clean_names()
    
    assign(x = "player_dictionary", dt, envir = .GlobalEnv)
    
}

scrape_player_dictionary()

#### scrape team dictionary ----
scrape_team_dictionary <- function() {
    
    headers = c(
        `Sec-Fetch-Site` = "same-site",
        `Accept` = "*/*",
        `Origin` = "https://www.nba.com",
        `Sec-Fetch-Dest` = "empty",
        `Accept-Language` = "en-US,en;q=0.9",
        `Sec-Fetch-Mode` = "cors",
        `Host` = "stats.nba.com",
        `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        `Referer` = "https://www.nba.com/",
        `Accept-Encoding` = "gzip, deflate, br",
        `Connection` = "keep-alive"
    )
    
    params = list(
        `LeagueID` = "00",
        `Season` = "2023-24"
    )
    
    res <- httr::GET(url = "https://stats.nba.com/stats/franchisehistory",
                     httr::add_headers(.headers=headers), query = params)
    
    data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
    
    column_names <- data$headers %>%
        as.character()
    
    dt <- rbindlist(data$rowSet) %>%
        setnames(column_names) %>%
        clean_names()
    
    assign(x = "team_dictionary", dt, envir = .GlobalEnv)
    
}

scrape_team_dictionary()


#### basketball reference ---- re-code team and player stats
## player advanced & player totals & player per game ----
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

#### MAMBA ----
# DBI::dbWriteTable(NBAdb, "nba_schedule_current", nba_schedule, overwrite = T)       # automated ---
# DBI::dbWriteTable(NBAdb, "nba_odds", nba_odds, append = T)                          # automated --- 2014-2024
# DBI::dbWriteTable(NBAdb, "mamba_raw_stats", mamba_raw_stats, append = T)            # automated --- 2020-2024
# DBI::dbWriteTable(NBAdb, "mamba_lag_long", mamba_lag_long, append = T)              # automated --- 2020-2024
# DBI::dbWriteTable(NBAdb, "mamba_lag_wide", mamba_lag_wide, append = T)              # automated --- 2020-2024
# DBI::dbWriteTable(NBAdb, "mamba_raw_odds", mamba_raw_odds, append = T)              # automated --- 2020-2024
# DBI::dbWriteTable(NBAdb, "mamba_long_odds", mamba_long_odds, append = T)            # automated --- 2020-2024
# DBI::dbWriteTable(NBAdb, "mamba_wide_odds", mamba_wide_odds, append = T)            # automated --- 2020-2024

#### Team & Player Stats ----
# DBI::dbWriteTable(NBAdb, "box_scores_team", box_scores_team, append = T)            # automated --- 1997-2023
# DBI::dbWriteTable(NBAdb, "box_scores_player", box_scores_player, append = T)        # automated --- 1997-2023

#### Shots & Scores ----
# DBI::dbWriteTable(NBAdb, "all_shots", shots, append = T)                            # automated --- 1997-2023
# DBI::dbWriteTable(NBAdb, "league_avg", league_avg, append = T)                      # automated --- 1997-2023
# DBI::dbWriteTable(NBAdb, "all_shots_processed", shots_processed, append = T)        # automated --- 1997-2023
# DBI::dbWriteTable(NBAdb, "league_avg_processed", league_avg_processed, append = T)  # automated --- 1997-2023
# DBI::dbWriteTable(NBAdb, "all_nba_scores", all_nba_scores, append = T)              # automated --- 1997-2023

#### Play by Play & Win Probability ----
# DBI::dbWriteTable(NBAdb, "play_by_play", pbp_df, append = T)                        # automated --- 2019-2023
# DBI::dbWriteTable(NBAdb, "win_probability", wp_df, append = T)                      # automated --- 2019-2023

#### Dictionaries ----
# DBI::dbWriteTable(NBAdb, "player_dictionary", player_dictionary, overwrite = T)     # automated ---
# DBI::dbWriteTable(NBAdb, "team_dictionary", team_dictionary, overwrite = T)         # automated ---


dbDisconnect(NBAdb)

## query db
df <- tbl(NBAdb, "nba_odds") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))

game_ids <- df %>%
    filter(season_year >= 2020) %>%
    select(game_id) %>%
    distinct()

game_ids <- as.character(game_ids$game_id)


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


#### scrape all player matchups ----
scrape_matchups <- function(game_ids) {
    
    # loop that pauses 5 minutes between scrapes - 100 games at a time
    game_ids <- unique(game_ids)
    sleeper <- 90
    games_per_batch <- 100
    game_counter <- 0
    matchups_final <- data.frame()
    
    for (game_id in game_ids) {
        
        if (game_counter >= games_per_batch) {
            Sys.sleep(sleeper)
            game_counter <- 0
        }
        
        game_counter <- game_counter + 1
        
        tryCatch({
            
            print(paste0("Getting Game ", game_id))
            
            generate_headers <- function() {
                headers = c(
                    `Sec-Fetch-Site` = "same-site",
                    `Accept` = "*/*",
                    `Origin` = "https://www.nba.com",
                    `Sec-Fetch-Dest` = "empty",
                    `Accept-Language` = "en-US,en;q=0.9",
                    `Sec-Fetch-Mode` = "cors",
                    `Host` = "stats.nba.com",
                    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
                    `Referer` = "https://www.nba.com/",
                    `Accept-Encoding` = "gzip, deflate, br",
                    `Connection` = "keep-alive"
                )
                return(headers)
            }
            
            generate_params  <- function(game_id) {
                params = list(
                    `GameID` = as.character(game_id), # 0022300345
                    `LeagueID` = "00",
                    `endPeriod` = "0",
                    `endRange` = "28800",
                    `rangeType` = "0",
                    `startPeriod` = "0",
                    `startRange` = "0"
                )
                return(params)
            }
            
            headers <- generate_headers()
            params <- generate_params(game_id)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/boxscorematchupsv3",
                             httr::add_headers(.headers=headers), query = params)
            
            json <- res$content %>%
                rawToChar() %>%
                jsonlite::fromJSON(simplifyVector = T)
            
            matchups_away <- list(json$boxScoreMatchups$awayTeam) %>%
                purrr::map_df(flatten) %>%
                unnest(cols = c(matchups), names_sep = "_") %>%
                unnest(cols = c(matchups_statistics), names_sep = "_") %>%
                rename_with(~gsub("^matchups_statistics_", "", .x),
                            starts_with("matchups_statistics_")) %>%
                clean_names() %>%
                mutate(game_id = json$boxScoreMatchups$gameId,
                       location = "away")
            
            matchups_home <- list(json$boxScoreMatchups$homeTeam) %>%
                purrr::map_df(flatten) %>%
                unnest(cols = c(matchups), names_sep = "_") %>%
                unnest(cols = c(matchups_statistics), names_sep = "_") %>%
                rename_with(~gsub("^matchups_statistics_", "", .x),
                            starts_with("matchups_statistics_")) %>%
                clean_names() %>%
                mutate(game_id = json$boxScoreMatchups$gameId,
                       location = "home")
            
            matchups_df <- bind_rows(matchups_away, matchups_home) %>%
                select(game_id, location, team_id:shooting_fouls)
            
            matchups_final <- bind_rows(matchups_final, matchups_df)
            
        }, error = function(e) {
            # Print an error message
            cat("Error in processing game ", game_id, ": ",
                conditionMessage(e), "\n")
            
            return(NULL) # return NULL to indicate that there was an error
        })
        
    }
    
    assign(x = "matchups_final", matchups_final, envir = .GlobalEnv)
    
}

scrape_matchups(game_ids)








#### playoffs odds
headers = c(
    `Accept` = "application/json",
    `Origin` = "https://www.actionnetwork.com",
    `Referer` = "https://www.actionnetwork.com/nba/odds",
    `Sec-Fetch-Dest` = "empty",
    `Sec-Fetch-Mode` = "cors",
    `Sec-Fetch-Site` = "same-site",
    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15"
)

res <- httr::GET(url = "https://api.actionnetwork.com/web/v2/scoreboard/nba?bookIds=15,30,76,75,123,69,68,972,71,247,79&date=20240416&periods=event", httr::add_headers(.headers=headers))





#### scrape playoff teams ----
scrape_standings <- function(seasons) {
    
    all_standings <- data.frame()
    
    generate_headers <- function() {
        headers = c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    headers <- generate_headers()
    
    generate_parameters <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params = list(
            `GroupBy` = "conf",
            `LeagueID` = "00",
            `Season` = season,
            `SeasonType` = "Regular Season",
            `Section` = "overall"
        )
        return(params)
    }
    
    for (year in seasons) {
        params <- generate_parameters(year)
        
        res <- httr::GET(url = "https://stats.nba.com/stats/leaguestandingsv3",
                         httr::add_headers(.headers=headers), query = params)
        
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()
        dt <- rbindlist(data$rowSet) %>%
            setnames(column_names) %>%
            clean_names() %>%
            mutate(season_year = year)
        
        all_standings <- bind_rows(all_standings, dt)
        
        print(paste0(params$Season))
    }
    
    assign(x = "all_standings", all_standings, envir = .GlobalEnv)
    
}

scrape_standings(2023:2024)

playoff_teams <- all_standings %>%
    filter((playoff_rank >= 1 & playoff_rank <= 8) | clinched_playoff_birth == 1)



#### scrape stats for mamba model - playoffs ----
mamba_nba_post <- function(seasons) {
    
    generate_headers <- function() {
        headers <- c(
            `Sec-Fetch-Site` = "same-site",
            `Accept` = "*/*",
            `Origin` = "https://www.nba.com",
            `Sec-Fetch-Dest` = "empty",
            `Accept-Language` = "en-US,en;q=0.9",
            `Sec-Fetch-Mode` = "cors",
            `Host` = "stats.nba.com",
            `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            `Referer` = "https://www.nba.com/",
            `Accept-Encoding` = "gzip, deflate, br",
            `Connection` = "keep-alive"
        )
        return(headers)
    }
    
    generate_parameters_reg <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type,
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Regular Season",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    generate_parameters_post <- function(year, measure_type) {
        year <- (year - 1)
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        params <- list(
            `DateFrom` = "",
            `DateTo` = "",
            `GameSegment` = "",
            `ISTRound` = "",
            `LastNGames` = "0",
            `LeagueID` = "00",
            `Location` = "",
            `MeasureType` = measure_type,
            `Month` = "0",
            `OpponentTeamID` = "0",
            `Outcome` = "",
            `PORound` = "0",
            `PaceAdjust` = "N",
            `PerMode` = "Totals",
            `Period` = "0",
            `PlusMinus` = "N",
            `Rank` = "N",
            `Season` = season,
            `SeasonSegment` = "",
            `SeasonType` = "Playoffs",
            `ShotClockRange` = "",
            `VsConference` = "",
            `VsDivision` = ""
        )
        return(params)
    }
    
    headers <- generate_headers()
    
    # Regular Season
    all_data_list_reg <- list()
    
    # Define available measure types
    available_measure_types <- c("Base", "Advanced", "Four Factors", "Misc", "Scoring")
    
    for (measure_type in available_measure_types) {
        all_data <- data.frame()
        
        for (year in seasons) {
            params <- generate_parameters_reg(year, measure_type)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                             httr::add_headers(.headers = headers), query = params)
            data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
            column_names <- data$headers %>% as.character()
            dt <- rbindlist(data$rowSet) %>% setnames(column_names)
            
            all_data <- bind_rows(all_data, dt)
            
            print(paste0(params$Season, " ", params$MeasureType))
        }
        
        all_data_list_reg[[measure_type]] <- all_data
    }
    
    # Data transformation code
    cleaned_team_list_reg <- lapply(all_data_list_reg, function(df) {
        df <- clean_names(df) %>%
            select(-starts_with(c("e_","opp_")), -ends_with(c("_rank","_flag")))
        return(df)
    })
    
    combined_df_reg <- cleaned_team_list_reg[[1]]
    for (i in 2:length(cleaned_team_list_reg)) {
        df_to_join <- cleaned_team_list_reg[[i]]
        existing_cols <- intersect(names(combined_df_reg), names(df_to_join))
        existing_cols <- setdiff(existing_cols, c("game_id", "team_name"))
        df_to_join <- df_to_join %>% select(-any_of(existing_cols))
        combined_df_reg <- left_join(combined_df_reg, df_to_join, by = c("game_id", "team_name"))
    }
    
    team_all_stats_reg <- combined_df_reg %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:pct_uast_fgm)
    
    opp_all_stats_reg <- team_all_stats_reg %>%
        select(game_id, team_id, team_abbreviation, team_name,
               fgm:pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), -c(game_id)) %>%
        select(-opp_plus_minus)
    
    team_games_reg <- team_all_stats_reg %>%
        distinct(season_year, game_id, game_date, team_id, team_name) %>%
        group_by(season_year, team_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    opp_team_games_reg <- team_games_reg %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    raw_stats_reg <- team_all_stats_reg %>%
        inner_join(opp_all_stats_reg, by = c("game_id"), relationship = "many-to-many") %>%
        filter(team_name != opp_team_name) %>%
        left_join(team_games_reg, by = c("game_id", "team_name")) %>%
        left_join(opp_team_games_reg, by = c("game_id", "opp_team_name")) %>%
        mutate(season_type = "regular") %>%
        select(season_type,
               season_year, team_id, team_abbreviation, team_name,
               opp_team_id, opp_team_abbreviation, opp_team_name,
               game_id:min, pts, opp_pts, plus_minus,
               game_count_season, opp_game_count_season,
               is_b2b_first, is_b2b_second, opp_is_b2b_first, opp_is_b2b_second,
               fgm:opp_pct_uast_fgm) %>%
        arrange(game_date, game_id, location) %>%
        semi_join(playoff_teams,
                  by = c("season_year", "team_id")) %>%
        semi_join(playoff_teams,
                  by = c("season_year", "opp_team_id" = "team_id"))

    stats_vs_post <- raw_stats_reg %>%
        group_by(season_year, team_id, team_name) %>%
        summarize(wins_vs_post = sum(wl == "W"),
                  games_vs_post = n(),
                  win_pct_vs_post = wins_vs_post/games_vs_post) %>%
        ungroup() %>%
        left_join(playoff_teams %>% select(season_year, team_id, playoff_rank),
                  by = c("season_year", "team_id")) %>%
        select(season_year, team_id, wins_vs_post:playoff_rank)
    
    
    
    
    # Playoffs
    all_data_list_post <- list()
    
    # Define available measure types
    available_measure_types <- c("Base", "Advanced", "Four Factors", "Misc", "Scoring")
    
    for (measure_type in available_measure_types) {
        all_data <- data.frame()
        
        for (year in seasons) {
            params <- generate_parameters_post(year, measure_type)
            
            res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                             httr::add_headers(.headers = headers), query = params)
            data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
            column_names <- data$headers %>% as.character()
            dt <- rbindlist(data$rowSet) %>% setnames(column_names)
            
            all_data <- bind_rows(all_data, dt)
            
            print(paste0(params$Season, " ", params$MeasureType))
        }
        
        all_data_list_post[[measure_type]] <- all_data
    }
    
    # Data transformation code
    cleaned_team_list_post <- lapply(all_data_list_post, function(df) {
        df <- clean_names(df) %>%
            select(-starts_with(c("e_","opp_")), -ends_with(c("_rank","_flag")))
        return(df)
    })
    
    combined_df_post <- cleaned_team_list_post[[1]]
    for (i in 2:length(cleaned_team_list_post)) {
        df_to_join <- cleaned_team_list_post[[i]]
        existing_cols <- intersect(names(combined_df_post), names(df_to_join))
        existing_cols <- setdiff(existing_cols, c("game_id", "team_name"))
        df_to_join <- df_to_join %>% select(-any_of(existing_cols))
        combined_df_post <- left_join(combined_df_post, df_to_join, by = c("game_id", "team_name"))
    }
    
    team_all_stats_post <- combined_df_post %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1) %>%
        select(season_year:matchup, location, wl:pct_uast_fgm)
    
    opp_all_stats_post <- team_all_stats_post %>%
        select(game_id, team_id, team_abbreviation, team_name,
               fgm:pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), -c(game_id)) %>%
        select(-opp_plus_minus)
    
    team_games_post <- team_all_stats_post %>%
        distinct(season_year, game_id, game_date, team_id, team_name) %>%
        group_by(season_year, team_id) %>%
        mutate(
            game_count_season = 1:n(),
            days_rest_team = ifelse(game_count_season > 1,
                                    (game_date - lag(game_date) - 1),
                                    120),
            days_next_game_team =
                ifelse(game_count_season < 82,
                       ((
                           lead(game_date) - game_date
                       ) - 1),
                       120),
            days_next_game_team = days_next_game_team %>% as.numeric(),
            days_rest_team = days_rest_team %>% as.numeric(),
            is_b2b = if_else(days_next_game_team == 0 |
                                 days_rest_team == 0, TRUE, FALSE),
            is_b2b_first = if_else(lead(days_next_game_team) == 0, TRUE, FALSE),
            is_b2b_second = if_else(lag(days_next_game_team) == 0, TRUE, FALSE)
        ) %>%
        ungroup() %>%
        mutate_if(is.logical, ~ ifelse(is.na(.), FALSE, .)) %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season)
    
    opp_team_games_post <- team_games_post %>%
        select(game_id, team_name,
               is_b2b_first, is_b2b_second, game_count_season) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    raw_stats_post <- team_all_stats_post %>%
        inner_join(opp_all_stats_post, by = c("game_id"), relationship = "many-to-many") %>%
        filter(team_name != opp_team_name) %>%
        left_join(team_games_post, by = c("game_id", "team_name")) %>%
        left_join(opp_team_games_post, by = c("game_id", "opp_team_name")) %>%
        mutate(season_type = "post") %>%
        select(season_type,
               season_year, team_id, team_abbreviation, team_name,
               opp_team_id, opp_team_abbreviation, opp_team_name,
               game_id:min, pts, opp_pts, plus_minus,
               game_count_season, opp_game_count_season,
               is_b2b_first, is_b2b_second, opp_is_b2b_first, opp_is_b2b_second,
               fgm:opp_pct_uast_fgm) %>%
        arrange(game_date, game_id, location)
    

        
    
    raw_stats <- raw_stats_reg %>%
        bind_rows(raw_stats_post)
    
    stats_mov_avg <- raw_stats %>%
        mutate(pts_2pt_mr = round(pct_pts_2pt_mr*pts,0),
               ast_2pm = round(pct_ast_2pm*(fgm-fg3m),0),
               ast_3pm = round(pct_ast_3pm*fg3m,0),
               opp_pts_2pt_mr = round(opp_pct_pts_2pt_mr*opp_pts,0),
               opp_ast_2pm = round(opp_pct_ast_2pm*(opp_fgm-opp_fg3m),0),
               opp_ast_3pm = round(opp_pct_ast_3pm*opp_fg3m,0)
        ) %>%
        group_by(season_year, team_id, location) %>%
        mutate(across(c(fgm:opp_pct_uast_fgm),
                      \(x) pracma::movavg(x, n = 10, type = 'e'))
        ) %>%
        ungroup() %>%
        mutate(fg_pct = fgm/fga,
               fg3_pct = fg3m/fg3a,
               ft_pct = ftm/fta,
               ast_pct = ast/fgm,
               oreb_pct = oreb/(oreb+opp_dreb),
               dreb_pct = dreb/(dreb+opp_oreb),
               reb_pct = reb/(reb+opp_reb),
               tm_tov_pct = tov/poss,
               efg_pct = (fgm+(0.5*fg3m))/fga,
               ts_pct = pts/(2*(fga+0.44*fta)),
               fta_rate = fta/fga,
               pct_fga_2pt = (fga-fg3a)/fga,
               pct_fga_3pt = 1-pct_fga_2pt,
               pct_pts_2pt = ((fgm-fg3m)*2)/pts,
               pct_pts_2pt_mr = pts_2pt_mr/pts,
               pct_pts_3pt = (fg3m*3)/pts,
               pct_pts_fb = pts_fb/pts,
               pct_pts_ft = ftm/pts,
               pct_pts_off_tov = pts_off_tov/pts,
               pct_pts_paint = pts_paint/pts,
               pct_ast_2pm = ast_2pm/(fgm-fg3m),
               pct_uast_2pm = 1-pct_ast_2pm,
               pct_ast_3pm = ast_3pm/fg3m,
               pct_uast_3pm = 1-pct_ast_3pm,
               pct_ast_fgm = ast_pct,
               pct_uast_fgm = 1-ast_pct,
               opp_fg_pct = opp_fgm/opp_fga,
               opp_fg3_pct = opp_fg3m/opp_fg3a,
               opp_ft_pct = opp_ftm/opp_fta,
               opp_ast_pct = opp_ast/opp_fgm,
               opp_oreb_pct = opp_oreb/(opp_oreb+dreb),
               opp_dreb_pct = opp_dreb/(opp_dreb+oreb),
               opp_reb_pct = opp_reb/(reb+opp_reb),
               opp_tm_tov_pct = opp_tov/opp_poss,
               opp_efg_pct = (opp_fgm+(0.5*opp_fg3m))/opp_fga,
               opp_ts_pct = opp_pts/(2*(opp_fga+0.44*opp_fta)),
               opp_fta_rate = opp_fta/opp_fga,
               opp_pct_fga_2pt = (opp_fga-opp_fg3a)/opp_fga,
               opp_pct_fga_3pt = 1-opp_pct_fga_2pt,
               opp_pct_pts_2pt = ((opp_fgm-opp_fg3m)*2)/opp_pts,
               opp_pct_pts_2pt_mr = opp_pts_2pt_mr/opp_pts,
               opp_pct_pts_3pt = (opp_fg3m*3)/opp_pts,
               opp_pct_pts_fb = opp_pts_fb/opp_pts,
               opp_pct_pts_ft = opp_ftm/opp_pts,
               opp_pct_pts_off_tov = opp_pts_off_tov/opp_pts,
               opp_pct_pts_paint = opp_pts_paint/opp_pts,
               opp_pct_ast_2pm = opp_ast_2pm/(opp_fgm-opp_fg3m),
               opp_pct_uast_2pm = 1-opp_pct_ast_2pm,
               opp_pct_ast_3pm = opp_ast_3pm/opp_fg3m,
               opp_pct_uast_3pm = 1-opp_pct_ast_3pm,
               opp_pct_ast_fgm = opp_ast_pct,
               opp_pct_uast_fgm = 1-opp_ast_pct) %>%
        select(-c(pts_2pt_mr, ast_2pm, ast_3pm,
                  opp_pts_2pt_mr, opp_ast_2pm, opp_ast_3pm))
    
    stats_lag <- stats_mov_avg %>%
        group_by(season_year, team_name, location) %>%
        mutate(across(fgm:opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
        ungroup() %>%
        na.exclude()
    
    base_stats_full <- stats_lag %>%
        select(season_type:opp_is_b2b_second)
    
    team_stats <- stats_lag %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("team_", .), fgm:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    opp_stats <- stats_lag %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), team_name:opp_pct_uast_fgm) %>%
        select(-season_year)
    
    nba_final_full <- base_stats_full %>%
        left_join(team_stats, by = c("game_id" = "game_id",
                                     "team_name" = "team_name")) %>%
        left_join(opp_stats, by = c("game_id" = "game_id",
                                    "opp_team_name" = "opp_team_name")) %>%
        arrange(game_date, game_id, location) %>%
        na.exclude() %>%
        left_join(stats_vs_post, by = c("season_year", "team_id")) %>%
        filter(season_type == "post")


    # lagged stats in mamba format - long
    assign(x = "mamba_lag_long_post", nba_final_full, envir = .GlobalEnv)

}

mamba_nba_post(2023:2024)



mamba_post <- mamba_lag_long_post %>%
    group_by(season_year, team_id) %>%
    mutate(
        post_wins = cumsum(wl == "W"),
        opp_post_wins = cumsum(wl == "L"),
        series_wins = if_else(post_wins > 4, post_wins-4, post_wins),
        opp_series_wins = if_else(opp_post_wins > 4, opp_post_wins-4, opp_post_wins)
    )






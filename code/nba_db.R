#### NBA DATABASE -----------------------------------------------------------------

library(tidyverse)
library(janitor)
library(data.table) 
library(nbastatR)
library(hoopR)
library(RSQLite)
library(DBI)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)
options(scipen = 999999)

#### load database ----
NBAdb <- dbConnect(SQLite(), "../nba_sql_db/nba_db")


#### MAMBA DATABASE ####

#### nba schedule scraper function ----
scrape_nba_schedule <- function(url, headers) {
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
    
    nba_schedule <- rbindlist(schedule_df) %>%
        clean_names() %>%
        filter(!series_text %in% c("Preseason","Championship","All-Star Game") & team_name != "") %>%
        mutate(game_date = as_date(game_date_est),
               team_name = paste0(team_city, " ", team_name)) %>%
        select(game_date, game_id, location, team_name) %>%
        arrange(game_date, game_id) %>%
        pivot_wider(
            id_cols = c(game_date, game_id),
            names_from = location,
            values_from = team_name) %>%
        rename(away_team_name = away,
               home_team_name = home)
    
    b2b_away <- nba_schedule %>%
        distinct(game_date, game_id, away_team_name) %>%
        group_by(away_team_name) %>%
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
        select(game_id, is_b2b_first, is_b2b_second)
    
    b2b_home <- nba_schedule %>%
        distinct(game_date, game_id, home_team_name) %>%
        group_by(home_team_name) %>%
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
        select(game_id, is_b2b_first, is_b2b_second) %>%
        rename_with(~paste0("opp_", .), -c(game_id))
    
    b2b_schedule <- b2b_away %>% left_join(b2b_home)
    
    nba_schedule <- nba_schedule %>% left_join(b2b_schedule)
    
    return(nba_schedule)
}

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

# Call the function to scrape the NBA schedule
nba_schedule <- scrape_nba_schedule(url, headers)

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")
DBI::dbWriteTable(NBAdb, "nba_schedule_current", nba_schedule, overwrite = T)
DBI::dbDisconnect(NBAdb)

# saveRDS(nba_schedule, "./nba_schedule.rds")

nba_schedule <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "nba_schedule_current") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))


#### scrape stats for mamba model ----
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

mamba_nba <- function(seasons) {
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
        select(game_id, team_name, fgm:pct_uast_fgm) %>%
        rename_with(~paste0("opp_", .), -c(game_id)) %>%
        select(-opp_plus_minus)
    
    all_stats <- team_all_stats %>%
        inner_join(opp_all_stats, by = c("game_id"), relationship = "many-to-many") %>%
        filter(team_name != opp_team_name) %>%
        select(season_year:team_name, opp_team_name,
               game_id:min, pts, opp_pts, plus_minus, fgm:opp_pct_uast_fgm) %>%
        mutate(pts_2pt_mr = round(pct_pts_2pt_mr*pts,0),
               ast_2pm = round(pct_ast_2pm*(fgm-fg3m),0),
               ast_3pm = round(pct_ast_3pm*fg3m,0),
               opp_pts_2pt_mr = round(opp_pct_pts_2pt_mr*opp_pts,0),
               opp_ast_2pm = round(opp_pct_ast_2pm*(opp_fgm-opp_fg3m),0),
               opp_ast_3pm = round(opp_pct_ast_3pm*opp_fg3m,0)
        ) %>%
        group_by(season_year, team_id, location) %>%
        mutate(across(c(fgm:opp_pct_uast_fgm),
                      \(x) pracma::movavg(x, n = 15, type = 'e'))
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
    
    odds_df <- dplyr::tbl(DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db"),
                          "nba_odds") %>% 
        collect() %>%
        select(game_id, away_spread:home_implied_prob)
    
    base_stats <- all_stats %>%
        filter(location == "away") %>%
        left_join(odds_df, by = "game_id") %>%
        left_join(team_games, by = c("game_id", "team_name")) %>%
        left_join(opp_team_games, by = c("game_id", "opp_team_name")) %>%
        select(season_year:plus_minus, is_b2b_first, is_b2b_second,
               opp_is_b2b_first, opp_is_b2b_second, away_spread:home_implied_prob)
    
    away_stats <- all_stats %>%
        filter(location == "away") %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("away_", .), -c(season_year, game_id, team_name)) %>%
        group_by(season_year, team_name) %>%
        mutate(across(away_fgm:away_opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
        ungroup() %>%
        select(-season_year)
    
    home_stats <- all_stats %>%
        filter(location == "home") %>%
        select(season_year, game_id, team_name, fgm:opp_pct_uast_fgm) %>%
        rename_with(~paste0("home_", .), -c(season_year, game_id, team_name)) %>%
        group_by(season_year, team_name) %>%
        mutate(across(home_fgm:home_opp_pct_uast_fgm, \(x) lag(x, n = 1))) %>%
        ungroup() %>%
        select(-season_year)
    
    nba_final <- base_stats %>%
        left_join(away_stats, by = c("game_id" = "game_id",
                                     "team_name" = "team_name")) %>%
        left_join(home_stats, by = c("game_id" = "game_id",
                                     "opp_team_name" = "team_name")) %>%
        na.exclude() %>%
        arrange(game_date, game_id)
    
    return(nba_final)
}

mamba <- mamba_nba(seasons = 2024)

NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")
DBI::dbWriteTable(NBAdb, "mamba_stats", mamba, append = T)
DBI::dbDisconnect(NBAdb)

mamba <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "mamba_stats") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))

df_check <- mamba %>%
    group_by(season_year) %>% tally()

# saveRDS(nba_final, "./mamba_stats_w15.rds")


#### NBA odds scraper ----
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

scrape_nba_odds <- function(seasons) {
    headers <- generate_headers()
    all_data <- data.frame()
    
    for (year in seasons) {
        params <- generate_parameters(year, "Base")
        
        res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                         httr::add_headers(.headers = headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()
        dt <- rbindlist(data$rowSet) %>% setnames(column_names)
        
        all_data <- bind_rows(all_data, dt)
        
        print(paste0(params$Season, " ", params$MeasureType))
    }
    
    statr_schedule <- all_data %>%
        clean_names() %>%
        arrange(game_date, game_id) %>%
        mutate(location = if_else(grepl("@", matchup) == T, "away", "home"),
               game_date = as_date(game_date),
               season_year = as.numeric(substr(season_year, 1, 4)) + 1,
               team_name = str_replace_all(team_name,
                                           "Los Angeles Clippers", "LA Clippers")) %>%
        select(season_year, game_id, game_date, location, team_name) %>%
        filter(location == "away") %>%
        arrange(game_id)
    
    hoopr_schedule <- as.data.frame(hoopR::load_nba_schedule(seasons = 2024))
    hoopr_schedule <- hoopr_schedule %>%
        filter(type_id == 1) %>%
        select(id, date, away_display_name) %>%
        mutate(date = ymd_hm(gsub("T|Z", " ", date), tz = "UTC"),
               date = date(with_tz(date, tzone = "America/New_York")))
    
    nba_bridge <- statr_schedule %>%
        left_join(hoopr_schedule, by = c("game_date" = "date",
                                         "team_name" = "away_display_name")) %>%
        rename(hoopr_id = id)
    
    # check missing ids
    nba_bridge_missing <- nba_bridge %>%
        filter(is.na(hoopr_id))
    
    print(paste0("hoopR games missing: ", length(nba_bridge_missing$hoopr_id)))
    
    ids <- unique(nba_bridge$hoopr_id)
    
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
    
    # Transform odds data
    odds_clean <- odds_df %>%
        filter(provider_name == "ESPN Bet") %>%
        mutate(away_spread = -spread) %>%
        rename(hoopr_id = game_id,
               home_spread = spread,
               away_moneyline = away_team_odds_money_line,
               home_moneyline = home_team_odds_money_line) %>%
        select(hoopr_id, away_spread, home_spread, away_moneyline, home_moneyline,
               over_under)
    
    odds_wpo <- odds_clean %>%
        mutate(away_moneyline = odds.converter::odds.us2dec(away_moneyline),
               home_moneyline = odds.converter::odds.us2dec(home_moneyline)) %>%
        select(away_moneyline, home_moneyline)
    
    odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')
    
    odds_final <- odds_clean %>%
        mutate(away_implied_prob = odds_wpo$probabilities[,1],
               home_implied_prob = odds_wpo$probabilities[,2])
    
    # attach odds
    odds_db <- nba_bridge %>%
        left_join(odds_final) %>%
        arrange(game_date)
    
    return(odds_db)
}

odds_db <- scrape_nba_odds(2024)

odds_db_missing <- odds_db %>%
    filter(is.na(away_spread))

saveRDS(odds_db, "./nba_odds_2024.rds")

saveRDS(odds_db_missing, "./missing_odds_2024.rds")

# NBAdb <- DBI::dbConnect(RSQLite::SQLite(), "../nba_sql_db/nba_db")
# DBI::dbWriteTable(NBAdb, "nba_odds", odds_db, append = T)
# DBI::dbDisconnect(NBAdb)

odds_db <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "nba_odds") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))


#### fixes missing odds ----
odds_db_missing <- read_rds("./missing_odds.rds")

new_odds <- odds_db_missing

lookup_table <- data.frame(
    game_id = c(odds_db_missing$game_id),
    away_spread = c(-4.5, -12.5, 3, 0, 6.5,
                    6.5, 5.5, -1.5, 9.5, -7,
                    -6.5, 14.5, 8, -7.5, -5,
                    6.5, 4.5, 7.5),
    home_spread = c(4.5, 12.5, -3, 0, -6.5,
                    -6.5, -5.5, 1.5, -9.5, 7,
                    6.5, -14.5, -8, 7.5, 5,
                    -6.5, -4.5, -7.5),
    away_moneyline = c(-190, -1130, 135, -110, 240,
                       220, 190, -130, 400, -290,
                       -280, 1100, 310, -325, -220,
                       230, 155, 270),
    home_moneyline = c(165, 730, -155, -110, -290,
                       -260, -230, 110, -525, 240,
                       230, -2500, -380, 260, 180,
                       -280, -175, -330),
    over_under = c(186.5, 228, 213, 206, 203.5,
                   214, 208.5, 198, 211, 194,
                   195.5, 187.5, 192, 201, 207,
                   218.5, 216.5, 210.5)
)

new_odds <- odds_db_missing %>%
    left_join(lookup_table, by = "game_id", suffix = c("", "_lookup")) %>%
    mutate(
        away_spread = coalesce(away_spread, away_spread_lookup),
        home_spread = coalesce(home_spread, home_spread_lookup),
        away_moneyline = coalesce(away_moneyline, away_moneyline_lookup),
        home_moneyline = coalesce(home_moneyline, home_moneyline_lookup),
        over_under = coalesce(over_under, over_under_lookup)
    ) %>%
    select(-ends_with("_lookup"))

odds_wpo <- new_odds %>%
    mutate(away_moneyline = odds.converter::odds.us2dec(away_moneyline),
           home_moneyline = odds.converter::odds.us2dec(home_moneyline)) %>%
    select(away_moneyline, home_moneyline)

odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')

odds_db <- new_odds %>%
    mutate(away_implied_prob = odds_wpo$probabilities[,1],
           home_implied_prob = odds_wpo$probabilities[,2],
           season_year = c(2014, 2014, 2014, 2014, 2014, 2014, 2014, 2014, 2014,
                           2015, 2015, 2015, 2016, 2016, 2017, 2018, 2021, 2021)) %>%
    select(season_year, game_id:home_implied_prob)

df_check <- df %>%
    group_by(season_year) %>%
    tally()



#### NBA STATS DATABASE ####

## team_shots ----
team_shots_db <- tbl(dbConnect(SQLite(),
                               "../nba_sql_db/nba_db"),"team_shots") %>% collect()

team_shots_new <- teams_shots(team_ids = unique(team_logs$idTeam),
                              seasons = 2024,
                              season_types = "Regular Season",
                              all_active_teams = T)

team_shots <- team_shots_new %>% filter(!idGame %in% team_shots_db$idGame)


## box scores team & box scores player ----

#### scrape all team stats ----
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

scrape_nba_team_stats <- function(seasons) {
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
        left_join(team_games, by = c("game_id", "team_name"))
    
    return(nba_final)
}

df <- scrape_nba_team_stats(seasons = c(2014:2023))

DBI::dbWriteTable(NBAdb, "box_scores_team", df, overwrite = T)

#### scrape all player stats ----
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

scrape_nba_player_stats <- function(seasons) {
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
        arrange(game_id, location)
    
    return(nba_final)
}

df <- scrape_nba_player_stats(seasons = c(2014:2023))

DBI::dbWriteTable(NBAdb, "box_scores_player", df, overwrite = T)

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
    
    pbp_df <<- pbp_df
    wp_df <<- wp_df
    
}

scrape_nba_play_by_play(game_ids)

### scrape play by play ----
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
    
    return(pbp_df)
}

pbp <- scrape_nba_play_by_play(game_ids)

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
    
    return(wp_df)
    
}

win_prob <- scrape_nba_win_probability(game_ids)

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
        
        res <- httr::GET(paste0(url = "https://stats.nba.com/stats/infographicfanduelplayer/?gameId=", game_id, "0022200334"), httr::add_headers(.headers=headers))
        
        json <- res$content %>% rawToChar() %>% jsonlite::fromJSON(simplifyVector = T)
        
        data <- json$resultSets$rowSet[[1]] %>%
            data.frame(stringsAsFactors = F) %>%
            as_tibble()
        
        json_names <- json$resultSets$headers[[1]]
        
        data <- data %>% set_names(json_names) %>% clean_names() 
        
    }

}

fanduel <- scrape_fanduel(game_ids)






## player_profiles ----
players <- player_profiles(player_ids = unique(TeamShots_db$idPlayer))


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
# DBI::dbWriteTable(NBAdb, "nba_schedule_current", nba_schedule, overwrite = T)       # automated --- 2024
# DBI::dbWriteTable(NBAdb, "mamba_stats", mamba, append = T)                          # automated --- 2024
# DBI::dbWriteTable(NBAdb, "nba_odds", odds_db, append = T)                           # automated --- 2023


#### Team & Player Stats ----
# DBI::dbWriteTable(NBAdb, "box_scores_team", box_scores_team, append = T)            # automated --- 2014-2023
# DBI::dbWriteTable(NBAdb, "box_scores_player", box_scores_player, append = T)        # automated --- 2014-2023


# DBI::dbWriteTable(NBAdb, "team_shots", team_shots, append = T)                      # automated --- redo?
# DBI::dbWriteTable(NBAdb, "all_shots", df)                                           # model --- good?


# DBI::dbWriteTable(NBAdb, "player_dictionary", df_nba_player_dict, overwrite = T)    # automated --- 2023
# DBI::dbWriteTable(NBAdb, "team_dictionary", team_dict, overwrite = T)               # as needed ---
# DBI::dbWriteTable(NBAdb, "player_profiles", players, overwrite = T)                 # automated ---

dbDisconnect(NBAdb)

## query db
df <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "team_shots") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))






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



















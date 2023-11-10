### hoopR DB ------------------------------
library(tidyverse)
library(nbastatR)
library(data.table)
library(janitor)
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


#### NBA odds scraper ----

#### build a bridge ----
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





















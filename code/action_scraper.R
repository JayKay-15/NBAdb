#### Action Scraper ----

# working scraper for odds
library(tidyverse)
library(httr)
library(jsonlite)
library(rvest)


# https://www.actionnetwork.com/nba/odds

# https://www.actionnetwork.com/nba/props/game-props

date_id <- 20241022

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

res <- httr::GET(url = paste0("https://api.actionnetwork.com/web/v2/scoreboard/nba?bookIds=15,30,75,123,69,68,972,71,247,79&date=",date_id,"&periods=event"),
                 httr::add_headers(.headers=headers))

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)

odds_game_info <- json$games %>%
    select(id, season, start_time, away_team_id, home_team_id) %>%
    rename(event_id = id)

remove_standings <- function(team) {
    team[["standings"]] <- NULL
    return(team)
}

json[["games"]][["teams"]] <- lapply(json[["games"]][["teams"]], remove_standings)

odds_team_info <- data.table::rbindlist(json[["games"]][["teams"]]) %>%
    select(id, full_name, abbr)



odds_spread <- data.table::rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["spread"]]) %>%
    select(event_id, side, value) %>%
    pivot_wider(names_from = side,
                values_from = c(value)) %>%
    rename_with(~paste0("spread_", .), -c(event_id))

odds_moneyline <- data.table::rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["moneyline"]]) %>%
    select(event_id, side, odds) %>%
    pivot_wider(names_from = side,
                values_from = c(odds)) %>%
    rename_with(~paste0("moneyline_", .), -c(event_id))

odds_totals <- data.table::rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["total"]]) %>%
    select(event_id, value, side) %>%
    filter(side == "over") %>%
    select(-side) %>%
    rename_with(~paste0("total_", .), -c(event_id))


odds_final <- odds_game_info %>%
    left_join(odds_team_info, by = c("away_team_id" = "id")) %>%
    left_join(odds_team_info, by = c("home_team_id" = "id"),
              suffix = c("_away", "_home")) %>%
    left_join(odds_spread, by = "event_id") %>%
    left_join(odds_moneyline, by = "event_id") %>%
    left_join(odds_totals, by = "event_id") %>%
    mutate(
        start_time = format(with_tz(ymd_hms(start_time),
                                    tzone = "America/Chicago"), "%Y-%m-%d")
    )



season_start <- format(with_tz(ymd_hms(json$league$reg_season_start),
                               tzone = "America/Chicago"), "%Y-%m-%d")
season_end <- format(with_tz(ymd_hms(json$league$reg_season_end),
                             tzone = "America/Chicago"), "%Y-%m-%d")
season_blacklist <- format(with_tz(ymd_hms(json$league$blacklist_dates),
                                   tzone = "America/Chicago"), "%Y-%m-%d")
season_blacklist <- as_date(season_blacklist)

season_dates <- seq(as_date(season_start), as_date(season_end), by = "days")

season_active <- season_dates[!(season_dates %in% season_blacklist)]

dates_before_today <- season_active[season_active < Sys.Date()]
dates_after_today <- season_active[season_active >= Sys.Date()]


season_active <- data.frame(game_date = season_active)

season_active <- as_date(season_active$game_date)


# scraper function
scrape_nba_odds <- function(date_range) {
    
    season_active <- tbl(NBAdb, "odds_season_active") %>%
        collect() %>%
        mutate(game_date = as_date(game_date, origin ="1970-01-01")) %>%
        filter(game_date < Sys.Date())
    
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
        
        odds_game_info <- json$games %>%
            select(id, season, start_time, away_team_id, home_team_id) %>%
            rename(event_id = id)
        
        remove_standings <- function(team) {
            team[["standings"]] <- NULL
            return(team)
        }
        
        json[["games"]][["teams"]] <- lapply(json[["games"]][["teams"]], remove_standings)
        
        odds_team_info <- rbindlist(json[["games"]][["teams"]]) %>%
            select(id, full_name, abbr)
        
        odds_spread <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["spread"]]) %>%
            select(event_id, side, value) %>%
            pivot_wider(names_from = side,
                        values_from = c(value)) %>%
            rename_with(~paste0("spread_", .), -c(event_id))
        
        odds_moneyline <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["moneyline"]]) %>%
            select(event_id, side, odds) %>%
            pivot_wider(names_from = side,
                        values_from = c(odds)) %>%
            rename_with(~paste0("moneyline_", .), -c(event_id))
        
        odds_totals <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["total"]]) %>%
            select(event_id, value, side) %>%
            filter(side == "over") %>%
            select(-side) %>%
            rename_with(~paste0("total_", .), -c(event_id))
        
        
        odds_clean <- odds_game_info %>%
            left_join(odds_team_info, by = c("away_team_id" = "id")) %>%
            left_join(odds_team_info, by = c("home_team_id" = "id"),
                      suffix = c("_away", "_home")) %>%
            left_join(odds_spread, by = "event_id") %>%
            left_join(odds_moneyline, by = "event_id") %>%
            left_join(odds_totals, by = "event_id") %>%
            mutate(
                start_time = as_date(format(with_tz(ymd_hms(start_time),
                                                    tzone = "America/Chicago"),
                                            "%Y-%m-%d"))
            ) %>%
            rename(
                season_year = season,
                game_date = start_time,
                away_team_name = full_name_away,
                home_team_name = full_name_home,
                away_abbr = abbr_away,
                home_abbr = abbr_home,
                away_spread = spread_away,
                home_spread = spread_home,
                away_moneyline = moneyline_away,
                home_moneyline = moneyline_home,
                over_under = total_value
            )
        
        odds_wpo <- odds_clean %>%
            mutate(away_moneyline = odds.converter::odds.us2dec(away_moneyline),
                   home_moneyline = odds.converter::odds.us2dec(home_moneyline)) %>%
            select(away_moneyline, home_moneyline)
        
        odds_wpo <- implied::implied_probabilities(odds_wpo, method = 'wpo')
        
        odds_final <- odds_clean %>%
            mutate(away_implied_prob = odds_wpo$probabilities[,1],
                   home_implied_prob = odds_wpo$probabilities[,2])
        
        odds_df <- bind_rows(odds_df, odds_final)
        
    }
    
    assign(x = "nba_odds", odds_df, envir = .GlobalEnv)
}

scrape_nba_odds(season_active$game_date)

nba_odds_away <- odds_df %>%
    select(event_id, season_year, game_date,
           away_team_id, away_team_name, away_abbr, away_spread, away_moneyline,
           over_under, away_implied_prob) %>%
    rename_with(~gsub("^away_", "", .), starts_with("away_")) %>%
    mutate(location = "away")

nba_odds_home <- nba_odds %>%
    select(event_id, season_year, game_date,
           home_team_id, home_team_name, home_abbr, home_spread, home_moneyline,
           over_under, home_implied_prob) %>%
    rename_with(~gsub("^home_", "", .), starts_with("home_")) %>%
    mutate(location = "home")

nba_odds_long <- bind_rows(nba_odds_away, nba_odds_home) %>%
    arrange(game_date, event_id, location)

odds_final <- all_nba_scores %>%
    select(season_year, game_id, game_date, team_name, location) %>%
    left_join(nba_odds_long) %>%
    arrange(game_date, game_id, location) %>%
    select(season_year, game_id, game_date, team_name, location,
           spread, moneyline, over_under, implied_prob)


saveRDS(nba_odds, "/Users/jesse/Desktop/nba_odds.rds")



#### props scraper ----
props_list <- c(
    "core_bet_type_145_first_fg_scorer",
    "core_bet_type_112_first_basket_scorer",
    "core_bet_type_27_points",
    "core_bet_type_23_rebounds",
    "core_bet_type_26_assists",
    "core_bet_type_21_3fgm",
    "core_bet_type_24_steals",
    "core_bet_type_25_blocks",
    "core_bet_type_114_triple-double",
    "core_bet_type_113_double-double",
    "core_bet_type_580_turnovers",
    "core_bet_type_201_695_player_points_milestones_10_or_more",
    "core_bet_type_201_696_player_points_milestones_15_or_more",
    "core_bet_type_201_697_player_points_milestones_20_or_more",
    "core_bet_type_201_698_player_points_milestones_25_or_more",
    "core_bet_type_201_699_player_points_milestones_30_or_more",
    "core_bet_type_201_700_player_points_milestones_35_or_more",
    "core_bet_type_201_701_player_points_milestones_40_or_more",
    "core_bet_type_201_702_player_points_milestones_45_or_more",
    "core_bet_type_203_747_player_assists_milestones_2_or_more",
    "core_bet_type_203_748_player_assists_milestones_3_or_more",
    "core_bet_type_203_749_player_assists_milestones_4_or_more",
    "core_bet_type_203_750_player_assists_milestones_5_or_more",
    "core_bet_type_203_751_player_assists_milestones_6_or_more",
    "core_bet_type_203_752_player_assists_milestones_7_or_more",
    "core_bet_type_203_753_player_assists_milestones_8_or_more",
    "core_bet_type_203_754_player_assists_milestones_10_or_more",
    "core_bet_type_203_755_player_assists_milestones_12_or_more",
    "core_bet_type_202_1243_player_rebounds_milestones_2_or_more",
    "core_bet_type_202_736_player_rebounds_milestones_4_or_more",
    "core_bet_type_202_737_player_rebounds_milestones_5_or_more",
    "core_bet_type_202_738_player_rebounds_milestones_6_or_more",
    "core_bet_type_202_739_player_rebounds_milestones_7_or_more",
    "core_bet_type_202_740_player_rebounds_milestones_8_or_more",
    "core_bet_type_202_741_player_rebounds_milestones_10_or_more",
    "core_bet_type_202_742_player_rebounds_milestones_12_or_more",
    "core_bet_type_202_743_player_rebounds_milestones_13_or_more",
    "core_bet_type_202_744_player_rebounds_milestones_14_or_more",
    "core_bet_type_202_745_player_rebounds_milestones_15_or_more",
    "core_bet_type_202_746_player_rebounds_milestones_16_or_more",
    "core_bet_type_204_760_player_threes_made_milestones_1_or_more",
    "core_bet_type_204_761_player_threes_made_milestones_2_or_more",
    "core_bet_type_204_762_player_threes_made_milestones_3_or_more",
    "core_bet_type_204_763_player_threes_made_milestones_4_or_more",
    "core_bet_type_204_764_player_threes_made_milestones_5_or_more",
    "core_bet_type_204_1052_player_threes_made_milestones_6_or_more",
    "core_bet_type_85_points_rebounds_assists",
    "core_bet_type_86_points_rebounds",
    "core_bet_type_88_rebounds_assists",
    "core_bet_type_87_points_assists",
    "core_bet_type_89_steals_blocks"
)


# Function to pull NBA game props data
get_nba_props <- function(props_list, date) {
    headers <- c(
        `Sec-Fetch-Site` = "same-site",
        Accept = "application/json",
        Origin = "https://www.actionnetwork.com",
        `Sec-Fetch-Dest` = "empty",
        `Accept-Language` = "en-US,en;q=0.9",
        `Sec-Fetch-Mode` = "cors",
        Host = "api.actionnetwork.com",
        `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
        Referer = "https://www.actionnetwork.com/nba/props/points",
        `Accept-Encoding` = "gzip, deflate, br",
        Connection = "keep-alive"
    )
    
    # Initialize list to store results
    results <- list()
    
    # Loop through each prop in the props list
    for (prop_name in props_list) {
        # Set the API URL with the prop name
        url <- paste0("https://api.actionnetwork.com/web/v1/leagues/4/props/", prop_name)
        
        # Define the query parameters
        params <- list(
            date = date
        )
        
        # Make the API request
        res <- httr::GET(url, httr::add_headers(.headers = headers), query = params)
        
        # Check if the request was successful
        if (res$status_code == 200) {
            # Convert the response to JSON
            json <- res$content %>%
                rawToChar() %>%
                jsonlite::fromJSON(simplifyVector = TRUE)
            
            # Store the result in the list with the prop name
            results[[prop_name]] <- json
        } else {
            # Handle errors (e.g., API request failed)
            warning(paste("Failed to get data for prop:", prop_name, "- Status Code:", res$status_code))
        }
    }
    
    return(results)
}

date <- "20241022"

# Call the function to get NBA props data
nba_props_data <- get_nba_props(props_list, date)



# headers = c(
#     `Sec-Fetch-Site` = "same-site",
#     Accept = "application/json",
#     Origin = "https://www.actionnetwork.com",
#     `Sec-Fetch-Dest` = "empty",
#     `Accept-Language` = "en-US,en;q=0.9",
#     `Sec-Fetch-Mode` = "cors",
#     Host = "api.actionnetwork.com",
#     `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
#     Referer = "https://www.actionnetwork.com/nba/props/points",
#     `Accept-Encoding` = "gzip, deflate, br",
#     Connection = "keep-alive"
# )
# 
# params = list(
#     date = "20241022"
# )
# 
# res <- httr::GET(url = past0("https://api.actionnetwork.com/web/v1/leagues/4/props/",
#                              prop_name),
#                  httr::add_headers(.headers=headers), query = params)
# 
# json <- res$content %>%
#     rawToChar() %>%
#     jsonlite::fromJSON(simplifyVector = T)






# espn
cookies = c(
    `_chartbeat4` = "t=COSpFRP3wSSCCn0s7CJtakxDS97aQ&E=13&x=0&c=5.79&y=2786&w=407",
    `espn-prev-page` = "fantasy:basketball:league:toolsprojections",
    s_c24 = "1729459904170",
    s_c24_s = "Less than 1 day",
    s_sq = "wdgespcom%2Cwdgespge=%26pid%3Dfantasy%253Abasketball%253Aleague%253Atoolsprojections%26pidt%3D1%26oid%3Dhttps%253A%252F%252Ffantasy.espn.com%252Fbasketball%252Fplayers%252Fprojections%2523%26ot%3DA",
    `ESPN-ONESITE.WEB-PROD-ac` = "XUS",
    `ESPN-ONESITE.WEB-PROD.idn` = "0028be476b",
    IR_9070 = "1729459560979|0|1729459560979||",
    dtcAuth = "ESPN_PLUS,ESPN_PLUS_UFC_PPV_261,ESPN_PLUS_UFC_PPV_264",
    espn_s2 = "AECU6XUpBo2NGe/8h4wkUqz4ac1mqLE/Zsg8iHWrEEIASCQ7IKxwYpjHtmb9bKHhYLw6E5TGSRYDO27xE27M6c9EPXckol2akHESPhskJqIwe1LL4C1KoSYOPu2DYkgaJgzV41jixSUgCJHZ9lWtQygyl2k7YhIyBCCJ1s/03QufC1ZQtBSFC4ECdwMossuu8A5s1ibY9A6pFVmpf8WRgwxA0eetJMi8ng3Wy3GMnPqKcEQQnu0BN7rQPCu/CGTHkr350LwQU2hh8FYzo9lZ3wdVZ662J/tiwuE/69d8w7T2ig==",
    s_c6 = "1729459561991-Repeat",
    s_cc = "true",
    s_gpv_pn = "fantasy:basketball:league:toolsprojections",
    OptanonConsent = "isGpcEnabled=0&datestamp=Sun Oct 20 2024 16:26:00 GMT-0500 (Central Daylight Time)&version=202407.2.0&browserGpcFlag=0&isIABGlobal=false&identifierType=Cookie Unique Id&hosts=&consentId=582843f6-e709-414d-9f53-8fe553eaadf0&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001:1,C0003:1,BG1145:1,C0002:1,C0004:1,C0005:1&AwaitingReconsent=false",
    s_ensNR = "1729459560334-Repeat",
    `_cb` = "BBvBkWrgxeJtO8Bb",
    `_cb_svref` = "https://www.google.com/",
    `_chartbeat2` = ".1723685021748.1729459559957.1100000010000001.CVPKhfCfVzOPCC46klC8TL-0DeC8h4.3",
    `AMCV_EE0201AC512D2BE80A490D4C%40AdobeOrg` = "-330454231|MCIDTS|20017|MCMID|56722948564101544611826494119055652129|MCAID|NONE|MCOPTOUT-1729466642s|NONE|MCAAMLH-1730064242|9|MCAAMB-1730064242|j8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI|vVersion|3.1.2",
    `ab.storage.deviceId.96ad02b7-2edc-4238-8442-bc35ba85853c` = '{"g":"001c269c-441f-c7a6-197c-2f2084109ef5","c":1723991631105,"l":1729459441885}',
    `ab.storage.sessionId.96ad02b7-2edc-4238-8442-bc35ba85853c` = '{"g":"c12d4fcd-cb6b-0d42-d811-66660a3f47d3","e":1729461241885,"c":1729459441885,"l":1729459441885}',
    `ab.storage.userId.96ad02b7-2edc-4238-8442-bc35ba85853c` = '{"g":"{C1834457-BAA6-4AC7-AC1F-24C04CCFEF2A}","c":1723991631103,"l":1729459441886}',
    block.check = "false|false",
    tveAuth = "",
    tveMVPDAuth = "",
    tveProviderName = "",
    `__eoi` = "ID=281f2ef4eea0dc54:T=1729434986:RT=1729459436:S=AA-Afjbswp9AadRRkQTisMDUb-vh",
    `__gads` = "ID=449734baf8371c5e:T=1729434986:RT=1729459436:S=ALNI_MY5gnkEgksXRwwxf_0mM2CGn9rR-A",
    `__gpi` = "UID=00000f2d6d177044:T=1729434986:RT=1729459436:S=ALNI_MY2M5KT8Gih_lxtyDv1omGYdGXCoA",
    check = "true",
    mbox = "PC#3b933063bd304e14bdb04e1a0bdf6190.34_0#1792704237|session#068aedb7f64f43058e1fbeba0f3777c7#1729461297",
    mboxEdgeCluster = "34",
    `AMCV_5BFD123F5245AECB0A490D45%40AdobeOrg` = "-50417514|MCIDTS|20017|MCMID|62996123027633830733090503193667854453|MCAAMLH-1728835268|9|MCAAMB-1729434987|6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y|MCOPTOUT-1729456269s|NONE|MCAID|NONE|vVersion|5.5.0",
    `_fbp` = "fb.1.1729434989031.850068094598153697",
    espnAuth = '{"swid":"{C1834457-BAA6-4AC7-AC1F-24C04CCFEF2A}"}',
    nol_fpid = "gtulxnpyzax6ntrgytqdbscx2koj41729434989|1729434989014|1729449057282|1729449057309",
    `AMCVS_EE0201AC512D2BE80A490D4C%40AdobeOrg` = "1",
    `AMCVS_5BFD123F5245AECB0A490D45%40AdobeOrg` = "1",
    IR_gbd = "espn.com",
    country = "us",
    hashedIp = "58bad0d973b0b8856462d3955726687432e45c71ca3196b89953601ff08b7233",
    userZip = "75225",
    `ESPN-ONESITE.WEB-PROD.token` = "5=eyJhY2Nlc3NfdG9rZW4iOiJleUpyYVdRaU9pSm5kV1Z6ZEdOdmJuUnliMnhzWlhJdExURTJNakF4T1RNMU5EUWlMQ0poYkdjaU9pSkZVekkxTmlKOS5leUpxZEdraU9pSTVObFJCY1ZoSVJtdGhOMDFpZVU1elZrNUxYMWRuSWl3aWFYTnpJam9pYUhSMGNITTZMeTloZFhSb0xuSmxaMmx6ZEdWeVpHbHpibVY1TG1kdkxtTnZiU0lzSW1GMVpDSTZJblZ5Ympwa2FYTnVaWGs2YjI1bGFXUTZjSEp2WkNJc0luTjFZaUk2SW50RE1UZ3pORFExTnkxQ1FVRTJMVFJCUXpjdFFVTXhSaTB5TkVNd05FTkRSa1ZHTWtGOUlpd2lhV0YwSWpveE56STVORE0wT1RnM0xDSnVZbVlpT2pFM01qUXdNVGcyTVRFc0ltVjRjQ0k2TVRjeU9UVXlNVE00Tnl3aVkyeHBaVzUwWDJsa0lqb2lSVk5RVGkxUFRrVlRTVlJGTGxkRlFpMVFVazlFSWl3aWJHbGtJam9pT1RZeE9UWTFOell0T0dZeFlTMDBNVGxqTFdGbU5tTXRZalUzWlRVM01qTXdaREZqSWl3aVkyRjBJam9pWjNWbGMzUWlMQ0pwWkdWdWRHbDBlVjlwWkNJNklqZG1ZakJqTnpKbUxUVTFZakV0TkRBMVpDMDVaamszTFdSbU5EWm1aRGhpWlRjd05TSjkuXy1tRm54aDJDdTJsaDZQZ3RVTDJCbHVnZWpYQ1cyNlNRMndqQmZ2ZHBnS0EyZ0RhSDRnR2JfakY3elJvazlaVjFfdElPOGVadTlfSU5NRVd0YmFfc2ciLCJyZWZyZXNoX3Rva2VuIjoiZXlKcmFXUWlPaUpuZFdWemRHTnZiblJ5YjJ4c1pYSXRMVEUyTWpBeE9UTTFORFFpTENKaGJHY2lPaUpGVXpJMU5pSjkuZXlKcWRHa2lPaUpqWjBRNFdGTTNSbWczUnpsYVFqUmtTRmRZVG5CM0lpd2ljM1ZpSWpvaWUwTXhPRE0wTkRVM0xVSkJRVFl0TkVGRE55MUJRekZHTFRJMFF6QTBRME5HUlVZeVFYMGlMQ0pwYzNNaU9pSm9kSFJ3Y3pvdkwyRjFkR2d1Y21WbmFYTjBaWEprYVhOdVpYa3VaMjh1WTI5dElpd2lZWFZrSWpvaWRYSnVPbVJwYzI1bGVUcHZibVZwWkRwd2NtOWtJaXdpYVdGMElqb3hOekk1TkRNME9UZzNMQ0p1WW1ZaU9qRTNNalF3TVRnMk1URXNJbVY0Y0NJNk1UYzBORGs0TmprNE55d2lZMnhwWlc1MFgybGtJam9pUlZOUVRpMVBUa1ZUU1ZSRkxsZEZRaTFRVWs5RUlpd2lZMkYwSWpvaWNtVm1jbVZ6YUNJc0lteHBaQ0k2SWprMk1UazJOVGMyTFRobU1XRXROREU1WXkxaFpqWmpMV0kxTjJVMU56SXpNR1F4WXlJc0ltbGtaVzUwYVhSNVgybGtJam9pTjJaaU1HTTNNbVl0TlRWaU1TMDBNRFZrTFRsbU9UY3RaR1kwTm1aa09HSmxOekExSW4wLkwwNC15UkhseUJTUFd0cWx2bWlRUGFWQm13dS1TSGgyd2hUR2FSQXgzaTNPaVUxVmFHbndtaWdvZ0pDNXJBYUFaOU53NVQ0ZFFMWXFseTRaamRMMUFBIiwic3dpZCI6IntDMTgzNDQ1Ny1CQUE2LTRBQzctQUMxRi0yNEMwNENDRkVGMkF9IiwidHRsIjo4NjM5OSwicmVmcmVzaF90dGwiOjE1NTUxOTk5LCJoaWdoX3RydXN0X2V4cGlyZXNfaW4iOjAsImluaXRpYWxfZ3JhbnRfaW5fY2hhaW5fdGltZSI6MTcyNDAxODYxMTAwMCwiaWF0IjoxNzI5NDM0OTg3MDAwLCJleHAiOjE3Mjk1MjEzODcwMDAsInJlZnJlc2hfZXhwIjoxNzQ0OTg2OTg3MDAwLCJoaWdoX3RydXN0X2V4cCI6MTcyNDAyMDQxMTAwMCwic3NvIjpudWxsLCJhdXRoZW50aWNhdG9yIjpudWxsLCJsb2dpblZhbHVlIjpudWxsLCJjbGlja2JhY2tUeXBlIjpudWxsLCJzZXNzaW9uVHJhbnNmZXJLZXkiOiIwVmdyRTVnUlZyRnJjXzV4Zk9wWGJWYVRPWlZoa1RiVlZKT0hCZFY2eGJVYlZURGcxWlFlT2Z0M3Fvd1RYY085R1RMMjZKX1lsMHNjWmFmNEZQLW4ya2RSMVdtRW5fV1oxVkhqeW41QkNBd0t5d1ROaWhFIiwiY3JlYXRlZCI6IjIwMjQtMTAtMjBUMTQ6MzY6MjguMjIwWiIsImxhc3RDaGVja2VkIjoiMjAyNC0xMC0yMFQxNDozNjoyOC4yMjBaIiwiZXhwaXJlcyI6IjIwMjQtMTAtMjFUMTQ6MzY6MjcuMDAwWiIsInJlZnJlc2hfZXhwaXJlcyI6IjIwMjUtMDQtMThUMTQ6MzY6MjcuMDAwWiJ9|eyJraWQiOiJndWVzdGNvbnRyb2xsZXItLTE2MjAxOTM1NDQiLCJhbGciOiJFUzI1NiJ9.eyJqdGkiOiJnQzYwbzE4ZDVOWkxXM0p4RzZiWURBIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLnJlZ2lzdGVyZGlzbmV5LmdvLmNvbSIsImF1ZCI6IkVTUE4tT05FU0lURS5XRUItUFJPRCIsInN1YiI6IntDMTgzNDQ1Ny1CQUE2LTRBQzctQUMxRi0yNEMwNENDRkVGMkF9IiwiaWF0IjoxNzI5NDM0OTg3LCJuYmYiOjE3MjQwMTg2MTEsImV4cCI6MTcyOTUyMTM4NywiY2F0IjoiaWR0b2tlbiIsImVtYWlsIjoiamVzc2Uua2FydGVzQGdtYWlsLmNvbSIsImlkZW50aXR5X2lkIjoiN2ZiMGM3MmYtNTViMS00MDVkLTlmOTctZGY0NmZkOGJlNzA1In0.ZlmKe3hajffYMX30nrh7S-rgyt45rQFKyLS8VtnA5La7W9H73QI4aFQdP1J95p5rrerAXriJuUJFEYLAWil5Sg",
    SWID = "{C1834457-BAA6-4AC7-AC1F-24C04CCFEF2A}",
    device_12323e2e = "2444e25b-aa12-437c-aec2-89f1ead4df78"
)

headers = c(
    Accept = "application/json",
    `Sec-Fetch-Site` = "same-site",
    `Accept-Language` = "en-US,en;q=0.9",
    `Accept-Encoding` = "gzip, deflate, br",
    `Sec-Fetch-Mode` = "cors",
    Origin = "https://fantasy.espn.com",
    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0.1 Safari/605.1.15",
    Referer = "https://fantasy.espn.com/",
    Connection = "keep-alive",
    Host = "lm-api-reads.fantasy.espn.com",
    `Sec-Fetch-Dest` = "empty",
    `X-Fantasy-Source` = "kona",
    `X-Fantasy-Filter` = '{"players":{"filterStatsForExternalIds":{"value":[2024,2025]},"filterSlotIds":{"value":[0,1,2,3,4,5,6,7,8,9,10,11]},"filterStatsForSourceIds":{"value":[0,1]},"useFullProjectionTable":{"value":true},"sortAppliedStatTotal":{"sortAsc":false,"sortPriority":3,"value":"102025"},"sortDraftRanks":{"sortPriority":2,"sortAsc":true,"value":"STANDARD"},"sortPercOwned":{"sortPriority":4,"sortAsc":false},"limit":400,"filterStatsForTopScoringPeriodIds":{"value":5,"additionalValue":["002025","102025","002024","012025","022025","032025","042025"]}}}',
    `X-Fantasy-Platform` = "kona-PROD-652b677c73811482e562046ffeb891262541819e"
)

params = list(
    view = "kona_player_info"
)

res <- httr::GET(url = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/2025/segments/0/leaguedefaults/2",
                 httr::add_headers(.headers=headers), query = params, httr::set_cookies(.cookies = cookies))

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)



# numberFire


# sportsline
url <- paste0("https://www.sportsline.com/nba/expert-projections/simulation/")
webpage <- read_html(url)


col_names <- webpage %>% 
    html_nodes("thead.sc-36594fa2-8.bdPbeW > tr > th") %>% 
    html_attr("id")    

data <- webpage %>% 
    html_nodes("table.sc-36594fa2-7.ZvKHg > tbody > tr > td") %>% 
    html_text() %>%
    matrix(ncol = length(col_names), byrow = TRUE) %>%
    as_tibble()

names(data) <- col_names

data <- data %>%
    mutate(game_date = as_date("2024-10-22"))


#fantasy pros
url <- paste0("https://www.fantasypros.com/nba/projections/daily-overall.php?signedin")
webpage <- read_html(url)


col_names <- webpage %>% 
    html_nodes("table#data > thead") %>% 
    html_text2() %>%
    strsplit("\\t") %>%
    `[[`(1)

data <- webpage %>% 
    html_nodes("table#data > tbody > tr > td") %>% 
    html_text() %>%
    matrix(ncol = length(col_names), byrow = TRUE) %>%
    as_tibble()

names(data) <- col_names

data <- data %>%
    mutate(game_date = as_date("2024-10-22"))



#fantasy sp
url <- paste0("https://www.fantasysp.com/projections/basketball/daily/")
webpage <- read_html(url)


col_names <- webpage %>% 
    html_nodes("table.table > thead") %>% 
    html_text2() %>%
    strsplit("\\t") %>%
    `[[`(1)

data <- webpage %>% 
    html_nodes("table.table > tbody > tr > td") %>% 
    html_text() %>%
    matrix(ncol = length(col_names), byrow = TRUE) %>%
    as_tibble()

names(data) <- col_names

data <- data %>%
    mutate(game_date = as_date("2024-10-22"))




# draft edge
cookies = c(
    `_ga_MEWCEVEL81` = "GS1.1.1729460329.2.1.1729460346.43.0.0",
    deid = "0",
    draftedge = "0",
    status = "134534533643422626724543254",
    `_ga` = "GA1.1.1799603938.1729440438",
    `_gcl_au` = "1.1.1106700733.1729440438"
)

headers = c(
    Accept = "text/plain, */*; q=0.01",
    `Sec-Fetch-Site` = "same-origin",
    `Accept-Language` = "en-US,en;q=0.9",
    `Accept-Encoding` = "gzip, deflate, br",
    `Sec-Fetch-Mode` = "cors",
    Host = "draftedge.com",
    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0.1 Safari/605.1.15",
    Connection = "keep-alive",
    Referer = "https://draftedge.com/nba/nba-daily-projections/",
    `Sec-Fetch-Dest` = "empty",
    `X-Requested-With` = "XMLHttpRequest"
)

params = list(
    `_` = "1729460346533"
)

res <- httr::GET(url = "https://draftedge.com/draftedge-data/nba_proj_dk.json", httr::add_headers(.headers=headers), query = params, httr::set_cookies(.cookies = cookies))

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)




#### write rds
# action network
write_rds(nba_props_data, "./data/nba_props_data_20241022.rds")
write_rds(json, "./data/game_odds_20241022.rds")

#sportsline
write_rds(data, "./data/sportsline_20241022.rds")






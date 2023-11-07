library(tidyverse)
library(data.table)
library(janitor)
library(magrittr)

#### Scrape a single stat and year ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Four Factors" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2023-24",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                 httr::add_headers(.headers=headers), query = params)
data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)


#### Scrape a single stat category loop years ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Four Factors" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2022-23",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

# Define the range of years you want to scrape
start_year <- 2013
end_year <- 2022

# Initialize an empty data frame to store the results
all_data <- data.frame()

# Loop through the years and scrape data
for (year in start_year:end_year) {
    tryCatch({
        # Convert the year to the "YYYY-YY" format
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        
        # Update the Season parameter in the params list
        params$Season <- season
        
        # Make the HTTP request
        res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                         httr::add_headers(.headers = headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()  
        dt <- rbindlist(data$rowSet) %>% setnames(column_names)
        
        # Append the data to the all_data data frame
        all_data <- bind_rows(all_data, dt)
        
        print(season)
    }, error = function(e) {
        # If an error occurs, print a message and continue to the next year
        cat("Error for year", year, ":", conditionMessage(e), "\n")
    })
}

# Now, 'all_data' contains data for all the years from 1996-97 to 2022-23


#### Scrape all stat categories loop years ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Four Factors" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2022-23",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

# Define the range of years you want to scrape
start_year <- 2009
end_year <- 2022

# Define the list of MeasureType values
measure_types <- c("Base", "Advanced", "Four Factors", "Misc", "Scoring")

# Initialize an empty list to store the data frames
all_data_list <- list()

# Loop through the measure types
for (measure_type in measure_types) {
    # Initialize an empty data frame for the current measure type
    all_data <- data.frame()
    
    # Loop through the years and scrape data
    for (year in start_year:end_year) {
        # Convert the year to the "YYYY-YY" format
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        
        # Update the MeasureType and Season parameters in the params list
        params$MeasureType <- measure_type
        params$Season <- season
        
        # Make the HTTP request
        res <- httr::GET(url = "https://stats.nba.com/stats/teamgamelogs",
                         httr::add_headers(.headers = headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()
        dt <- rbindlist(data$rowSet) %>% setnames(column_names)
        
        # Append the data to the current measure type data frame
        all_data <- bind_rows(all_data, dt)
        
        print(season)
    }
    
    # Store the data frame in the list with a name based on the measure type
    all_data_list[[measure_type]] <- all_data
    
    print(measure_type)
}

# Each data frame contains data for all seasons for the respective MeasureType
saveRDS(all_data_list, file = "team_stats_list.rds")


#### Scrape a single stat and year ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Usage" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2022-23",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

res <- httr::GET(url = "https://stats.nba.com/stats/playergamelogs",
                 httr::add_headers(.headers=headers), query = params)
data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)


#### Scrape a single stat category loop years ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Usage" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2022-23",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

# Define the range of years you want to scrape
start_year <- 2009
end_year <- 2022

# Initialize an empty data frame to store the results
all_data <- data.frame()

# Loop through the years and scrape data
for (year in start_year:end_year) {
    tryCatch({
        # Convert the year to the "YYYY-YY" format
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        
        # Update the Season parameter in the params list
        params$Season <- season
        
        # Make the HTTP request
        res <- httr::GET(url = "https://stats.nba.com/stats/playergamelogs",
                         httr::add_headers(.headers=headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()  
        dt <- rbindlist(data$rowSet) %>% setnames(column_names)
        
        # Append the data to the all_data data frame
        all_data <- bind_rows(all_data, dt)
        
        print(season)
    }, error = function(e) {
        # If an error occurs, print a message and continue to the next year
        cat("Error for year", year, ":", conditionMessage(e), "\n")
    })
}




#### Scrape all stat categories loop years ----
# Define common headers and parameters
headers = c(
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

params = list(
    `DateFrom` = "",
    `DateTo` = "",
    `GameSegment` = "",
    `ISTRound` = "",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `MeasureType` = "Base", # "Base" "Advanced" "Usage" "Misc" "Scoring"
    `Month` = "0",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `PaceAdjust` = "N",
    `PerMode` = "Totals",
    `Period` = "0",
    `PlusMinus` = "N",
    `Rank` = "N",
    `Season` = "2022-23",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `VsConference` = "",
    `VsDivision` = ""
)

# Define the range of years you want to scrape
start_year <- 2009
end_year <- 2022

# Define the list of MeasureType values
measure_types <- c("Base", "Advanced", "Usage", "Misc", "Scoring")

# Initialize an empty list to store the data frames
all_data_list <- list()

# Loop through the measure types
for (measure_type in measure_types) {
    # Initialize an empty data frame for the current measure type
    all_data <- data.frame()
    
    # Loop through the years and scrape data
    for (year in start_year:end_year) {
        # Convert the year to the "YYYY-YY" format
        season <- sprintf("%d-%02d", year, (year + 1) %% 100)
        
        # Update the MeasureType and Season parameters in the params list
        params$MeasureType <- measure_type
        params$Season <- season
        
        # Make the HTTP request
        res <- httr::GET(url = "https://stats.nba.com/stats/playergamelogs",
                         httr::add_headers(.headers=headers), query = params)
        data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
        column_names <- data$headers %>% as.character()
        dt <- rbindlist(data$rowSet) %>% setnames(column_names)
        
        # Append the data to the current measure type data frame
        all_data <- bind_rows(all_data, dt)
        
        print(season)
    }
    
    # Store the data frame in the list with a name based on the measure type
    all_data_list[[measure_type]] <- all_data
    
    print(measure_type)
}

# Each data frame contains data for all seasons for the respective MeasureType
saveRDS(all_data_list, file = "player_stats_list.rds")







### Team Tracking
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

# headers = c(
#     `Origin` = 'https://www.nba.com',
#     `Referer` = 'https://www.nba.com/',
#     `Accept` = '*/*',
#     `User-Agent` = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15'
# )


sched <- nbastatR::game_logs(seasons = 2023,
                             result_types = "team",
                             season_types = "Regular Season") %>%
    select(5) %>%
    distinct()

# test <- seq(as.Date("2021-10-18"), as.Date("2022-10-19"), by="days")
gm_df <- format(sched$dateGame, "%m/%d/%Y")


df <- data.frame()

i <- 1
h <- length(gm_df)

for (i in i:h) {
    
    g <- gm_df[i]
    
    params = list(
        `College` = '',
        `Conference` = '',
        `Country` = '',
        `DateFrom` = g,
        `DateTo` = g,
        `Division` = '',
        `DraftPick` = '',
        `DraftYear` = '',
        `GameScope` = '',
        `Height` = '',
        `LastNGames` = '0',
        `LeagueID` = '00',
        `Location` = '',
        `Month` = '0',
        `OpponentTeamID` = '0',
        `Outcome` = '',
        `PORound` = '0',
        `PerMode` = 'PerGame',
        `PlayerExperience` = '',
        `PlayerOrTeam` = 'Team',
        `PlayerPosition` = '',
        `PtMeasureType` = 'Rebounding',
        `Season` = '2022-23',
        `SeasonSegment` = '',
        `SeasonType` = 'Regular Season',
        `StarterBench` = '',
        `TeamID` = '0',
        `VsConference` = '',
        `VsDivision` = '',
        `Weight` = ''
    )
    
    res <- httr::GET(url = 'https://stats.nba.com/stats/leaguedashptstats', httr::add_headers(.headers=headers), query = params)
    data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
    column_names <- data$headers %>% as.character()  
    dt <- rbindlist(data$rowSet) %>% setnames(column_names) %>% mutate(date = as_date(params[["DateFrom"]], format = "%m/%d/%Y"))
    print(params[["DateFrom"]])
    
    df <- bind_rows(df, dt)
    
}

openxlsx::write.xlsx(df, file = "./output/rebounding.xlsx")

#####

### Team Playtype
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

params = list(
        `LeagueID` = '00',
        `PerMode` = 'PerGame',
        `PlayType` = 'Isolation',
        `PlayerOrTeam` = 'T',
        `SeasonType` = 'Regular Season',
        `SeasonYear` = '2021-22',
        `TypeGrouping` = 'offensive'
)

res <- httr::GET(url = 'https://stats.nba.com/stats/synergyplaytypes', httr::add_headers(.headers=headers), query = params)
data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)

openxlsx::write.xlsx(dt, file = "./output/isolation_off.xlsx")

#####

### Player Tracking
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

params = list(
    `LeagueID` = '00',
    `PerMode` = 'PerGame',
    `PlayType` = 'Isolation',
    `PlayerOrTeam` = 'T',
    `SeasonType` = 'Regular Season',
    `SeasonYear` = '2021-22',
    `TypeGrouping` = 'offensive'
)

res <- httr::GET(url = 'https://stats.nba.com/stats/synergyplaytypes', httr::add_headers(.headers=headers), query = params)
data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)

openxlsx::write.xlsx(dt, file = "./output/isolation_off.xlsx")

#####

### Player Play Type
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

params = list(
    `LeagueID` = '00',
    `PerMode` = 'PerGame',
    `PlayType` = 'Isolation',
    `PlayerOrTeam` = 'T',
    `SeasonType` = 'Regular Season',
    `SeasonYear` = '2021-22',
    `TypeGrouping` = 'offensive'
)

res <- httr::GET(url = 'https://stats.nba.com/stats/synergyplaytypes', httr::add_headers(.headers=headers), query = params)
data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)

openxlsx::write.xlsx(dt, file = "./output/isolation_off.xlsx")




# json <-
#     res$content %>%
#     rawToChar() %>%
#     jsonlite::fromJSON(simplifyVector = T)






#### nba schedule scraper ----
headers = c(
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

res <- httr::GET(url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json", httr::add_headers(.headers=headers))
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
    filter(series_text != "Preseason" & team_name != "") %>%
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

saveRDS(nba_schedule, "./nba_schedule.rds")







# Create a function to scrape NBA schedule
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
        filter(series_text != "Preseason" & team_name != "") %>%
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

df <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "nba_schedule_current") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"))


















library(tidyverse)
library(data.table)
library(janitor)
library(magrittr)

#### Scrape a single stat and year - teamgamelogs ----
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


#### Scrape a single stat category loop years - teamgamelogs ----
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


#### Scrape all stat categories loop years - teamgamelogs ----
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


#### Scrape a single stat and year - playergamelogs ----
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


#### Scrape a single stat category loop years - playergamelogs ----
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




#### Scrape all stat categories loop years - playergamelogs ----
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









### Team Tracking -- loop to scrape each day
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
    `College` = '',
    `Conference` = '',
    `Country` = '',
    `DateFrom` = "02/25/2024",
    `DateTo` = "02/25/2024",
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
    `PerMode` = "PerGame",
    `PlayerExperience` = "",
    `PlayerOrTeam` = "Team",
    `PlayerPosition` = "",
    `PtMeasureType` = "Efficiency",
    `Season` = "2023-24",
    `SeasonSegment` = '',
    `SeasonType` = 'Regular Season',
    `StarterBench` = '',
    `TeamID` = '0',
    `VsConference` = '',
    `VsDivision` = '',
    `Weight` = ''
)


res <- httr::GET(url = 'https://stats.nba.com/stats/leaguedashptstats',
                 httr::add_headers(.headers=headers), query = params)

data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>%
    setnames(column_names) %>%
    clean_names()




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

res <- httr::GET(url = 'https://stats.nba.com/stats/synergyplaytypes',
                 httr::add_headers(.headers=headers), query = params)

data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names) %>% clean_names()




#### Box Score ----
# boxscorematchupsv3
# boxscoreplayertrackV3
# boxscoredefensivev2
# boxscorehustlev2
# boxscorefourfactorsv3
# boxscoretraditionalv3 -- player stats scrape
# boxscoreadvancedv3 -- player stats scrape
# boxscoreusagev3 -- player stats scrape
# boxscoremiscv3 -- player stats scrape
# boxscorescoringV3 -- player stats scrape
# playbyplay


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

params = list(
    `GameID` = "0022300345",
    `LeagueID` = "00",
    `endPeriod` = "0",
    `endRange` = "28800",
    `rangeType` = "0",
    `startPeriod` = "0",
    `startRange` = "0"
)

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
    

#### Matchups ----
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
    `DateFrom` = "",
    `DateTo` = "",
    `DefPlayerID` = "",
    `DefTeamID` = "",
    `LeagueID` = "00",
    `Matchup` = "Defense",
    `OffPlayerID` = "",
    `OffTeamID` = "",
    `Outcome` = "",
    `PORound` = "0",
    `PerMode` = "Totals",
    `Season` = "2023-24",
    `SeasonType` = "Regular Season"
)

res <- httr::GET(url = "https://stats.nba.com/stats/leagueseasonmatchups",
                 httr::add_headers(.headers=headers), query = params)

data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names) %>% clean_names()





##### player props ----
headers = c(
    `Sec-Fetch-Site` = "same-site",
    `Accept` = "application/json",
    `Origin` = "https://www.actionnetwork.com",
    `Sec-Fetch-Dest` = "empty",
    `Accept-Language` = "en-US,en;q=0.9",
    `Sec-Fetch-Mode` = "cors",
    `Host` = "api.actionnetwork.com",
    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    `Referer` = "https://www.actionnetwork.com/nba/props/game-props",
    `Accept-Encoding` = "gzip, deflate, br",
    `Connection` = "keep-alive"
)

params = list(
    `date` = "20240225",
    `periods` = "event"
)

res <- httr::GET(url = "https://api.actionnetwork.com/web/v2/scoreboard/nba",
                 httr::add_headers(.headers=headers), query = params)

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)










# working scraper for odds
library(tidyverse)
library(httr)
library(jsonlite)

# https://www.actionnetwork.com/nba/odds

# https://www.actionnetwork.com/nba/props/game-props

date_id <- 20240114

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

DBI::dbWriteTable(NBAdb, "odds_season_active", season_active, append = T)





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

nba_odds <- readxl::read_xlsx("/Users/jesse/Desktop/odds_db.xlsx")





nba_odds_away <- nba_odds %>%
    select(game_id, season_year, game_date,
           away_team_name, away_abbr, away_spread, away_moneyline,
           over_under, away_implied_prob) %>%
    rename_with(~gsub("^away_", "", .), starts_with("away_")) %>%
    mutate(location = "away")

nba_odds_home <- nba_odds %>%
    select(game_id, season_year, game_date,
           home_team_name, home_abbr, home_spread, home_moneyline,
           over_under, home_implied_prob) %>%
    rename_with(~gsub("^home_", "", .), starts_with("home_")) %>%
    mutate(location = "home")

nba_odds_long <- bind_rows(nba_odds_away, nba_odds_home) %>%
    arrange(game_date, game_id, location)

odds_final_2 <- all_nba_scores %>%
    select(season_year, game_id, game_date, team_name, location) %>%
    left_join(nba_odds_long) %>%
    arrange(game_date, game_id, location) %>%
    select(season_year, game_id, game_date, team_name, location,
           spread, moneyline, over_under, implied_prob) %>%
    mutate(implied_prob = as.numeric(implied_prob))





saveRDS(odds_final_2, "/Users/jesse/Desktop/nba_odds_14_23_reworked.rds")
saveRDS(odds_final, "/Users/jesse/Desktop/nba_odds_24_reworked.rds")




NBAdb <- dbConnect(SQLite(), "../nba_sql_db/nba_db")
season_active <- tbl(NBAdb, "odds_season_active") %>%
    collect() %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01")) %>%
    filter(game_date < Sys.Date())

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
            mutate(location = "away")
        
        odds_game_home <- json$games %>%
            select(id, season, start_time, away_team_id, home_team_id) %>%
            rename(event_id = id,
                   team_id = home_team_id,
                   opp_team_id = away_team_id) %>%
            mutate(location = "home")
        
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
        
        all_spread <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["spread"]])
        all_moneyline <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["moneyline"]])
        all_over_under <- rbindlist(json[["games"]][["markets"]][["15"]][["event"]][["total"]])
        
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
            ) %>%
            mutate(game_date = as_date(game_date))
        
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

scrape_nba_odds(season_active$game_date)



odds_final_2 <- read_rds("/Users/jesse/Desktop/nba_odds_14_23_reworked.rds")

odds_final_2 <- odds_final_2 %>%
    left_join(odds_final_2 %>% select(game_id, team_name, spread,
                                      moneyline, implied_prob),
              by = "game_id",
              relationship = "many-to-many",
              suffix = c("_team", "_opp")) %>%
    filter(team_name_team != team_name_opp) %>%
    rename(team_name = team_name_team,
           opp_team_name = team_name_opp,
           team_spread = spread_team,
           opp_spread = spread_opp,
           team_moneyline = moneyline_team,
           opp_moneyline = moneyline_opp,
           team_implied_prob = implied_prob_team,
           opp_implied_prob = implied_prob_opp) %>%
    select(season_year, game_date, location, team_name, opp_team_name,
           team_spread, opp_spread, team_moneyline, opp_moneyline, over_under,
           team_implied_prob, opp_implied_prob)


all_nba_odds <- bind_rows(odds_final_2, nba_odds) %>% mutate(game_date = as_date(game_date))





# daily line up scraper 
headers = c(
    `sec-ch-ua` = '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    `Referer` = "https://www.nba.com/",
    `sec-ch-ua-mobile` = "?0",
    `User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    `sec-ch-ua-platform` = '"Windows"'
)

res <- httr::GET(url = "https://stats.nba.com/js/data/leaders/00_daily_lineups_20240210.json", httr::add_headers(.headers=headers))

json <- res$content %>% rawToChar() %>% jsonlite::fromJSON(simplifyVector = T)

starters <- json$games %>%
    data.frame(stringsAsFactors = F) %>%
    as_tibble() %>%
    select(gameId:gameStatusText)

ht_players <- data.table::rbindlist(json$games$homeTeam$players)
at_players <- data.table::rbindlist(json$games$awayTeam$players)









# fic testing
mamba_fic <- df %>%
    select(
        season_year:player_name, team_id, team_name, game_id, game_date,
        location, min, base_pts, base_fga, base_fta, base_oreb, base_dreb,
        base_ast, base_tov, base_stl, base_blk, base_pf
    ) %>%
    group_by(player_id, location) %>%
    mutate(
        across(min:base_pf, cummean),
        across(min:base_pf, \(x) lag(x, n = 1)),
        fic = round(base_pts + base_oreb + (base_dreb*0.75) + base_ast +
                        base_stl + base_blk - (base_fga*0.75) -
                        (base_fta*0.375) - base_tov - (base_pf*0.5), 3),
        fic40 = round(if_else(!fic, 0, (fic/min)*40), 3)
    ) %>%
    ungroup() %>%
    select(game_date, game_id, team_id, team_name, 
           player_id, player_name, location, fic, fic40) %>%
    na.exclude()



nba_final <- tbl(dbConnect(SQLite(), "../nba_sql_db/nba_db"), "mamba_stats") %>%
    collect() %>%
    rename(team_winner = wl,
           team_score = pts,
           opp_score = opp_pts) %>%
    mutate(game_date = as_date(game_date, origin ="1970-01-01"),
           team_winner = if_else(team_winner == "W", "win", "loss"),
           team_winner = factor(team_winner, levels = c("win", "loss"))) %>%
    filter(season_year >= 2019)




lineup_2014 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2014/data.rds")
# lineup_2015 <- read_rds("/Users/jesse/Desktop/nba_pbp_data-main/lineup-final2015/data.rds")
lineup_2016 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2016/data.rds")
lineup_2017 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2017/data.rds")
lineup_2018 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2018/data.rds")
lineup_2019 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2019/data.rds")
lineup_2020 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2020/data.rds")
lineup_2021 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2021/data.rds")
lineup_2022 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2022/data.rds")
lineup_2023 <- read_rds("../nba_in_R/nba_pbp_data-main/lineup-final2023/data.rds")


all_lineups <- bind_rows(lineup_2014, lineup_2016, lineup_2017, lineup_2018,
                         lineup_2019, lineup_2020, lineup_2021, lineup_2022, lineup_2023)

lineups <- bind_rows(lineup_2019, lineup_2020, lineup_2021, lineup_2022, lineup_2023) %>%
    filter(period == 1 & stint == 1) %>%
    separate(col = lineup_team,
             into = c("starter_1", "starter_2", "starter_3", "starter_4", "starter_5"),
             sep = ",\\s?", extra = "drop") %>%
    mutate(across(starts_with("starter_"), ~as.numeric(gsub("[^0-9]", "", .)))) %>%
    select(game_id, location = location_team, team, starter_1:starter_5) %>%
    pivot_longer(cols = starts_with("starter_"),
                 names_to = "starter_number", values_to = "starter")


starter_fic <- lineups %>%
    left_join(mamba_fic, by = c("game_id" = "game_id",
                                "location" = "location",
                                "starter" = "player_id")) %>%
    select(game_id:starter, team_id, fic) %>%
    mutate(fic = replace_na(fic, 0)) %>%
    group_by(game_id, team_id, location) %>%
    summarise(across(fic, mean)) %>%
    ungroup() %>%
    select(-team_id) %>%
    filter(fic != 0) %>%
    pivot_wider(names_from = location, values_from = fic,
                names_prefix = "fic_") %>%
    na.exclude() %>%
    rename(away_fic = fic_away,
           home_fic = fic_home)


write_csv(starter_fic, "/Users/jesse/Desktop/starter_fic.csv")

starter_fic <- read_csv("/Users/jesse/Desktop/starter_fic.csv")



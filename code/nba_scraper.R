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



#### Hustle Box Score ----
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

res <- httr::GET(url = "https://stats.nba.com/stats/boxscorehustlev2",
                 httr::add_headers(.headers=headers), query = params)

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)












# shots data

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
    `AheadBehind` = "",
    `CFID` = "",
    `CFPARAMS` = "",
    `ClutchTime` = "",
    `Conference` = "",
    `ContextFilter` = "",
    `ContextMeasure` = "FGA",
    `DateFrom` = "",
    `DateTo` = "",
    `Division` = "",
    `EndPeriod` = "0",
    `EndRange` = "28800",
    `GROUP_ID` = "",
    `GameEventID` = "",
    `GameID` = "0022300748",
    `GameSegment` = "",
    `GroupID` = "",
    `GroupMode` = "",
    `GroupQuantity` = "5",
    `LastNGames` = "0",
    `LeagueID` = "00",
    `Location` = "",
    `Month` = "0",
    `OnOff` = "",
    `OppPlayerID` = "",
    `OpponentTeamID` = "0",
    `Outcome` = "",
    `PORound` = "0",
    `Period` = "0",
    `PlayerID` = "1629652",
    `PlayerID1` = "",
    `PlayerID2` = "",
    `PlayerID3` = "",
    `PlayerID4` = "",
    `PlayerID5` = "",
    `PlayerPosition` = "",
    `PointDiff` = "",
    `Position` = "",
    `RangeType` = "0",
    `RookieYear` = "",
    `Season` = "2023-24",
    `SeasonSegment` = "",
    `SeasonType` = "Regular Season",
    `ShotClockRange` = "",
    `StartPeriod` = "0",
    `StartRange` = "0",
    `StarterBench` = "",
    `TeamID` = "1610612760",
    `VsConference` = "",
    `VsDivision` = "",
    `VsPlayerID1` = "",
    `VsPlayerID2` = "",
    `VsPlayerID3` = "",
    `VsPlayerID4` = "",
    `VsPlayerID5` = "",
    `VsTeamID` = ""
)

res <- httr::GET(url = "https://stats.nba.com/stats/shotchartdetail", httr::add_headers(.headers=headers), query = params)

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)

data <- httr::content(res) %>% .[['resultSets']] %>% .[[1]]
column_names <- data$headers %>% as.character()  
dt <- rbindlist(data$rowSet) %>% setnames(column_names)






# Install and load the necessary packages
library(tidyverse)
library(rvest)

# Read the HTML file
html_content <- readLines("https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line/full-game/?date=2024/01/05", warn = FALSE)

# Parse the HTML content with rvest
html <- html_content %>% paste(collapse = "\n") %>% read_html()

html_product <- html %>% html_elements("#tbody-nba")

table <- html_product %>% html_element("span")












library(tidyverse)
library(httr)
library(jsonlite)



res <- httr::GET(url = "https://www.rotowire.com/betting/nba/tables/games-archive.php")

json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)






require(httr)

cookies = c(
    `_sg_b_v` = "4;1904;1707611565",
    `XSRF-TOKEN` = "eyJpdiI6IlQ4QmQ3OThNY0FQZWdPQjZLVzQ3SlE9PSIsInZhbHVlIjoiSzdEWEM2UGJ5NFRNTFFUc2kyQTArZUJreGZ6ZTNxcXJzc2sxNFhvMmxZRVU2WFFHZHpuTUdRMkowcWVHaTRBcVgxQmM2SENhVzVMVktnOHRzUERxWHpPLzc5c2IzM2tvWkM1SnFIVXFVYXd2NlZoaDNDT3N6U09LSVU5aGFvTGgiLCJtYWMiOiJhYzdhMTI2NDdlODMxODA1ZTM4MTZjYWQyZGFlZGQ4ZTIyMmRlNmMyYjAwYmIwN2I0MDZiYmY4MWY4MjMzMTRmIiwidGFnIjoiIn0=",
    `oddsportalcom_session` = "eyJpdiI6IjlveTFnZlhmeHVlUThUaHBpQjhZd3c9PSIsInZhbHVlIjoiYkVPamNSVVVVKyttWDFGTmNHTGIwNGo1S1MycmJ2MVE2eFRHb3NrL1E4S3JUay9kT2R6aGZ6b05kMHR4SHhlNG1CdndFNmdtaEMxdnlyc2NHVXh6cHR3eTNkeFZWK3NFZ3Z1Nlo1TVBBKzdwUEpSVHlMcTJGeU5qRG5jWlROWjUiLCJtYWMiOiIyMjhlYTc1NWNmZTI5ZjAwZjQ5MzE2MjNhNzdhMjU0ZmRmZGRmNTlmYTI3NmEyZWQ3OWFiOTQ4NmFmMTZlNDYzIiwidGFnIjoiIn0=",
    `OptanonConsent` = "isGpcEnabled=0&datestamp=Sat Feb 10 2024 18:33:17 GMT-0600 (Central Standard Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=da1caeb4-083d-47d2-8efb-14b89bd10b0a&interactionCount=1&landingPath=NotLandingPage&groups=C0001:1,C0002:0,C0004:0,V2STACK42:0&hosts=H194:1,H302:1,H236:1,H198:1,H203:1,H190:0,H301:0,H303:0,H304:0,H230:0,H305:0&genVendors=V2:0,&geolocation=US;TX&AwaitingReconsent=false",
    `_sg_b_p` = "/,/,/basketball/usa/nba/,/basketball/usa/nba/,/basketball/usa/nba/results/,/basketball/usa/nba/results/,/basketball/usa/nba/results/,/basketball/usa/nba/results/,/basketball/usa/nba/results/",
    `op_user_cookie` = "7403065303",
    `op_user_hash` = "9c6b29dbe7d8662e393e9234d1002f94",
    `op_user_time` = "1707065881",
    `_sg_b_n` = "1707065900690",
    `OptanonAlertBoxClosed` = "2024-02-04T16:58:10.054Z",
    `eupubconsent-v2` = "CP5d47AP5d47AAcABBENAmEgAAAAAAAAAChQAAAAAACBIAIC8x0AEBeZKACAvMpABAXm.YAAAAAAAAAAA",
    `op_user_full_time_zone` = "10",
    `op_user_time_zone` = "-6",
    `op_lang` = "en",
    `op_cookie-test` = "ok"
)

headers = c(
    `Content-Type` = "application/json",
    `Accept` = "application/json, text/plain, */*",
    `Sec-Fetch-Site` = "same-origin",
    `Accept-Language` = "en-US,en;q=0.9",
    `Accept-Encoding` = "gzip, deflate, br",
    `Sec-Fetch-Mode` = "cors",
    `Host` = "www.oddsportal.com",
    `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    `Referer` = "https://www.oddsportal.com/basketball/usa/nba/results/",
    `Connection` = "keep-alive",
    `Sec-Fetch-Dest` = "empty",
    `X-Requested-With` = "XMLHttpRequest",
    `X-XSRF-TOKEN` = "eyJpdiI6IlQ4QmQ3OThNY0FQZWdPQjZLVzQ3SlE9PSIsInZhbHVlIjoiSzdEWEM2UGJ5NFRNTFFUc2kyQTArZUJreGZ6ZTNxcXJzc2sxNFhvMmxZRVU2WFFHZHpuTUdRMkowcWVHaTRBcVgxQmM2SENhVzVMVktnOHRzUERxWHpPLzc5c2IzM2tvWkM1SnFIVXFVYXd2NlZoaDNDT3N6U09LSVU5aGFvTGgiLCJtYWMiOiJhYzdhMTI2NDdlODMxODA1ZTM4MTZjYWQyZGFlZGQ4ZTIyMmRlNmMyYjAwYmIwN2I0MDZiYmY4MWY4MjMzMTRmIiwidGFnIjoiIn0="
)

params = list(
    `_` = "1707611844035"
)

res <- httr::GET(url = "https://www.oddsportal.com/ajax-sport-country-tournament-archive_/3/IoGXixRr/X0/1/0/", httr::add_headers(.headers=headers), query = params, httr::set_cookies(.cookies = cookies))


json <- res$content %>%
    rawToChar() %>%
    jsonlite::fromJSON(simplifyVector = T)



























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


    



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

column_names <- json$headers %>% as.character() 

dt <- rbindlist(json$rowSet) %>% setnames(column_names)












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












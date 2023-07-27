###### to get game ids ------------------------------------------
library(tidyverse)
library(lubridate)
library(rvest)

########
# PARAMETERS
########
year <- "2023"
monthList <- c("october", "november", "december", "january", "february",
               "march", "april")
playoff_startDate <- ymd("2023-04-10")
outputfile <- "NBA_2023_game_data.rds"

# year <- "2021"
# monthList <- c("december", "january", "february",
#                "march", "april", "may", "june", "july")
# playoff_startDate <- ymd("2021-05-18")
# outputfile <- "NBA-2021_game_data.rds"
# 
# year <- "2020"
# monthList <- c("october-2019", "november","december", "january", "february",
#                "march")
# playoff_startDate <- ymd("2020-07-30")
# outputfile <- "NBA-2020_game_data.rds"
# 
# year <- "2019"
# monthList <- c("october", "november", "december", "january", "february",
#                "march", "april")
# playoff_startDate <- ymd("2019-04-13")
# outputfile <- "NBA-2019_game_data.rds"
# 
# year <- "2018"
# monthList <- c("october", "november", "december", "january", "february",
#                "march", "april")
# playoff_startDate <- ymd("2018-04-14")
# outputfile <- "NBA-2018_game_data.rds"

########
# SCRIPT FOR SCRAPING DATA STARTS HERE
########
df <- data.frame()
for (month in monthList) {
    # get webpage
    url <- paste0("https://www.basketball-reference.com/leagues/NBA_", year, 
                  "_games-", month, ".html")
    webpage <- read_html(url)
    
    # get column names
    col_names <- webpage %>% 
        html_nodes("table#schedule > thead > tr > th") %>% 
        html_attr("data-stat")    
    col_names <- c("game_id", col_names)
    
    # extract dates column
    # note that in april, there is a break in the table which just says 
    # "Playoffs". this messes with the data merging later, so we get rid of it
    dates <- webpage %>% 
        html_nodes("table#schedule > tbody > tr > th") %>% 
        html_text()
    dates <- dates[dates != "Playoffs"]
    
    # extract game id
    # we need to remove the NA that is due to the "Playoffs" row in april
    game_id <- webpage %>% 
        html_nodes("table#schedule > tbody > tr > th") %>%
        html_attr("csk")
    game_id <- game_id[!is.na(game_id)]
    
    # extract all columns (except date)
    data <- webpage %>% 
        html_nodes("table#schedule > tbody > tr > td") %>% 
        html_text() %>%
        matrix(ncol = length(col_names) - 2, byrow = TRUE)
    
    # combine game IDs, dates and columns in dataframe for this month, add col names
    month_df <- as.data.frame(cbind(game_id, dates, data), stringsAsFactors = FALSE)
    names(month_df) <- col_names
    
    # add to overall dataframe
    df <- rbind(df, month_df)
}

# change columns to the correct types
df$visitor_pts <- as.numeric(df$visitor_pts)
df$home_pts    <- as.numeric(df$home_pts)
df$attendance  <- as.numeric(gsub(",", "", df$attendance))
df$date_game   <- mdy(df$date_game)

# add column to indicate if regular season or playoff
df$game_type <- with(df, ifelse(date_game >= playoff_startDate, 
                                "Playoff", "Regular"))

# drop boxscore column
df$box_score_text <- NULL

# save to file
saveRDS(df, outputfile)

## Box Scores ----------------------------------------------------

inputfile <- "./box_scores/NBA_2023_game_data.rds"
outputfile <- "./box_scores/NBA_2023_advanced_box_scores.rds"
game_df <- as_tibble(readRDS(inputfile))
adv_df <- as_tibble(readRDS(outputfile))

game_df <- game_df %>% filter(date_game < Sys.Date()) %>% select(-arena_name)
game_df <- subset(game_df, !(game_df$game_id %in% adv_df$game_id))

##############################
# SCRIPT STARTS HERE - Advanced
##############################
# game_df <- as_tibble(readRDS(inputfile))
# 
# game_df$date_game <- as.Date(game_df$date_game, origin ="1970-01-01")
# 
# game_df <- game_df %>% filter(game_type=="Regular") %>% arrange(date_game)

master_df <- data.frame()

for (current_id in game_df$game_id) {
    print(current_id)
    
    ##########
    # get box scores
    ##########
    url <- paste0("https://www.basketball-reference.com/boxscores/", current_id,
                  ".html")
    webpage <- read_html(url)
    tables <- webpage %>% html_nodes("table") %>%
        html_table()
    tables <- discard(tables, function(z) ncol(z) != 17)
    names(tables) <- c("visitor_adv_boxscore","home_adv_boxscore")
    
    a_box <- tables[[1]]
    names(a_box) <- as.character(a_box[1,])
    a_box <- a_box[-1,]
    a_box <- a_box %>%
        filter(Starters != 'Reserves' & Starters != 'Team Totals') %>%
        mutate(across(c(MP:BPM), na_if, "Did Not Play")) %>%
        mutate(game_id = current_id)  %>%
        mutate(loc = "A") %>%
        left_join(game_df[c(1,2,4)], by = "game_id") %>%
        rename("team" = "visitor_team_name") %>%
        rename("player" = "Starters")
    a_box[a_box == ""] <- "0"
    
    h_box <- tables[[2]]
    names(h_box) <- as.character(h_box[1,])
    h_box <- h_box[-1,]
    h_box <- h_box %>%
        filter(Starters != 'Reserves' & Starters != 'Team Totals') %>%
        mutate(across(c(MP:BPM), na_if, "Did Not Play")) %>%
        mutate(game_id = current_id) %>%
        mutate(loc = "H") %>%
        left_join(game_df[c(1,2,6)], by = "game_id") %>%
        rename("team" = "home_team_name") %>%
        rename("player" = "Starters")
    h_box[h_box == ""] <- "0"
    
    full_box <- rbind(a_box, h_box)
    full_box <- full_box %>%
        select(18,20,19,21,1,2:17)
    
    full_box[, 7:ncol(full_box)] <- lapply(7:ncol(full_box), function(x) as.numeric(full_box[[x]]))
    
    master_df <- rbind(master_df,full_box)
}


master_df <- rbind(adv_df, master_df)

saveRDS(master_df, "./box_scores/NBA_2023_advanced_box_scores.rds")



##############################
# SCRIPT STARTS HERE - Basic
##############################
inputfile <- "./box_scores/NBA_2023_game_data.rds"
outputfile <- "./box_scores/NBA_2023_basic_box_scores.rds"
game_df <- as_tibble(readRDS(inputfile))
basic_df <- as_tibble(readRDS(outputfile))

game_df <- game_df %>% filter(date_game < Sys.Date()) %>% select(-arena_name)
game_df <- subset(game_df, !(game_df$game_id %in% basic_df$game_id))

# game_df <- as_tibble(readRDS(inputfile))

master_df <- data.frame()

for (current_id in game_df$game_id) {
    print(current_id)

    ##########
    # get box scores
    ##########
    url <- paste0("https://www.basketball-reference.com/boxscores/", current_id,
                  ".html")
    webpage <- read_html(url)
    tables <- webpage %>% html_nodes("table") %>%
        html_table()
    
    ot <- game_df %>% filter(game_id==current_id)
    
    if (ot$overtimes == "") {
        tables <- tables[c(1,9)]
    } else if (ot$overtimes == "OT") {
        tables <- tables[c(1,10)]
    } else if (ot$overtimes == "2OT") {
        tables <- tables[c(1,11)]
    } else if (ot$overtimes == "3OT") {
        tables <- tables[c(1,13)]
    } else if (ot$overtimes == "4OT") {
        tables <- tables[c(1,14)]
    } else if (ot$overtimes == "5OT") {
        tables <- tables[c(1,15)]
    } else if (ot$overtimes == "6OT") {
        tables <- tables[c(1,16)]
    }
    
    names(tables) <- c("visitor_boxscore","home_boxscore")

    a_box <- tables[[1]]
    names(a_box) <- as.character(a_box[1,])
    a_box <- a_box[-1,]
    a_box <- a_box %>%
        filter(Starters != 'Reserves' & Starters != 'Team Totals') %>%
        mutate(across(c(MP:`+/-`), na_if, "Did Not Play")) %>%
        mutate(game_id = current_id)  %>%
        mutate(loc = "A") %>%
        left_join(game_df[c(1,2,4)], by = "game_id") %>%
        rename("team" = "visitor_team_name") %>%
        rename("player" = "Starters")
    a_box[a_box == ""] <- "0"

    h_box <- tables[[2]]
    names(h_box) <- as.character(h_box[1,])
    h_box <- h_box[-1,]
    h_box <- h_box %>%
        filter(Starters != 'Reserves' & Starters != 'Team Totals') %>%
        mutate(across(c(MP:`+/-`), na_if, "Did Not Play")) %>%
        mutate(game_id = current_id) %>%
        mutate(loc = "H") %>%
        left_join(game_df[c(1,2,6)], by = "game_id") %>%
        rename("team" = "home_team_name") %>%
        rename("player" = "Starters")
    h_box[h_box == ""] <- "0"

    full_box <- rbind(a_box, h_box)
    full_box <- full_box %>%
        select(22,24,23,25,1,2:21)

    full_box[, 7:ncol(full_box)] <- lapply(7:ncol(full_box), function(x) as.numeric(full_box[[x]]))

    master_df <- rbind(master_df,full_box)
}

master_df <- rbind(basic_df, master_df)

saveRDS(master_df, "./box_scores/NBA_2023_basic_box_scores.rds")






####### Adding FIC & VORP -----------------------------------------------


# VORP - Value Over Replacement Player (available since the 1973-74 season in the NBA); 
# a box score estimate of the points per 100 TEAM possessions that a player contributed above a replacement-level (-2.0) player, 
# translated to an average team and prorated to an 82-game season. Multiply by 2.70 to convert to wins over replacement. 
# Please see the article About Box Plus/Minus (BPM) for more information.

df_basic <- as_tibble(readRDS("./box_scores/2023_NBA_basic_box_scores.rds"))
df_adv <- as_tibble(readRDS("./box_scores/2023_NBA_advanced_box_scores.rds"))



# calculate fic

fic <- df_adv

fic$MP <- hms::as_hms(strptime(fic$MP, "%M:%S"))
fic$MP <- round(period_to_seconds(hms(fic$MP))/60,3)

master_fic <- fic %>%
    mutate(FIC = round(PTS + ORB + (0.75*DRB) + AST + STL + BLK - (0.75*FGA) - (0.375*FTA) - TOV - (0.5*PF),1)) %>%
    mutate(FIC40 = round(ifelse(!FIC, NA, (FIC/MP)*40),1))




# calculate vorp

vorp <- df_adv

vorp$MP <- hms::as_hms(strptime(vorp$MP, "%M:%S"))

vorp <- vorp %>%
    group_by(game_id, team) %>%
    mutate(tm = sum(MP, na.rm = T)) %>%
    ungroup() %>%
    mutate(sec = period_to_seconds(hms(vorp$MP)))

vorp$pt <- (vorp$sec*5)/as.numeric(vorp$tm)
vorp$VORP <- round((vorp$BPM-(-2))*(vorp$pt),1)

master_vorp <- vorp %>% select(-c(22:24))





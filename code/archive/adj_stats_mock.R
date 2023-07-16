#### New Adjusted Stats Method

### Creates adjusted year to date stats for score and win predictions

if (!require("pacman")) install.packages("pacman"); library(pacman)
pacman::p_load(tidyverse, readxl, lubridate, openxlsx, nbastatR, rvest)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

rm(list=ls())
setwd("/Users/Jesse/Documents/MyStuff/NBA Betting/NBA-Betting-21-22/")

### Pull game logs & arrange by date

game_logs(seasons = c(2017:2022), result_types = c("team","players"), season_types = c("Regular Season","Playoffs"))

dataGameLogsTeam <- dataGameLogsTeam %>% arrange(dateGame,idGame)
dataGameLogsTeam$dateGame <- as_date(dataGameLogsTeam$dateGame)
dataGameLogsPlayer <- dataGameLogsPlayer %>% arrange(dateGame,idGame)

### Attach game logs to itself to get all stats for each game in one row

gl <- left_join(dataGameLogsTeam, dataGameLogsTeam, by = c("idGame" = "idGame", "slugTeam" = "slugOpponent"))

gl <- gl %>%
    select(13,8,17,62,7,45,90,34,79,
           24,25,27,28,35,36,37,38,39,40,43,41,42,44,
           69,70,72,73,80,81,82,83,84,85,88,86,87,89)

colnames(gl) <- c("Date", "teamLoc", "teamName", "opptName", "teamRslt", 
                  "teamPTS", "opptPTS", "teamMin", "opptMin", 
                  "teamFGM", "teamFGA", "team3PM", "team3PA", "teamFTM",
                  "teamFTA", "teamORB", "teamDRB", "teamTRB", "teamAST",
                  "teamTOV", "teamSTL", "teamBLK", "teamPF", 
                  "opptFGM", "opptFGA", "oppt3PM", "oppt3PA", "opptFTM", 
                  "opptFTA", "opptORB", "opptDRB", "opptTRB", "opptAST", 
                  "opptTOV", "opptSTL", "opptBLK", "opptPF")


raw_adv <- gl


raw_adv$Poss <- with(raw_adv, teamFGA - teamORB + teamTOV + (.44 * teamFTA))
raw_adv$oPoss <- with(raw_adv, opptFGA - opptORB + opptTOV + (.44 * opptFTA))
raw_adv$Pace <- with(raw_adv, 48 * (Poss + oPoss) / (2 * (teamMin/5)))
raw_adv$oPace <- with(raw_adv, 48 * (Poss + oPoss) / (2 * (opptMin/5)))
raw_adv$ORtg <- with(raw_adv, (teamPTS / Poss) * 100)
raw_adv$DRtg <- with(raw_adv, (opptPTS / oPoss) * 100)

raw_adv$FG <- with(raw_adv, teamFGM / teamFGA)
raw_adv$SR2 <- with(raw_adv, (teamFGA - team3PA) / teamFGA)
raw_adv$FG3 <- with(raw_adv, team3PM / raw_adv$team3PA)
raw_adv$SR3 <- with(raw_adv, team3PA / teamFGA)
raw_adv$FT <- with(raw_adv, teamFTM / teamFTA)
raw_adv$FTR <- with(raw_adv, teamFTM / teamFGA)
raw_adv$ORB <- with(raw_adv, teamORB / (teamORB + opptDRB))
raw_adv$DRB <- with(raw_adv, teamDRB / (teamDRB + opptORB))
raw_adv$TRB <- with(raw_adv, teamTRB / (teamTRB + opptTRB))
raw_adv$AST <- with(raw_adv, teamAST / teamFGM)
raw_adv$TOV <- with(raw_adv, teamTOV / Poss)
raw_adv$STL <- with(raw_adv, teamSTL / oPoss)
raw_adv$BLK <- with(raw_adv, teamBLK / (opptFGA - oppt3PA))
raw_adv$PF <- with(raw_adv, teamPF / oPoss)
raw_adv$eFG <- with(raw_adv, (teamFGM + .5 * team3PM) / teamFGA)
raw_adv$TS <- with(raw_adv, teamPTS / (2 * teamFGA + .44 * teamFTA))

raw_adv$oFG <- with(raw_adv, opptFGM / opptFGA)
raw_adv$oSR2 <- with(raw_adv, (opptFGA - oppt3PA) / opptFGA)
raw_adv$oFG3 <- with(raw_adv, oppt3PM / oppt3PA)
raw_adv$oSR3 <- with(raw_adv, oppt3PA / opptFGA)
raw_adv$oFT <- with(raw_adv, opptFTM / opptFTA)
raw_adv$oFTR <- with(raw_adv, opptFTM / opptFGA)
raw_adv$oORB <- with(raw_adv, opptORB / (opptORB + teamDRB))
raw_adv$oDRB <- with(raw_adv, opptDRB / (opptDRB + teamORB))
raw_adv$oTRB <- with(raw_adv, opptTRB / (teamTRB + opptTRB))
raw_adv$oAST <- with(raw_adv, opptAST / opptFGM)
raw_adv$oTOV <- with(raw_adv, opptTOV / oPoss)
raw_adv$oSTL <- with(raw_adv, opptSTL / Poss)
raw_adv$oBLK <- with(raw_adv, opptBLK / (teamFGA - team3PA))
raw_adv$oPF <- with(raw_adv, opptPF / Poss)
raw_adv$oeFG <- with(raw_adv, (opptFGM + .5 * oppt3PM) / opptFGA)
raw_adv$oTS <- with(raw_adv, opptPTS / (2 * opptFGA + .44 * opptFTA))

raw_final <- raw_adv %>%
    select(1:4,44:75,42,43,40)

nba <- read_xlsx("/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/NBAdb1722_noadj.xlsx")

dates17 <- dataGameLogsTeam %>%
    filter(yearSeason == 2017) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 20, 0, 1))

dates17[c(1:11),3] <- dates17[c(1:11),1]

dates18 <- dataGameLogsTeam %>%
    filter(yearSeason == 2018) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 20, 0, 1))

dates18[c(1:11),3] <- dates18[c(1:11),1]

dates19 <- dataGameLogsTeam %>%
    filter(yearSeason == 2019) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 20, 0, 1))

dates19[c(1:11),3] <- dates19[c(1:11),1]

dates20 <- dataGameLogsTeam %>%
    filter(yearSeason == 2020) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 20, 0, 1))

dates20[c(1:11),3] <- dates20[c(1:11),1]

dates21 <- dataGameLogsTeam %>%
    filter(yearSeason == 2021) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 13, 0, 1))

dates21[c(1:11),3] <- dates21[c(1:11),1]

dates22 <- dataGameLogsTeam %>%
    filter(yearSeason == 2022) %>%
    distinct(dateGame) %>%
    mutate(stat_start = min(dateGame)) %>%
    mutate(stat_end = dateGame - 1) %>%
    mutate(adj = if_else(dateGame <= min(as.Date(dateGame)) + 20, 0, 1))

dates22[c(1:11),3] <- dates22[c(1:11),1]

dates <- as.data.frame(bind_rows(dates22, dates21, dates20, dates19, dates18, dates17))

dates_adj <- dates %>%
    filter(adj == 1)

raw_final <- raw_final %>%
    left_join(dates_adj, by = c("Date" = "dateGame")) %>%
    filter(adj == 1) %>%
    select(1:39)
    
new_adj_home <- raw_final %>%
    filter(teamLoc == 'H') %>%
    left_join(nba, by = c("Date"="Date", "teamName"="Home")) %>%
    select(1:4,43,42,44,45,81:115,46:80,5:39)

names(new_adj_home) <- gsub(x = names(new_adj_home), pattern = "_home", replacement = "_team") 
names(new_adj_home) <- gsub(x = names(new_adj_home), pattern = "_away", replacement = "_oppt")
colnames(new_adj_home)[5:6] <- c("teamScore", "opptScore")

new_adj_away <- raw_final %>%
    filter(teamLoc == 'A') %>%
    left_join(nba, by = c("Date"="Date", "teamName"="Away")) %>%
    select(1:4,42:115,5:39)

names(new_adj_away) <- gsub(x = names(new_adj_away), pattern = "_away", replacement = "_team") 
names(new_adj_away) <- gsub(x = names(new_adj_away), pattern = "_home", replacement = "_oppt")
colnames(new_adj_away)[5:6] <- c("teamScore", "opptScore")

new_adj <- new_adj_away %>%
    bind_rows(new_adj_home) %>%
    mutate(teamLoc = if_else(teamLoc == 'H', 1, -1)) %>%
    drop_na()


### loop


a <- 79
b <- 9
c <- 60

results <- new_adj

for ( a in 79:113) {
    
    nba_reg <- new_adj %>%
        select(a,2,b,c)
    
    X = as.matrix(nba_reg[-1])
    y = as.numeric(unlist(nba_reg[,1]))
    
    c_name <- paste0(names(nba_reg[1]),"_adj")
                     
    
    ### Ridge model
    lambdas <- 10^seq(2, -3, by = -.1)
    ridge_reg = glmnet(X, y, nlambda = 100, alpha = 0, family = 'gaussian', lambda = lambdas)
    
    par(mfrow = c(1, 2))
    plot(ridge_reg)
    plot(ridge_reg, xvar = "lambda", label = TRUE)
    
    summary(ridge_reg)
    
    
    cv_ridge <- cv.glmnet(X, y, alpha = 0, lambda = lambdas)
    plot(cv_ridge)
    
    
    optimal_lambda <- cv_ridge$lambda.min
    optimal_lambda
    
    
    # Prediction and evaluation on train data
    adj_stat <- predict(ridge_reg, s = optimal_lambda, newx = X)
    
    results <- results %>%
        bind_cols(adj_stat)
    
    names(results)[ncol(results)] <- c_name
    
    a <- a+1
    b <- b+1
    c <- c+1
    
}

results2 <- results %>%
    select(1,3,114:148)

nba_adj <- nba %>%
    left_join(results2, by = c("Date"="Date", "Away"="teamName")) %>%
    drop_na()

names(nba_adj) <- gsub(x = names(nba_adj), pattern = "_adj", replacement = "_new_away") 

nba_adj <- nba_adj %>%
    left_join(., results2, by = c("Date"="Date", "Home"="teamName")) %>%
    drop_na()

names(nba_adj) <- gsub(x = names(nba_adj), pattern = "_adj", replacement = "_new_home") 

names(nba_adj) <- gsub(x = names(nba_adj), pattern = "_new", replacement = "_adj") 



fn <- "ADJ"
u <- paste0("/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/",fn,".xlsx")

wb <- createWorkbook()
addWorksheet(wb, sheetName = "ADJ")
writeData(wb, "ADJ", x = nba_adj)
saveWorkbook(wb, file = u)








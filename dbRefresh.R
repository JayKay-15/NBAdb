### DB Refresh ------------------------------
library(tidyverse)
library(lubridate)
library(nbastatR)
library(RSQLite)
library(DBI)


options(dplyr.summarise.inform = FALSE)
Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

rm(list=ls())
setwd("/Users/Jesse/Documents/MyStuff/NBA Betting/NBAdb/")


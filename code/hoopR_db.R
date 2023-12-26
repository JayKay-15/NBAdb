### hoopR DB ------------------------------
library(tidyverse)
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
























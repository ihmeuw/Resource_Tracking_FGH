############################################
### Author: USERNAME
### Purpose: Combine COVID and NO_COVID estimates for final results
############################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the comment
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
date <- args[2]
comment <- args[3]

root_fold <- 'FILEPATH'
load(paste0(root_fold, 'FILEPATH'))
no_covid_root_fold <- paste0(root_fold, "FILEPATH")

# final estimates based on observed retrospective estimates through 2022 from COVID forecasts, 
# and then for 2023 onward use the NO_COVID forecasts.

# Make GHE/GDP
covid_GHE_GDP_draws <- data.table(read_feather(paste0(root_fold, "FILEPATH")))
covid_GHE_GDP_stats <- fread(paste0(root_fold, "FILEPATH"))
nocovid_GHE_GDP_draws <- data.table(read_feather(paste0(no_covid_root_fold, "FILEPATH")))
nocovid_GHE_GDP_stats <- fread(paste0(no_covid_root_fold, "FILEPATH"))

final_GHE_GDP_draws <- rbind(covid_GHE_GDP_draws[year <= metadata_list$end_fit], 
                           nocovid_GHE_GDP_draws[year > metadata_list$end_fit])
final_GHE_GDP_stats <- rbind(covid_GHE_GDP_stats[year <= metadata_list$end_fit], 
                           nocovid_GHE_GDP_stats[year > metadata_list$end_fit])

write_feather(final_GHE_GDP_draws, paste0(root_fold, "FILEPATH"))
fwrite(final_GHE_GDP_stats, paste0(root_fold, "FILEPATH"))

# Make GHE/GGE
covid_GHE_GGE_draws <- data.table(read_feather(paste0(root_fold, "FILEPATH")))
covid_GHE_GGE_stats <- fread(paste0(root_fold, "FILEPATH"))
nocovid_GHE_GGE_draws <- data.table(read_feather(paste0(no_covid_root_fold, "FILEPATH")))
nocovid_GHE_GGE_stats <- fread(paste0(no_covid_root_fold, "FILEPATH"))

final_GHE_GGE_draws <- rbind(covid_GHE_GGE_draws[year <= metadata_list$end_fit], 
                             nocovid_GHE_GGE_draws[year > metadata_list$end_fit])
final_GHE_GGE_stats <- rbind(covid_GHE_GGE_stats[year <= metadata_list$end_fit], 
                             nocovid_GHE_GGE_stats[year > metadata_list$end_fit])

write_feather(final_GHE_GGE_draws, paste0(root_fold, "FILEPATH"))
fwrite(final_GHE_GGE_stats, paste0(root_fold, "FILEPATH"))

# Make GHE per capita
covid_GHEpc_draws <- data.table(read_feather(paste0(root_fold, "FILEPATH")))
covid_GHEpc_stats <- fread(paste0(root_fold, "FILEPATH"))
nocovid_GHEpc_draws <- data.table(read_feather(paste0(no_covid_root_fold, "FILEPATH")))
nocovid_GHEpc_stats <- fread(paste0(no_covid_root_fold, "FILEPATH"))

final_GHEpc_draws <- rbind(covid_GHEpc_draws[year <= metadata_list$end_fit], 
                           nocovid_GHEpc_draws[year > metadata_list$end_fit])
final_GHEpc_stats <- rbind(covid_GHEpc_stats[year <= metadata_list$end_fit], 
                           nocovid_GHEpc_stats[year > metadata_list$end_fit])

write_feather(final_GHEpc_draws, paste0(root_fold, "FILEPATH"))
fwrite(final_GHEpc_stats, paste0(root_fold, "FILEPATH"))



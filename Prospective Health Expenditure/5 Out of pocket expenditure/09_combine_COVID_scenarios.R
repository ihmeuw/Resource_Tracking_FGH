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

# Final estimates based on observed retro values through 2022 from COVID forecasts, 
# and then for 2023 onward use the NO_COVID forecasts.

# Make OOP/GDP
covid_OOP_GDP_draws <- data.table(read_feather(paste0(root_fold, "FILEPATH")))
covid_OOP_GDP_stats <- fread(paste0(root_fold, "FILEPATH"))
nocovid_OOP_GDP_draws <- data.table(read_feather(paste0(no_covid_root_fold, "FILEPATH")))
nocovid_OOP_GDP_stats <- fread(paste0(no_covid_root_fold, "FILEPATH"))

final_OOP_GDP_draws <- rbind(covid_OOP_GDP_draws[year <= metadata_list$end_fit], 
                           nocovid_OOP_GDP_draws[year > metadata_list$end_fit])
final_OOP_GDP_stats <- rbind(covid_OOP_GDP_stats[year <= metadata_list$end_fit], 
                           nocovid_OOP_GDP_stats[year > metadata_list$end_fit])

write_feather(final_OOP_GDP_draws, paste0(root_fold, "FILEPATH"))
fwrite(final_OOP_GDP_stats, paste0(root_fold, "FILEPATH"))

# Make OOP per capita
covid_OOPpc_draws <- data.table(read_feather(paste0(root_fold, "FILEPATH")))
covid_OOPpc_stats <- fread(paste0(root_fold, "FILEPATH"))
nocovid_OOPpc_draws <- data.table(read_feather(paste0(no_covid_root_fold, "FILEPATH")))
nocovid_OOPpc_stats <- fread(paste0(no_covid_root_fold, "FILEPATH"))

final_OOPpc_draws <- rbind(covid_OOPpc_draws[year <= metadata_list$end_fit], 
                           nocovid_OOPpc_draws[year > metadata_list$end_fit])
final_OOPpc_stats <- rbind(covid_OOPpc_stats[year <= metadata_list$end_fit], 
                           nocovid_OOPpc_stats[year > metadata_list$end_fit])

write_feather(final_OOPpc_draws, paste0(root_fold, "FILEPATH"))
fwrite(final_OOPpc_stats, paste0(root_fold, "FILEPATH"))



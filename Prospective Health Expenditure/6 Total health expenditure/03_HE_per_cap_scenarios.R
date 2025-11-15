############################################
### Author: USERNAME
### Purpose: Rake scenarios of health expenditure sources into THE
############################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
variable <- args[1]
version <- args[2]

root_fold <- 'FILEPATH'

load(paste0(root_fold, 'FILEPATH'))

## Get location metadata
source('FILEPATH')
locs <- get_location_metadata(location_set_id = metadata_list$location_set_id, release_id = metadata_list$release_id)
locs <- locs[level == 3, .(iso3 = ihme_loc_id, location_name, super_region_name, region_name, location_id)]

########################
### (0) Load data
########################

load(paste0(root_fold, "FILEPATH"))


########################
### (1) Reshape all datasets long by draw
########################

dah_per_cap <- melt.data.table(dah_per_cap, c("iso3", "scenario", "year"), value.name = "dah_pc_stage1", variable.name = "draw")
ghes_per_cap <- melt.data.table(ghes_per_cap, c("iso3", "scenario", "year"), value.name = "ghes_pc_stage1", variable.name = "draw")
ppp_per_cap <- melt.data.table(ppp_per_cap, c("iso3", "scenario", "year"), value.name = "ppp_pc_stage1", variable.name = "draw")
oop_per_cap <- melt.data.table(oop_per_cap, c("iso3", "scenario", "year"), value.name = "oop_pc_stage1", variable.name = "draw")
the_per_cap_scenarios <- melt.data.table(the_per_cap_scenarios, c("iso3", "scenario", "year"), value.name = "the_pc_stage1", variable.name = "draw")

### Merge all data
all_data_merg <- merge(the_per_cap_scenarios, oop_per_cap, c("iso3", "scenario", "year", "draw"))
all_data_merg <- merge(all_data_merg, ppp_per_cap, c("iso3", "scenario", "year", "draw"))
all_data_merg <- merge(all_data_merg, ghes_per_cap, c("iso3", "scenario", "year", "draw"))
all_data_merg <- merge(all_data_merg, dah_per_cap, c("iso3", "scenario", "year", "draw"), all.x = T)
all_data_merg[is.na(dah_pc_stage1), dah_pc_stage1 := 0] # Fill in high income country year DAH


########################
### (2) Step 1: Rake all of the health expenditure sources to sum to THE per capita
########################

print("Rake all of the HEs to sum to THEpc")

## Create rake factor
all_data_merg[, rake_factor_1 := the_pc_stage1 / (ghes_pc_stage1 + ppp_pc_stage1 + oop_pc_stage1 + dah_pc_stage1)]

## Multiply all HEs with rake_factor_1
all_data_merg[, paste0(c("dah", "ghes", "ppp", "oop"), "_pc_stage2") := lapply(
  paste0(c("dah", "ghes", "ppp", "oop"), "_pc_stage1"),
  function(x) get(x) * rake_factor_1
)]

########################
### (3) Step 2: Get mean of the stage 2 HEs, and check if in
###     any instances, we have ref>better or ref<worse (in log space)
########################

print("Get deviation vectors")

scenario_fixer_s2 <- all_data_merg[, .(
  ghes_pc_s2 = mean(ghes_pc_stage2), ppp_pc_s2 = mean(ppp_pc_stage2),
  oop_pc_s2 = mean(oop_pc_stage2), dah_pc_s2 = mean(dah_pc_stage2)
),
by = c("iso3", "scenario", "year")
]

## Function to get the deviation fixer
scenario_fixing_funk <- function(data = scenario_fixer_s2, varname) {
  c_on_S <- dcast.data.table(scenario_fixer_s2[, .SD, .SDcols = c("iso3", "scenario", "year", varname)],
                             iso3 + year ~ scenario,
                             value.var = varname
  )
  
  ## Get the deviations in log space
  c_on_S[`0` == 0, `0` := 1e-7] # adding offset for zero DAH values
  c_on_S[`1` == 0, `1` := 1e-7] # adding offset for zero DAH values
  c_on_S[`-1` == 0, `-1` := 1e-7] # adding offset for zero DAH values
  c_on_S[, move_worse := log(`0`) - log(`-1`)]
  c_on_S[, move_better := log(`0`) - log(`1`)]
  
  c_on_S <- c_on_S[, .(iso3, year, move_worse, move_better)]
  
  ## Zero out all rows where move_* is non problem
  c_on_S[move_worse >= 0, move_worse := 0]
  c_on_S[move_better <= 0, move_better := 0]
  
  setnames(
    c_on_S,
    c("move_worse", "move_better"),
    paste0(varname, c("_worse", "_better"))
  )
  
  c_on_S
}

ghes_fixer <- scenario_fixing_funk(scenario_fixer_s2, "ghes_pc_s2")
ppp_fixer <- scenario_fixing_funk(scenario_fixer_s2, "ppp_pc_s2")
oop_fixer <- scenario_fixing_funk(scenario_fixer_s2, "oop_pc_s2")
dah_fixer <- scenario_fixing_funk(scenario_fixer_s2, "dah_pc_s2")


########################
### (4) Step 3: Convert all stage 2 HEs into logs, apply deviation to all draws
###             and remake THE per capita
########################

stage3_data <- all_data_merg[, .SD, .SDcols = c("iso3", "scenario", "year", "draw", 
                                                paste0(c("dah", "ghes", "ppp", "oop"), "_pc_stage2"))]
summary(stage3_data$dah_pc_stage2)

## Merge on the devs
stage3_data <- merge(stage3_data, ghes_fixer, c("iso3", "year"), all.x = T)
stage3_data <- merge(stage3_data, ppp_fixer, c("iso3", "year"), all.x = T)
stage3_data <- merge(stage3_data, oop_fixer, c("iso3", "year"), all.x = T)
stage3_data <- merge(stage3_data, dah_fixer, c("iso3", "year"), all.x = T)

## Set all NAs to zeros
stage3_data[is.na(stage3_data)] <- 0

## Convert to logs with small offset for zeroes, add divvy by scenario
## exponentiate, remove logs, and set lower bound to 0

## Set deviations to zero (for reference)

print("Correct scenarios")

stage3_data[scenario == 1, ghes_pc_stage3 := exp(log(ghes_pc_stage2 + 1e-7) + ghes_pc_s2_better) - 1e-7]
stage3_data[scenario == -1, ghes_pc_stage3 := exp(log(ghes_pc_stage2 + 1e-7) + ghes_pc_s2_worse) - 1e-7]
stage3_data[scenario == 0, ghes_pc_stage3 := ghes_pc_stage2]
stage3_data[ghes_pc_stage3 < 0, ghes_pc_stage3 := 0]

stage3_data[scenario == 1, ppp_pc_stage3 := exp(log(ppp_pc_stage2 + 1e-7) + ppp_pc_s2_better) - 1e-7]
stage3_data[scenario == -1, ppp_pc_stage3 := exp(log(ppp_pc_stage2 + 1e-7) + ppp_pc_s2_worse) - 1e-7]
stage3_data[scenario == 0, ppp_pc_stage3 := ppp_pc_stage2]
stage3_data[ppp_pc_stage3 < 0, ppp_pc_stage3 := 0]

stage3_data[scenario == 1, dah_pc_stage3 := exp(log(dah_pc_stage2 + 1e-6) + dah_pc_s2_better) - 1e-6]
stage3_data[scenario == -1, dah_pc_stage3 := exp(log(dah_pc_stage2 + 1e-6) + dah_pc_s2_worse) - 1e-6]
stage3_data[scenario == 0, dah_pc_stage3 := dah_pc_stage2]

stage3_data[scenario == 1, oop_pc_stage3 := exp(log(oop_pc_stage2 + 1e-6) + oop_pc_s2_better) - 1e-6]
stage3_data[scenario == -1, oop_pc_stage3 := exp(log(oop_pc_stage2 + 1e-6) + oop_pc_s2_worse) - 1e-6]
stage3_data[scenario == 0, oop_pc_stage3 := oop_pc_stage2]
stage3_data[oop_pc_stage3 < 0, oop_pc_stage3 := 0]


# Apply original reference scenarios ----------------------------------------------
stage3_ref <- copy(stage3_data)[scenario == 0 & year <= metadata_list$end_fit]
stage3_ref <- stage3_ref[, scenario := NULL][rep(1:.N, 3)][, scenario:= c(-1,0,1), by = c('iso3', 'year', 'draw')]
stage3_data <- rbindlist(list(stage3_ref, stage3_data[year > metadata_list$end_fit]),
                         use.names = T)

stage3_data[dah_pc_stage3 < 0, dah_pc_stage3 := 0]

stage3_data <- stage3_data[, .(iso3, scenario, year, draw,
                               ghes_pc = ghes_pc_stage3,
                               ppp_pc = ppp_pc_stage3,
                               dah_pc = dah_pc_stage3,
                               oop_pc = oop_pc_stage3
)]

## Remake THE per capita
stage3_data[, the_pc := ghes_pc + ppp_pc + dah_pc + oop_pc]

## Save out temporary file as checkpoint
saveRDS(stage3_data, file = paste0(root_fold, "FILEPATH"))


########################
### (5) Step 4: Create derivatives (per GDP, total, and per THE)
########################

print("Create derivatives (per GDP, total, and per THE)")

## Get best GDP per capita
gdppc <- fread('FILEPATH')
gdppc <- gdppc[scenario == 4.5 & year %in% c(metadata_list$start_year:metadata_list$end_FC), .(iso3, year, gdppc = mean)]

## Get populations
total_pop <- fread('FILEPATH')
total_pop <- total_pop[iso3 %in% locs$iso3]
total_pop <- total_pop[, .(year, iso3, pop_totes = population)]

#####

### Full dataset matrix ###
full_data <- merge(stage3_data, gdppc, c("iso3", "year"), all.x = T)
full_data <- merge(full_data, total_pop, c("iso3", "year"), all.x = T)
setkeyv(full_data, c("iso3", "scenario", "year", "draw"))


#### First, make HEs per GDP (HE per capita / GDPpc)
full_data[, paste0(c("ghes", "ppp", "dah", "oop", "the"), "_gdp") := lapply(
  c("ghes", "ppp", "dah", "oop", "the"),
  function(d) get(paste0(d, "_pc")) / gdppc
)]

#### Then, make totals (HE per capita * pop_totes)
full_data[, paste0(c("ghes", "ppp", "dah", "oop", "the"), "_totes") := lapply(
  c("ghes", "ppp", "dah", "oop", "the"),
  function(d) get(paste0(d, "_pc")) * pop_totes
)]

summary(full_data$dah_totes)


#### Finally, make HE per THE (each total HE over THE total)
full_data[, paste0(c("ghes", "ppp", "dah", "oop"), "_the") := lapply(
  c("ghes", "ppp", "dah", "oop"),
  function(d) get(paste0(d, "_totes")) / the_totes
)]
full_data[, gdppc := NULL]
full_data[, pop_totes := NULL]

### Pooled expenditure estimates: POOLED = DAH + GHE + PPP
full_data[, pooled_pc := ghes_pc + ppp_pc + dah_pc]
full_data[, pooled_gdp := ghes_gdp + ppp_gdp + dah_gdp]
full_data[, pooled_the := ghes_the + ppp_the + dah_the]
full_data[, pooled_totes := ghes_totes + ppp_totes + dah_totes]


print("Save out full matrix")

### Save out full matrix of draws
saveRDS(full_data, file = paste0(root_fold, "FILEPATH"))
write_feather(full_data, paste0(root_fold, "FILEPATH"))


###### Calculate means and UIs ######

## Get all the column names requiring statistics calculation
stat_cols <- setdiff(colnames(full_data), c("iso3", "year", "scenario", "draw"))

print("Stat over them using threads")
registerDoParallel(cores = length(stat_cols))

print("Stat everything")
system.time(full_stats <- foreach(var = stat_cols) %dopar% {
  data_use <- full_data[, .SD, .SDcols = c("iso3", "scenario", "year", "draw", var)]
  setnames(data_use, var, "data")
  stat_dat <- stat_maker(data_use, melt = F, merge = F, idvar = c("iso3", "scenario", "year"))
  stat_dat[, metric := paste0(var)]
  stat_dat
})


full_stats_binded <- rbindlist(full_stats)


########################
### (6) Step 5: save out
########################

## Full stats
write_feather(full_stats_binded, paste0(root_fold, "FILEPATH"))

print("Save individual metric over threads")

dir.create(paste0(root_fold, "FILEPATH"))
dir.create(paste0(root_fold, "FILEPATH"))

system.time(foreach(var = stat_cols) %dopar% {
  
  ## Save stats
  fwrite(
    full_stats_binded[metric == var, .(iso3, scenario, year, mean, lower, upper)],
    paste0(root_fold, "FILEPATH")
  )
  
  ## Save draws
  write_feather(
    full_data[, .SD, .SDcols = c("iso3", "scenario", "draw", "year", var)],
    paste0(root_fold, "FILEPATH")
  )
  
  return(NULL)
})

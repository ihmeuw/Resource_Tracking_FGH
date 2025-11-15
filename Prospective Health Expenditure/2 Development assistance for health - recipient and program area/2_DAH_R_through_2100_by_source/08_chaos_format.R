############################################
### Author: USERNAME
### Purpose: Processes chaos data and correlates draws
############################################

rm(list = ls())

library(AFModel)

## Open the parser
args <- commandArgs(trailingOnly = TRUE)
source <- args[1]
variable <- args[2]
date <- args[3]
comment <- args[4]
split_run <- args[5]
nons_path <- args[6]

root_fold <- 'FILEPATH'
load(paste0(root_fold, 'FILEPATH'))

final_cum_sum_hack <- function (root_fold, chaos, scenario = F, oos_years, N_draws, 
                                rev_trans = NULL, pop_data = NULL, pop_var = "total_pop", 
                                pop_action = NULL, hack_drop_NAs = F, hack_zero_bads = F) 
{
  if (chaos) {
    if (!scenario) {
      yvar_draws <- input_data <- fread(gsub(".csv", paste0("_", source, ".csv"), metadata_list$prepped_inputs_path))[year <= metadata_list$end_fit]
      yvar_draws <- yvar_draws[, .(iso3, year, draw_1 =  orig_logit_actual_dah_per_totes)]
      yvar_draws[, paste0('draw_', c(2:metadata_list$N_draws)):= draw_1]
      yvar_draws <- yvar_draws[year == 1999]
      
      print("Bring all the Chaos compiles into memory")
      chaos_compiles <- rbindlist(lapply(c(1:oos_years), 
                                         function(f) readRDS(paste0(root_fold, "FILEPATH"))))
      chaos_compiles <- chaos_compiles[year >= 2000]
      chaos_compiles <- rbind(chaos_compiles, yvar_draws)
      setorder(chaos_compiles, "iso3", "year")
    }
    else {
      print("Bring all the Chaos compiles into memory")
      chaos_compiles_worse <- rbindlist(lapply(c(1:oos_years), 
                                               function(f) readRDS(paste0(root_fold, "FILEPATH"))))[, `:=`(scenario, -1)]
      chaos_compiles_better <- rbindlist(lapply(c(1:oos_years), 
                                                function(f) readRDS(paste0(root_fold, "FILEPATH"))))[, `:=`(scenario, 1)]
      chaos_compiles <- rbindlist(list(chaos_compiles_better, 
                                       chaos_compiles_worse))
    }
  }
  else {
    if (!scenario) {
      print("Bring all the draws into memory")
      chaos_compiles <- readRDS(paste0(root_fold, "FILEPATH"))
    }
    else {
      print("Bring all the draws into memory")
      chaos_compiles_worse <- readRDS(paste0(root_fold, "FILEPATH"))[, `:=`(scenario, -1)]
      chaos_compiles_better <- readRDS(paste0(root_fold, "FILEPATH"))[, `:=`(scenario, 1)]
      chaos_compiles <- rbindlist(list(chaos_compiles_better, 
                                       chaos_compiles_worse))
    }
  }
  if (!scenario) {
    setkeyv(chaos_compiles, c("iso3", "year"))
    print("Cumsum all draws for each country, and reverse transform")
    chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                          lapply(paste0("draw_", c(1:N_draws)), function(x) cumsum(get(paste0(x))))), 
                   by = c("iso3")]
    if (!is.null(rev_trans)) {
      chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                            lapply(paste0("draw_", c(1:N_draws)), function(x) get(rev_trans)(get(paste0(x)))))]
    }
    if (!is.null(pop_data)) {
      chaos_compiles <- merge(chaos_compiles, pop_data, 
                              c("iso3", "year"))
      if (pop_action == "div") {
        chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                              lapply(paste0("draw_", c(1:N_draws)), function(x) get(paste0(x))/get(pop_var)))]
      }
      else if (pop_action == "multiply") {
        chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                              lapply(paste0("draw_", c(1:N_draws)), function(x) get(paste0(x)) * 
                                       get(pop_var)))]
      }
      chaos_compiles[, `:=`((pop_var), NULL)]
    }
    if (hack_drop_NAs) {
      chaos_compiles <- chaos_compiles[!is.na(draw_1)]
    }
    if (hack_zero_bads) {
      chaos_compiles[is.na(chaos_compiles)] <- 0
      chaos_compiles <- replace(chaos_compiles, is.na(chaos_compiles), 
                                0)
    }
    chaos_stats <- stat_maker(data = chaos_compiles, melt = T, 
                              merge = F)
  }
  else {
    setkeyv(chaos_compiles, c("iso3", "scenario", "year"))
    print("Cumulative sum all draws for each country, and reverse transform")
    chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                          lapply(paste0("draw_", c(1:N_draws)), function(x) cumsum(get(paste0(x))))), 
                   by = c("iso3", "scenario")]
    chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                          lapply(paste0("draw_", c(1:N_draws)), function(x) get(rev_trans)(get(paste0(x)))))]
    if (!is.null(pop_data)) {
      chaos_compiles <- merge(chaos_compiles, pop_data, 
                              c("iso3", "year"))
      if (pop_action == "div") {
        chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                              lapply(paste0("draw_", c(1:N_draws)), function(x) get(paste0(x))/get(pop_var)))]
      }
      else if (pop_action == "multiply") {
        chaos_compiles[, `:=`(paste0("draw_", c(1:N_draws)), 
                              lapply(paste0("draw_", c(1:N_draws)), function(x) get(paste0(x)) * 
                                       get(pop_var)))]
      }
      chaos_compiles[, `:=`((pop_var), NULL)]
    }
    if (hack_drop_NAs) {
      chaos_compiles <- chaos_compiles[!is.na(draw_1)]
    }
    if (hack_zero_bads) {
      chaos_compiles[is.na(chaos_compiles)] <- 0
      chaos_compiles <- replace(chaos_compiles, is.na(chaos_compiles), 
                                0)
    }
    chaos_stats <- stat_maker(data = chaos_compiles, idvar = c("iso3", 
                                                               "scenario", "year"), melt = T, merge = F)
  }
  return(list(draws = chaos_compiles, stats = chaos_stats))
}

########################################
#### (7) Fix past draw data because missingness and alignment with GPR draws ######
########################################


## Make draws
yvar_draws <- input_data <- fread(gsub(".csv", paste0("_", source, ".csv"), metadata_list$prepped_inputs_path))[year <= metadata_list$end_fit]
yvar_draws <- yvar_draws[, .(iso3, year, draw_1 =  orig_logit_actual_dah_per_totes)]
yvar_draws[, paste0('draw_', c(2:metadata_list$N_draws)):= draw_1]


yvar_draws[, paste0("diff_draw_", c(1:metadata_list$N_draws)) := lapply(
  paste0("draw_", c(1:metadata_list$N_draws)),
  function(v) get(v) - data.table::shift(get(v))
), by = "iso3"]

yvar_draws[year > metadata_list$start_year, paste0("draw_",
                                                   c(1:metadata_list$N_draws)) := lapply(
                                                     paste0("diff_draw_", c(1:metadata_list$N_draws)),
                                                     function(v) (get(v))
                                                   )]
yvar_draws[, paste0("diff_draw_", c(1:metadata_list$N_draws)) := NULL]


zero_countries <- input_data[year==metadata_list$end_fit & dah_recip_total==0, iso3]
non_country_recipients <- c("INKIND", "OTH", "WLD", "UNALLOCABLE_DAH_INK", "UNALLOCABLE_DAH_WLD", "UNALLOCABLE_DAH_QZA")
yvar_draws2 <- yvar_draws[!(iso3 %in% c(zero_countries, non_country_recipients))]


print("Make past draw corrections")

if (metadata_list$chaos) {
  chaos_output <- readRDS(paste0(root_fold, "FILEPATH"))
  chaos_output <- chaos_output[year >= eval(metadata_list$end_fit + 1)]
  chaos_output <- rbindlist(list(chaos_output, yvar_draws2), use.names = T)
  setkeyv(chaos_output, c("iso3", "year"))
  
  saveRDS(chaos_output, paste0(root_fold, "FILEPATH"))
  
} else {
  chaos_output <- readRDS(paste0(root_fold, "FILEPATH"))
  chaos_output <- chaos_output[year >= eval(metadata_list$end_fit + 1)]
  chaos_output <- rbindlist(list(chaos_output, yvar_draws2), use.names = T)
  setkeyv(chaos_output, c("iso3", "year"))
  
  saveRDS(chaos_output, paste0(root_fold, "FILEPATH"))
}


## Cumulative sum all the data and convert to GDP per capita
if(source == "GATES") {
  chaos_list <- final_cum_sum_hack(
    root_fold = root_fold,
    chaos = metadata_list$chaos,
    oos_years = metadata_list$oos_years,
    N_draws = metadata_list$N_draws,
    rev_trans = "invlogit",
    hack_zero_bads = T
  )
  
} else {
  chaos_list <- final_cum_sum(
    root_fold = root_fold,
    chaos = metadata_list$chaos,
    oos_years = metadata_list$oos_years,
    N_draws = metadata_list$N_draws,
    rev_trans = "invlogit",
    hack_zero_bads = T
  )
  
}


## Drop all non countries
chaos_list[["draws"]] <- chaos_list[["draws"]][!(iso3 %in% non_country_recipients)]
chaos_list[["stats"]] <- chaos_list[["stats"]][!(iso3 %in% non_country_recipients)]


### Add back zero countries and zero those fractions out into the future
zerocounts <- input_data[(iso3 %in% as.character(zero_countries)), .(iso3, year, orig_logit_actual_dah_per_totes)]
zerocounts <- dcast(zerocounts, iso3 ~ year, value.var = "orig_logit_actual_dah_per_totes")
zerocounts[, paste0(c(eval(metadata_list$end_fit + 1):metadata_list$end_FC)):= get(as.character(metadata_list$end_fit))]
zerocounts <- melt(zerocounts, c('iso3'), variable.name = 'year', value.name = 'orig_logit_actual_dah_per_totes')
zerocounts[, year:= as.numeric(as.character(year))]
zerocounts <- zerocounts[, .(iso3, year, draw_1 =  invlogit(orig_logit_actual_dah_per_totes))]
zerocounts[, paste0('draw_', c(2:metadata_list$N_draws)):= draw_1]
setkeyv(zerocounts, c('iso3', 'year'))

zerocounts_stats <- stat_maker(zerocounts, melt = T, merge = F, idvar = c('iso3', 'year'))

chaos_list[["draws"]] <- rbindlist(list(chaos_list[["draws"]][!(iso3 %in% as.character(zero_countries))], zerocounts), fill = F, use.names = T)
chaos_list[["stats"]] <- rbindlist(list(chaos_list[["stats"]][!(iso3 %in% as.character(zero_countries))], zerocounts_stats), fill = F, use.names = T)


## Manually zero out countries
zero_countries <- input_data[year==metadata_list$end_fit & dah_recip_total==0, iso3]
chaos_list[["draws"]][iso3 %in% as.character(zero_countries) & year>=metadata_list$end_fit,  paste0('draw_', c(1:metadata_list$N_draws)):= 0]
chaos_list[["stats"]][iso3 %in% as.character(zero_countries) & year>=metadata_list$end_fit,  c('mean', 'upper', 'lower'):= 0]

chaos_list[["draws"]] <- chaos_list[["draws"]][order(iso3, year)] 
chaos_list[["stats"]] <- chaos_list[["stats"]][order(iso3, year)] 

## Save out
write_feather(chaos_list[["draws"]], paste0(root_fold, "FILEPATH"))
fwrite(chaos_list[["stats"]], paste0(root_fold, "FILEPATH"))

# Get observed historic data through 2030
# Intercept shift fraction forecasts to actual draws at 2030
if(metadata_list$no_covid_model) {
  actual_input_data <- fread(gsub(".csv", paste0("_", source, ".csv"), metadata_list$prepped_inputs_path))[year <= 2030]
  actual_input_data <- actual_input_data[, .(iso3, year, draw_1 =  orig_dah_per_totes)]
  actual_input_data[, paste0('draw_', c(2:metadata_list$N_draws)):= draw_1]
  actual_input_data <- data.table::melt(actual_input_data, id.vars = c("year", "iso3"))
  
  draws_dt <- chaos_list[["draws"]]
  draws_dt <- data.table::melt(draws_dt, id.vars = c("year", "iso3"))
  draws_dt <- merge(draws_dt, actual_input_data, by = c("year", "iso3", "variable"), all = T)
  draws_dt[year == 2030, int_shift := value.x - value.y]
  draws_dt[, int_shift := mean(int_shift, na.rm = T), by = c("iso3", "variable")]
  draws_dt[year >= 2030, shifted_forecast := value.x - int_shift]
  draws_dt[year < 2030, shifted_forecast := value.y]
  draws_dt[shifted_forecast < 0, shifted_forecast := 1e-10]
  draws_dt <- draws_dt[, .(year, iso3, variable, shifted_forecast)]
  draws_dt_stats <- copy(draws_dt)
  draws_dt_stats <- draws_dt_stats[, .(mean = mean(shifted_forecast), 
                                       lower=quantile(shifted_forecast, 0.025), 
                                       upper = quantile(shifted_forecast, 0.975)), by = c("iso3", "year")]
  draws_dt <- data.table::dcast(draws_dt, iso3 + year ~ variable, value.var = "shifted_forecast")
  
  chaos_list[["draws"]] <- draws_dt
  chaos_list[["stats"]] <- draws_dt_stats
  metadata_list$end_fit <- 2030
  
  write_feather(chaos_list[["draws"]], paste0(root_fold, "FILEPATH"))
  fwrite(chaos_list[["stats"]], paste0(root_fold, "FILEPATH"))
}


########################################
#### (7) Bring on the non countries and normalize fractions
########################################

## Bind on to chaos draws - retrospective non countries only
new_draws <- copy(chaos_list$draws)[year > metadata_list$end_fit]

## Normalize all fractions so that they sum to 1 per year
new_draws <- melt(new_draws, c('iso3', 'year'))
new_draws[, value_normd:= value / sum(value), by = c('variable', 'year')]
new_draws[is.nan(value_normd), value_normd := 0]
new_draws[, value := NULL]

## Rake to envelope less non-donor countries
non_countries <- fread(gsub(".csv", paste0("_source_", source, ".csv"), nons_path))

non_countries_frac <- non_countries[year > metadata_list$end_fit & year <= metadata_list$end_FC,
                                    .(non_prop_totes = sum(dah_prop_totes)), by = year]

non_countries[, paste0('draw_', c(1:metadata_list$N_draws)):= dah_prop_totes]
non_countries[, dah_prop_totes := NULL]


## Rake
new_draws <- merge(new_draws, non_countries_frac, by = "year")
new_draws[year > metadata_list$end_fit, value_normd := value_normd*(1-non_prop_totes)]
new_draws[, non_prop_totes := NULL]


## Bind on to chaos draws
non_countries <- data.table::melt(non_countries[year > metadata_list$end_fit],
                                  id.vars = c("iso3", "year"),
                                  value.name = "value_normd")
new_draws <- rbindlist(list(new_draws, non_countries), use.names = T)


retro_draws <- copy(chaos_list$draws)[year <= metadata_list$end_fit]
retro_draws <- data.table::melt(retro_draws, id.vars = c("iso3", "year"),
                                value.name = "value_normd")
new_draws <- rbind(retro_draws, new_draws)


## Cast fractions and save out
setnames(new_draws, 'value_normd', 'data')
frax_stats <- stat_maker(new_draws, melt = F, merge = F, idvar = c('iso3', 'year'))
frax_draws <- dcast(new_draws, iso3 + year ~ variable, value.var = 'data')

write_feather(frax_draws, paste0(root_fold, "FILEPATH"))
fwrite(frax_stats, paste0(root_fold, "FILEPATH"))

# ## Correlated draws with AR1 covariance

if(date %like% "ref") {
  dah_d <- fread(paste0("FILEPATH"))
} else if(date %like% "better") {
  dah_d <- fread(paste0('FILEPATH'))
  setnames(dah_d, 'mean', 'dah_donor_ref')
} else if(date %like% "worse") {
  dah_d <- fread(paste0("FILEPATH"))
  setnames(dah_d, 'dah_donor_ref_ACTUAL', 'dah_donor_ref')
}
dah_d <- dah_d[year == 2025]
dah_d <- dah_d[!iso3 %in% c("AGGREGATE", "AGG", "PRIVATE", "DEBT", "OTHER", "UNALL", "CHN", "OTHERPUB_NONGBD")]
top_10_sources <- dah_d[order(-dah_donor_ref)][1:10, iso3]
all_sources <- c(top_10_sources, "RESID")


## Convert into total space by multiplying with draws of the envelope
if(date %like% "ref") {
  env_draws <- data.table(fread(paste0('FILEPATH')))[iso3 != "AGGREGATE"]
  setnames(env_draws, "dah_donor_ref", "dah_donor")
} else if(date %like% "better") {
  env_draws <- fread(paste0('FILEPATH'))[iso3 != 'AGGREGATE']
  setnames(env_draws, "mean", "dah_donor")
} else if(date %like% "worse") {
  env_draws <- data.table(fread(paste0('FILEPATH')))[iso3 != "AGGREGATE"]
  setnames(env_draws, "dah_donated_worse", "dah_donor")
}

env_draws[!iso3 %in% top_10_sources, iso3 := "RESID"]
env_draws <- env_draws[iso3 == source]
env_draws <- env_draws[, .(dah_donor = sum(dah_donor)), by = "year"]

## Merge with frax and multiply to get total DAH received
frax_draws_long <- data.table::melt(frax_draws, id.vars = c('iso3', 'year'))
dah_totes <- merge(env_draws, frax_draws_long, c('year'), all.x=T)

## Calculate
dah_totes[, data:= value*dah_donor]
dah_totes[, c('dah_donor', 'value'):= NULL]

if(source == "GATES") {
  dah_totes <- dah_totes[year >= 1999]
}

dah_totes_stats <- stat_maker(dah_totes, melt = F, merge = F, idvar = c('iso3', 'year'))
dah_totes <- dcast(dah_totes, iso3 + year ~ variable, value.var = 'data')


write_feather(dah_totes, paste0(root_fold, "FILEPATH"))
fwrite(dah_totes_stats, paste0(root_fold, "FILEPATH"))

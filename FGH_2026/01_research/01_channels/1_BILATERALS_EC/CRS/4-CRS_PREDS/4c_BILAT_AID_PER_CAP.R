#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Create CRS Bilateral Aid per capita
#---------------------# ####
#' Note: this is used by the NGO channel. Make sure that the population data
#' loaded below exists through the report-year, because the NGO channel needs
#' it to extend through the report-year in order to make predictions

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(dah.roots$k, 'FILEPATH/get_population.R'))
pacman::p_load(crayon)

# Variable prep
last_obs_year <- get_dah_param('CRS', 'data_year')
data_lag <- dah_cfg$report_year - last_obs_year
#----------------------------# ####


cat('\n\n')
cat(green(' ##################################\n'))
cat(green(' #### CRS BILAT AID PER CAPITA ####\n'))
cat(green(' ##################################\n\n'))


cat('  Read in bilateral preds\n')
#----# Read in bilateral preds #----# ####
dt <- fread(get_path('BILAT_PREDICTIONS', 'fin', 'Bilateral_predictions_[report_year]_[crs.update_mmyy].csv'))
dt <- dt[, lapply(.SD, sum, na.rm = TRUE),
         by = .(YEAR, ISO3),
         .SDcols = c("ODA",
                     grep(paste0(dah_cfg$abrv_year, "|frct"), names(dt), value = TRUE))
         ]

# Convert to millions
for (col in c('ODA', names(dt)[names(dt) %like% dah.roots$abrv_year | names(dt) %like% 'frct'])) {
  dt[, eval(col) := get(col) / 10^6]
  dt[get(col) == 0, eval(col) := as.numeric(NA)]
}

dt <- dt[ISO3 == 'USA', ]

if (data_lag == 2) {
  dt[YEAR == dah.roots$report_year,
     eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := get(paste0('final_prediction_', dah.roots$abrv_year))]
}
dt[YEAR >= last_obs_year, eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := get(paste0('final_prediction_', dah.roots$abrv_year))]
dt <- dt[, c('YEAR', paste0('OUTFLOW_', dah.roots$abrv_year)), with=F]
setnames(dt, 'YEAR', 'year')
#-----------------------------------# ####

cat('  Pull population data\n')
#----# Pull population data #----# ####
pop <- get_population(age_group_id = 22, # all ages
                      release_id = get_dah_param("gbd_release_id"),
                      location_id = 102, # USA
                      sex_id = 3, #both
                      year_id = c(1990:dah.roots$report_year))[, c('year_id', 'population')] 
                      
setnames(pop, c('population', 'year_id'), c('total_pop', 'year'))
pop[, ISO3 := 'USA']

stopifnot(max(pop$year) >= dah.roots$report_year) 
#--------------------------------# ####

cat('  Merge + format\n')
#----# Merge + format #----# ####
dt <- merge(pop, dt, by='year', all.x=T)
dt[, total_pop := total_pop / 10^6]
dt[, eval(paste0('US_OUTFLOW_', dah.roots$abrv_year, '_PC')) :=
       get(paste0('OUTFLOW_', dah.roots$abrv_year)) / total_pop]
#--------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'US BILAT AID PER CAPITA 1990-[report_year]_[crs.update_mmyy]',
             'ngo', 'raw')
#------------------------# ####

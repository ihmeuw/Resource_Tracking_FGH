#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Configure + launch CRS EC TT_Smooth
#  We have already predicted overall EC DAH and disaggregated by HFA. Now we take
#  these estimates and disaggregate them into the EC member countries.
#  So we launch TT smooth again to predict the amount of each HFA that belongs
#  to each member country.
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon, magrittr, stringr)
# Variable prep
last_obs_year <- get_dah_param('CRS', 'data_year')
data_lag <- dah.roots$report_year - last_obs_year
defl <- paste0(dah.roots$j, 'FILEPATH')
#----------------------------# ####


cat('\n\n')
cat(green(' #############################################\n'))
cat(green(' #### CRS CONFIGURE + LAUNCH EC TT_SMOOTH ####\n'))
cat(green(' #############################################\n\n'))


cat('  Read in CRS ADB_PDB\n')
#----# Read in CRS ADB_PDB #----# ####
dt <- fread(get_path("crs", "int", "adb_for_preds.csv"))
dt <- dt[ELIM_CH != 1 & CHANNEL == 'EC', ]
#-------------------------------# ####

cat('  Format data for TT_smooth launching\n')
#----# Format data for TT_smooth launching #----# ####
dt[, INCOME_SECTOR := ISO_CODE]

# Merge deflators + deflate
rates <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
dt <- merge(dt, rates, by='YEAR', all.x=T)

for (col in names(dt)[names(dt) %like% '_DAH']) {
  dt[, eval(paste0(col, '_', dah.roots$abrv_year)) := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
}
rm(rates, defl, col)

dt <- collapse(dt, 'sum', c('YEAR', 'INCOME_SECTOR'), names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year)])
dt <- dcast.data.table(dt, formula = 'YEAR ~ INCOME_SECTOR', value.var = names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year) & 
                                                                                      !(names(dt) %like% 'unalloc')])

# Merge EC preds by HFA
ec <- fread(get_path('BILAT_EC', 'fin', 'EC_PREDS_BY_HFA_1990_[report_year]_[crs.update_mmyy].csv'))
ec <- ec[, c("YEAR",
             grep(paste0("_DAH_", dah_cfg$abrv_year), names(ec), value = TRUE)),
         with = FALSE]
ec <- merge(ec, dt, by='YEAR', all.x=T)
ec[, CHANNEL := "EC"]

dt <- copy(ec)
rm(ec)
#-----------------------------------------------# ####

cat('  Configure + launch TT_Smooth\n')
#----# Configure + launch TT_Smooth #----# ####
# Clean out old TT_smooth files
to_delete <- list.files(get_path('CRS', 'int', 'tt_smooth_datasets/EC/'))
for (file in to_delete) {
  unlink(get_path('CRS', 'int', paste0('tt_smooth_datasets/EC/', file)))
}
rm(file, to_delete)

# Create logs folder if it doesn't exist
dir.create(paste0(dah.roots$h, 'crs_tt_smooth_logs'), showWarnings = FALSE)

# Create HFA list
hfas <- names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year) & !(names(dt) %like% paste0('_DAH_', dah.roots$abrv_year, '_'))] %>%
  str_replace_all(paste0('_DAH_', dah.roots$abrv_year), '')
# Create countries list
countries <- names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year, '_')]
countries <- substr(countries, nchar(countries) - 2, nchar(countries)) %>% unique()
dir.create(get_path('CRS', 'int', 'tt_smooth_datasets/EC'), showWarnings = FALSE)
for (hfa in hfas) {
  t <- copy(dt[, c('YEAR', paste0(hfa, '_DAH_', dah.roots$abrv_year), paste0(hfa, '_DAH_', dah.roots$abrv_year, '_', countries)), with=F])
  t <- rowtotal(t, paste0(hfa, '_DAH_', dah.roots$abrv_year, '_new'), paste0(hfa, '_DAH_', dah.roots$abrv_year, '_', countries))
  t[YEAR <= last_obs_year, eval(paste0(hfa, '_DAH_', dah.roots$abrv_year)) := get(paste0(hfa, '_DAH_', dah.roots$abrv_year, '_new'))]
  t[, eval(paste0(hfa, '_DAH_', dah.roots$abrv_year, '_new')) := NULL]
  t[get(paste0(hfa, '_DAH_', dah.roots$abrv_year)) == 0, eval(paste0(hfa, '_DAH_', dah.roots$abrv_year)) := 1]
  
  # Save pre-tt-smooth data
  save.dta13(data = t,
             file = paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/EC/crs_ec_', hfa, '_pre_tt_smooth.dta'))
  # create tt_config
  create_TT_config(data_path = paste0(get_path('CRS', 'int'), 'tt_smooth_datasets/EC/crs_ec_', hfa, '_'),
                   channel_name = paste0('crs_ec_', hfa), 
                   total_colname = paste0(hfa, '_DAH_', dah.roots$abrv_year),
                   year_colname = 'YEAR',
                   num_yrs_forecast = data_lag,
                   is_test = 0, 
                   hfa_list = paste0(hfa, '_DAH_', dah.roots$abrv_year, '_', countries))
  # launch_tt-smooth
  launch_TT_smooth(channel_name = paste0('crs_ec_', hfa),
                   runtime='00:01:00',
                   job_mem = 1,
                   errors_path = paste0(dah.roots$h, 'crs_tt_smooth_logs/ec/', hfa, '.err'),
                   output_path = paste0(dah.roots$h, 'crs_tt_smooth_logs/ec/', hfa, '.out'))
}
#----------------------------------------# ####
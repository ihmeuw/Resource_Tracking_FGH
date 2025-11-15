#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Configure + launch CRS EC TT_smooth
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)

# Variable prep
last_obs_year <- get_dah_param('CRS', 'data_year')
data_lag <- dah.roots$report_year - last_obs_year

#----------------------------# ####


cat('\n\n')
cat(green(' #############################################\n'))
cat(green(' #### CRS CONFIGURE + LAUNCH EC TT_SMOOTH ####\n'))
cat(green(' #############################################\n\n'))


cat('  Read in EU Member Trends Preds\n')
#----# Read in EU Member Trends Preds #----# ####
dt <- fread(get_path('BILAT_EC', 'fin',
                     'EC_PREDS_EU_MEMBER_TRENDS_1990_[report_year]_[crs.update_mmyy].csv'))
dt[CHANNEL == "", CHANNEL := 'EC']
dt <- dt[, !c('unalloc_DAH', 'other_DAH')]
dt <- setDT(dt)

to_calc <- names(dt)[names(dt) %like% '_DAH']
for (col in to_calc) {
  dt[, eval(paste0(col, '_', dah.roots$abrv_year)) := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
  dt[YEAR > last_obs_year, eval(col) := NA]
}

hfas_def <- paste0(to_calc, '_', dah.roots$abrv_year)
hfas_def_other <- c(hfas_def, paste0('other_DAH_', dah.roots$abrv_year))

dt <- rowtotal(dt, 'total_alloc', hfas_def)
dt[, eval(paste0('other_DAH_', dah.roots$abrv_year)) := get(paste0('OUTFLOW_', dah.roots$abrv_year)) - total_alloc]
dt[YEAR > last_obs_year, eval(paste0('other_DAH_', dah.roots$abrv_year)) := NA]
#------------------------------------------# ####

cat('  Configure + launch EC TT_Smooth\n')
#----# Configure + launch EC TT_Smooth #----# ####
# Save pre_tt_smooth dataset
save.dta13(dt, get_path('CRS', 'int', 'crs_ec_pre_tt_smooth.dta'))

post_tt <- get_path('CRS', 'int', 'crs_ec_post_tt_smooth.dta')
if (file.exists(post_tt))
    file.remove(post_tt)

# Create tt_config
create_TT_config(data_path = paste0(get_path('CRS', 'int'), 'crs_ec_'),
                 channel_name = 'crs_ec', 
                 total_colname = paste0('OUTFLOW_', dah.roots$abrv_year),
                 year_colname = 'YEAR', 
                 num_yrs_forecast = data_lag,
                 is_test = 0,
                 hfa_list = hfas_def_other)
# Launch tt_smooth
launch_TT_smooth(channel_name = 'crs_ec', runtime='00:01:00', job_mem = 1,
                 output_path = file.path(dah_cfg$h, "4g_ec.out"),
                 errors_path = file.path(dah_cfg$h, "4g_ec.err")
                 )
#-------------------------------------------# ####
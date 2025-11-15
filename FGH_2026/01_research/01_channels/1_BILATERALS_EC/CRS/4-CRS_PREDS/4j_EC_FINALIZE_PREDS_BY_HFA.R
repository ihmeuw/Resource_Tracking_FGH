#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize CRS EC Predictions by HFA
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon, magrittr, stringr)

last_obs_year <- get_dah_param('CRS', 'data_year')
dah_yy <- paste0("DAH_", dah_cfg$abrv_year)
#----------------------------# ####


cat('\n\n')
cat(green(' ######################################\n'))
cat(green(' #### CRS FINALIZE EC PREDS BY HFA ####\n'))
cat(green(' ######################################\n\n'))


cat('  Read in post-TT_Smooth data\n')
#----# Read in post-TT_Smooth data #----# ####
dt <- data.table()
all <- list.files(get_path('CRS', 'int', 'tt_smooth_datasets/EC/'))
pre <- all[all %like% 'pre_']
post <- all[all %like% 'post_']

# If not all files output properly
if (length(pre) != length(post)) {
  stop(red('    FATAL ERROR!! NOT ALL JOBS FINISHED. CHECK OUTPUT LOGS & RERUN FAILED JOBS.'))
} else {
  cat(yellow(paste0('    ', length(post), ' datasets to append\n      ')))
  for (file in post) {
    cat(paste0(match(file, post), ','))
    t <- as.data.table(
        read.dta13(get_path('CRS', 'int', c('tt_smooth_datasets' , 'EC', file)))
    )
    if (nrow(dt) == 0) {
      dt <- copy(t)
    } else {
      dt <- merge(dt, t, by='YEAR', all=T)
    }
    rm(t)
  }
  cat('\n')
}
#---------------------------------------# ####

cat('  Update HFAs with predictions\n')
#----# Update HFAs with predictions #----# ####
# Create HFA list
hfas <- gsub(paste0("_", dah_yy), "",
             grep(paste0("_", dah_yy, "$"), names(dt), value = TRUE))
# Create countries list
countries <- names(dt)[names(dt) %like% paste0('_DAH_', dah.roots$abrv_year, '_')]
countries <- substr(countries, nchar(countries) - 2, nchar(countries)) %>% unique()

h <- 1
for (hfa in hfas) {
  cat(yellow('Working on ',hfa, ' (',h, ' of ', length(hfas),'):\n' ))
  for (source in paste0(hfa, '_DAH_', dah.roots$abrv_year, '_', countries)) {
    cat(source,', ')
    dt[YEAR > last_obs_year, eval(source) := get(paste0('pr_', source))]
  }
  h <- h + 1
  cat('\n')
}
rm(h)
dt[, CHANNEL := 'EC']
# Reshape
dt <- dt[, !c(names(dt)[names(dt) %like% 'pr_' | (names(dt) %like% paste0('_DAH_', dah.roots$abrv_year) & 
                                                    !(names(dt) %like% paste0('_DAH_', dah.roots$abrv_year, '_')))]), with=F]
dt <- melt.data.table(dt, measure.vars = names(dt)[names(dt) %like% 'DAH'])
dt$variable <- as.character(dt$variable)
dt[, INCOME_SECTOR := substr(variable, nchar(variable) - 2, nchar(variable))]
dt[, variable := substr(variable, 1, nchar(variable) - 4)]
dt <- dcast.data.table(dt, formula='YEAR + CHANNEL + INCOME_SECTOR ~ variable', value.var='value')

for (col in names(dt)[names(dt) %like% '_DAH_']) {
  dt[is.na(get(col)), eval(col) := 0]
}
setnames(dt, names(dt)[names(dt) %like% '_DAH_'], paste0('pr_', names(dt)[names(dt) %like% '_DAH_']))
dt[, ISO_CODE := INCOME_SECTOR]
dt <- rowtotal(dt, paste0('OUTFLOW_', dah.roots$abrv_year), names(dt)[names(dt) %like% '_DAH_'])
#----------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'EC_PREDS_BY_HFA_SOURCE_1990_[report_year]_[crs.update_mmyy]',
             'BILAT_EC', 'fin')
#------------------------# ####

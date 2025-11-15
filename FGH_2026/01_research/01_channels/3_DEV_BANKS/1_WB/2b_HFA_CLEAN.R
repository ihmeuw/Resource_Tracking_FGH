#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Post keyword search cleaning
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

# Variable prep
HFA <- paste0(dah.roots$j, 'FILEPATH')
WB_THEMES <- file.path(code_repo, paste0("FGH_", report_year),
                       "dahutil", "inst", "wb_themes.csv")

# Checking function
testing_check <- function(data, stub) {
  if (nrow(data[get(paste0('testing_', stub)) != get(paste0('norm_', stub, '_fraction'))]) > 0) {
    cat(red(paste0('      FATAL ERROR: SPLITTING ', toupper(stub), ' FAILED!!\n')))
    stop()
  } else {
    cat(green(paste0('      SUCCESS: SPLIT ', toupper(stub), ' PROPERLY!!\n')))
  }
}
#----------------------------# ####


cat('\n\n')
cat(green(' #########################################\n'))
cat(green(' #### WB POST KEYWORD SEARCH CLEANING ####\n'))
cat(green(' #########################################\n\n'))


cat('  Read in post keyword search data\n')
#----# Read in post keyword search data #----# ####
pkws <- setDT(readstata13::read.dta13(paste0(get_path('WB', 'int'), 'post_kws.dta')))
#--------------------------------------------# ####

cat('  Calculate health sector percentage\n')
#----# Calculate health sector percentage #----# ####
pkws[, `:=`(sector_name = 'health',
            health_percentage = 0)]
for (i in 1:5) {
  pkws[, eval(paste0('sector', i, '1')) := tstrsplit(get(paste0('sector', i)), '!', keep=1)]
  pkws[, eval(paste0('sector', i, '3')) := tstrsplit(get(paste0('sector', i)), '!', keep=3)]
  pkws[, eval(paste0('sector', i, '3')) := as.numeric(get(paste0('sector', i, '3')))]
  pkws[get(paste0('sector', i, '1')) %like% 'Health' | get(paste0('sector', i, '1')) %like% 'health', health_percentage := health_percentage + get(paste0('sector', i, '3'))]
  pkws[!(get(paste0('sector', i, '1')) %like% 'Health' | get(paste0('sector', i, '1')) %like% 'health'), eval(paste0('sector', i, '3')) := 0]
  pkws[, eval(paste0('sector', i, '1')) := NULL]
  pkws[, eval(paste0('sector', i, '3')) := NULL]
  pkws[, eval(paste0('sector', i)) := NULL]
}
# Calculate total percent
pkws <- rowtotal(pkws, 'total_percent', names(pkws)[names(pkws) %like% '_percent' & !(names(pkws) %like% '_percentage')])
pkws[, health_percentage := health_percentage / 100]
pkws[total_percent >= health_percentage, theme_greater := 1] # Observing translation differences here due to floating point accuracy between total & health percents
pkws[is.na(theme_greater), theme_greater := 0]
pkws[theme_greater == 0 & total_percent != 0, other_percent := health_percentage - total_percent]
pkws[theme_greater == 1 & total_percent	!= 0, other_percent := 0]
pkws[, total_percent2 := total_percent]
pkws[theme_greater == 0 & total_percent >= 0, total_percent2 := health_percentage]

# Read in wb_themes
wb_themes <- fread(WB_THEMES)
hfas_for_themes <- c(unique(wb_themes$HFA), 'other')
for (i in 1:length(hfas_for_themes)) {
  pkws[, eval(paste0('norm_', hfas_for_themes[i], '_fraction')) := get(paste0(hfas_for_themes[i], '_percent')) / total_percent2]
}
setnames(pkws, 'norm_swap_hss_other_fraction', 'norm_swap_other_fraction')
for (hfa in c('hiv', 'mal', 'nch', 'rmh', 'ncd', 'oid', 'swap', 'tb')) {
  pkws <- rowtotal(pkws, paste0('wb_', hfa, '_denom'), names(pkws)[names(pkws) %like% paste0('final_', hfa, '_')])
}
for (col in names(pkws)[names(pkws) %like% 'norm' | names(pkws) %like% 'denom']) {
  pkws[is.na(get(col)), eval(col) := 0]
}
#----------------------------------------------# ####

cat('  Normalize HFA fractions\n')
cat('    HIV\n')
#----# Normalize HIV frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_hiv_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_hiv_denom * norm_hiv_other_fraction]
}
pkws <- rowtotal(pkws, 'testing_hiv_other', names(pkws)[names(pkws) %like% 'wb_final_hiv_'])
pkws[norm_hiv_other_fraction != 0 & testing_hiv_other == 0 , wb_final_hiv_other_frct := norm_hiv_other_fraction]
pkws[, testing_hiv_other := NULL]
pkws <- rowtotal(pkws, 'testing_hiv_other', names(pkws)[names(pkws) %like% 'wb_final_hiv_'])
pkws[, `:=`(testing_hiv_other = round(testing_hiv_other, 6), norm_hiv_other_fraction = round(norm_hiv_other_fraction, 6))]
testing_check(pkws, 'hiv_other')
pkws[, testing_hiv_other := NULL]
#-------------------------------# ####

cat('    MAL\n')
#----# Normalize MAL frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_mal_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_mal_denom * norm_mal_other_fraction]
}
pkws <- rowtotal(pkws, 'testing_mal_other', names(pkws)[names(pkws) %like% 'wb_final_mal_'])
pkws[norm_mal_other_fraction != 0 & testing_mal_other == 0, wb_final_mal_other_frct := norm_mal_other_fraction]
pkws[, testing_mal_other := NULL]
pkws <- rowtotal(pkws, 'testing_mal_other', names(pkws)[names(pkws) %like% 'wb_final_mal_'])
testing_check(pkws, 'mal_other')
pkws[, testing_mal_other := NULL]
#-------------------------------# ####

cat('    NCD\n')
#----# Normalize NCD frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_ncd_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_ncd_denom * norm_ncd_other_fraction]
}
pkws <- rowtotal(pkws, 'testing_ncd_other', names(pkws)[names(pkws) %like% 'wb_final_ncd_'])
pkws[norm_ncd_other_fraction != 0 & testing_ncd_other == 0, wb_final_ncd_other_frct := norm_ncd_other_fraction]
pkws[, testing_ncd_other := NULL]
pkws <- rowtotal(pkws, 'testing_ncd_other', names(pkws)[names(pkws) %like% 'wb_final_ncd_'])
testing_check(pkws, 'ncd_other')
pkws[, testing_ncd_other := NULL]
#-------------------------------# ####

cat('    OID\n')
#----# Normalize OID frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_oid_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_oid_denom * norm_oid_other_fraction]
}
pkws <- rowtotal(pkws, 'testing_oid_other', names(pkws)[names(pkws) %like% 'wb_final_oid_'])
pkws[norm_oid_other_fraction != 0 & testing_oid_other == 0, wb_final_oid_other_frct := norm_oid_other_fraction]
pkws[, testing_oid_other := NULL]
pkws <- rowtotal(pkws, 'testing_oid_other', names(pkws)[names(pkws) %like% 'wb_final_oid_'])
testing_check(pkws, 'oid_other')
pkws[, testing_oid_other := NULL]
#-------------------------------# ####

cat('    TB\n')
#----# Normalize TB frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_tb_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_tb_denom * norm_tb_other_fraction]
}
pkws <- rowtotal(pkws, 'testing_tb_other', names(pkws)[names(pkws) %like% 'wb_final_tb_'])
pkws[norm_tb_other_fraction != 0 & testing_tb_other == 0, wb_final_tb_other_frct := norm_tb_other_fraction]
pkws[, testing_tb_other := NULL]
pkws <- rowtotal(pkws, 'testing_tb_other', names(pkws)[names(pkws) %like% 'wb_final_tb_'])
testing_check(pkws, 'tb_other')
pkws[, testing_tb_other := NULL]
#-------------------------------# ####

cat('    SWAP\n')
#----# Normalize SWAP frcts #----# ####
pkws[, wb_final_swap_hss_pp_frct := norm_swap_hss_pp_fraction]
pkws[is.na(wb_final_swap_hss_pp_frct), wb_final_swap_hss_pp_frct := 0]
for (col in c("final_swap_hss_hrh_frct", "final_swap_hss_other_frct")) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_swap_denom * norm_swap_other_fraction]
  pkws[is.na(get(paste0('wb_', col))), eval(paste0('wb_', col)) := 0]
}
pkws <- rowtotal(pkws, 'testing_swap_other', c("wb_final_swap_hss_hrh_frct", "wb_final_swap_hss_other_frct"))
pkws[norm_swap_other_fraction != 0 & testing_swap_other == 0, wb_final_swap_hss_other_frct := norm_swap_other_fraction]
pkws[, testing_swap_other := NULL]
pkws <- rowtotal(pkws, 'testing_swap_other', c("wb_final_swap_hss_hrh_frct", "wb_final_swap_hss_other_frct"))
testing_check(pkws, 'swap_other')
pkws[, testing_swap_other := NULL]
#-------------------------------# ####

cat('    NCH\n')
#----# Normalize NCH frcts #----# ####
pkws[, wb_final_nch_cnn_frct := norm_nch_cnn_fraction]
pkws[is.na(wb_final_nch_cnn_frct), wb_final_nch_cnn_frct := 0]

for (col in c('final_nch_hss_other_frct', 'final_nch_hss_hrh_frct', 'final_nch_cnv_frct', 'final_nch_other_frct')) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_nch_denom * norm_nch_other_fraction]
  pkws[is.na(get(paste0('wb_', col))), eval(paste0('wb_', col)) := 0]
}
pkws <- rowtotal(pkws, 'testing_nch_other', c('wb_final_nch_hss_other_frct', 'wb_final_nch_hss_hrh_frct', 'wb_final_nch_cnv_frct', 'wb_final_nch_other_frct'))
pkws[norm_nch_other_fraction != 0 & testing_nch_other == 0, wb_final_nch_other_frct := norm_nch_other_fraction]
pkws[, testing_nch_other := NULL]
pkws <- rowtotal(pkws, 'testing_nch_other', c('wb_final_nch_hss_other_frct', 'wb_final_nch_hss_hrh_frct', 'wb_final_nch_cnv_frct', 'wb_final_nch_other_frct'))
testing_check(pkws, 'nch_other')
pkws[, testing_nch_other := NULL]
#-------------------------------# ####

cat('    RMH\n')
#----# Normalize RMH frcts #----# ####
for (col in names(pkws)[names(pkws) %like% 'final_rmh_']) {
  pkws[, eval(paste0('wb_', col)) := get(col) / wb_rmh_denom * norm_rmh_other_fraction]
  pkws[is.na(get(paste0('wb_', col))), eval(paste0('wb_', col)) := 0]
}
pkws <- rowtotal(pkws, 'testing_rmh_other', names(pkws)[names(pkws) %like% 'wb_final_rmh'])
pkws[norm_rmh_other_fraction != 0 & testing_rmh_other == 0, wb_final_rmh_other_frct := norm_rmh_other_fraction]
pkws[, testing_rmh_other := NULL]
pkws <- rowtotal(pkws, 'testing_rmh_other', names(pkws)[names(pkws) %like% 'wb_final_rmh'])
testing_check(pkws, 'rmh_other')
pkws[, testing_rmh_other := NULL]
#-------------------------------# ####

cat('    OID\n')
#---- Normalize OID frcts #----# ####
pkws[final_oid_ebz_frct > 0, wb_final_oid_ebz_frct := 1]
pkws[is.na(wb_final_oid_ebz_frct), wb_final_oid_ebz_frct := 0]
for (col in names(pkws)[names(pkws) %like% 'wb_final']) {
  if (col != 'wb_final_oid_ebz_frct') {
    pkws[wb_final_oid_ebz_frct == 1, eval(col) := 0]
  }
}
pkws[, wb_final_other_frct := norm_other_fraction]

# Replace wb_final fractions with results from kws when no other health theme info tagged
pkws <- rowtotal(pkws, 'total_norm', names(pkws)[names(pkws) %like% 'norm'])
for (col in names(pkws)[names(pkws) %like% 'wb_final']) {
  nm <- gsub('wb_', '', col)
  pkws[total_norm == 0, eval(col) := get(nm)]
}
pkws[, total_norm := NULL]
pkws <- rowtotal(pkws, 'testing_everything', names(pkws)[names(pkws) %like% 'wb_final'])
pkws[testing_everything > 1.01, wb_final_other_frct := 0]
pkws[testing_everything == 0, wb_final_other_frct := 1]

pkws <- rowtotal(pkws, 'testing_everything', names(pkws)[names(pkws) %like% 'wb_final'])
if (nrow(pkws[testing_everything < 0.99 | testing_everything > 1.01]) > 0) {
  cat(red('    FATAL ERROR: WE MESSED UP THEMES SOMEWHERE ABOVE!!\n'))
  stop()
} else {
  cat(green('    SUCCESS: ALL HFAS SPLIT PROPERLY!!\n'))
}
pkws[, testing_everything := NULL]
#------------------------------# ####

cat('  Distribute commitments & disbursements\n')
#----# Distribute commitments & disbursements #----# ####
to_drop <- names(pkws)[(names(pkws) %like% 'final_' & names(pkws) %like% '_frct') & !(names(pkws) %like% 'wb_')]
pkws[, eval(to_drop) := NULL]

for (col in names(pkws)[names(pkws) %like% 'wb_final_']) {
  if (col %ni% c('wb_final_total_frct', 'wb_final_mnch_cn_frct')) {
    nm <- gsub('wb_final_', '', col)
    nm <- gsub('_frct', '', nm)
    pkws[, eval(paste0(nm, '_DAH')) := get(col) * DAH]
  }
}

pkws <- pkws[!is.na(DAH)]

# Test that it worked out
pkws <- rowtotal(pkws, 'total_DAH', names(pkws)[names(pkws) %like% '_DAH'])
pkws[, test := abs(total_DAH - DAH)]
if (nrow(pkws[test > 50]) > 0) {
  cat(red('    FATAL ERROR: DAH NOT PROPERLY DISTRIBUTED!!\n'))
  stop()
} else {
  cat(green('    SUCCESS: DAH DISTRIBUTED PROPERLY!!\n'))
}
pkws[, `:=`(test = NULL, total_DAH = NULL)]
#--------------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(pkws, 'cleaned_pkws', 'WB', 'int')
#------------------------# ####

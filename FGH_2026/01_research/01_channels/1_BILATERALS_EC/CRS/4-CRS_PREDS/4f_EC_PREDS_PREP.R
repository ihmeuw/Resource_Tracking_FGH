#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Create CRS EC member trends predictions
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, readstata13)

# Variable prep
defl <- paste0(dah.roots$j, 'FILEPATH')
last_obs_year <- get_dah_param("crs", "data_year")
data_lag <- dah.roots$report_year - last_obs_year

#----------------------------# ####


cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### CRS EC PREDS MEMBER TRENDS ####\n'))
cat(green(' ####################################\n\n'))


cat('  Read in CRS disbursement data\n')
#----# Read in CRS disbursement data #----# ####
ec_dah <- fread(get_path("crs", "int", "adb_for_preds.csv"))
ec_dah <- ec_dah[ELIM_CH != 1 & CHANNEL == 'EC']
ec_dah <- collapse(ec_dah, 'sum', c('CHANNEL', 'YEAR'), c(names(ec_dah)[names(ec_dah) %like% 'DAH']))

setnames(ec_dah, 'DAH', 'OUTFLOW')

rates <- fread(get_path("meta", "defl",
                        paste0("imf_usgdp_deflators_", dah.roots$defl_MMYY, ".csv")))
ec_dah <- merge(ec_dah, rates, by='YEAR', all.x=T)
ec_dah[, eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := OUTFLOW / get(paste0('GDP_deflator_', dah.roots$report_year))]

rm(rates, defl)
#-----------------------------------------# ####

cat('  Read in EC member predictions\n')
#----# Read in EC member predictions #----# ####
dt <- fread(get_path('BILAT_PREDICTIONS', 'fin',
                     'Bilateral_predictions_[report_year]_[crs.update_mmyy].csv'))

# update if we add new DAC members that are part of the European Union
ec_isos <- c("AT" = "AUT",
             "BE" = "BEL",
             "CZ" = "CZE",
             "DK" = "DNK",
             "DE" = "DEU",
             "EE" = "EST",
             "ES" = "ESP",
             "FI" = "FIN",
             "FR" = "FRA",
             "IE" = "IRL",
             "IT" = "ITA",
             "LT" = "LTU",
             "LU" = "LUX",
             "NL" = "NLD",
             "PL" = "POL",
             "PT" = "PRT",
             "SI" = "SVN",
             "SK" = "SVK",
             "SE" = "SWE")

dt <- dt[ISO3 %in% ec_isos & YEAR == last_obs_year, ]
dt[is.na(get(paste0('diff_', dah.roots$report_year))),
   eval(paste0('diff_', dah.roots$report_year)) := 0]

if (data_lag == 2) {
  dt[is.na(get(paste0('diff_', dah.roots$prev_report_year))),
     eval(paste0('diff_', dah.roots$prev_report_year)) := 0]
  dt <- dt[, c('YEAR', 'ISO3', paste0('diff_', dah.roots$prev_report_year), paste0('diff_', dah.roots$report_year)), with=F]
} else if (data_lag == 1) {
  dt <- dt[, c('YEAR', 'ISO3', paste0('diff_', dah.roots$report_year)), with=F]
}

# Drop duplicates
dt <- unique(dt)
#-----------------------------------------# ####

cat('  Prepare EC member contributions\n')
#----# Prepare EC member contributions #----# ####
ec <- fread(get_path('BILAT_EC', 'fin',
                     'EC_Member_national_contributions_[prev_report_year].csv'))
# finding Row and Column values for start and end of dataframe
x <- as.matrix(ec)
i_str <- min(grep("EUR million", x)) # first occurance of the string since that is the colnames
t_str <- min(grep("Total national contribution",x,ignore.case=TRUE))

row_str <- 1 + (i_str - 1) %% nrow(x)   # number of positions outside full columns
col_str <- 1 + (i_str - 1) %/% nrow(x)  # number of full columns before position i
# drop all rows before the first row with "YYYY (EUR million)" for header purposes
ec <- ec[row_str:nrow(ec)]
ec <- ec[,which(unlist(lapply(ec, function(x)!all(is.na(x))))),with=F]
rm(x,i_str,row_str,col_str)

#-----------------------------------------------------------------------------------------------
colnames(ec) <- as.character(ec[1,])
setnames(ec, names(ec)[names(ec) %like% 'EUR million'], 'eurmillion')

ec <- ec[, c("eurmillion", "", names(ec_isos)), with = FALSE]

x <- as.matrix(ec)
t_str <- min(grep("Total national contribution", x, ignore.case = TRUE))
rowt_str <- 1 + (t_str - 1) %% nrow(x)   # number of positions outside full columns
# select the total national contributions row
ec <- ec[rowt_str, ]
ec <- melt(ec[, names(ec_isos), with = FALSE],
           measure.vars = names(ec_isos))
ec[, variable := as.character(variable)]
ec[, value := as.numeric(gsub(',', '', value))]

colnames(ec) <- c('country',
                  paste0('total_contribution_', dah.roots$prev_report_year))

ec[, ISO3 := ec_isos[country]]
ec[, YEAR := last_obs_year]
#-------------------------------------------# ####

cat('  Merge predictions + contributions\n')
#----# Merge predictions + contributions #----# ####
dt <- merge(dt, ec, by=c('ISO3', 'YEAR'), all=T)
rm(ec)

dt[, weighted_total := sum(get(paste0('total_contribution_', dah.roots$prev_report_year)), na.rm=T)]
dt[, weighting := get(paste0('total_contribution_', dah.roots$prev_report_year)) / weighted_total]

if (data_lag == 2) {
  dt[, eval(paste0('weighted_diff_', dah.roots$prev_report_year)) := get(paste0('diff_', dah.roots$prev_report_year)) * weighting]
}
dt[, eval(paste0('weighted_diff_', dah.roots$report_year)) := get(paste0('diff_', dah.roots$report_year)) * weighting]

if (data_lag == 2) {
  dt <- collapse(dt, 'sum', '', c(paste0('weighted_diff_', dah.roots$prev_report_year), paste0('weighted_diff_', dah.roots$report_year)))
} else if (data_lag == 1) {
  dt <- collapse(dt, 'sum', '', paste0('weighted_diff_', dah.roots$report_year))
}

dt[, YEAR := last_obs_year]
dt <- rbind(dt, do.call('rbind', replicate(2, dt, simplify = F)))
dt$YEAR <- c(min(dt$YEAR):(min(dt$YEAR) + nrow(dt) - 1))

if (data_lag == 2) {
  setnames(dt, paste0('weighted_diff_', dah.roots$prev_report_year), paste0('ec_diff_', dah.roots$prev_report_year))
}
setnames(dt, paste0('weighted_diff_', dah.roots$report_year), paste0('ec_diff_', dah.roots$report_year))

dt <- merge(dt, ec_dah, by='YEAR', all=T)
rm(ec_dah)

if (data_lag == 2) {
  dt[, outflow2 := shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag') *
                     (1 + get(paste0('ec_diff_', dah.roots$prev_report_year)))]
  dt[YEAR == dah.roots$prev_report_year, eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := outflow2]
  dt[, outflow2 := NULL]
}

dt[, outflow2 := shift(get(paste0('OUTFLOW_', dah.roots$abrv_year)), n=1, type='lag') *
                  (1 + get(paste0('ec_diff_', dah.roots$report_year)))]
dt[YEAR == dah.roots$report_year, eval(paste0('OUTFLOW_', dah.roots$abrv_year)) := outflow2]
dt[, outflow2 := NULL]
#---------------------------------------------# ####

dt <- dt[YEAR <= dah.roots$report_year]

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'EC_PREDS_EU_MEMBER_TRENDS_1990_[report_year]_[crs.update_mmyy]',
             'BILAT_EC', 'fin')
#------------------------# ####

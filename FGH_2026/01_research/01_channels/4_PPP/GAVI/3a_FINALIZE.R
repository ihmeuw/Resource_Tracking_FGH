#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Complete Data Cleaning and Save Datasets
#---------------------# ####
#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
income_update_year <- get_dah_param('GAVI', 'income_update_year')
disb_update_year <- get_dah_param('GAVI', 'disb_update_year')
crs_mmyy <- get_dah_param('CRS', 'update_MMYY')
#----------------------------# ####


cat('\n\n')
cat(green(' ################################\n'))
cat(green(' #### COMPLETE DATA CLEANING ####\n'))
cat(green(' ################################\n\n'))


cat('  Read Stage 2a Data\n')
#----# Read Stage 2a Data #----# ####
noneg <- fread(paste0(get_path('GAVI', 'int'), '2a_HFA_ASSIGN.csv'),
               encoding = "Latin-1")[YEAR <= dah.roots$report_year]
noneg[RECIPIENT_COUNTRY %like% "d'Ivoire", `:=`(
    RECIPIENT_COUNTRY = "Cote d'Ivoire",
    ISO3_RC = "CIV"
)]
noneg[RECIPIENT_COUNTRY == "Eswatini", ISO3_RC := "SWZ"]
noneg[RECIPIENT_COUNTRY == "Palestine", ISO3_RC := "PSE"]

## separate covid/covax pipeline, so this column is deprecated
noneg[, oid_covid := NULL]


income <- fread(paste0(get_path('GAVI', 'int'), 'GAVI_income.csv'))
income[, is_covax := NULL]

#------------------------------# ####
cat('  Add Inkind to Income Data\n') 
#----# Add Inkind to Income #----# ####
# Total disbursements by year
temp_disb <- copy(noneg)
temp_disb <- collapse(temp_disb, 'sum', 'YEAR', c('COMMITMENT', 'DISBURSEMENT'))
# Read in previous inkind
temp_inkind <- fread(paste0(get_path('GAVI', 'raw'), 'P_GAVI_INKIND_', dah.roots$prev_report_year, 'update.csv'))[, .(YEAR, INKIND_RATIO)]
# Calculate inkind
inkind <- copy(income)
inkind <- merge(income, temp_disb, by = 'YEAR')
inkind[is.na(DISBURSEMENT), DISBURSEMENT := 0]
setnames(inkind, 'DISBURSEMENT', 'DISB_TOTAL')
inkind <- merge(inkind, temp_inkind, by = 'YEAR', all.x = T)
inkind[, DISBURSEMENT := DISB_TOTAL * INKIND_RATIO * INCOME_ALL_SHARE]
inkind[, `:=`(OUTFLOW = DISBURSEMENT,
              INKIND = 1,
              INKIND_RATIO = NULL,
              INCOME_ALL = 0)]

# Combine inkind with income
incdb <- merge(income, temp_disb, by = 'YEAR', all = T)
incdb[is.na(DISBURSEMENT), DISBURSEMENT := 0]
setnames(incdb, 'DISBURSEMENT', 'DISB_TOTAL')
incdb[, DISBURSEMENT := DISB_TOTAL * INCOME_ALL_SHARE]
incdb[, OUTFLOW := DISBURSEMENT]

# Fill DONOR_COUNTRY and ISO3
incdb[is.na(DONOR_COUNTRY) & INCOME_SECTOR == 'PUBLIC', DONOR_COUNTRY := "NA"]
incdb[is.na(ISO3) & INCOME_SECTOR == 'PUBLIC', ISO3 := 'NA']
incdb <- rbind(incdb, inkind, fill=T)
incdb[is.na(INKIND), INKIND := 0]
#--------------------------------# #### 

cat('  Save ADB Dataset\n')
#----# Save ADB Dataset #----# ####
setnames(incdb, 'ISO3', 'ISO_CODE')
incdb[, GHI := 'GAVI']
incdb[, YEAR := as.numeric(YEAR)]
incdb <- incdb[!is.na(YEAR)]

save_dataset(incdb, paste0('P_GAVI_INC_FGH', dah.roots$report_year), 'GAVI', 'fin')
#----------------------------# ####

cat('  Adding Inkind to Disbursement Data\n')
#----# Add Inkind to Disbursement #----# ####
disb_inkind <- copy(inkind)
disb_inkind <- collapse(disb_inkind, 'sum', 'YEAR', 'DISBURSEMENT')
disb_inkind[, `:=`(FUNDING_COUNTRY = 'UNSP', ISO3_FC = 'NA',
                   FUNDING_TYPE = 'In Kind', FUNDING_AGENCY = 'GAVI',
                   FUNDING_AGENCY_SECTOR = 'PPP', RECIPIENT_AGENCY_SECTOR = 'GOV',
                   RECIPIENT_AGENCY_TYPE = 'UNSP', PURPOSE = 'In Kind',
                   ISO_RC = 'NA', PROJECT_NAME = 'NA',
                   PROJECT_ID = 'NA')]
disb_inkind <- disb_inkind[!is.na(YEAR)]
#--------------------------------------# ####

cat('  Save PDB Dataset\n') 
#----# Save PDB Dataset #----# ####
intpdb <- copy(noneg)
intpdb[, INKIND := 0]
disb_inkind[, INKIND := 1]
intpdb <- rbind(intpdb, disb_inkind, fill=T)
intpdb <- intpdb[YEAR <= dah.roots$report_year]
intpdb[, PROJ_INVEST_DISB := NULL]


save_dataset(intpdb, paste0('P_GAVI_INTPDB_FGH', dah.roots$report_year), 'GAVI', 'fin')
#----------------------------# #### 

cat('  Merging ADB and PDB\n') 
#----# Merge ADB and PDB #----# ####
temp_disb2 <- copy(noneg)
setnames(temp_disb2, 'DISBURSEMENT', 'DAH')
for (var in c('mnch_cnv', 'hss', 'ncd_other')) {
  temp_disb2[, eval(paste0(var, '_DAH')) := get(var) * DAH]
}

temp_disb2[RECIPIENT_COUNTRY %in% c(
    'Non country specific',
    'Developing countries, unspecified'
    ), ISO3_RC := 'WLD']
stopifnot( temp_disb2[ISO3_RC == "" | is.na(ISO3_RC), .N] == 0 )
temp_disb2 <- collapse(temp_disb2, 'sum',
                       c('YEAR', 'ISO3_RC'),
                       c('DAH', 'mnch_cnv_DAH', 'hss_DAH', 'ncd_other_DAH'))
setnames(temp_disb2,
         c('DAH', 'mnch_cnv_DAH', 'hss_DAH', 'ncd_other_DAH'),
         c('DAH_TOTAL', 'mnch_cnv_DAH_TOTAL', 'hss_DAH_TOTAL', 'ncd_other_DAH_TOTAL'))
temp_disb2 <- dcast.data.table(temp_disb2, formula = 'YEAR ~ ISO3_RC', value.var = c('DAH_TOTAL', 'mnch_cnv_DAH_TOTAL', 'hss_DAH_TOTAL', 'ncd_other_DAH_TOTAL'))
temp_disb2 <- temp_disb2[YEAR <= dah.roots$report_year]

adbpdb <- copy(income)
adbpdb <- merge(adbpdb, temp_disb2, by = c('YEAR'), all = T)
setnames(adbpdb, 'ISO3', 'ISO_CODE')

# Reshape long
adbpdb <- melt.data.table(adbpdb,
                          id.vars = c('DONOR_COUNTRY', 'DONOR_NAME', 'INCOME_ALL', 'INCOME_ALL_SHARE', 'INCOME_EB', 'INCOME_OTHER', 'INCOME_REG', 'INCOME_SECTOR', 'INCOME_TOTAL_YR',
                                   'INCOME_TYPE', 'ISO_CODE', 'SOURCE_DOC', 'YEAR', 'CHANNEL'),
                          variable.factor = F)
adbpdb[, ISO3_RC := substr(variable, nchar(variable) - 2, nchar(variable))]
adbpdb[, variable := gsub('(TOTAL).*', '\\1', variable)]
adbpdb <- dcast.data.table(adbpdb, formula = '... ~ variable', value.var = 'value') # melts too long, must reshape wide

for (var in c('DAH_TOTAL', 'hss_DAH_TOTAL', 'mnch_cnv_DAH_TOTAL', 'ncd_other_DAH_TOTAL')) {
  adbpdb[, eval(paste0(var, '_share')) := get(var) * INCOME_ALL_SHARE]
}
setnames(adbpdb, c('DAH_TOTAL_share', 'hss_DAH_TOTAL_share', 'mnch_cnv_DAH_TOTAL_share', 'ncd_other_DAH_TOTAL_share'), 
         c('DAH', 'hss_DAH', 'mnch_cnv_DAH', 'ncd_other_DAH'))
#-----------------------------# ####

cat('  Generating Imputed Inkind Disbursement\n') 
#----# Generate Imputed Inkind #----# ####
inkind_final <- copy(adbpdb)
inkind_final <- merge(inkind_final, temp_inkind, by='YEAR', all.x = TRUE)

stopifnot(inkind_final[is.na(INKIND_RATIO), .N] == 0)

for (var in c('DAH_TOTAL', 'hss_DAH_TOTAL', 'mnch_cnv_DAH_TOTAL', 'ncd_other_DAH_TOTAL')) {
  inkind_final[, eval(paste0(var, '_share')) := (INKIND_RATIO * get(var)) * INCOME_ALL_SHARE]
}

inkind_final <- inkind_final[, !c('DAH', 'hss_DAH', 'mnch_cnv_DAH', 'ncd_other_DAH')]
setnames(inkind_final, c('DAH_TOTAL_share', 'hss_DAH_TOTAL_share', 'mnch_cnv_DAH_TOTAL_share', 'ncd_other_DAH_TOTAL_share'), 
         c('DAH', 'hss_DAH', 'mnch_cnv_DAH', 'ncd_other_DAH'))
inkind_final[, `:=`(INKIND_RATIO = NULL,
                    INKIND = 1,
                    INCOME_ALL = 0)]
#-----------------------------------# ####

cat('  Appending Final Inkind\n')
#----# Appending Final Inkind #----# ####
adbpdb <- rbind(adbpdb, inkind_final, fill=T)
adbpdb[is.na(INKIND), INKIND := 0]
adbpdb <- adbpdb[!is.na(DAH)] # Disbursements for the most recent year are not complete
adbpdb[, `:=`(DAH_TOTAL=NULL, hss_DAH_TOTAL=NULL, mnch_cnv_DAH_TOTAL=NULL, ncd_other_DAH_TOTAL=NULL)]
setnames(adbpdb, c('mnch_cnv_DAH', 'hss_DAH'), c('nch_cnv_DAH', 'nch_hss_other_DAH'))
adbpdb[, `:=`(RECIPIENT_AGENCY = 'GAVI', RECIPIENT_AGENCY_SECTOR = 'GOV', gov = 1)]

adbpdb[DONOR_NAME == "BMGF", INCOME_SECTOR := "BMGF"]
#----------------------------------# ####


cat(' Finalize and append on COVAX data\n')
#----# Finalize and append on COVAX data #----# ####
covax <- fread(get_path("gavi", "fin", "gavi_covax_disbursements.csv"))
covax <- covax[, .(
    DAH = sum(value, na.rm = TRUE) ## aggregate cash and doses
), by = .(
    YEAR = year,
    DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE,
    ISO3_RC = iso3_rc
)]
covax[, oid_covid_DAH := DAH]

## merge on in-kind ratio to estimate admin expense
covax <- merge(
    covax,
    temp_inkind,
    by = "YEAR",
    all.x = TRUE
)
stopifnot( covax[is.na(INKIND_RATIO), .N] == 0 )
covax[, INKIND := 0]
cov_ink <- copy(covax)
cov_ink[, INKIND := 1]
cov_ink[, `:=`(
    DAH = INKIND_RATIO * DAH,
    oid_covid_DAH = INKIND_RATIO * oid_covid_DAH
)]
covax <- rbind(covax, cov_ink)

## add additional columns to match ADB PDB
covax[, `:=`(
    CHANNEL = "GAVI",
    RECIPIENT_AGENCY = "GAVI",
    RECIPIENT_AGENCY_SECTOR = "GOV",
    gov = 1,
    INKIND_RATIO = NULL
)]

## bind covax with adbpdb
adbpdb <- rbind(adbpdb, covax, fill = TRUE)
setnafill(adbpdb, fill = 0, cols = grep("DAH", names(adbpdb), value = TRUE))
setorder(adbpdb, YEAR)
#----------------------------------# ####


cat('  Saving ADB_PDB Dataset\n') 
#----# Save ADB_PDB Dataset #----# ####
save_dataset(adbpdb, paste0('P_GAVI_ADB_PDB_FGH', dah.roots$report_year), 'GAVI', 'fin')
#--------------------------------# ####
#----# Docstring #----# ####
# Project:  FGH 2020
# Purpose:  Create WB ADB PDB dataset
# Date:     07/23/2020
# Author:   Kyle Simpson
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
#----------------------------# ####


cat('\n\n')
cat(green('\t###########################\n','\t#### WB ADB PDB CREATE ####\n','\t###########################\n\n'))


cat('  Merge ADB and PDB\n')
#----# Merge ADB and PDB #----# ####
wb_pdb <- setDT(fread(paste0(get_path('WB', 'int'), 'wb_pdb.csv')))
wb_pdb <- wb_pdb[is.na(INKIND)]

# Recode ISOs
wb_pdb[RECIPIENT_COUNTRY == "Eastern Africa", ISO3_RC := 'QMA']
wb_pdb[RECIPIENT_COUNTRY == "Western Africa", ISO3_RC := "QMA"]
wb_pdb[RECIPIENT_COUNTRY == "South of Sahara, regional", ISO3_RC := "QMA"]
wb_pdb[RECIPIENT_COUNTRY == "Caribbean", ISO3_RC := 'QNB']
wb_pdb[RECIPIENT_COUNTRY == "Central America", ISO3_RC := 'QNC']
wb_pdb[RECIPIENT_COUNTRY == "Caribbean & Central America, regional", ISO3_RC := 'QNC']
wb_pdb[RECIPIENT_COUNTRY == "Central Asia", ISO3_RC := 'QRS']
wb_pdb[ISO3_RC == "", ISO3_RC := "QZA"]

# Collapse yearly disbursements
dt <- collapse(wb_pdb, 'sum', c('YEAR', 'ISO3_RC', 'CHANNEL'), c('DAH', names(wb_pdb)[names(wb_pdb) %like% '_DAH']))
setnames(dt, 'DAH', 'DAH_TOTAL')
setnames(dt, names(wb_pdb)[names(wb_pdb) %like% '_DAH'], 
         gsub('_DAH', '_DAH_TOTAL', names(wb_pdb)[names(wb_pdb) %like% '_DAH']))

# Reshape wide
dt <- dcast.data.table(dt, formula = 'YEAR + CHANNEL ~ ISO3_RC', value.var = c(names(dt)[names(dt) %like% 'DAH_TOTAL']))
dt <- dt[YEAR >= 1990]

# Merge with adb data
wb_adb <- setDT(fread(paste0(get_path('WB', 'int'), 'wb_adb.csv')))
dt <- merge(dt, wb_adb, by = c('CHANNEL', 'YEAR'), all = T)
#-----------------------------# ####

cat('  Reshape data\n')
#----# Reshape data #----# ####
# Reshape long
options(warn = -1) 
test <- melt.data.table(dt, measure.vars = c(names(dt)[names(dt) %like% 'DAH_TOTAL']))
options(warn = 0)
test[, variable := as.character(variable)]
test[, ISO3_RC := substr(variable, nchar(variable) - 2, nchar(variable))]
test[, variable := substr(variable, 1, nchar(variable) - 4)]
data <- dcast.data.table(test, formula = 'CHANNEL + YEAR + INCOME_SECTOR + INCOME_TYPE + DONOR_NAME + DONOR_COUNTRY + ISO_CODE + INCOME_ALL + SOURCE + INCOME_TOTAL_YR + INCOME_ALL_SHARE + 
                               DISB_TOTAL + DISBURSEMENT + OUTFLOW + INKIND_RATIO + INKIND + GHI + ISO3_RC ~ variable', 
              value.var = 'value')
#------------------------# ####

cat('  Calculate imputed disbursement & inkind\n')
#----# Calculate imputed disbursement & inkind #----# ####
# Generate imputed disbursement
data[CHANNEL == "WB_IBRD", INCOME_ALL_SHARE := 1]
for (col in names(data)[names(data) %like% '_TOTAL' & !(names(data) %like% 'INCOME')]) {
  data[, eval(paste0(col, '_share')) := get(col) * INCOME_ALL_SHARE]
}

# Generate imputed inkind
for (col in names(data)[names(data) %like% '_TOTAL' & !(names(data) %like% '_share') & !(names(data) %like% 'INCOME')]) {
  data[INKIND == 1, eval(paste0(col, '_share')) := (INKIND_RATIO * get(col)) * INCOME_ALL_SHARE]
}
setnames(data, names(data)[names(data) %like% '_TOTAL_share'], gsub('_TOTAL_share', '', names(data)[names(data) %like% '_TOTAL_share']))
#---------------------------------------------------# ####

cat('  Column renaming & final cleaning\n')
#----# Column renaming & final cleaning #----# ####
# Keep only needed data
fin <- copy(data)
fin <- fin[!is.na(DAH)]
to_drop <- c(names(fin)[names(fin) %like% '_TOTAL' & !(names(fin) %like% 'INCOME')])
fin[, eval(to_drop) := NULL]
fin[, `:=`(RECIPIENT_AGENCY_SECTOR = 'GOV', LEVEL = 'COUNTRY')]
fin[ISO3_RC %in% c("QMA", "QMD", "QME", "QNA", "QNB", "QNC", "QNE", "QRA", "QRB", "QRC", "QRD",
                   "QRE", "QRS", "QSA", "QTA"), LEVEL := 'REGIONAL']
fin[ISO3_RC %in% c("QZA", "WLD"), LEVEL := 'GLOBAL']
fin[, gov := 0]
fin[LEVEL == "COUNTRY" & RECIPIENT_AGENCY_SECTOR == "GOV", gov := 1]
fin[LEVEL == "REGIONAL" & RECIPIENT_AGENCY_SECTOR == "GOV", gov := 1]
fin[gov == 0, gov := NA]
fin[is.na(INKIND), INKIND := 0]
#--------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(fin, 'wb_adb_pdb', 'WB', 'int')
save_dataset(fin, paste0('WB_ADB_PDB_FGH', dah.roots$report_year), 'WB', 'fin')
#------------------------# ####

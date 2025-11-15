#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Prepare CRS ADB_PDB before PMI data
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon, stringr, readxl)

# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####


cat('\n\n')
cat(green(' ########################################\n'))
cat(green(' #### CRS PREPARE ADB_PDB BEFORE PMI ####\n'))
cat(green(' ########################################\n\n'))


cat('  Read in BIL_ODA_4 dataset\n')
#----# Read in BIL_ODA_4 dataset #----# ####
dt <- fread(get_path('crs', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_4_backcast.csv'))
#-------------------------------------# ####

cat('  Read in inkind_by_channel dataset\n')
#----# Read in inkind_by_channel dataset #----# ####
# This file should be updated annually; see step 10 of CRS how-to document for how to do this. 
# This file only needs to contain data up to the last year of observed CRS data.
inkind <- read_excel(get_path('CRS', 'raw', 'inkind_ratios_bychannel_1990_[prev_report_year].xlsx'),
                     sheet = 'stata')
setDT(inkind)
setnames(inkind, c('CHANNEL', 'YEAR'), c('donor_agency', 'year'))
inkind <- inkind[year <= get_dah_param("crs", "data_year")]

for (da in unique(dt$donor_agency)) {
    if (da %in% c("", "EU Institutions")) next()
    if (!da %in% unique(inkind$donor_agency)) {
        warning("Warning: Donor '", da, "' not found in inkind_ratios dataset\n")
    }
}

# we make a copy of the entire database, with the inkind ratio merge on
# - then below we multiply the disbursements by the inkind ratio and count this
#   as the inkind value, to be added on to the final disbursements
inkind[, m_m := 1]
dt[, u_m := 2]
inkind <- merge(dt, inkind, by=c('donor_agency', 'year'), all.x=T)
inkind[, merge := u_m + m_m]
inkind <- inkind[isocode != 'EC' & merge != 1, !c('u_m', 'm_m', 'merge')]

diseases <- names(inkind)[names(inkind) %like% '_disbyr1_cons']
diseases <- str_replace_all(diseases, '_disbyr1_cons', '')
for (disease in diseases) {
  for (unit in c('curr', 'cons')) {
    # scale estimated disbursement by inkind ratio
    inkind[, eval(paste0('final_', disease, '_disb_', unit)) :=
               get(paste0('final_', disease, '_disb_', unit)) * INKIND_RATIO]
    inkind[, eval(paste0(disease, '_disbest_', unit, '_dach')) := 0]
  }
}
for (col in c('ia_all_commcurr', 'ia_all_disbcurr', 'ia_all_commcons',
              'ia_all_disbcons', 'health_dac_all_commcons', 'health_dac_all_commcurr',
              'all_commcurr_dach', 'all_commcons_dach', 'all_disbcurr_dach',
              'all_disbcons_dach')) {
  inkind[, eval(col) := NA]
}
inkind[, INKIND := 1]

# Append datasets (combine inkind with regular dah)
dt[, u_m := NULL]
dt <- rbind(dt, inkind, fill=T)
dt[is.na(INKIND), INKIND := 0]
## 'dummy' projects added in previous script represent DAC funding that isn't
## accounted for by project-level data from CRS
dt[crs_id == 'DUMMY',
   `:=`(final_unalloc_disb_curr = final_all_disb_curr,
        final_unalloc_disb_cons = final_all_disb_cons)]
rm(inkind, disease, col, unit)
#---------------------------------------------# ####

cat('  Add consistent channel names\n')
#----# Add consistent channel names #----# ####
# Fix to elimate some UN channels where the exact UN agency was not specified by channel_code_rev bu was caught by keyword search
dt[INKIND != 1 & donor_name != "EU Institutions" & channel_name=="United Nations Agencies, Funds and Commissions" & 
     channel_reported_name=="United Nations agency, fund or commission (UN)" & (unicef == 1 | unfpa == 1 | unaids == 1 | who == 1 | paho == 1), 
   eliminations := 1]
# fill in channel names for channels that are tagged as double counting
dt[channel_code_rev == 41110, CHANNEL := 'UNAIDS']
dt[channel_code_rev == 41119, CHANNEL := 'UNFPA']
dt[channel_code_rev == 41122, CHANNEL := 'UNICEF']
dt[channel_code_rev %in% c(41307, 41143, 41321), CHANNEL := 'WHO']
dt[channel_code_rev == 42001 | channel_code_rev == 42003, CHANNEL := 'EC'] 
dt[channel_code_rev == 47045, CHANNEL := 'GFATM']
dt[channel_code_rev == 47122 | channel_code_rev == 47107, CHANNEL := 'GAVI']
dt[channel_code_rev == 31006, CHANNEL := "CEPI"]
dt[channel_code_rev == 47083, CHANNEL := 'PAHO']
dt[channel_code_rev == 46002 | channel_code_rev == 46003, CHANNEL := 'AfDB']
dt[channel_code_rev == 46005  | channel_code_rev == 46004, CHANNEL := 'AsDB']
dt[channel_code_rev == 46012 | channel_code_rev == 46013 | upper_project_title=="HIV AIDS IDBMEETING LA",
   CHANNEL := 'IDB'] ## tags 2 too many (but does this correctly, Stata missing these)
dt[ngo_eliminations ==1 & donor_name!= "EU Institutions", CHANNEL := 'NGO']
dt[ingo_eliminations == 1 & donor_name!= "EU Institutions", CHANNEL := 'INTL_NGO']
dt[channel_code_rev == 30010, CHANNEL := 'UNITAID']
dt[(nih == 1 & isocode == "USA"), CHANNEL := 'NIH']
dt[channel_code_rev == 44000, CHANNEL := 'WB']
dt[channel_code_rev == 44002, CHANNEL := 'WB_IDA']
dt[channel_code_rev %in% c(44001, 44007), CHANNEL := 'WB_IBRD']

# fill in the donor country as the channel for non-elimination projects
dt[isocode == "EC" & (eliminations!=1 | is.na(eliminations)), CHANNEL := 'EC'] 
dt[(eliminations!=1 | is.na(eliminations)) & ((ngo_eliminations!=1 | is.na(ngo_eliminations)) & (ingo_eliminations!=1 | is.na(ingo_eliminations))) & (CHANNEL!="EC" | is.na(CHANNEL)), 
   CHANNEL := 'BIL_DAH'] 

# tracking ngos using channel_code (21000 and 22000)
dt[channel_code >= 21000  & channel_code < 22000 & donor_name!= "EU Institutions", CHANNEL := 'INTERNATIONALNGOS']
dt[channel_code >= 22000  & channel_code < 23000 & donor_name!= "EU Institutions", CHANNEL := 'DONORCOUNTRYNGOS']
dt[CHANNEL == "NGO", CHANNEL := 'DONORCOUNTRYNGOS']

# set channel to donor bilateral for double counted inkind projects
dt[INKIND !=1 & donor_name != "EU Institutions" & channel_name=="United Nations Agencies, Funds and Commissions" & 
     channel_reported_name=="United Nations agency, fund or commission (UN)" & (unicef == 1 | unfpa == 1 | unaids == 1 | who == 1 | paho == 1), 
   CHANNEL := 'BIL_DAH']
dt[INKIND == 1 & (eliminations == 1 | ngo_eliminations == 1 | ingo_eliminations == 1),
   CHANNEL := 'BIL_DAH']
dt[eliminations == 1 &
       (channel_code_rev %in% c(10000, 11000, 12001, 13000, 30005, 31000,
                                40000, 41000, 41114, 41400, 42000, 44004, 47000,
                                47112, 90000, 0)),
   CHANNEL := 'BIL_DAH']
dt[eliminations == 1 & channel_code_rev %in% c(20000, 23000),
   CHANNEL := "INTERNATIONALNGOS"]

if (dt[is.na(CHANNEL), .N] > 0) {
    cat(red('    ERROR: SOME UNTAGGED CHANNELS!!\n'))
    stop("untagged channels")
}
#----------------------------------------# ####

cat('  Prepare for INTNEW output\n')
#----# Prepare for INTNEW output #----# ####
dt[recipient_name == "Bilateral, unspecified", ISO3_RC := 'WLD']
dt[crs_id == "DUMMY", ISO3_RC := 'QZA']
dt[ISO3_RC == "QZA" | ISO3_RC == "WLD", LEVEL := "GLOBAL"]
dt[LEVEL == "GLOBAL", ISO3_RC := 'WLD']

dt[LEVEL == "GLOBAL", gov := 0]
dt[is.na(gov), gov := 0]

if (nrow(dt[is.na(ISO3_RC)]) > 0) {
  cat(red('    ERROR: MISSING SOME ISO3_RC OBSERVATIONS!!'))
  stop("missing ISO3_RC")
}

# Note that we do not want to eliminate inkind projects to double counted channels. This is because we want to preserve the values of admin 
# costs for the originating agency of funding.
dt[(eliminations == 1 | ngo_eliminations == 1 | ingo_eliminations == 1) & INKIND != 1,
   ELIM_CH := 1]
dt[is.na(ELIM_CH), ELIM_CH := 0]

dt[, grant_loan := "unkown"]
dt[flow_name %like% "Loan", grant_loan := "loan"]
dt[flow_name %like% "Grant", grant_loan := "grant"]
#-------------------------------------# ####

cat('  Save INTNEW dataset\n')
#----# Save INTNEW dataset #----# ####
save_dataset(dt,
             'B_CRS_[crs.update_mmyy]_INTNEW',
             'CRS', 'fin')
#-------------------------------# ####

cat('  Further process bilaterals data\n')
#----# Further process bilaterals data #----# ####
dt <- dt[isocode != 'EC', ]
final_disb_cols <- grep("final_.+_disb_curr", names(dt), value = TRUE)
dt <- dt[, lapply(.SD, sum, na.rm = TRUE),
         by = c('isocode', 'donor_name', 'ISO3_RC', 'year', 'INKIND', 'ELIM_CH',
                'CHANNEL', 'gov', 'grant_loan', 'is_climate'),
         .SDcols = final_disb_cols]
dt[donor_name == '', donor_name := NA]

temp <- unique(dt[!is.na(donor_name), c('donor_name', 'isocode')])
setnames(temp, 'donor_name', 'supp_donor_name')
dt <- merge(dt, temp, by='isocode', all.x=T)
dt[is.na(donor_name), donor_name := supp_donor_name]
dt[, supp_donor_name := NULL]

setnames(dt,
         final_disb_cols,
         gsub("final_", "", gsub("_disb_curr", "_DAH", final_disb_cols))
         )
setnames(dt,
         c('all_DAH', 'donor_name', 'isocode'),
         c('DAH', 'DONOR_COUNTRY', 'ISO_CODE'))
dt[, `:=`(INCOME_SECTOR = 'PUBLIC',
          INCOME_TYPE = 'CENTRAL',
          SOURCE_DOC = paste0('OECD Creditor Reporting System v ', get_dah_param('CRS', 'update_MMYY')))]
bilaterals <- copy(dt)
rm(dt, temp)
#-------------------------------------------# ####

cat('  Prep income data from DAC\n')
#----# Prep income data from DAC #----# ####
# Unlike all other bilateral agencies, the EC receives donations from other sources.
# This is estimated based on OUTFLOW that was just calculated. 
temp_fc <- fread(get_path("meta", "locs", "countrycodes_official.csv"),
                 select = c('iso3', 'country_lc'))
setnames(temp_fc, c('iso3', 'country_lc'), c('ISO_CODE', 'DONOR_NAME'))
#-------------------------------------# ####

cat('  Prep DAC disbursement by donor data\n')
#----# Prep DAC disbursement by donor data #----# ####
# load DAC2A and filter
dt <- fread(get_path('DAC', 'raw', 'OECD.DCD.FSD,DSD_DAC2@DF_DAC2A.csv'))
dt[Donor == "Korea", Donor := "S Korea"]
dt <- dt[Recipient %in% c("European Commission [EC]",
                          "European Development Fund [EDF]",
                          "Other EU institutions") &
             Donor %in% dah_cfg$crs$donors &
             Measure == "Gross ODA" &
             `Price base` == "Current prices" &
             TIME_PERIOD >= 1990]

setnames(dt,
         c("PRICE_BASE", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE", "FLOW_TYPE"),
         c("price_base_code", "unit_measure_code", "year", "value", "flow_type_code"))
id_cols <- grep("^[[:upper:]]+$", names(dt), value = TRUE)
setnames(dt, id_cols, paste0(id_cols, "_code"))
names(dt) <- gsub(" ", "_", tolower(names(dt)))

setnames(dt, c('donor', 'recipient'), c('DONOR_NAME', 'CHANNEL'))
dt[, value := as.numeric(value)]
dt[, year := as.character(year)]
setnames(dt, "value", "INCOME")

dt <- merge(dt, temp_fc, by='DONOR_NAME', all.x=T)
dt[DONOR_NAME == "Czechia", ISO_CODE := "CZE"]
dt[, CHANNEL := "EC"]

dt <- dt[, .(INCOME = sum(INCOME, na.rm = TRUE)),
         by = .(DONOR_NAME, ISO_CODE, CHANNEL, year)]
dt[, TOT_INC := sum(INCOME, na.rm=T), by='year']
dt[, INC_SHARE := INCOME/TOT_INC]
dt[, TOT_SHARE := sum(INC_SHARE, na.rm=T), by='year']

ec_income <- copy(dt)
rm(dt, temp_fc)
#-----------------------------------------------# ####

cat('  Generate EC inkind expenses\n')
#----# Generate inkind expenses #----# ####
ec_inkind <- copy(ec_income)
ec_inkind[, `:=`(INKIND_RATIO = 0.08517, INCOME = 0, INKIND = 1)]

ec_adb <- rbind(ec_income, ec_inkind, fill=T)
ec_adb[is.na(INKIND), INKIND := 0]
ec_adb[, `:=`(INCOME_SECTOR = 'PUBLIC', INCOME_TYPE = 'CENTRAL',
              DONOR_COUNTRY = DONOR_NAME,
              SOURCE_DOC = 'DAC Table 2a, CRS, EuropeAid Annual Report')]
ec_adb[, year := as.numeric(year)]
rm(ec_inkind, ec_income)
#------------------------------------# ####

cat('  Format for ADB_PDB\n')
#----# Format for ADB_PDB #----# ####
dt <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_INTNEW.csv'))
dt <- dt[isocode == 'EC', ]

dt[recipient_name == "Bilateral, unspecified", ISO3_RC := 'WLD']
dt[crs_id == "DUMMY", ISO3_RC := 'QZA']
dt[ISO3_RC == "", ISO3_RC := 'QZA']
dt[LEVEL == "GLOBAL", ISO3_RC := 'WLD']
dt[is.na(gov), gov := 0]
#------------------------------# ####

cat('  Merge income with total DAH\n')
#----# Merge income with total DAH #----# ####
final_disb_cols <- grep("final_.+_disb_curr", names(dt), value = TRUE)
data <- data.table()
for (i in 0:1) {
  t <- dt[eliminations == i,]
  t <- t[, lapply(.SD, sum, na.rm = TRUE),
          by = c('isocode', 'CHANNEL', 'ISO3_RC', 'year',
                 'eliminations', 'gov', 'grant_loan', 'is_climate'),
          .SDcols = final_disb_cols]
  setnames(t,
           final_disb_cols,
           gsub("final_", "", gsub("_disb_curr", "_DAH", final_disb_cols)))
  setnames(t, 'all_DAH', 'DAH')
  t <- t[DAH != 0, ] # A few more observations kept here
  t[, total := sum(DAH, na.rm=T), by='year']
  t[, fraction := DAH / total]
  t[, tot_fraction := sum(fraction, na.rm=T), by='year']
  t <- merge(t, ec_adb[, -"CHANNEL"], by='year', allow.cartesian = T)
  for (col in grep("DAH", names(t), value = TRUE)) {
    setnames(t, col, paste0('temp_', col))
    t[, eval(col) := get(paste0('temp_', col)) * INC_SHARE]
    t[INKIND == 1, eval(col) := get(paste0('temp_', col)) * INC_SHARE * INKIND_RATIO]
  }
  t[, grep("temp_", names(t)) := NULL]

  data <- rbind(data, t, fill=T)
  rm(t)
}

data[, ELIM_CH := 0]
data[eliminations == 1, ELIM_CH := 1]
data[INKIND == 1, ELIM_CH := 0]
data[INKIND == 1, CHANNEL := 'EC']

data <- rbind(data, bilaterals, fill=T)

data[is.na(INKIND), INKIND := 0]
data[is.na(ELIM_CH), ELIM_CH := 0]
data[is.na(gov), gov := 0]

dah_hfa_cols <- grep("_DAH", names(data), value = TRUE)
data[, total_alloc := rowSums(.SD, na.rm = TRUE), .SDcols = dah_hfa_cols]
data[, other_DAH := DAH - total_alloc]
testme2 <- copy(data)
rm(dt, bilaterals, ec_adb, col, i)
#---------------------------------------# ####

cat('  Cancel out negative disbursements\n')
#----# Cancel out negative disbursements #----# ####

dah_cols <- grep("DAH", names(testme2), value = TRUE)
dah_hfa_cols <- grep("_DAH", names(testme2), value = TRUE)
diseases <- c("all", gsub("_DAH", "", dah_hfa_cols))

dt <- data.table()
for (i in 0:1) {
  t <- testme2[ELIM_CH == i, ]
  
  # Collapse so that unique identifier is: source, channel, recipient, year
  # CHANNEL DONOR_COUNTRY ISO_CODE ISO3_RC INKIND gov SOURCE_DOC
  t[CHANNEL == "BIL_DAH", CHANNEL := paste0('BIL_', ISO_CODE)]
  t <- t[, lapply(.SD, sum, na.rm = TRUE),
          by = c('year', 'CHANNEL', 'DONOR_COUNTRY', 'ISO_CODE', 'ISO3_RC',
                 'INKIND', 'gov', 'SOURCE_DOC', 'is_climate'),
          .SDcols = dah_cols]
  
  # Subtract negative HFA funding from other, and set the negative HFA to 0
  setnames(t, 'DAH', 'all_DAH')
  for (disease in diseases[diseases != 'all']) {
    t[get(paste0(disease, '_DAH')) < 0, eval(paste0('neg_', disease)) := 1]
    t[get(paste0('neg_', disease)) == 1,
      other_DAH := other_DAH + get(paste0(disease, '_DAH'))]
    t[get(paste0('neg_', disease)) == 1, eval(paste0(disease, '_DAH')) := 0]
  }
  
  # Loop through years from data_yr to 1990, and for each year add negative values
  #  of other DAH to the previous year's other DAH (by temp_id), and set the negative other to 0 
  yearlist <- get_dah_param('CRS', 'data_year'):1991
  setorder(t, CHANNEL, DONOR_COUNTRY, ISO_CODE, ISO3_RC, INKIND, gov, SOURCE_DOC, year)
  
  for (yr in yearlist) {
    t[, shift_other := shift(other_DAH, n=1, type='lead')]
    t[shift_other < 0 & year == yr - 1, other_DAH := other_DAH + shift_other]
    t[year == yr & other_DAH < 0, other_DAH := 0]
    t[, shift_other := NULL]
  }
  
  # If other is negative in 1990, reset it to 0 since there are no more previous years to subtract it from	
  t[year==1990 & other_DAH<0, other_DAH := 0]
  t[, ELIM_CH := i]
  
  # calculate a new total as the sum of the HFAs
  t[, new_tot := rowSums(.SD, na.rm = TRUE), .SDcols = dah_hfa_cols]
  t[, all_DAH := new_tot]
  
  dt <- rbind(dt, t, fill=T)
  rm(t, disease, yr, yearlist, i)
}

setnames(dt, c("all_DAH", "year"), c("DAH", "YEAR"))
dt[, `:=`(INCOME_SECTOR = 'PUBLIC',
          INCOME_TYPE = "CENTRAL",
          DONOR_NAME = DONOR_COUNTRY,
          REPORTING_AGENCY = 'CRS')]

drop_cols <- c("new_tot", grep("^neg_", names(dt), value = TRUE))
dt <- dt[!(CHANNEL == 'EC' & DAH == 0),
         !drop_cols,
         with = FALSE]

dt[is.na(is_climate), is_climate := FALSE]
#---------------------------------------------# ####

#----# Save dataset #----# ####
cat('  Save main dataset\n')
save_dataset(dt, 'B_CRS_[crs.update_mmyy]_ADB_PDB_beforePMI',
             'CRS', 'fin')
#------------------------# ####
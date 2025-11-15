########################################################################################
## UNICEF Recipient CFix using CRS data
## Description: Aggregates UNCEF data and then merges on CRS data for UNICEF to fill in 
## recipient information where possible. See Hub page describing how to use CRS data as a
## secondary data source
########################################################################################
rm(list = ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
asn_date <- format(Sys.time(), "%Y%m%d")

# Variable prep
codes <- get_path("meta", "locs")
sources <- fread(paste0(dah.roots$j, 'FILEPATH/income_sector_and_type_assignments_2021.csv'))
sources[, DONOR_NAME := trimws(DONOR_NAME, which = c('both'))]

# Read in data
crs_raw <- fread(paste0(get_path('CRS', 'fin'), 'B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNICEF.csv'))
hfas <- names(crs_raw)[names(crs_raw) %like% 'DAH']

# CRS recipient info
crs_recip <- copy(crs_raw)
crs_recip <- crs_recip[INKIND == 0]
crs_recip <- crs_recip[, lapply(.SD,sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                                 'REPORTING_AGENCY'), .SDcols = names(crs_recip)[names(crs_recip) %like% 'DAH']]

setnames(crs_recip, 'ISO3_RC', 'ISO3_RC_new')

# Read in unicef ADB_PDB (without DC)
unicef <- setDT(read.dta13(paste0(get_path('UNICEF', 'fin'), "UNICEF_ADB_PDB_FGH", dah.roots$report_year, "_noDC.dta")))

# Fix income type since it is missing
unicef[, donor_name := tolower(DONOR_NAME)] # need to change in order to get the special characters cleaned
unicef[, donor_name:= gsub("<ad>", " ", donor_name)]
unicef[, donor_name:= gsub("<ed>", "i", donor_name)]
unicef[, donor_name:= gsub("<92>", " ", donor_name)]
unicef[, donor_name:= gsub("<96>", " ", donor_name)]
unicef[, donor_name:= gsub("<c9>", "e", donor_name)]
unicef[, donor_name:= gsub("<93>", "o", donor_name)]
unicef[, donor_name:= gsub("<f4>", "o", donor_name)]
unicef[, donor_name:= gsub("<a1>", "i", donor_name)]
unicef[, donor_name:= gsub("<fa>", "u", donor_name)]
unicef[, donor_name:= gsub("<ec>", "i", donor_name)]
unicef[, donor_name:= gsub("<e8>", "e", donor_name)]
unicef[, donor_name:= gsub("<e9>", "e", donor_name)]
unicef[, donor_name:= gsub("<ca>", "e", donor_name)]
unicef[, donor_name:= gsub("<fc>", "u", donor_name)]
unicef[, donor_name:= gsub("<dc>", "u", donor_name)]
unicef[, donor_name:= gsub("<94>", " ", donor_name)]
unicef[, donor_name:= gsub("<d4>", "o", donor_name)]
unicef[, donor_name:= gsub("<c4>", "a", donor_name)]
unicef[, donor_name:= gsub("<e4>", "a", donor_name)]
unicef[, donor_name:= gsub("<d1>", "n", donor_name)]
unicef[, donor_name:= gsub("<f3>", "o", donor_name)]
unicef[, donor_name:= gsub("<97>", " ", donor_name)]
unicef[, donor_name:= gsub("<80>", " ", donor_name)]
unicef[, donor_name:= gsub("<c0>", "a", donor_name)]
unicef[, donor_name:= gsub("<e0>", "a", donor_name)]
unicef[, donor_name:= gsub("<c2>", "a", donor_name)]
unicef[, donor_name:= gsub("<c7>", "c", donor_name)]
unicef[, donor_name:= gsub("<c3>", "a", donor_name)]

unicef <- string_clean(unicef, 'donor_name')
unicef[, upper_donor_name := trimws(upper_donor_name, which = c('both'))]

## Fix some donor names
unicef[upper_donor_name == "ZINBABWE", upper_donor_name := "ZIMBABWE"]
unicef[upper_donor_name == "ZANBIA", upper_donor_name := "ZAMBIA"]
unicef[upper_donor_name == "YUGOSLAVIA FEDERAL", upper_donor_name := "YUGOSLAVIA"]
unicef[upper_donor_name == "ZINBABWE", upper_donor_name := "ZIMBABWE"]
unicef[upper_donor_name == "BRITISH VLRGLN ISIANDS", upper_donor_name := "BRITISH VIRGIN ISLANDS"]
unicef[upper_donor_name == "BRUNEI DARUSSALEM", upper_donor_name := "BRUNEI DARUSSALAM"]
unicef[upper_donor_name == "CENTRAL AFRICAN REPUBLLC", upper_donor_name := "CENTRAL AFRICAN REPUBLIC"]	
unicef[upper_donor_name %in% c("CZECHOSLOVAKIA B", "CZECHSLOVAKIA"), upper_donor_name := "CZECHOSLOVAKIA"]
unicef[upper_donor_name == "IRAN LALAMIC REPUBLIC OF", upper_donor_name := "IRAN ISLAMIC REPUBLIC OF"]
unicef[upper_donor_name == "ITATY", upper_donor_name := "ITALY"]
unicef[upper_donor_name == "LAO PEOPLE S DEMOCRATIC REPUBIIC", upper_donor_name := "LAO PEOPLE S DEMOCRATIC REPUBLIC"]
unicef[upper_donor_name == "LIBYAN ARAB JEMEHIRIYA", upper_donor_name := "LIBYAN ARAB JAMAHIRIYA"]	
unicef[upper_donor_name == "REPUBILC OF KOREA", upper_donor_name := "REPUBLIC OF KOREA"]
unicef[upper_donor_name == "SWAZLILAND", upper_donor_name := "ESWATINI"]
unicef[upper_donor_name == "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN", upper_donor_name := "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"]	
unicef[upper_donor_name %in% c('END VIOLENCE AGAINST CHILDREN', 'END VIOLENCE AGAINST CHILDREN FUND', 'END VIOLENCEAGAINST CHILDREN FUND'), upper_donor_name := 'END VIOLENCE FUND']  
unicef[upper_donor_name == 'KOREA DEMOCRATIC PEOPLE S REPUBLIC OF', upper_donor_name := 'DEMOCRATIC PEOPLE S REPUBLIC OF KOREA']
unicef[upper_donor_name == 'AGENCE DE COOPERATION CULTURELLE ET TECHNIQUE AGENCY FOR CULTURAL AND TECHNICAL COOPERATION', upper_donor_name := 'AGENCE DE COOPERATION CULTURELLE ET TECHNIQUE']
unicef[upper_donor_name == 'MACEDONIA THE FORMER YUGOSLAV REPUBLIC OF', upper_donor_name := 'THE FORMER YUGOSLAV REPUBLIC OF MACEDONIA']
unicef[upper_donor_name == 'THE INTERNATIONAL TELECOMMUNICATION UNION', upper_donor_name := 'INTERNATIONAL TELECOMMUNICATION UNION ITU']
unicef[upper_donor_name %in% c('UNITED NATIONS MULTI PARTNER TRUST FUND', 'UNITED NATIONS MULTI PARTNER TRUST<A'), upper_donor_name := 'UN MULTI PARTNER TRUST FUND']
unicef[upper_donor_name == 'UNITED NATIONS RELIEF AND WORKS AGENCY UNRWA', upper_donor_name := 'UNITED NATIONS RELIEF AND WORKS AGENCY FOR PALESTINE REFUGEES']
unicef[upper_donor_name == 'UNITED NATIONS RESIDENT COORDINATORS OFFICE', upper_donor_name := 'UNITED NATIONS THE RESIDENT COORDINATOR OFFICE']
unicef[upper_donor_name == 'OFFICE FOR THE HIGH COMMISSIONER FOR HUMAN RIGHTS OHCHR', upper_donor_name := 'OHCHR']
unicef[upper_donor_name == 'INTERNATIONAL FEDERATION OF RED CROSS AND CRESCENT', upper_donor_name := 'INTERNATIONAL FEDERATION OF RED CROSS AND RED CRESCENT']
unicef[upper_donor_name == 'INTERNATIONAL DEVELOPMENT RESESARCH CENTRE', upper_donor_name := 'INTERNATIONAL DEVELOPMENT RESEARCH CENTRE']
unicef[upper_donor_name == 'DEVELOPMENT BANK OF LATIN AMERICA', upper_donor_name := 'CORPORACION ANDINA DE FOMENTO CAF']
unicef[upper_donor_name %in% c('EUROPEAN COMMISSION ECHO', 'EUROPEAN COMMISSION HUMANITARIAN AID OFFICE', 'EUROPEAN COMMISSION HUMANITARIAN OFFICE'), upper_donor_name := 'EUROPEAN COMMISSION ECHO']
unicef[upper_donor_name == 'MISCELLANEAOUS INCOME', upper_donor_name := 'MISCELLANEAOUS INCOME']
unicef[upper_donor_name == 'OTHERS REGULAR RESOURCES', upper_donor_name := 'OTHER REGULAR RESOURCES']
unicef[upper_donor_name == 'SPECIAL REPRESENTATIVE OF THE SECRETARY GENERAL ON VIOLENCE AGAINST CHILDREN', upper_donor_name := 'SPECIAL REPRESENTATIVE OF THE SECRETARY GENERAL SRSG ON VIOLENCE AGAINST CHILDREN']
                                           
 
unicef2 <- merge(unicef, sources, by.x = 'upper_donor_name', by.y = 'DONOR_NAME', all.x = T)
unicef2[, INCOME_SECTOR.x := INCOME_SECTOR.y] 
unicef2[, INCOME_TYPE.x := INCOME_TYPE.y] # replace all income type since it was not filled out


# Fix ones that didn't match 
unicef2[grepl('OTHER PRIVATE DONOR FROM ', upper_donor_name) & is.na(INCOME_SECTOR.y), `:=` (INCOME_SECTOR.x = "PRIVATE", INCOME_TYPE.x = "OTHER")]
unicef2[grepl('UNICEF NATIONAL COMMITTEE', upper_donor_name) & is.na(INCOME_SECTOR.y), `:=` (INCOME_SECTOR.x = 'OTHER', INCOME_TYPE.x = 'UN')]
unicef2[upper_donor_name == 'EUROPEAN COMMISSION ECHO', `:=` (INCOME_SECTOR.x = 'OTHER', INCOME_TYPE.x = 'EC')]
unicef2[upper_donor_name == 'INTERNATIONAL ON LINE DONATIONS', `:=` (INCOME_SECTOR.x = 'UNALL', INCOME_TYPE.x = 'UNALL')]
unicef2[upper_donor_name == 'MISCELLANEAOUS INCOME', `:=` (INCOME_SECTOR.x = 'UNALL', INCOME_TYPE.x = 'UNALL')]
unicef2[upper_donor_name == 'ONE OFF DONATIONS', `:=` (INCOME_SECTOR.x = 'UNALL', INCOME_TYPE.x = 'UNALL')]
unicef2[upper_donor_name == 'UNITED NATIONS ASSISTANCE MISSION FOR SOMALIA UNSOM', `:=` (INCOME_SECTOR.x = 'OTHER', INCOME_TYPE.x = 'UN')]
unicef2[upper_donor_name == 'UNITED NATIONS FRAMEWORK CONVENTION ON CLIMATE CHANGE UNFCCC', `:=` (INCOME_SECTOR.x = 'OTHER', INCOME_TYPE.x = 'UN')]

# Fix isocodes
unicef2[ISO_CODE.y != "" & !is.na(ISO_CODE.y), ISO_CODE.x := ISO_CODE.y]
unicef2 <- unicef2[, !c('INCOME_SECTOR.y', 'INCOME_TYPE.y', 'ISO_CODE.y', 'donor_name')]
setnames(unicef2, c('INCOME_SECTOR.x', 'INCOME_TYPE.x', 'ISO_CODE.x'), c('INCOME_SECTOR', 'INCOME_TYPE', 'ISO_CODE'))

rm(unicef)

# continue
unicef_inkind <- copy(unicef2)
unicef_inkind <- unicef_inkind[INKIND == 1]
unicef_inkind <- unicef_inkind[, lapply(.SD, sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                                          'REPORTING_AGENCY', 'GOV', 'DONOR_COUNTRY'),
                               .SDcols = names(unicef_inkind)[names(unicef_inkind) %like% 'DAH']]

## Prepare CRS data for merging to remove unallocable amounts
crs <- copy(crs_raw)
crs <- crs[INKIND == 0]
crs <- crs[, lapply(.SD,sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE',
                                     'REPORTING_AGENCY'), .SDcols = names(crs)[names(crs) %like% 'DAH']]
setnames(crs, names(crs)[names(crs) %like% 'DAH'],
         gsub('DAH', 'DAH_crs', names(crs)[names(crs) %like% 'DAH']))

## Prepare unicef data for merging
unicef <- unicef2[INKIND == 0]

unicef <- unicef[, lapply(.SD, sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                            'REPORTING_AGENCY', 'GOV', 'DONOR_COUNTRY'),
                 .SDcols = names(unicef)[names(unicef) %like% 'DAH']]
setnames(unicef, names(unicef)[names(unicef) %like% 'DAH'],
         gsub('DAH', 'DAH_un', names(unicef)[names(unicef) %like% 'DAH']))

## Merge together
dt <- merge(unicef, crs, by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE'), all = T)
crs_no_keep <- copy(dt[is.na(REPORTING_AGENCY.x)])
crs_no_keep[, drop := 1]

dt <- dt[!is.na(REPORTING_AGENCY.x)] # removes CRS rows we don't want

## Start re-allocation
# Calculate difference (Channel - CRS) to get new unallocable row

# First separate out rows that merged with ones that do not have recipient data
dt_no_merge <- copy(dt)
dt_no_merge <- dt_no_merge[is.na(REPORTING_AGENCY.y)]

dt_merge <- copy(dt)
dt_merge <- dt_merge[REPORTING_AGENCY.y == 'CRS']

# Subtract CRS HFA values from Channel HFA values (these are the pas in the crs that are not in the un channel)
hfa_crs <- c(  "mal_diag_DAH", "oid_hss_hrh_DAH", "mal_hss_other_DAH", "tb_hss_hrh_DAH", "mal_con_irs_DAH", 
               "mal_hss_hrh_DAH", "nch_hss_hrh_DAH", "mal_con_oth_DAH", "oid_zika_DAH", "oid_amr_DAH",   
               "ncd_hss_other_DAH", "mal_comm_con_DAH", "ncd_hss_hrh_DAH", "hiv_hss_hrh_DAH", "mal_amr_DAH",
               "oid_hss_other_DAH", "unalloc_DAH", "mal_treat_DAH", "swap_hss_me_DAH", "rmh_hss_me_DAH",
               "nch_hss_me_DAH", "hiv_hss_me_DAH", "mal_hss_me_DAH", "tb_hss_me_DAH", "oid_hss_me_DAH", "ncd_hss_me_DAH")

for(hfa in hfa_crs) {
  dt_merge[, paste0(eval(hfa), '_un') := 0]
}

for (hfa in hfas) {
  dt_merge[, paste0(eval(hfa), '_diff') := get(paste0(hfa,'_un')) - get(paste0(hfa,'_crs'))]
}

# reallocate negatives in unallocable (un rows) - The diff will be the new un rows once CRS is added in
# Where we have more CRS money than unicef total we are keeping all CRS and zeroing out the unicef data
# Where there are negatives in the new  unallocable row (_diff) we first try adding negatives to other and if 
# other is negative we scale down the positive hfas equally and make other zero

# Make unicef unallocable rows 0 if DAH_diff is negative because that means CRS envelope is more
hfas2 <- names(crs_raw)[names(crs_raw) %like% '_DAH']

for (hfa in hfas2) {
  dt_merge[DAH_diff < 0, paste0(eval(hfa), '_diff') := 0]
}
dt_merge[DAH_diff < 0, DAH_diff := 0]

# Remove negatives in hfas by:
# 1) try to remove from other
# 2) if there are still negatives scale down all positive hfas and remove the negative
hfas3 <- copy(hfas2)
hfas3 <- hfas3[hfas3 != "other_DAH"] # need to remove other or else it will double the negative other at the end

dt_merge[, total_positive_DAH_diff := 0]
for (hfa in hfas3) {
  dt_merge[get(paste0(hfa, '_diff')) < 0, other_DAH_diff := other_DAH_diff + get(paste0(hfa,'_diff'))]
  dt_merge[get(paste0(hfa, '_diff')) > 0, total_positive_DAH_diff := total_positive_DAH_diff + get(paste0(hfa,'_diff'))]
}

dt_merge[, new_tot := total_positive_DAH_diff + other_DAH_diff]
dt_merge[other_DAH_diff < 0, scale := new_tot / total_positive_DAH_diff] # scale down other projects if there is any negative amount leftover
dt_merge[other_DAH_diff >= 0, scale := 1] # make scale 1 if other is NOT negative (otherwise it will add funds)

for (hfa in hfas3) {
  dt_merge[get(paste0(hfa, '_diff')) > 0, paste0(eval(hfa), '_diff') := get(paste0(hfa,'_diff')) * scale] # scale down positive hfas
  dt_merge[get(paste0(hfa, '_diff')) < 0, paste0(eval(hfa), '_diff') := 0] # make negative hfas 0
}

dt_merge[other_DAH_diff < 0, other_DAH_diff := 0]

# CHECK new totals are correct (use hfas2 to include other_DAH_diff)
dt_merge[, total_DAH_diff_check := 0]
for (hfa in hfas2) {
  dt_merge[, total_DAH_diff_check := total_DAH_diff_check + get(paste0(hfa,'_diff'))]
}
dt_merge[, diff := DAH_diff - total_DAH_diff_check]

stopifnot(count(dt_merge[diff > 5 | diff < -5]) == 0)

# Format new unallocable rows
keep <- c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE', 'ISO3_RC', 'REPORTING_AGENCY.x', 'GOV',
          'DONOR_COUNTRY', names(dt_merge)[names(dt_merge) %like% 'DAH_diff'])
dt_merge <- dt_merge[, keep, with = FALSE]
setnames(dt_merge, c(names(dt_merge)[names(dt_merge) %like% 'DAH_diff'], 'REPORTING_AGENCY.x'), c(gsub('_diff', '', names(dt_merge)[names(dt_merge) %like% 'DAH_diff']), 'REPORTING_AGENCY'))

# format old not matched rows
keep2 <- c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE', 'ISO3_RC', 'REPORTING_AGENCY.x', 'GOV',
           'DONOR_COUNTRY', names(dt_no_merge)[names(dt_no_merge) %like% 'DAH_un'])
dt_no_merge <- dt_no_merge[, keep2, with = FALSE]
setnames(dt_no_merge, c(names(dt_no_merge)[names(dt_no_merge) %like% 'DAH_un'], 'REPORTING_AGENCY.x'), c(gsub('_un', '', names(dt_no_merge)[names(dt_no_merge) %like% 'DAH_un']), 'REPORTING_AGENCY'))

## append updated merged and not merged unallocable rows
dt_unicef <- rbind(dt_merge, dt_no_merge, fill = T)
dt_unicef <- rbind(dt_unicef, unicef_inkind, fill = T) #add inkind back in
dt_unicef[, ISO3_RC := "QZA"]

## add in CRS rows (only ones that had matched with UNICEF)
setnames(crs_recip, 'ISO3_RC_new', 'ISO3_RC')
crs_no_keep <- crs_no_keep[, c('YEAR', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE', 'drop')]
crs_keep <- merge(crs_recip, crs_no_keep, by = c('YEAR', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE'), all.x = T)
crs_keep[is.na(drop), drop := 0]
crs_keep <- crs_keep[drop != 1]
crs_keep[, drop := NULL]

dt <- rbind(dt_unicef, crs_keep, fill = T)

# replace Nas added where new columns were added for CRS that were not in unicef
for (hfa in hfas) {
  dt[is.na(get(hfa)), eval(hfa) := 0]
}

## save 
save_dataset(dt, paste0('UNICEF_ADB_PDB_FGH', dah.roots$report_year, '_noDC_recipient_fix_2'), 'UNICEF', 'fin')
save_dataset(dt, paste0('UNICEF_ADB_PDB_FGH', dah.roots$report_year, '_noDC_recipient_fix_2'), 'UNICEF', 'fin', write_dta = T)


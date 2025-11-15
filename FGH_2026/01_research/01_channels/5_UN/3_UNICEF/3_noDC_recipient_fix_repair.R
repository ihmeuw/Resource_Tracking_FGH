#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  UNICEF recipient fix Graphing code
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
defl <- get_path("meta", "defl")
# TODO: update, should be same as defl
prev_defl <- dirname(get_path("meta", "defl"))
cdefl <- setDT(read.dta13(paste0(defl, 'imf_usgdp_deflators_', dah.roots$defl_MMYY, '.dta')))[, c('YEAR', paste0('GDP_deflator_', dah.roots$report_year)), with=F]
pdefl <- setDT(read.dta13(paste0(prev_defl, '/imf_usgdp_deflators_', dah.roots$prev_defl_MMYY, '.dta')))[, c('YEAR', paste0('GDP_deflator_', dah.roots$prev_report_year)), with=F]
#----------------------------# ####
sources <- fread(paste0(dah.roots$j, 'FILEPATH/income_sector_and_type_assignments_2021.csv'))
sources$DONOR_NAME <- trimws(sources$DONOR_NAME, which = c("both"))

# read in old data
unicef <- data.table(read.dta13(paste0(get_path('UNICEF', 'fin'),
                                    paste0("UNICEF_ADB_PDB_FGH", dah.roots$report_year, "_noDC.dta"))))

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
unicef$upper_donor_name <- trimws(unicef$upper_donor_name, which = c("both"))
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

unicef2 <- merge(unicef, sources, by.x = 'upper_donor_name', by.y = 'DONOR_NAME', all.x = T)
unicef2[, INCOME_SECTOR.x := INCOME_SECTOR.y] 
unicef2[, INCOME_TYPE.x := INCOME_TYPE.y] 


# Fix ones that didn't match even though they are on the list (look into later)
unicef2[upper_donor_name == "AGENCE DE COOPERATION CULTURELLE ET TECHNIQUE AGENCY FOR CULTURAL AND TECHNICAL COOPERATION", `:=` (INCOME_SECTOR.x = "PRIVATE", INCOME_TYPE.x = "OTHER")]
unicef2[upper_donor_name == "EUROPEAN COMMISSION ECHO", `:=` (INCOME_SECTOR.x = "OTHER", INCOME_TYPE.x = "EC")]
unicef2[upper_donor_name == "EUROPEAN COMMISSION HUMANITARIAN AID OFFICE", `:=` (INCOME_SECTOR.x = "OTHER", INCOME_TYPE.x = "EC")]
unicef2[upper_donor_name == "EUROPEAN COMMISSION HUMANITARIAN OFFICE", `:=` (INCOME_SECTOR.x = "OTHER", INCOME_TYPE.x = "EC")]
unicef2[upper_donor_name == "INTERNATIONAL DEVELOPMENT RESESARCH CENTRE", `:=` (INCOME_SECTOR.x = "PUBLIC", INCOME_TYPE.x = "CENTRAL", ISO_CODE.x = "CAN")]
unicef2[upper_donor_name == "INTERNATIONAL FEDERATION OF RED CROSS AND CRESCENT", `:=` (INCOME_SECTOR.x = "PRIVATE", INCOME_TYPE.x = "NGO")]
unicef2[upper_donor_name == "KOREA DEMOCRATIC PEOPLE S REPUBLIC OF", `:=` (INCOME_SECTOR.x = "PUBLIC", INCOME_TYPE.x = "CENTRAL", ISO_CODE.x = "PRK")]
unicef2[upper_donor_name == "MACEDONIA THE FORMER YUGOSLAV REPUBLIC OF", `:=` (INCOME_SECTOR.x = "PUBLIC", INCOME_TYPE.x = "CENTRAL", ISO_CODE.x = "XYG")]
unicef2[upper_donor_name == "MISCELLANEAOUS INCOME", `:=` (INCOME_SECTOR.x = "UNALL", INCOME_TYPE.x = "UNALL")]
unicef2[upper_donor_name == "OTHERS REGULAR RESOURCES", `:=` (INCOME_SECTOR.x = "UNALL", INCOME_TYPE.x = "UNALL")]

# Fix isocodes
unicef2[ISO_CODE.y != "" & !is.na(ISO_CODE.y), ISO_CODE.x := ISO_CODE.y]
unicef2 <- unicef2[, !c('INCOME_SECTOR.y', 'INCOME_TYPE.y', 'ISO_CODE.y', 'donor_name')]
setnames(unicef2, c('INCOME_SECTOR.x', 'INCOME_TYPE.x', 'ISO_CODE.x'), c('INCOME_SECTOR', 'INCOME_TYPE', 'ISO_CODE'))

old <- copy(unicef2)

# read in new data
new <- data.table(read.dta13(paste0(get_path('UNICEF', 'fin'),
                                    paste0("UNICEF_ADB_PDB_FGH", dah.roots$report_year, "_noDC_recipient_fix_2.dta"))))


# Prepare fgh graphs income data
old2 <- copy(old)
old2 <- old2[, lapply(.SD,sum), by = c('YEAR', 'INCOME_SECTOR', 'ISO_CODE'), .SDcols = 'DAH']
setnames(old2, 'DAH', 'dah_old')

new2 <- copy(new)
new2 <- new2[, lapply(.SD,sum), by = c('YEAR', 'INCOME_SECTOR', 'ISO_CODE'), .SDcols = 'DAH']
setnames(new2, 'DAH', 'dah_new')

source <- merge(old2, new2, by = c('YEAR', 'INCOME_SECTOR', 'ISO_CODE'), all = T)
source[INCOME_SECTOR == "PUBLIC", source := ISO_CODE]
source[INCOME_SECTOR %in% c('MULTI', 'OTHER'), source := "OTHER"]
source[source == "", source := INCOME_SECTOR]

save_dataset(source, 'unicef_ADB_PDB_FGH2020_noDC_recipient_fix_compare_source_fgh_graphs_3', 'UNICEF', 'fin')

rm(old2, new2)

# Prepare fgh graphs recipient data
old2 <- copy(old)
old2 <- old2[, lapply(.SD,sum), by = c('YEAR', 'ISO3_RC'), .SDcols = 'DAH']
old2[, ISO3_RC := "QZA"]
setnames(old2, 'DAH', 'dah_old')

new2 <- copy(new)
new2 <- new2[, lapply(.SD,sum), by = c('YEAR', 'ISO3_RC'), .SDcols = 'DAH']
setnames(new2, 'DAH', 'dah_new')

recipient <- merge(old2, new2, by = c('YEAR', 'ISO3_RC'), all = T)

save_dataset(recipient, 'unicef_ADB_PDB_FGH2020_noDC_recipient_fix_compare_recipient_fgh_graphs_2', 'UNICEF', 'fin')

rm(old2, new2)
# Prepare fgh graphs program areas
old2 <- copy(old)
old2 <- melt.data.table(old2, id.vars = 'YEAR', measure.vars = names(old2)[names(old2) %like% '_DAH'])
setnames(old2, c('variable', 'value'), c('hfa', 'dah_old'))
old2 <- old2[, lapply(.SD, sum), by = c('YEAR', 'hfa'), .SDcols = 'dah_old']

new2 <- copy(new)
new2 <- melt.data.table(new2, id.vars = 'YEAR', measure.vars = names(new2)[names(new2) %like% '_DAH'])
setnames(new2, c('variable', 'value'), c('hfa', 'dah_new'))
new2 <- new2[, lapply(.SD, sum), by = c('YEAR', 'hfa'), .SDcols = 'dah_new']

hfa <- merge(old2, new2, by = c('YEAR', 'hfa'))

hfa[, hfa := str_replace(hfa, "_DAH", "")]

save_dataset(hfa, 'unicef_ADB_PDB_FGH2020_noDC_recipient_fix_compare_hfas_fgh_graphs_2', 'UNICEF', 'fin')

# compare how much is from CRS

old3 <- copy(old)
old3 <- old3[, lapply(.SD, sum), by = c('YEAR', 'REPORTING_AGENCY'), .SDcols = c('DAH')]

new3 <- copy(new)
new3 <- new3[, lapply(.SD, sum), by = c('YEAR', 'REPORTING_AGENCY'), .SDcols = c('DAH')]
setnames(new3, 'DAH', 'DAH_new')

dt <- merge(old3, new3, by = c('YEAR', 'REPORTING_AGENCY'), all = T)

new3 <- dcast.data.table(new3, YEAR ~ REPORTING_AGENCY, value.var = 'DAH_new')
new3[, total := CRS + UNICEF]
new3[, pct_crs := (CRS / total)*100]

fwrite(new3, paste0(dah.roots$h, 'unicef_pct_crs_2.csv'))

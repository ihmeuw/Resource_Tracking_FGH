#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize GFATM INT_PDB
#---------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#----------------------------#


cat('\n\n')
cat(green(' ################################\n'))
cat(green(" #### GFATM INT_PDB FINALIZE ####\n"))
cat(green(' ################################\n\n'))


cat('  Bring in disb data\n')
#----# Bring in disb data #----# ####
noneg <- setDT(fread(paste0(get_path('GFATM', 'int'), 'noneg.csv')))
setnames(noneg, 'DAH', 'DISBURSEMENT')
noneg[ISO3_RC == 'ZAN', ISO3_RC := 'TZA']
# Replace commitment = 0 if it's not the first observation of a give project
noneg <- noneg[order(PROJECT_ID, YEAR), ]
noneg[, dummy := 1]
noneg[, disb_no := 1:sum(dummy), by='PROJECT_ID']
noneg[, total_disb_no := sum(dummy), by='PROJECT_ID']
noneg[disb_no != 1, COMMITMENT := 0]
noneg[, `:=`(dummy = NULL, disb_no = NULL, total_disb_no = NULL)]
# Create a bunch of new columns
noneg[, `:=`(FUNDING_COUNTRY = "NA", ISO3_FC = "NA",
             FUNDING_TYPE = "GRANT", FUNDING_AGENCY = 'GFATM',
             FUNDING_AGENCY_SECTOR = 'PPP', FUNDING_AGENCY_TYPE = 'NA',
             PROJECT_NAME = "NA", DATA_LEVEL = 'PROJECT',
             SECTOR = 'HEALTH')]
setnames(noneg, c('PR_TYPE', 'DISEASE_COMPONENT'), c('RECIPIENT_AGENCY_TYPE', 'PURPOSE'))
#------------------------------#

cat('  Fill recipient agency type\n')
#----# Fill recipient agency type #----# ####
f_words <- c('Fundacao', 'Foundation', 'Fundacion', 'Fondation')
for (c in f_words) {
  noneg[, eval(c) := 0]
  noneg[grepl(toupper(c), RECIPIENT_AGENCY) == 1, eval(c) := 1]
}
noneg[, fn := rowSums(noneg[, c(f_words), with=F])]
noneg[, eval(f_words) := NULL]
# Split the string
noneg[, RECIPIENT_AGENCY_SECTOR := tstrsplit(RECIPIENT_AGENCY_TYPE, ':', keep = 1)]
# Corrections
noneg[RECIPIENT_AGENCY_SECTOR == "Government" |  RECIPIENT_AGENCY_SECTOR == "Governmental", RECIPIENT_AGENCY_SECTOR := 'GOV']
noneg[RECIPIENT_AGENCY_SECTOR == "Civil Society" | RECIPIENT_AGENCY_SECTOR == "Community Sector" , RECIPIENT_AGENCY_SECTOR := 'CSO']
noneg[RECIPIENT_AGENCY_SECTOR == "Multilateral Organization" | RECIPIENT_AGENCY_SECTOR == "Multilateral"  | RECIPIENT_AGENCY_SECTOR == "Multilateral/Intergovernmental"  , RECIPIENT_AGENCY_SECTOR := 'IGO']
noneg[RECIPIENT_AGENCY_SECTOR == "", RECIPIENT_AGENCY_SECTOR := 'UNSP']
noneg[RECIPIENT_AGENCY_TYPE == "Private Sector" & fn == 1, RECIPIENT_AGENCY_SECTOR := 'CSO']
noneg[RECIPIENT_AGENCY_TYPE == "Private Sector" & fn != 1, RECIPIENT_AGENCY_SECTOR := 'PSCORP']

noneg[RECIPIENT_AGENCY_TYPE == "Ministry of Health" | RECIPIENT_AGENCY_TYPE == "Ministry of Finance", RECIPIENT_AGENCY_TYPE := 'MIN']
noneg[RECIPIENT_AGENCY_TYPE == "Other", RECIPIENT_AGENCY_TYPE := 'OTHER']
noneg[RECIPIENT_AGENCY_TYPE == "United Nations Development Program", RECIPIENT_AGENCY_TYPE := 'UN']
noneg[RECIPIENT_AGENCY == "United Nations Interim Administration in Kosovo" | RECIPIENT_AGENCY == "United Nations Children's Fund" |  RECIPIENT_AGENCY == "The United Nations Children's Fund", RECIPIENT_AGENCY_TYPE := 'UN']
noneg[fn == 1, RECIPIENT_AGENCY_TYPE := 'FOUND']
noneg[RECIPIENT_AGENCY_TYPE == "", RECIPIENT_AGENCY_TYPE := 'UNSP']
noneg[RECIPIENT_AGENCY_TYPE  == "Private Sector- Other", RECIPIENT_AGENCY_TYPE := 'NA']
#--------------------------------------#

cat('  Add inkind for each project\n')
#----# Add inkind for each project #----# ####
inkind <- setDT(fread(paste0(get_path('GFATM', 'raw'), 'P_GFATM_INKIND.csv')))
inkind_pdb <- merge(noneg, inkind[, c('YEAR', 'INKIND_RATIO')], by='YEAR', all=T)
to_calc <- c('DISBURSEMENT', names(inkind_pdb)[names(inkind_pdb) %like% '_DAH'])
for (col in to_calc) {
  inkind_pdb[, eval(col) := get(col) * INKIND_RATIO]
}
inkind_pdb[, INKIND := 1]
#---------------------------------------#

cat('  Append datasets\n')
#----# Append datasets #----# ####
intpdb <- rbind(noneg, inkind_pdb, fill=T)
intpdb[is.na(INKIND), INKIND := 0]
intpdb <- intpdb[, c('YEAR', 'PROJECT_ID', 'PROJECT_NAME', 'DATA_LEVEL',
                     'FUNDING_COUNTRY', 'ISO3_FC', 'FUNDING_AGENCY',
                     'FUNDING_AGENCY_SECTOR', 'FUNDING_AGENCY_TYPE',
                     'RECIPIENT_AGENCY', 'RECIPIENT_AGENCY_TYPE',
                     'RECIPIENT_AGENCY_SECTOR', 'ISO3_RC', 'RECIPIENT_COUNTRY',
                     'COMMITMENT', 'DISBURSEMENT', 'SECTOR', 'PURPOSE',
                     'FUNDING_TYPE', 'INKIND',
                     names(intpdb)[
                       names(intpdb) %like% '_DAH$' | 
                         str_detect(names(intpdb), '^(?!final).*_frct$')]), with=F]

### VALIDATION
TOLERANCE <- 1e-8
to_calc <- c("hiv_amr", "hiv_treat", "hiv_prev", "hiv_pmtct", "hiv_other",
             "hiv_care", "hiv_ovc", "hiv_ct", "hiv_hss_other",
             "hiv_hss_hrh", "hiv_hss_me",
             "tb_hss_other", "tb_hss_hrh", "tb_treat", "tb_diag", "tb_amr",
             "tb_other", "tb_hss_me",
             "mal_hss_other", "mal_hss_hrh", "mal_diag", "mal_con_nets",
             "mal_con_irs", "mal_con_oth", "mal_hss_me",
             "mal_treat", "mal_comm_con", "mal_amr", "mal_other",
             "swap_hss_other", "swap_hss_pp", "swap_hss_hrh", "swap_hss_me")
frct_cols <- paste0(to_calc, "_frct")
if (!all(abs(intpdb[, rowSums(.SD, na.rm = T), .SDcols = frct_cols] - 1) <= TOLERANCE)) {
  stop("At least one row has fractions not summing to 1")
}

## Check if sums are close to 1 in all cases
if (!all(abs(intpdb[, rowSums(.SD, na.rm = T), .SDcols = frct_cols] - 1) <= 0.1)) {
  stop("At least one row has fractions not summing to 1")
}

intpdb <- intpdb[YEAR <= dah.roots$report_year]
###

rm(inkind, inkind_pdb, noneg, c, col, f_words, to_calc)
#---------------------------#

cat('  Save database\n')
#----# Save database #----# ####
save_dataset(intpdb, paste0('P_GFATM_INTPDB_1990_', dah.roots$report_year), 'GFATM', 'fin')
#-------------------------#
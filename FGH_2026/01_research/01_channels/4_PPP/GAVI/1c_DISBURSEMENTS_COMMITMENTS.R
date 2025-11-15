#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intake and Format GAVI Data
#---------------------# 

# First step here is to go into dah_parameters.yml and update the date tags under the GAVI section!

#----# Environment Prep #----# ####
rm(list=ls())


if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, haven, zoo)

# Variable prep
disb_update_year <- get_dah_param('GAVI', 'disb_update_year')
crs_mmyy <- get_dah_param('CRS', 'update_MMYY')

CODES <- get_path("meta", "locs")
region_codes <- fread(paste0(CODES, "fgh_location_set.csv"))[level==3, c('ihme_loc_id', 'location_name', 'region_name')]
country_codes <- fread(file.path(CODES, "countrycodes_official.csv"),
                       select = c("iso3", "country_lc"))
country_codes[country_lc == 'Côte d\'Ivoire', country_lc := 'Cote d\'Ivoire']
#----------------------------# 


cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### GAVI DISBURSEMENT DATA ####\n'))
cat(green(' ####################################\n\n'))

#------------------------------# 

cat('  Calculating Yearly Disbursements\n') 
#----# Calc Yearly Disbursement #----# ####
newdisb <- fread(get_path("GAVI", "int", "Disbursements.csv"))


#### Format 2021 disb data:
disb_2021 <- setDT(read_excel(paste0(get_path('GAVI', 'raw', report_year = 2021),
                                     'P_GAVI_Disbursement_correspondence_Feb_2022.xlsx'), skip = 2))
setnames(disb_2021, c('Funds Center', 'Type of Support', 'Applicant', 'Sum of 1 - Vaccine Total', 
                 'Sum of 2 - HSIS Programmes Total', 'Sum of 3 - Total Other Programmes', 'Sum of Grand total'),
         c('COUNTRY', 'PROGRAM_TYPE', 'PROGRAM', 'disb_vac', 'disb_hsis',
           'disb_other', 'disb_total'))

## remove grand total and covid rows
disb_2021 <- head(disb_2021, -1)
disb_2021 <- disb_2021[!PROGRAM %in% c('COV', 'COVAX')]

## format recipient country
disb_2021[, COUNTRY := sub(pattern = '[0-9 ]+', replacement = "", x = COUNTRY)]
disb_2021[COUNTRY == 'Bolivia', COUNTRY := 'Bolivia (Plurinational State of)']
disb_2021[COUNTRY == 'Bosnia & Herz', COUNTRY := 'Bosnia and Herzegovina']
disb_2021[COUNTRY == 'CAR', COUNTRY := 'Central African Republic']
disb_2021[COUNTRY %in% c('Congo DRC', 'Congo, Democratic Republic of the'), COUNTRY := 'Democratic Republic of the Congo']
disb_2021[COUNTRY == 'Congo, Republic of', COUNTRY := 'Congo']
disb_2021[COUNTRY == 'Côte d\'Ivoire', COUNTRY := 'Cote d\'Ivoire']
disb_2021[COUNTRY == 'Guinea Bissau', COUNTRY := 'Guinea-Bissau']
disb_2021[COUNTRY %in% c('Korea', 'Korea DPR'), COUNTRY := 'Republic of Korea']
disb_2021[COUNTRY == 'Kyrgyz Rep', COUNTRY := 'Kyrgyzstan']
disb_2021[COUNTRY %in% c('Lao PDR', 'Laos'), COUNTRY := 'Lao People\'s Democratic Republic']
disb_2021[COUNTRY == 'Moldova', COUNTRY := 'Republic of Moldova']
disb_2021[COUNTRY == 'Papua NG', COUNTRY := 'Papua New Guinea']
disb_2021[COUNTRY == 'Sao Tome', COUNTRY := 'Sao Tome and Principe']
disb_2021[COUNTRY == 'Solomon Isl', COUNTRY := 'Solomon Islands']
disb_2021[COUNTRY == 'Sudan South', COUNTRY := 'South Sudan']
disb_2021[COUNTRY == 'Sudan, Republic of', COUNTRY := 'Sudan']
disb_2021[COUNTRY == 'Syria', COUNTRY := 'Syrian Arab Republic']
disb_2021[COUNTRY == 'Tanzania', COUNTRY := 'United Republic of Tanzania']
disb_2021[COUNTRY == 'Vietnam', COUNTRY := 'Viet Nam']

## format program type
old_type <- unique(disb_2021$PROGRAM_TYPE)
new_type <- c(NA, 'operational support', 'nvs', 'vaccine introduction grant', 'product switch grant', 'yellow fever diagnostics', 'PBF')
disb_2021[, PROGRAM_TYPE := plyr::mapvalues(PROGRAM_TYPE, old_type, new_type)]
disb_2021[PROGRAM == 'CCEOP', PROGRAM_TYPE := 'cceop']
disb_2021[PROGRAM == 'HSS' & is.na(PROGRAM_TYPE), PROGRAM_TYPE := 'hss']
disb_2021[PROGRAM == 'ISD', PROGRAM_TYPE := 'isd']
disb_2021[PROGRAM == 'OSI', PROGRAM_TYPE := 'osi']

## format program 
disb_2021[, PROGRAM := plyr::mapvalues(PROGRAM, c('(blank)', 'EBL', '0', 'M'), c(NA, 'Ebola - Operational costs', NA, 'Measles'))]
disb_2021[PROGRAM_TYPE == 'operational support' & PROGRAM == 'IPV', PROGRAM := 'IPV op']
disb_2021[PROGRAM_TYPE == 'operational support' & PROGRAM == 'MenA', PROGRAM := 'MenA op']
disb_2021[PROGRAM_TYPE == 'operational support' & PROGRAM == 'MR', PROGRAM := 'MR - Operational costs']
disb_2021[PROGRAM_TYPE == 'operational support' & PROGRAM == 'TCV', PROGRAM := 'TCV op']
disb_2021[PROGRAM_TYPE == 'operational support' & PROGRAM == 'YF', PROGRAM := 'YF op']

#### Read prev disb data & rename
disb <- fread(paste0(get_path('GAVI', 'raw', report_year = 2021),
                     'All-Countries-Disbursements_by_year_paid_', disb_update_year, '.csv'),
              encoding = "Latin-1")
setnames(disb, c('Country', 'Region', 'High Level Category', 'Sub-category', 'Grand Total'),
         c('COUNTRY', 'REGION', 'PROGRAM_TYPE', 'PROGRAM', 'GRAND_TOTAL'))

# Fix special chars, country names
disb[COUNTRY %like% "d'Ivoire Total", COUNTRY := "Cote d'Ivoire Total"]
disb[COUNTRY == "Sâo Tomé", COUNTRY := "Sao Tome and Principe"]
disb[COUNTRY == "Sâo Tomé Total", COUNTRY := "Sao Tome and Principe Total"]
disb[COUNTRY == "Sâo Tomé", COUNTRY := "Sao Tome and Principe Total"]
disb[COUNTRY == 'Cameroun', COUNTRY := 'Cameroon']
disb[COUNTRY == 'Bolivia', COUNTRY := 'Bolivia (Plurinational State of)']
disb[COUNTRY == 'Bosnia & Herz', COUNTRY := 'Bosnia and Herzegovina']
disb[COUNTRY == 'CAR', COUNTRY := 'Central African Republic']
disb[COUNTRY == 'Congo DRC', COUNTRY := 'Democratic Republic of the Congo']
disb[COUNTRY == 'Congo, Democratic Republic of the', COUNTRY := 'Democratic Republic of the Congo']
disb[COUNTRY == 'Congo, Republic of', COUNTRY := 'Congo']
disb[COUNTRY == 'Guinea Bissau', COUNTRY := 'Guinea-Bissau']
disb[COUNTRY == 'Korea', COUNTRY := 'Republic of Korea']
disb[COUNTRY == 'Korea DPR', COUNTRY := 'Republic of Korea']
disb[COUNTRY == 'Kyrgyz Rep', COUNTRY := 'Kyrgyzstan']
disb[COUNTRY == 'Lao PDR', COUNTRY := 'Lao People\'s Democratic Republic']
disb[COUNTRY == 'Laos', COUNTRY := 'Lao People\'s Democratic Republic']
disb[COUNTRY == 'Moldova', COUNTRY := 'Republic of Moldova']
disb[COUNTRY == 'Papua NG', COUNTRY := 'Papua New Guinea']
disb[COUNTRY == 'Sao Tome', COUNTRY := 'Sao Tome and Principe']
disb[COUNTRY == 'Solomon Isl', COUNTRY := 'Solomon Islands']
disb[COUNTRY == 'Sudan South', COUNTRY := 'South Sudan']
disb[COUNTRY == 'Sudan, Republic of', COUNTRY := 'Sudan']
disb[COUNTRY == 'Syria', COUNTRY := 'Syrian Arab Republic']
disb[COUNTRY == 'Tanzania', COUNTRY := 'United Republic of Tanzania']
disb[COUNTRY == 'Vietnam', COUNTRY := 'Viet Nam']


# Destring
disb_cols <- c(names(disb)[grepl('YR_', names(disb))])
for (col in disb_cols) {
  disb[, eval(col) := str_replace_all(get(col), ',', '')]
  disb[, eval(col) := as.double(get(col))]
}

disb <- disb[!is.na(GRAND_TOTAL) & !grepl('Total', COUNTRY)]
disb[, GRAND_TOTAL := NULL]

# Format program types
disb[PROGRAM_TYPE == "", PROGRAM_TYPE := NA]
disb[grepl('CSO', PROGRAM), PROGRAM_TYPE := 'CSO']
disb[PROGRAM %in% c('Penta', 'Pneumo', 'Tetra DTP-HepB', 'Rotavirus', 'HPV', 
                    'Hib mono', 'Measles', 'Yellow Fever', 'Measles-Rubella', 'Measles 1st and 2nd dose',
                    'Meningitis A - campaign', 'Tetra DTP-Hib', 'Yellow Fever - campaign',
                    'HPV Demo', 'HPV MAC', 'HepB mono', 'IPV', 'Measles SIA', 'JEV',
                    'Meningitis A - mini catch-up campaign', 'Meningitis A', 'MR 1st and 2nd dose', 'MR 1st dose',
                    'MR 2nd dose'), 
     PROGRAM_TYPE := 'NVS']
disb[grepl('Operational|op.|op costs', PROGRAM, ignore.case = T), PROGRAM_TYPE := 'operational support']
disb[PROGRAM == 'IPV Catch-up RI', PROGRAM_TYPE := 'IPV']
disb[PROGRAM == 'IPV Catch-up RI', PROGRAM_TYPE := 'IPV']
disb[PROGRAM %in% c('ISD', 'Injection Safety Devices'), PROGRAM_TYPE := 'ISD']
disb[PROGRAM == 'OSI', PROGRAM_TYPE := 'OSI']
disb[PROGRAM == 'Additional Intro Support', PROGRAM_TYPE := 'AIS']
disb[PROGRAM == 'JEV-Routine', PROGRAM_TYPE := 'JEV']
disb[PROGRAM == 'TCV Outbreak', PROGRAM_TYPE := 'TCV']
disb[PROGRAM == 'TCV', PROGRAM_TYPE := 'TCV']
disb[, PROGRAM_TYPE := tolower(PROGRAM_TYPE)]

# Format program
disb[PROGRAM == 'YF - Operational costs', PROGRAM := 'YF op']
disb[PROGRAM %in% c('Yellow Fever', 'Yellow Fever Diagnostics'), PROGRAM := 'YF']
disb[PROGRAM_TYPE == 'operational support' & PROGRAM == 'IPV-Catch-up campaign op.costs', PROGRAM := 'IPV op']
disb[PROGRAM_TYPE == 'operational support' & PROGRAM == 'Meningitis A - operational costs', PROGRAM := 'MenA op']
disb[grepl('Pneumo', PROGRAM), PROGRAM := 'PCV']
disb[grepl('Rotavirus', PROGRAM), PROGRAM := 'RV']
disb[PROGRAM %in% c('TCV - Outbreak op. costs','TCV-Catch-up campaign op.costs'), PROGRAM := 'TCV op']
disb[grepl('Injection Safety Devices', PROGRAM), PROGRAM := 'ISD']
disb[PROGRAM_TYPE == 'nvs' & PROGRAM == 'Penta', PROGRAM := 'PENTA']

# merge disb 2021 with the rest (total from each year is in its own column)
disb <- merge(disb, disb_2021[, c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE', 'disb_total')], 
                  by.x = c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE'), by.y = c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE'), all = T)
disb <- merge(disb, newdisb[, c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE', 'COVID', 'RECIPIENT_AGENCY', 'YR_2022', 'YR_2023', 'YR_2024')], 
              by.x = c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE'), by.y = c('COUNTRY', 'PROGRAM', 'PROGRAM_TYPE'), all = T)
setnames(disb, 'disb_total', 'YR_2021')
disb[, (names(disb)) := lapply(.SD, function(x) {
  if (is.numeric(x)) {
    x[is.na(x)] <- 0
  }
  return(x)
})]

# Fill REGION column
disb[, REGION := NULL]
disb <- merge(disb, region_codes[, .(location_name, region_name)], by.x = 'COUNTRY', by.y = 'location_name', all.x = T)
setnames(disb, 'region_name', 'REGION')

# Reshape long & keep non-NAs
yr_cols <- names(disb)[grepl('YR_', names(disb))]
disb <- melt.data.table(disb, measure.vars= yr_cols, variable.name = 'YEAR', variable.factor = F, value.name = 'DISBURSEMENT_yr_paid')
disb <- disb[DISBURSEMENT_yr_paid != 0]
disb[, YEAR := as.numeric(str_replace(YEAR, 'YR_', ''))]
rm(yr_cols, disb_cols, disb_2021)
#------------------------------------# 

cat('  Calculating Yearly Commitments\n')
#----# Calc Yearly Commitment #----# ####
# Read data & rename
comt <- fread(paste0(get_path('GAVI', 'raw', report_year = 2021),
                     'All-Countries-Commitments_0221.csv'),
              encoding = "Latin-1")
setnames(comt, c('Country', 'Region', 'Sub-category', 'High Level Category', 'Grand Total'),
         c('COUNTRY', 'REGION', 'PROGRAM', 'PROGRAM_TYPE', 'GRAND_TOTAL'))
comt[COUNTRY %like% "d'Ivoire$", COUNTRY := "Cote d'Ivoire"]
comt[COUNTRY %like% "d'Ivoire Total", COUNTRY := "Cote d'Ivoire Total"]
comt[COUNTRY == "Sâo Tomé", COUNTRY := "Sao Tome and Principe"]
comt[COUNTRY == "Sâo Tomé Total", COUNTRY := "Sao Tome and Principe Total"]
comt[COUNTRY == 'Cameroun', COUNTRY := 'Cameroon']
comt[COUNTRY == 'Bolivia', COUNTRY := 'Bolivia (Plurinational State of)']
comt[COUNTRY == 'Bosnia & Herz', COUNTRY := 'Bosnia and Herzegovina']
comt[COUNTRY == 'CAR', COUNTRY := 'Central African Republic']
comt[COUNTRY %in% c('Congo DRC', 'Congo, Democratic Republic of the'), COUNTRY := 'Democratic Republic of the Congo']
comt[COUNTRY == 'Congo, Republic of', COUNTRY := 'Congo']
comt[COUNTRY == 'Guinea Bissau', COUNTRY := 'Guinea-Bissau']
comt[COUNTRY %in% c('Korea', 'Korea DPR'), COUNTRY := 'Republic of Korea']
comt[COUNTRY == 'Kyrgyz Rep', COUNTRY := 'Kyrgyzstan']
comt[COUNTRY %in% c('Lao PDR', 'Laos'), COUNTRY := 'Lao People\'s Democratic Republic']
comt[COUNTRY == 'Moldova', COUNTRY := 'Republic of Moldova']
comt[COUNTRY == 'Papua NG', COUNTRY := 'Papua New Guinea']
comt[COUNTRY == 'Solomon Isl', COUNTRY := 'Solomon Islands']
comt[COUNTRY == 'Sudan South', COUNTRY := 'South Sudan']
comt[COUNTRY == 'Sudan, Republic of', COUNTRY := 'Sudan']
comt[COUNTRY == 'Syria', COUNTRY := 'Syrian Arab Republic']
comt[COUNTRY == 'Tanzania', COUNTRY := 'United Republic of Tanzania']
comt[COUNTRY == 'Vietnam', COUNTRY := 'Viet Nam']

# Destring
comt_cols <- c(names(comt)[grepl('YR_', names(comt))], 'GRAND_TOTAL')
for (col in comt_cols) {
  comt[, eval(col) := str_replace_all(get(col), ',', '')]
  comt[, eval(col) := as.double(get(col))]
}
# Fill REGION column
comt <- comt[!is.na(GRAND_TOTAL) & !grepl('Total', COUNTRY)]
comt[, REGION := NULL]
comt <- merge(comt, region_codes[,c('location_name', 'region_name')], by.x = 'COUNTRY', by.y = 'location_name', all.x = T)
setnames(comt, 'region_name', 'REGION')

# Recode PROGRAM_TYPE
comt[PROGRAM_TYPE == '', PROGRAM_TYPE := NA]
comt[grepl('CSO', PROGRAM), PROGRAM_TYPE := 'CSO']
comt[PROGRAM %in% c('Penta', 'Pneumo', 'Tetra DTP-HepB', 'Rotavirus', 'HPV', 
                    'Hib mono', 'Measles', 'Yellow Fever', 'Measles-Rubella', 'Measles 1st and 2nd dose',
                    'Meningitis A - campaign', 'Tetra DTP-Hib', 'Yellow Fever - campaign',
                    'HPV Demo', 'HPV MAC', 'HepB mono', 'IPV', 'Measles SIA', 'JEV',
                    'Meningitis A - mini catch-up campaign', 'Meningitis A', 'MR 1st and 2nd dose', 'MR 2nd dose',
                    'MR 1st dose'), PROGRAM_TYPE := 'NVS']
comt[grepl('operational|op.|op costs|campaign', PROGRAM), PROGRAM_TYPE := 'operational support']
comt[PROGRAM == 'Injection Safety Devices', PROGRAM_TYPE := 'ISD']
comt[PROGRAM == 'IPV Catch-up RI', PROGRAM_TYPE := 'NVS']
comt[PROGRAM == 'OSI', PROGRAM_TYPE := 'OSI']
comt[PROGRAM == 'Additional Intro Support', PROGRAM_TYPE := 'AIS']
comt[PROGRAM == 'JEV-Routine', PROGRAM_TYPE := 'JEV']
comt[PROGRAM == 'TCV Outbreak', PROGRAM_TYPE := 'TCV']
comt[PROGRAM == 'TCV', PROGRAM_TYPE := 'TCV']
comt[, PROGRAM_TYPE := tolower(PROGRAM_TYPE)]

# Format program
comt[PROGRAM == 'YF - Operational costs', PROGRAM := 'YF op']
comt[PROGRAM %in% c('Yellow Fever', 'Yellow Fever Diagnostics'), PROGRAM := 'YF']
comt[PROGRAM_TYPE == 'operational support' & PROGRAM == 'IPV-Catch-up campaign op.costs', PROGRAM := 'IPV op']
comt[PROGRAM_TYPE == 'operational support' & PROGRAM == 'Meningitis A - operational costs', PROGRAM := 'MenA op']
comt[grepl('Pneumo', PROGRAM), PROGRAM := 'PCV']
comt[grepl('Rotavirus', PROGRAM), PROGRAM := 'RV']
comt[PROGRAM %in% c('TCV - Outbreak op. costs','TCV-Catch-up campaign op.costs'), PROGRAM := 'TCV op']
comt[grepl('Injection Safety Devices', PROGRAM), PROGRAM := 'ISD']
comt[PROGRAM_TYPE == 'nvs' & PROGRAM == 'Penta', PROGRAM := 'PENTA']

# Reshape long
comt <- melt.data.table(comt, measure.vars = comt_cols[comt_cols != 'GRAND_TOTAL'], variable.factor = F, variable.name = 'YEAR', value.name = 'COMMITMENT')
comt[, YEAR := as.numeric(str_replace(YEAR, 'YR_', ''))]

newcomt <- fread(get_path("GAVI", "int", "Commitments.csv"))
comt <- rbind(newcomt, comt, fill = TRUE)

# Collapse sum of COMMITMENTS
comt <- comt[, .(COMMITMENT = sum(COMMITMENT, na.rm = T)),
             by = .(COUNTRY, REGION, PROGRAM_TYPE, PROGRAM, YEAR, RECIPIENT_AGENCY, COVID)]
comt <- comt[COMMITMENT != 0]
rm(col, comt_cols)
#----------------------------------#  

cat('  Calculating Yearly Investments\n') 
#----# Calc Yearly Investment #----# ####
# Read data and rename
inv <- fread(paste0(get_path('GAVI', 'raw', report_year = 2021),
                          'All-Countries-Investment_cases_0221.csv'),
             encoding = "Latin-1")
setnames(inv, c('Investment Case', 'Programme', 'Country', 'Grand Total'),
         c('INVESTMENT_CASE', 'PROGRAM', 'COUNTRY', 'GRAND_TOTAL'))
inv[COUNTRY %like% "d'Ivoire", COUNTRY := "Cote d'Ivoire"]
inv[COUNTRY == 'Cameroun', COUNTRY := 'Cameroon']
inv[COUNTRY == 'CAR', COUNTRY := 'Central African Republic']
inv[COUNTRY %in% c('Congo DRC', 'Congo, Democratic Republic of the'), COUNTRY := 'Democratic Republic of the Congo']
inv[COUNTRY == 'East Timor', COUNTRY := 'Timor-Leste']
inv[COUNTRY == 'Guinea Bissau', COUNTRY := 'Guinea-Bissau']
inv[COUNTRY == 'Sudan South', COUNTRY := 'South Sudan']
inv[COUNTRY == 'Sudan, Republic of', COUNTRY := 'Sudan']
inv[COUNTRY == 'Syria', COUNTRY := 'Syrian Arab Republic']
inv[COUNTRY == 'Tanzania', COUNTRY := 'United Republic of Tanzania']

# Destring
inv_cols <- c(names(inv)[grepl('YR_', names(inv))], 'GRAND_TOTAL')
for (col in inv_cols) {
  inv[, eval(col) := str_replace_all(get(col), '-', '')]
  inv[, eval(col) := str_replace_all(get(col), ',', '')]
  inv[get(col) == '', eval(col) := NA]
  inv[, eval(col) := as.double(get(col))]
}

# Drop CEPI, COVID, and totals data
inv <- inv[!grepl('Total', INVESTMENT_CASE)]

# Fill PROGRAM column
inv[PROGRAM == '', PROGRAM := NA]
inv[, PROGRAM := zoo::na.locf(PROGRAM)]

# Collapse sum & reshape
inv <- melt.data.table(inv, measure.vars = inv_cols[inv_cols != 'GRAND_TOTAL'], variable.factor = F, variable.name = 'YEAR', value.name = 'INVESTMENT')
inv <- inv[, .(INVESTMENT = sum(INVESTMENT, na.rm = T)),
           by = .(PROGRAM_originalname = PROGRAM, COUNTRY, YEAR)]
inv <- inv[INVESTMENT != 0]
inv[, YEAR := as.numeric(str_replace(YEAR, 'YR_', ''))]

# Split PROGRAM
inv[, c('PROGRAM', paste0('PROGRAM_originalname', 2:3)) := tstrsplit(PROGRAM_originalname, " - ", fixed = TRUE)]

# Recode PROGRAM & collapse sum
inv[PROGRAM_originalname2 != 'vaccines', PROGRAM := PROGRAM_originalname]
inv[, `:=`(PROGRAM_originalname = NULL, PROGRAM_originalname2 = NULL, PROGRAM_originalname3 = NULL)]
inv <- inv[, .(INVESTMENT = sum(INVESTMENT, na.rm = T)),
           by = .(COUNTRY, YEAR, PROGRAM)]
rm(inv_cols)
#----------------------------------#  

cat('  Merging Disbursements, Commitments, and Investments\n') 
#----# Merge above three #----# ####
# Merge datasets
dci <- merge(comt, disb, by = c('COUNTRY', 'YEAR', 'REGION', 'PROGRAM', 'PROGRAM_TYPE', 'RECIPIENT_AGENCY', 'COVID'), all = T)
dci <- merge(dci, inv, by = c('COUNTRY', 'YEAR', 'PROGRAM'), all = T)
dci[!is.na(INVESTMENT), PROGRAM_TYPE := 'investment case'] 


# Generate PURPOSE column
dci[, `:=`(SECTOR = 'HEALTH', PURPOSE = PROGRAM)]
dci[is.na(RECIPIENT_AGENCY), RECIPIENT_AGENCY := 'UNSP']
dci[PROGRAM == 'ISS', PURPOSE := 'Immunization Services']
dci[PROGRAM %in% c('INS', 'ISD'), PURPOSE := 'Injection Safety']
dci[PROGRAM == 'Vaccine Introduction Grant', PURPOSE := 'Introduction to New Vaccines']
dci[PROGRAM_TYPE == 'nvs', PURPOSE := 'New and Underused Vaccines']
dci[PROGRAM_TYPE == 'cso', PURPOSE := 'Civil Society Organization']
dci[PROGRAM_TYPE == 'hss', PURPOSE := 'Health Sector Support']
dci[PROGRAM_TYPE == 'investment case', PURPOSE := 'Investment Case']
dci[PROGRAM_TYPE == 'graduation grant', PURPOSE := 'Graduation from GAVI support']
dci[PROGRAM_TYPE == 'cash support', PURPOSE := 'Cash Support']
dci[grepl('operational support', PROGRAM_TYPE), PURPOSE := 'Operational Support']
dci[PROGRAM_TYPE == 'product switch grant', PURPOSE := 'Product Switch Grant']
dci[PROGRAM_TYPE == 'cceop', PURPOSE := 'Health Sector Support']
dci[PROGRAM_TYPE == 'ebola epi recovery plan', PURPOSE := 'Ebola EPI Recovery Plan']

# Fill countryname column
setnames(dci, 'COUNTRY', 'countryname')
dci[countryname == 'South Sudan' & YEAR < 2011, countryname := 'Sudan']

#-----------------------------#  

cat('  Adding Country Codes\n')
#----# Add Country Codes #----# ####
# Merge country_codes
setnames(dci, 'countryname', 'country_lc')
dci <- merge(dci, country_codes, by ='country_lc', all.x=T)
dci <- dci[!is.na(YEAR)]
# Rename, recode, and add required column
setnames(dci, c('country_lc', 'iso3'), c('RECIPIENT_COUNTRY', 'ISO3_RC'))
dci[, DATA_SOURCE := paste0('All Countries Commitments and Disbursements, downloaded ', disb_update_year, ' via GAVI website')]
dci[RECIPIENT_COUNTRY == 'South Sudan', ISO3_RC := 'SSD']
dci[RECIPIENT_COUNTRY %in% c('Non country specific', NA, 'PEF-MEL MGD', 'PEF-SFP MGD', 'Global'), ISO3_RC := 'QZA']


#-----------------------------# 

cat('  Calculating Total Disbursements\n') # Mostly fine here, just poor merging
#----# Calculate total disbursement #----# ####
# Generate new disbursement column and fill any NAs
value_cols <- c('COMMITMENT', 'DISBURSEMENT_yr_paid', 'INVESTMENT')
for (col in value_cols) {
  dci[is.na(get(col)), eval(col) := 0]
}
dci[, PROJ_INVEST_DISB := DISBURSEMENT_yr_paid + INVESTMENT]

# Add required columns
dci[, `:=`(DATA_LEVEL = 'Project', FUNDING_COUNTRY = 'NA',
           ISO3_FC = 'NA', FUNDING_TYPE = 'GRANT',
           FUNDING_AGENCY = 'GAVI', FUNDING_AGENCY_SECTOR = 'PPP',
           FUNDING_AGENCY_TYPE = 'NA', PROJECT_ID = 'NA',
           PROJECT_NAME = 'NA', RECIPIENT_AGENCY_SECTOR = 'GOV',
           RECIPIENT_AGENCY_TYPE = 'UNSP', REPORTING_AGENCY = 'GAVI'
)]
setnafill(dci, fill = 0, cols = "COVID")
#----------------------------------------# 

cat('  Save Stage 1a Data\n')
#----# Save Stage 1a Dataset #----# ####
save_dataset(dci, paste0('1a_INTAKE'), 'GAVI', 'int')
#---------------------------------# 
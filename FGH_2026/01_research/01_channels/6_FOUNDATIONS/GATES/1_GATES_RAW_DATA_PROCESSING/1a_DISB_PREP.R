#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Prepare Gates Foundation Grants_Disbursements dataset
#---------------------#
#----------------------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, magrittr)

# Variable prep
ngo <- paste0(dah.roots$j, 'FILEPATH')
codes <- paste0(dah.roots$j, 'FILEPATH')
#----------------------------# 


cat('\n\n')
cat(green(' ###########################################\n'))
cat(green(' #### BMGF GRANTS_DISBURSEMENTS PREPARE ####\n'))
cat(green(' ###########################################\n\n'))


cat('  Read in + prep NGOs data\n')
#----# Read in + prep NGOs data #----# ####
ngos <- setDT(read.dta13(paste0(ngo, 'Agency_ID_FF2017.dta')))[, !c('n')]
ngos[, agency := replace_r_hexadecimal(agency)]
ngos[, agency_upper := string_to_std_ascii(agency, pad_char = "")]
#------------------------------------#

cat('  Read in + prep INTL NGOs data\n')
#----# Read in + prep INTL NGOs data #----# ####
intl_ngos <- setDT(read.dta13(paste0(ngo, 'Intl_Agency_ID_FF2017.dta')))
intl_ngos[, agency := replace_r_hexadecimal(agency)]
intl_ngos[, agency_upper := string_to_std_ascii(agency, pad_char = "")]
intl_ngos[, n := 1:.N, by = agency_upper]
intl_ngos <- intl_ngos[n == 1, !c('n')]
#-----------------------------------------#

cat('  Read in historical Gates Foundation OECD report\n')
#----# Read in historical Gates Foundation OECD report #----# ####
# UPDATE:
# in fgh2022, we have observed report year data, normally we do not and this
# should load "oecd_reports_appended_", report_year, ".csv"
oecd <- fread(get_path('BMGF', 'raw',
                       paste0("oecd_reports_appended_", dah.roots$report_year, ".csv")),
              encoding = "Latin-1")
# convert col types
oecd[channel_code == "Unknown", channel_code := NA]
oecd[, `:=`(year = as.numeric(year),
            channel_code = as.numeric(channel_code),
            type_of_flow = as.numeric(type_of_flow),
            type_of_finance = as.numeric(type_of_finance))]

oecd[outflow == "-", outflow := NA]
oecd[, outflow := gsub(',', '', outflow)]
oecd[, outflow := as.numeric(outflow)]

oecd[commitment == "-", commitment := NA]
oecd[, commitment := gsub(',', '', commitment)]
oecd[, commitment := as.numeric(commitment)]

#-----------------------------------------------# 
oecd <- oecd[, project_title := string_to_std_ascii(project_title)]
oecd <- oecd[, purpose := replace_r_hexadecimal(purpose)]


cat('  Keep only health projects\n')
#----# Keep only health projects #----# ####
oecd[, code := substr(purpose_code, 1, 2)]
oecd <- oecd[code %in% c('12', '13', '99') & purpose_code != "99820"]
oecd <- oecd[!(code == '99' & !(str_detect(recipient_agency, 'Global Alliance for')) & !(str_detect(recipient_agency, 'Global Fund to'))),]
oecd[, `:=`(code = NULL,
            purpose_code = as.numeric(purpose_code),
            agency_upper = trimws(project_title))]
#-------------------------------------#

cat('  Merge with NGOs data\n')
#----# Merge with NGOs data #----# ####
all_ngos <- unique(rbind(
    ngos[, .(agency_upper, agency)],
    intl_ngos[, .(agency_upper, agency)]
))
all_ngos[, n := 1:.N, by = agency_upper]
all_ngos <- all_ngos[n == 1, -"n"] ## remove duplicates

all_ngos[, is_ngo := 1L]

# first - try merging on recipient agency
oecd[, ra_clean := string_to_std_ascii(recipient_agency, pad_char = "")]
oecd <- merge(
    oecd,
    all_ngos,
    by.x = 'ra_clean',
    by.y = 'agency_upper',
    all.x = TRUE
)
setnafill(oecd, fill = 0, cols = "is_ngo")

# second - try merging on project_title
oecd[, pt_clean := string_to_std_ascii(project_title, pad_char = "")]
oecd <- merge(
    oecd,
    all_ngos,
    by.x = 'pt_clean',
    by.y = 'agency_upper',
    all.x = TRUE,
    suffixes = c("", "_new")
)
setnafill(oecd, fill = 0, cols = "is_ngo_new")

oecd[is_ngo == 0 & is_ngo_new == 1, agency := agency_new]
oecd[is_ngo_new == 1, is_ngo := 1]
oecd[, `:=`(is_ngo_new = NULL,
            agency_new = NULL,
            ra_clean = NULL,
            pt_clean = NULL)]
#--------------------------------#

cat('  Read in CRS recipient codes\n')
#----# Read in CRS recipient codes #----# ####
cc <- setDT(fread(paste0(dah.roots$j, 'FILEPATH/CRS_codelist_30012013_countrycodes.csv')))
colnames(cc) <- tolower(colnames(cc)) %>% str_replace_all(' ', '_')
cc <- cc[, c('recipient_code', 'recipient_name_(en)')]
setnames(cc, c('recipient_code', 'recipient_name_(en)'), c('recipient_country', 'country_lc'))
#---------------------------------------#

cat('  Merge OECD data with CRS data\n')
#----# Merge OECD data with CRS data #----# ####
oecd[, recipient_country := as.numeric(recipient_country)]
oecd <- merge(oecd, cc, by='recipient_country', all.x=T)
oecd[is.na(country_lc), country_lc := 'N/A']
rm(cc)

# clean recipient agency var
oecd <- oecd[, recipient_agency := replace_r_hexadecimal(recipient_agency)]
oecd <- oecd[, recipient_agency_up := string_to_std_ascii(recipient_agency, pad_char = "")]
#-----------------------------------------#

cat('  Read in historical BMGF grant disbursements\n')
#----# Read in historical BMGF grant disbursements #----# ####

sector <- fread(paste0(get_path('BMGF', 'int', report_year = dah.roots$prev_report_year), 
                       'BMGF_GRANTS_DISBURSEMENTS_2009_', (dah.roots$prev_report_year), '.csv'),
                encoding = "Latin-1")
if ("recipient_agency.x" %in% names(sector)) {
    setnames(sector, "recipient_agency.x", "recipient_agency")
}
sector <- sector[, c('recipient_sector',  'recipient_legal_sector', 'recipient_agency_country',  'recipient_legal_type', 'recipient_agency', 'international', 'id')]
sector[, recipient_agency_up := string_to_std_ascii(recipient_agency, pad_char = "")]
sector <- sector[!duplicated(sector$recipient_agency_up), ]
sector <- sector[, -c("id", "international")]


# Clean columns
sector[recipient_agency_up == 'ASIAN DEVELOPMENT BANK ORDINARY CAPITAL', recipient_agency_up := 'ASIAN DEVELOPMENT BANK']
sector[recipient_agency_up == 'INTER AMERICAN DEVELOPMENT BANK', recipient_agency_up := 'INTER-AMERICAN DEVELOPMENT BANK ORDINARY CAPITAL, INTER-AMERICAN INVESTMENT CORPORATION AND MULTILATERAL INVESTMENT FUND']
sector[recipient_agency_up == 'INTERNATIONAL AIDS VACCINE INITIATIVE INC', recipient_agency_up := 'INTERNATIONAL AIDS VACCINE INITIATIVE']
sector[recipient_agency_up == 'INTERNATIONAL FEDERATION OF RED CROSS AND RED CRESCENT', recipient_agency_up := 'INTERNATIONAL FEDERATION OF RED CROSS AND RED CRESCENT SOCIETIES']
sector[recipient_agency_up == 'INTERNATIONAL PARTNERSHIP FOR MICROBICIDES', recipient_agency_up := 'INTERNATIONAL PARTNERSHIP ON MICROBICIDES']
sector[recipient_agency_up == 'INTERNATIONAL POTATO CENTER', recipient_agency_up := 'INTERNATIONAL POTATO CENTRE']
sector[recipient_agency_up == 'UNITED NATIONS PROGRAMME ON HIV/AIDS', recipient_agency_up := 'JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS']
sector[recipient_agency_up == 'PAN AMERICAN HEALTH ORGANIZATION', recipient_agency_up := 'PAN-AMERICAN HEALTH ORGANISATION']
sector[recipient_agency_up == 'INTER-AMERICAN DEVELOPMENT BANK ORDINARY CAPITAL, INTER-AMERICAN INVESTMENT CORPORATION AND MULTILATERAL INVESTMENT FUND', recipient_agency_up := 'INTER-AMERICAN DEVELOPMENT BANK']
sector[recipient_agency_up == 'UNICEF HEADQUARTERS"', recipient_agency_up := "UNITED NATIONS CHILDREN's FUND"]

sector <- sector[!duplicated(sector$recipient_agency_up), ]
#-------------------------------------------------------#

cat('  Clean OECD recipient agencies & merge with grant disbursements\n')
#----# Clean OECD recipient agencies & merge with grant disbursements #----# ####
oecd[recipient_agency_up %in% c('WORLD HEALTH ORGANISATION - ASSESSED CONTRIBUTIONS', 'WORLD HEALTH ORGANISATION - CORE VOLUNTARY CONTRIBUTIONS ACCOUNT'), recipient_agency_up := 'WORLD HEALTH ORGANISATION']
oecd[recipient_agency_up == 'SAVE THE CHILDREN - DONOR COUNTRY OFFICE', recipient_agency_up := 'SAVE THE CHILDREN']
oecd[recipient_agency_up == 'PUBLIC-PRIVATE PARTNERSHIP (PPP)', recipient_agency_up := 'PUBLIC-PRIVATE PARTNERSHIPS (PPP)']
oecd[recipient_agency_up == 'UNITED NATIONS OFFICE FOR PROJECT SERVICES', recipient_agency_up := 'UNITED NATIONS OFFICE OF THE UNITED NATIONS HIGH COMMISSIONER FOR REFUGEES']
oecd[recipient_agency_up == 'INTER-AMERICAN DEVELOPMENT BANK, INTER-AMERICAN INVESTMENT CORP AND MULTILATERAL INVESTMENT FUND', recipient_agency_up := 'INTER-AMERICAN DEVELOPMENT BANK']
oecd[recipient_agency_up == 'DEVELOPING COUNTRY-BASED NGO ', recipient_agency_up := 'DEVELOPING COUNTRY-BASED NGO']
oecd[recipient_agency_up == 'UNITED NATIONS POPULATION FUND ', recipient_agency_up := 'UNITED NATIONS POPULATION FUND']
oecd[recipient_agency_up == 'ASIAN DEVELOPMENT BANK ORDINARY CAPITAL', recipient_agency_up := 'ASIAN DEVELOPMENT BANK']

bmgf <- merge(oecd, sector, by='recipient_agency_up', all.x=T)
bmgf[is.na(recipient_agency.x) & !is.na(recipient_agency.y), recipient_agency.x := recipient_agency.y]
bmgf[, `:=`(recipient_agency.y = NULL)]
setnames(bmgf, c('recipient_agency.x'), c('recipient_agency'))
bmgf[recipient_agency_up=="CLEAN TECHNOLOGY FUND", country_lc := "Rwanda"]
bmgf[, recipient_agency_up := NULL]
rm(oecd, sector)
#--------------------------------------------------------------------------#

cat('  Read in & merge ISOs\n')
#----# Read in & merge ISOs #----# ####
isos <- setDT(fread(paste0(codes, 'fgh_custom_location_set.csv')))[, c('location_name', 'ihme_loc_id')]
setnames(isos, c('location_name', 'ihme_loc_id'), c('country_lc', 'iso3'))

# clean some country_lc
bmgf[country_lc == "Bolivia", country_lc := "Bolivia (Plurinational State of)"]
bmgf[country_lc == "Cape Verde", country_lc := "Cabo Verde"]
bmgf[country_lc == "Central African Rep.", country_lc := "Central African Republic"]
bmgf[country_lc %in% c("Congo, Dem. Rep.", "Congo, Rep."),
     country_lc := "Democratic Republic of the Congo"]
bmgf[country_lc == "Cote d'Ivoire", country_lc := "Côte d'Ivoire"]
bmgf[country_lc == "Iran", country_lc := "Iran (Islamic Republic of)"]
bmgf[country_lc == "Korea, Dem. Rep.",
     country_lc := "Democratic Republic of Korea"]
bmgf[country_lc == "Kyrgyz Republic", country_lc := "Kyrgyzstan"]
bmgf[country_lc == "Laos", country_lc := "Lao People's Democratic Republic"]
bmgf[country_lc == "Sao Tome & Principe", country_lc := "Sao Tome and Principe"]
bmgf[country_lc == "Swaziland", country_lc := "Eswatini"]
bmgf[country_lc == "Syria", country_lc := "Syrian Arab Republic"]
bmgf[country_lc == "Tanzania", country_lc := "United Republic of Tanzania"]
bmgf[country_lc == "Venezuela", country_lc := "Venezuela (Bolivarian Republic of)"]
bmgf[country_lc == "Vietnam", country_lc := "Viet Nam"]

bmgf <- merge(bmgf, isos, by='country_lc', all.x=T)
setnames(bmgf, c('country_lc', 'iso3'), c('country', 'countryiso'))

bmgf[, outflow := outflow * 1000]
#--------------------------------#

cat('  Save Dataset\n')
#----# Save Dataset #----# ####
save_dataset(bmgf, paste0('BMGF_GRANTS_DISBURSEMENTS_2009_', dah.roots$report_year, "_update"), 'BMGF', 'int')
save_dataset(bmgf, paste0('BMGF_GRANTS_DISBURSEMENTS_2009_', dah.roots$report_year), 'BMGF', 'int')
#------------------------#
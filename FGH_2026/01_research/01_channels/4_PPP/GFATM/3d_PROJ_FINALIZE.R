#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize the GFATM project-level data cleaning
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
cat(green(' #################################\n'))
cat(green(" #### GFATM FINALIZE PROJECTS ####\n"))
cat(green(' #################################\n\n'))


cat('  Read in noneg data\n')
#----# Read in noneg data #----# ####
noneg <- fread(paste0(get_path('GFATM', 'int'), 'noneg.csv'))
#------------------------------#

cat('  Split regional projects\n')
#----# Split regional projects #----# ####
noneg[RECIPIENT_COUNTRY == 'World', ISO3_RC := 'WLD']
noneg[ISO3_RC == 'ZAN', ISO3_RC := 'TZA']
noneg[RECIPIENT_COUNTRY == 'CCM Zanzibar', RECIPIENT_COUNTRY := 'Tanzania']
noneg <- noneg[!(ISO3_RC == '' & DAH == 0),]
noneg[PR_TYPE=="Governmental" | PR_TYPE=="Government", gov := 1]
noneg[is.na(gov), gov := 2]
noneg[is.na(PR_TYPE), gov := 0]

# Read in and format country codes
reg_splits <- fread(paste0(get_path('GFATM', 'raw'), 'GFATM_regional_projects_country_list.csv'))
# Drop columns that are all NA
for (col in sort(names(reg_splits))) {
  if (all(is.na(reg_splits[[col]]))) {
    reg_splits[, eval(col) := NULL]
  }
}
# Reshape long
reg_splits <- melt(reg_splits, measure.vars = names(reg_splits)[names(reg_splits) %like% 'country'])
setnames(reg_splits, c('variable', 'value'), c('country_num', 'country_lc'))
reg_splits[, country_num := as.numeric(gsub('country_', '', as.character(country_num)))]
reg_splits <- reg_splits[!is.na(country_lc) & country_lc != "", ]
reg_splits[, country_lc := str_trim(country_lc, 'both')]
# Fix a few countries
reg_splits[country_lc == 'Zanzibar', country_lc := 'United Republic of Tanzania']
reg_splits[country_lc %in% c('St Lucia', 'St. Lucia'), country_lc := 'Saint Lucia']
reg_splits[country_lc %in% c("St. Vincent and the Grenadines", "St. Vincent & the Grenadines", "St. Vincent And The Grenadines", 
                             "St Vincent And The Grenadines", 'St Vincent and the Grenadines'), 
           country_lc := 'Saint Vincent and the Grenadines']
reg_splits[country_lc %in% c("St. Kitts and Nevis", "St Kitts and Nevis", "St. Kitts & Nevis", "St. Kitss & Nevis", "St Kitts And Nevis"), 
           country_lc := 'Saint Kitts and Nevis']
reg_splits[country_lc == 'Democratic Republic Of The Congo', country_lc := 'Democratic Republic of the Congo']
reg_splits[country_lc == 'the Bahamas', country_lc := 'The Bahamas']
reg_splits[country_lc == 'Phillipines', country_lc := 'PHILIPPINES']
reg_splits[country_lc == 'Bolivia', country_lc := 'Bolivia (Plurinational State of)']
reg_splits[country_lc == 'Cape Verde', country_lc := 'Cabo Verde']
reg_splits[country_lc == "Cote d'Ivoire", country_lc := "Côte d'Ivoire"]
reg_splits[country_lc == 'Federated States of Micronesia', country_lc := 'Micronesia (Federated States of)']
reg_splits[country_lc == 'Iran', country_lc := 'Iran (Islamic Republic of)']
reg_splits[country_lc == 'Laos', country_lc := "Lao People's Democratic Republic"]
reg_splits[country_lc == 'Macedonia', country_lc := 'North Macedonia']
reg_splits[country_lc == 'Moldova', country_lc := 'Republic of Moldova']
reg_splits[country_lc == 'PHILIPPINES', country_lc := 'Philippines']
reg_splits[country_lc == 'Russia', country_lc := 'Russian Federation']
reg_splits[country_lc == 'Swaziland', country_lc := 'Eswatini']
reg_splits[country_lc == 'Syria', country_lc := 'Syrian Arab Republic']
reg_splits[country_lc == 'Tanzania', country_lc := 'United Republic of Tanzania']
reg_splits[country_lc == 'The Bahamas', country_lc := 'Bahamas']
reg_splits[country_lc == 'Timor Leste', country_lc := 'Timor-Leste']
reg_splits[country_lc == 'Venezuela', country_lc := 'Venezuela (Bolivarian Republic of)']
reg_splits[country_lc == 'Vietnam', country_lc := 'Viet Nam']
reg_splits[country_lc == 'West Bank and Gaza', country_lc := 'Palestine']
reg_splits[country_lc == "Carribean region", country_lc := "Caribbean"]

# Load country codes
isos <- fread(get_path("meta", "locs", 'fgh_location_set.csv'),
              select = c('ihme_loc_id', 'location_name', 'region_name'))

colnames(isos) <- c('iso3', 'countryname_ihme', 'gbd_region')
isos[, country_lc := countryname_ihme]
# Merge
isos[, u_m := 2]
reg_splits[, m_m := 1]
reg_splits <- merge(reg_splits, isos, by='country_lc', all=T)
reg_splits[, merge := rowSums(reg_splits[, c('u_m', 'm_m')], na.rm=T)]

reg_splits[country_lc == "Kosovo", iso3 := "KSV"]

reg_splits <- reg_splits[merge %in% c(1,3), !c('merge', 'u_m', 'm_m')]
setnames(reg_splits, 'countryname_ihme', 'countryname_')
reg_splits[, country_count := max(country_num, na.rm=T), by='PROJECT_ID']
reg_splits <- reg_splits[countryname_ != '', c('PROJECT_ID', 'countryname_', 'iso3', 'country_count')]
# Reshape wide
reg_splits <- dcast(unique(reg_splits),
                    formula = 'PROJECT_ID + country_count ~ iso3',
                    value.var = 'countryname_')
#-----------------------------------#

cat('  Prep single-country data\n')
#----# Prep single-country data #----# ####
reg_splits[, u_m := 2]
noneg[, m_m := 1]
# Merge
single_countries <- merge(noneg, reg_splits[, c('PROJECT_ID', 'u_m')], by='PROJECT_ID', all=T)
reg_splits[, u_m := NULL]
noneg[, m_m := NULL]
single_countries[, merge := rowSums(single_countries[, c('u_m', 'm_m')], na.rm=T)]
single_countries <- single_countries[merge == 1, !c('u_m', 'm_m')]
#------------------------------------#

cat('  Prep non-region split projects\n')
#----# Prep non-region split projects #----# ####
reg_splits[, u_m := 2]
noneg[, m_m := 1]
# Merge
reg <- merge(noneg, reg_splits, by='PROJECT_ID', all=T)
reg_splits[, u_m := NULL]
noneg[, m_m := NULL]
reg[, merge := rowSums(reg[, c('u_m', 'm_m')], na.rm=T)]
reg <- reg[merge == 3, !c('u_m', 'm_m')]
# Reshape long
reg <- melt(reg[, !c('merge')], measure.vars = names(reg)[nchar(names(reg)) == 3 & names(reg) != 'DAH' & names(reg) != 'gov'])
setnames(reg, c('variable', 'value'), c('iso3', 'countryname_'))
reg[, iso3 := as.character(iso3)]
reg <- reg[!is.na(countryname_),]
to_calc <- c('DAH', 'COMMITMENT', names(reg)[names(reg) %like% '_DAH'])
for (col in to_calc) {
  reg[, eval(col) := get(col) / country_count]
}
reg[, `:=`(ISO3_RC = iso3, RECIPIENT_COUNTRY = countryname_,
           countryname_ = NULL, country_count = NULL, iso3 = NULL)]
#------------------------------------------#

cat('  Small edits & collapse sum\n')
#----# Small edits & collapse sum #----# ####
post_split <- copy(single_countries)
post_split <- rbind(post_split, reg, fill=T)
# Edits
post_split[PROJECT_ID=="QMZ-102-G01-H", ISO3_RC := 'MKD']
post_split[PROJECT_ID=="QPA-H-SADC", ISO3_RC := 'QME']
post_split[ISO3_RC=="QRA", ISO3_RC := 'QNC']
post_split[ISO3_RC=="QUA", ISO3_RC := 'QTA']
# Collapse sum
post_split <- collapse(post_split, 'sum', c('YEAR', 'ISO3_RC', 'gov', 'RECIPIENT_AGENCY', 'ELIM_CH'),
                       c('DAH', names(post_split)[names(post_split) %like% '_DAH']))
rm(noneg, reg, reg_splits, single_countries, col, to_calc)
#--------------------------------------#

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(post_split, 'post_split', 'GFATM', 'int')
#------------------------#

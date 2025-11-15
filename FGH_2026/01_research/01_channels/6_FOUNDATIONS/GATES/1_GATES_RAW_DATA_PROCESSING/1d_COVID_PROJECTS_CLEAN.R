#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Clean web scraped COVID data
#---------------------#
#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# Variable prep
codes <- paste0(dah.roots$j, 'FILEPATH')
income_groups_max_yr <- report_year

#----------------------------# 


cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### BMGF CLEAN COVID PROJECTS ####\n'))
cat(green(' ###################################\n\n'))

cat('  Read in web scraped data\n')
#----# Read in web scraped data #----# ####

#new fully aggregated dataset variable reassignment:
#'division' to 'program'. Can safely cut 'region.served','grant.id'
#Going to merge grantee_country with grantee_location since the end goal is classification
# by country
dt <- setDT(fread(paste0(get_path('BMGF','raw'),'bmgf_covid_all_grants_data.csv')))
dt <- dt[,c('grantee','purpose','division','date','term','amount','grantee_country','topic','grantee_website')] 
setnames(dt, c('date', 'term', 'topic', 'grantee_country', 'grantee', 'amount', 'division','grantee_website'), 
         c('approval_date', 'proj_length', 'category', 'recipient_agency_location', 'recipient_agency', 'commitment','program','reference_link'))

dt[, srchstr := paste(toupper(purpose), toupper(program), toupper(category), sep = " ")]
covidrgx <- paste(dah.roots$covid_intercept_keywords, collapse = "|")

dt[, YEAR := data.table::year(approval_date)]
dt <- dt[paste(" ", srchstr, " ") %like% covidrgx & YEAR >= 2020]

dt <- dt[!(tolower(purpose) == "global growth and opportunity|united states"), ]
dt[tolower(purpose) %like% "development|health", new_flag := 0]
dt[tolower(purpose) %like% 'advocacy|gender|growth' & is.na(new_flag), new_flag := 1]
dt[, temp_flag := NA]
#------------------------------------#

cat('  Clean column data types\n')
#----# Clean column data types #----# ####
# commitment
dt[, commitment := gsub('\\$', '', commitment)]
dt[, commitment := gsub(',', '', commitment)]
dt[, commitment := as.numeric(commitment)]

# approval date
dt[, YEAR := data.table::year(approval_date)]

# Recipient agencies
dt[, recipient_agency := toupper(recipient_agency)]
dt[, recipient_agency := gsub('É', 'E', recipient_agency)]
dt[, recipient_agency := gsub('Ä', 'A', recipient_agency)]
dt[, recipient_agency := gsub('À', 'A', recipient_agency)]
dt[, recipient_agency := gsub('Ó', 'O', recipient_agency)]
dt[, recipient_agency := gsub('Ú', 'U', recipient_agency)]
dt[recipient_agency == 'UNITED STATES FUND FOR UNICEF', recipient_agency := 'UNICEF NATIONAL COMMITTEE/UNITED STATES OF AMERICA']

# Correct locations
dt[recipient_agency_location %like% 'Virginia' |
     recipient_agency_location %like% 'Michigan' |
     recipient_agency_location %like% 'Georgia' |
     recipient_agency_location %like% 'Maryland' |
     recipient_agency_location %like% 'California' |
     recipient_agency_location %like% 'Alabama' |
     recipient_agency_location %like% 'Massachusetts' |
     recipient_agency_location %like% 'New York' |
     recipient_agency_location %like% 'Colorado' |
     recipient_agency_location %like% 'Carolina' |
     recipient_agency_location %like% 'Texas' |
     recipient_agency_location %like% 'Ohio' |
     recipient_agency_location %like% 'Idaho' |
     recipient_agency_location %like% 'Washington' |
     recipient_agency_location %like% 'Indiana' |
     recipient_agency_location %like% 'Florida' |
     recipient_agency_location %like% 'Missouri' |
     recipient_agency_location %like% 'New Hampshire' |
     recipient_agency_location %like% 'Wisconsin' |
     recipient_agency_location %like% 'Minnesota' |
     recipient_agency_location %like% 'Utah' |
     recipient_agency_location %like% 'Oregon' |
     recipient_agency_location %like% 'Arizona' |
     recipient_agency_location %like% 'Pennsylvania' |
     recipient_agency_location %like% 'Tennessee' |
     recipient_agency_location %like% 'New Jersey' |
     recipient_agency_location %like% 'Nebraska' |
     recipient_agency_location %like% 'Rhode Island' |
     recipient_agency_location %like% 'Delaware' |
     recipient_agency_location %like% 'Iowa' |
     recipient_agency_location %like% 'United States',
   recipient_agency_location := 'United States of America']

dt[recipient_agency_location %in% c('Abuja', 'Kaduna, Kaduna',
                                    'Kano, Kano', 'Jabi, Abuja',
                                    'Abuja, Federal Capital Territory'), 
   recipient_agency_location := 'Nigeria']

dt[recipient_agency_location %in% c('Addis Ababa', 'Addis Ababa, Addis Ababa'), recipient_agency_location := 'Ethiopia']

dt[recipient_agency_location == 'Bangkok', recipient_agency_location := 'Thailand']

dt[recipient_agency_location == 'Barcelona', recipient_agency_location := 'Spain']

dt[recipient_agency_location %in% c('Bengaluru, Karnataka', 'Chennai, Tamil Nadu',
                                    'Hyderabad, Andhra Pradesh', 'New Delhi Delhi',
                                    'Hyderabad, Telegana', 'Pune, Maharashtra',
                                    'Bangalore, Karnataka'), 
   recipient_agency_location := 'India']

dt[recipient_agency_location %in% c('Cape Town, Western Cape', 'Gauteng',
                                    'Johannesburg, Gauteng', 'Durban, KwaZulu-Natal',
                                    'Johannesburg', 'Midrand, Gauteng', 'Stellenbosch'), 
   recipient_agency_location := 'South Africa']

dt[recipient_agency_location %in% c('Dakar, Dakar', 'Dakar'), recipient_agency_location := 'Senegal']

dt[recipient_agency_location == 'Dhaka', recipient_agency_location := 'Bangladesh']

dt[recipient_agency_location == 'Dhulikhel', recipient_agency_location := 'Nepal']

dt[recipient_agency_location %in% c('Geneva', 'Genève', 'Lausanne', 'Plan-Les-Ouates, Geneva',
                                    'Geneva 19'), 
   recipient_agency_location := 'Switzerland']

dt[recipient_agency_location %in% c('Islamabad', 'Karachi'), recipient_agency_location := 'Pakistan']

dt[recipient_agency_location == 'Kampala', recipient_agency_location := 'Uganda']

dt[recipient_agency_location == 'Kigali, Kigali City', recipient_agency_location := 'Rwanda']

dt[recipient_agency_location %in% c('Kisumu', 'Nairobi', 'Nairobi, Nairobi'), recipient_agency_location := 'Kenya']

dt[recipient_agency_location %in% c('Leuven, Flemish Brabant', 'Brussels'), recipient_agency_location := 'Belgium']

dt[recipient_agency_location %in% c('Liverpool', 'London', 'Oxford, Oxfordshire', 'Southampton, Hampshire',
                                    'Thurleigh, Bedford', 'Glasgow', 'Dundee', 'Great Abington, Cambridgeshire',
                                    'Holmfirth'), 
   recipient_agency_location := 'United Kingdom']

dt[recipient_agency_location %in% c('Montréal, Quebec', 'Montreal, Quebec', 'Saskatoon, Saskatchewan',
                                    'Laval, Quebec', 'Toronto, Ontario'), 
   recipient_agency_location := 'Canada']

dt[recipient_agency_location %in% c('Paris', '69002 Lyon', 'Fontenay-aux-Roses'), 
   recipient_agency_location := 'France']

dt[recipient_agency_location == 'Parkville, Victoria', recipient_agency_location := 'Australia']

dt[recipient_agency_location %in% c('Pudong New District, Shanghai', 'Beijing, Beijing',
                                    'Tianjin, Tianjin', 'Hong Kong'), 
   recipient_agency_location := 'China']

dt[recipient_agency_location == 'Rio de Janeiro, Rio de Janeiro', recipient_agency_location := 'Brazil']

dt[recipient_agency_location == 'San Salvador', recipient_agency_location := 'El Salvador']

dt[recipient_agency_location %in% c('Seongnam', 'seoul', 'Seoul', 'Suwon', 'Korea, Republic of'), recipient_agency_location := 'Republic of Korea']

dt[recipient_agency_location == 'Singapore', recipient_agency_location := 'Singapore']

dt[recipient_agency_location == 'Stockholm', recipient_agency_location := 'Sweden']

dt[recipient_agency_location == 'Tampere', recipient_agency_location := 'Finland']

dt[recipient_agency_location %in% c('Trieste', 'Rome, Rome'), recipient_agency_location := 'Italy']

dt[recipient_agency_location == 'Bandung', recipient_agency_location := 'Indonesia']

dt[recipient_agency_location == 'Buenos Aires', recipient_agency_location := 'Argentina']

dt[recipient_agency_location == 'Brazzaville', recipient_agency_location := 'Congo']

dt[recipient_agency_location == 'Congo, The Democratic Republic of the', recipient_agency_location := 'Congo']

dt[recipient_agency_location %in% c('Berlin', 'Waiblingen'), recipient_agency_location := 'Germany']

dt[recipient_agency_location %in% c('Amsterdam'), recipient_agency_location := 'Netherlands']

dt[recipient_agency_location %in% c('Craigavon'), recipient_agency_location := 'Ireland']

dt[recipient_agency_location %in% c('Lisboa'), recipient_agency_location := 'Portugal']

dt[recipient_agency_location %in% c('Legon'), recipient_agency_location := 'Ghana']

dt[recipient_agency_location %in% c('Oslo'), recipient_agency_location := 'Norway']

dt[recipient_agency_location %in% c('Tanzania, United Republic of'), recipient_agency_location := 'United Republic of Tanzania']

dt[recipient_agency_location == 'Rome', recipient_agency_location := 'Italy']
dt[recipient_agency_location == 'Mainz', recipient_agency_location := 'Germany']
dt[recipient_agency_location %like% c('Melbourne') | recipient_agency_location %like% c('Victoria'), recipient_agency_location := 'Australia']
#-----------------------------------#

#clean purpose columns for covid_intercept
dt$purpose <- string_to_std_ascii(dt$purpose)
dt$category <- string_to_std_ascii(dt$category)
dt$program <- string_to_std_ascii(dt$program)
dt <- covid_intercept(dt, year_colname = "YEAR", keyword_search_colnames = c("purpose", "category","program"))
dt <- dt[purpose_srch != 0]

dt <-dt[,c("recipient_agency", "purpose","program","approval_date","proj_length","commitment","reference_link","recipient_agency_location","category","YEAR",'temp_flag','new_flag')]

cat('  Break down projects by average daily disbursement\n')
#----# Break down projects by average daily disbursement #----# ####
#total amount of commits from temp_flags (GFATM funds) = 760687829; should be same after
#multi year split

# Determine end date, number of years the project spans
dt[, proj_length_days := as.numeric(proj_length) * (365/12)]
dt[, start_date := as.Date(approval_date, format= "%Y-%m-%d")]
dt[, start_date := as.Date(start_date, format="%y-%m-%d")]
dt[, proj_length_days := as.numeric(proj_length_days)]
dt[, end_date := start_date + proj_length_days]
dt[, start_yr := as.numeric(format(as.Date(start_date, format="%y-%m-%d"), "%y")) + 2000]
dt[, end_yr := as.numeric(format(as.Date(end_date, format="%y-%m-%d"), "%y")) + 2000]
dt[, yr_diff := end_yr - start_yr]

# Take multi-year projects and determine how many days to assign 
# each year (also create new rows for new years)
multi_yr <- copy(dt[yr_diff > 0])

multi_yr[, start_date2 := format(as.Date(start_date, format = "%y-%m-%d"), "%j")]
multi_yr[, end_date2 := format(as.Date(end_date, format = "%y-%m-%d"), "%j")]

multi_yr[, FY_days := 365 - (as.numeric(start_date2))] # number of days in first year
multi_yr[start_yr == 2020, FY_days := 366 - as.numeric(start_date2)] # 2020 is a leap year - this helps avoid any negatives
multi_yr[, LY_days := as.numeric(end_date2)] # number of days in the last year
multi_yr[, full_years := yr_diff - 1]
multi_yr[, MY_days := 365 * full_years] 
multi_yr[, start_date2 := NULL]
multi_yr[, end_date2 := NULL]

# Calculate disbursement per day
multi_yr[, total_days := FY_days + MY_days + LY_days] # length in days is slightly off (fractions so just use this as length)
multi_yr[, comm_per_day := commitment / total_days]

# create dataset long by year to merge onto
multi_yr_long <- setDT(multi_yr)[, .(reference_link, program, purpose, recipient_agency, recipient_agency_location, year = seq(start_yr, end_yr, by = 1)),
                                 .(grp = 1:nrow(multi_yr))][, grp := NULL][]
setnames(multi_yr_long, 'year', 'year_id')
all_multi <- merge(multi_yr, multi_yr_long, by = c('reference_link', 'program', 'purpose', 'recipient_agency', 'recipient_agency_location'), all = T)

all_multi[year_id == start_yr, num_days := FY_days] # fill in first year days
all_multi[year_id == end_yr, num_days := LY_days] # fill in last year days
all_multi[is.na(num_days), num_days := 365] # assign all middle years 365

all_multi[, new_commitment := num_days * comm_per_day]
save_dataset(all_multi, paste0('multi_year_COVID_projects'), 'BMGF', 'int')

all_multi[, commitment := new_commitment]
all_multi <- all_multi[, !c('FY_days', 'LY_days', 'full_years', 'MY_days', 
                            'num_days', 'total_days', 'comm_per_day', 'new_commitment')]

# Append new projects onto original data
dt <- dt[yr_diff == 0]
dt[, year_id := start_yr]
dt <- rbind(dt, all_multi, fill = T)
dt[, yr_diff := NULL]
dt[, YEAR := year_id]
dt <- dt[, !c('proj_length_days', 'start_yr', 'end_yr', 'year_id')]

rm(multi_yr, multi_yr_long, all_multi)

#-----------------------------------#

cat('  Format output data\n')
#----# Format output data #----# ####
dt[, source := 'BMGF']
dt[, channel := 'BMGF']
#channel is GFATM for temp_flag projects (see Note at top)
dt[temp_flag==1, channel := 'GFATM']
dt[, money_type := 'new money'] # This is all new money from the web scrape
dt[, YEAR := as.numeric(YEAR)]
#------------------------------#

cat('  Merge metadata\n')
#----# Merge metadata #----# ####
# Location IDs
isos <- fread(get_path("meta", "locs", "fgh_location_set.csv"),
              select = c("location_name", "ihme_loc_id", "region_name", "level")
)[level == 3]
dt <- merge(dt, isos, by.x='recipient_agency_location', by.y='location_name', all.x = T)
setnames(dt, c('ihme_loc_id', 'region_name'), c('iso3_rc', 'gbd_region'))
rm(isos)

# Income groups
ingr <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))[YEAR == income_groups_max_yr]
dt <- merge(dt, ingr[, c('ISO3_RC', 'INC_GROUP')], by.x='iso3_rc', by.y='ISO3_RC', all.x=T)
to_keep <- paste0(ingr[INC_GROUP %in% c('L', 'LM', 'UM')]$RECIPIENT_COUNTRY, collapse = ' | ') 

# Removing high income non-DAH grants
to_keep <- c(ingr[INC_GROUP %in% c('L', 'LM', 'UM')]$RECIPIENT_COUNTRY,
             'COVAX', 'GLOBE', 'DEVELOPING COUNTRIES', 'MODELING',
             'BIHAR', 'LAGOS', 'KAKAMEGA', 'DRC', 'PRIORITY COUNTRIES',
             'UNDERDEVELOPED', 'AFRICA', 'WAHO', ' UN ',
             'UNITED NATIONS', 'IMPACT FOR THE WORLD', 'LMICS',
             'LOW AND MIDDLE INCOME COUNTRIES', 'LMIC', 'LOW- AND MIDDLE-INCOME COUNTRIES',
             'mumbai',  'gambia', 'asian countries', 'low-resource settings',
             'democratic republic of the congo', 'marginalized populations', 'global health purposes',
             " DIAGNOSTICS MANUFACTUR", " THERAPEUTICS MANUFACTUR", " DIAGNOSTICS DEVELOP", " THERAPEUTICS DEVELOP", " VACCINE DEVELOP", " MODEL", " INNOVAT", " RESEARCH",
             " R D ", " R AND D ")
to_keep <- paste0(to_keep, collapse = '|')
dt[, flag := ifelse(INC_GROUP == 'H', 1, 0)]
dt[flag == 1 & grepl(to_keep, purpose, ignore.case = T), flag := 0]
dt[recipient_agency %in% c('WORLD HEALTH ORGANIZATION', 'UNICEF NATIONAL COMMITTEE/UNITED STATES OF AMERICA',
                           'INTERNATIONAL FEDERATION OF RED CROSS AND RED CRESCENT SOCIETIES',
                           'COALITION FOR EPIDEMIC PREPAREDNESS INNOVATIONS',
                           'UNITED WAY WORLDWIDE', 'RESULTS EDUCATIONAL FUND, INC.',
                           'VILLAGEREACH', 'NORTHEASTERN UNIVERSITY'), flag := 0]
#--------------------------#
# Note About GF Covid Data
#--------------------------#
# In the above "Merge metadata" section, flag == 1 denotes grants that are going to high-income countries
# that didn't get picked up by any of the keywords that were searched for. Normally these grants would be
# dropped because they are going to HIC's and not traditionally considered DAH. However, because GF
# focuses on global public goods, and because grants where flag == 1 all have to do with r&d related to
# COVID, these grants are considered DAH even though they are going to HIC's. The flag is used in the next
# section to manually assign these grants to r&d.
# as said above in first note, we are keeping all temp_flag donations to GFATM, which are
# marked as high income. These, along with the ones noted in the note directly above, should
# be the only HIC kept
#--------------------------#

# Detect double-counted channels:
dt[grepl("bank", recipient_agency, ignore.case = TRUE), sort(unique(recipient_agency))]

searches <- c(
  "INTERNATIONAL BANK FOR RECONSTRUCTION AND DEVELOPMENT",
  "GAVI ALLIANCE",
  "COALITION FOR EPIDEMIC PREPAREDNESS INNOVATIONS",
  "UNICEF NATIONAL COMMITTEE/UNITED STATES OF AMERICA",
  "WORLD HEALTH ORGANIZATION"
)

dt[, elim_ch := 0]
for (srch in searches) {
  dt[grepl(srch, recipient_agency, ignore.case = TRUE), elim_ch := 1]
}



cat('  Run COVID keyword search\n')
#----# Run COVID keyword search #----# ####
dt <- covid_kws(dataset = dt, keyword_search_colnames = 'purpose', languages = 'english', 
                keep_clean = F, keep_counts = F)

# Not normally a part of the COVID keyword search, see "Note About GF Covid Data" above
# For grants where flag == 1, assign r&d_prop == 1, and all other prop's == 0
cols <- c('m&e_prop', 'rcce_prop', 'sri_prop', 'nl_prop', 'ipc_prop', 'cm_prop', 'scl_prop',
          'hss_prop', 'ett_prop', 'other_prop')
dt[flag == 1 & new_flag == 0, (cols) := 0]
dt[flag == 1 & new_flag == 0, `:=`(`r&d_prop` = 1, COVID_total_prop = 1, COVID_total = 1)]

covid_stats_report(dataset = dt[YEAR==as.numeric(dah.roots$report_year)], amount_colname = 'commitment',
                   recipient_iso_colname = 'iso3_rc', save_plot = T,
                   output_path = get_path('BMGF', 'output'))
#------------------------------------#

cat('  Calculate amounts by HFA\n')
#----# Calculate amounts by HFA #----# ####
dt[, `:=`(COVID_total = NULL, COVID_total_prop = NULL)]

hfas <- gsub('_prop', '', names(dt)[names(dt) %like% '_prop'])
for (hfa in hfas) {
  dt[, eval(paste0(hfa, '_amt')) := commitment * get(paste0(hfa, '_prop'))]
  dt[, eval(paste0(hfa, '_prop')) := NULL]
}

# Check sum still holds
dt <- rowtotal(dt, 'amt_test', names(dt)[names(dt) %like% '_amt'])
dt[round(commitment, 2) == round(amt_test, 2), check := 1]
dt[, `:=`(amt_test = NULL, check = NULL)]

# Rename 
setnames(dt, 'commitment', 'total_amt')
#------------------------------------#

cat('  Save out COVID dataset\n')
#----# Save out COVID dataset #----# ####
dt <- dt[YEAR <= dah.roots$report_year]
dt <- dt[, c('YEAR', 'channel', 'source', 'iso3_rc', 'recipient_agency', 'recipient_agency_location', 'gbd_region', 
             'INC_GROUP', 'purpose',paste0(hfas, '_amt'), 'total_amt', 'money_type','temp_flag','elim_ch'),
         with=F]
save_dataset(dt, 'GRANTS_COVID_prepped', 'BMGF', 'fin') 
dt2 <- copy(dt)[YEAR==dah.roots$report_year]
save_dataset(dt2, paste0('COVID_prepped_', dah.roots$report_year), 'BMGF', 'int')
#----------------------------------#
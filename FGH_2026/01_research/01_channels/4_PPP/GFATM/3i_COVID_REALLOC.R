#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Reallocate 2020 COVID "repurposed" money
#---------------------# 

#----# Environment Prep #----# ####
rm(list = ls())

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, 'FUNCTIONS/helper_functions.R'))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# 

#----# Methods Documentation #----# ####

# The purpose of this script is to adjust the GFATM envelope and HFA distribution to
# account for the 2020 COVID money we are tracking as "repurposed". We are currently
# working with a definition of repurposed where any money tagged as repurposed in
# our COVID pipeline is understood to be already tracked somewhere in our estimation.
# Because we will have a 2020 COVID envelope which contains money we should have
# already tracked in our regular pipeline, we need to reduce the regular envelope
# such that all the repurposed money from the COVID pipeline will add directly
# to the top of our estimated envelope without double-counting money.

# The primary challenge with reallocating money for this channel is that our COVID
# data is less granular than our regular channel data. Because of this, we needed
# to get creative about how to reduce the regular channel DAH envelope.

###

# Process Overview
# 1. Read in the GFATM DAH ADB_PDB dataset we output in the 3f script, and the
#    COVID_prepped dataset we output in the 1f script. Subset the COVID data
#    to non-High Income recipients (we won't be able to reallocate this money)
#    and just repurposed money. Collapse the sum of repurposed money by 
#    recipient country.
# 2. Split COVID repurposed regional grants into their respective countries
#    (countries identified by reviewing the grant documents) and evenly
#    split the repurposed money from each regional grant between each 
#    identified country. Merge on location IDs and income groups to remove
#    High-Income recipient countries. Remove the regional grant observations
#    from the COVID dataset and append the newly generated country-specific
#    rows. Collapse the sum of repurposed money again by recipient country.
# 3. Begin adjusting the regular envelope.
#    Algorithm pseudocode:
#       instantiate `year` as the report_year
#       while there is still money to reallocate (the sum of `cov` is > 0):
#         create `adb` (a copy of `full_adb` subsetting to the current year, inkind = 0, and elim_ch = 0)
#         create `adj` by merging `adb` and `cov` keeping all `adb` matches
#         fill NA DAH and total_amt observations in `adj` with 0
#         create `reallocated` (an empty data.table we'll fill in a moment)
#         Foreach `country` in the list of unique countries in `adj` where the merged total_amt > 0:
#           create `t` (a temporary dataset subsetting `adj` to just the current `country`)
#           setup total values - total repurposed amount (`tot_amt`), total DAH (`tot_DAH`)
#           generate the DAH_frct (the fraction of DAH represented in each row). Fill NAs with 0
#           generate placeholder newDAH column as NAs
#           generate HFA proportions based on the pre-reallocated DAH value in each row
#           check if there is enough `tot_DAH` to be completely reallocated in this year
#           if there is not:
#             update `tot_amt` to take exactly the amount of `tot_DAH` available
#             also update `cov` to subtract the amount we will be able to reallocate
#           calculate the newDAH amount in each row (use of pmax() ensures we never get "almost 0" amounts like 1e-12)
#           if we were able to reallocate all the repurposed money in this country:
#             update `tot_amt` as 0
#             update the `cov` total in this country to be 0
#           check if any reallocated amounts are negative. if so, throw an error.
#           reallocate the HFAs based on newDAH and the calculated HFA fractions
#           append country data to `reallocated`
#         subset `cov` to just rows where the total_amt is > 0
#         print out the amount left to reallocate in which countries
#         remove unneeded rows from `reallocated`
#         subset `full_adb` to YEAR != current year, INKIND != 0, ELIM_CH != 0
#         append `reallocated` onto `full_adb`
#         subtract 1 from `year`
#         ... continue this loop while there is still money to reallocate, and continue stepping
#             back one year at a time to reallocate from the correct country where possible
# 4. Check to be sure that the original amount is equal to the amount we needed to repurpose + the 
#    new DAH amount (to the nearest dollar)
# 5. Save over existint GFATM ADB_PDB

#---------------------------------# 


cat('\n\n')
cat(green(' ################################\n'))
cat(green(" #### GFATM REPURPOSED FIXES ####\n"))
cat(green(' ################################\n\n'))


cat('  Read in datasets\n')
#----# Read in datasets #----# ####
full_adb <- setDT(fread(paste0(get_path('GFATM', 'fin'), 'P_GFATM_ADB_PDB_FGH', dah.roots$report_year, '_unreallocated.csv')))
orig_full_adb <- copy(full_adb)
# covid - but just repurposed funds
cov <- setDT(fread(paste0(get_path('GFATM', 'fin'), 'COVID_prepped_pre_donor.csv'))
             )[money_type == 'repurposed_money' & INC_GROUP != 'H']

MAXYR <- max(cov$year)

cov <- collapse(cov, 'sum', c('recipient_country', 'iso3_rc'), 'total_amt')
#----------------------------# 

cat('  Split regional grants\n')
#----# Split regional grants #----# ####
# Add countries from grant review
regs <- copy(cov)[iso3_rc == '']

# Multicountry Caribbean CARICOM-PANCAP
regs[recipient_country == 'Multicountry Caribbean CARICOM-PANCAP', 
     `:=`(c_1 = 'Antigua and Barbuda', c_2 = 'Barbados', c_3 = 'Belize',
          c_4 = 'Cuba', c_5 = 'Dominican Republic', c_6 = 'Guyana',
          c_7 = 'Haiti', c_8 = 'Jamaica', c_9 = 'Suriname', c_10 = 'Trinidad and Tobago', n = 10)]
# Multicountry Caribbean MCC
regs[recipient_country == 'Multicountry Caribbean MCC',
     `:=`(c_1 = 'Antigua and Barbuda', c_2 = 'Dominica', c_3 = 'Grenada',
          c_4 = 'Saint Kitts and Nevis', c_5 = 'Saint Lucia', c_6 = 'Saint Vincent and the Grenadines', n = 6)]
# Multicountry EECA APH
regs[recipient_country == 'Multicountry EECA APH',
     `:=`(c_1 = 'Belarus', c_2 = 'Georgia', c_3 = 'Republic of Moldova',
          c_4 = 'Kazakhstan', c_5 = 'Kyrgyzstan', c_6 = 'Russian Federation',
          c_7 = 'Tajikistan', c_8 = 'Uzbekistan', c_9 = 'Ukraine', c_10 = 'Bosnia and Herzegovina',
          c_11 = 'North Macedonia', c_12 = 'Montenegro', c_13 = 'Romania', c_14 = 'Serbia', n = 14)] 
# Multicountry East Asia and Pacific RAI
regs[recipient_country == 'Multicountry East Asia and Pacific RAI',
     `:=`(c_1 = 'Cambodia', c_2 = "Lao People's Democratic Republic", c_3 = 'Myanmar',
          c_4 = 'Thailand', c_5 = 'Viet Nam', n = 5)] 
# Multicountry Eastern Africa IGAD
regs[recipient_country == 'Multicountry Eastern Africa IGAD',
     `:=`(c_1 = 'Burundi', c_2 = 'Ethiopia', c_3 = 'Kenya',
          c_4 = 'United Republic of Tanzania', c_5 = 'Mauritius', c_6 = 'Seychelles',
          c_7 = 'Uganda', n = 7)] 
# Multicountry HIV Latin America ALEP
regs[recipient_country == 'Multicountry HIV Latin America ALEP',
     `:=`(c_1 = 'Bolivia (Plurinational State of)', c_2 = 'Brazil', c_3 = 'Chile',
          c_4 = 'Colombia', c_5 = 'Costa Rica', c_6 = 'Ecuador', c_7 = 'El Salvador',
          c_8 = 'Guatemala', c_9 = 'Honduras', c_10 = 'Mexico', c_11 = 'Nicaragua',
          c_12 = 'Panama', c_13 = 'Paraguay', c_14 = 'Peru', c_15 = 'Uruguay',
          c_16 = 'Venezuela (Bolivarian Republic of)', n = 16)]
# Multicountry HIV MENA IHAA and Multicountry MENA Key Populations
regs[recipient_country %in% c('Multicountry HIV MENA IHAA', 'Multicountry MENA Key Populations'),
     `:=`(c_1 = 'Algeria', c_2 = 'Egypt', c_3 = 'Jordan',
          c_4 = 'Lebanon', c_5 = 'Libya', c_6 = 'Morocco', c_7 = 'Sudan',
          c_8 = 'Tunisia', c_9 = 'Yemen', n = 9)] 
# Multicountry Middle East MER
regs[recipient_country == 'Multicountry Middle East MER',
     `:=`(c_1 = 'Iraq', c_2 = 'Jordan', c_3 = 'Lebanon',
          c_4 = 'Palestine', c_5 = 'Syrian Arab Republic', c_6 = 'Yemen', n = 6)] 
# Multicountry South-Eastern Asia AFAO
regs[recipient_country == 'Multicountry South-Eastern Asia AFAO',
     `:=`(c_1 = 'Indonesia', c_2 = 'Malaysia', c_3 = 'Philippines',
          c_4 = 'Thailand', n = 4)] 
# Multicountry Southern Africa MOSASWA
regs[recipient_country == 'Multicountry Southern Africa MOSASWA',
     `:=`(c_1 = 'Eswatini', c_2 = 'Mozambique', c_3 = 'South Africa', n = 3)]
# Multicountry Southern Africa WHC
regs[recipient_country == 'Multicountry Southern Africa WHC',
     `:=`(c_1 = 'Botswana', c_2 = 'Eswatini', c_3 = 'Lesotho',
          c_4 = 'Malawi', c_5 = 'Mozambique', c_6 = 'Namibia', c_7 = 'South Africa',
          c_8 = 'United Republic of Tanzania', c_9 = 'Zambia', c_10 = 'Zimbabwe', n = 10)]  
# Multicountry West Africa ALCO
regs[recipient_country == 'Multicountry West Africa ALCO',
     `:=`(c_1 = 'Benin', c_2 = "Côte d'Ivoire", c_3 = 'Ghana',
          c_4 = 'Nigeria', c_5 = 'Togo', n = 5)] 
# Multicountry Western Pacific
regs[recipient_country == 'Multicountry Western Pacific',
     `:=`(c_1 = 'Cook Islands', c_2 = 'Kiribati', c_3 = 'Marshall Islands',
          c_4 = 'Micronesia (Federated States of)', c_5 = 'Nauru', c_6 = 'Niue', c_7 = 'Palau',
          c_8 = 'Samoa', c_9 = 'Tonga', c_10 = 'Tuvalu', c_11 = 'Vanuatu', n = 11)] 
# Multicountry Americas EMMIE
regs[recipient_country == 'Multicountry Americas EMMIE',
     `:=`(c_1 = 'Belize', c_2 = 'Costa Rica', c_3 = 'Dominican Republic',
          c_4 = 'El Salvador', c_5 = 'Guatemala', c_6 = 'Haiti', c_7 = 'Honduras',
          c_8 = 'Nicaragua', c_9 = 'Panama', n = 9)] 
# Multicountry Americas ORAS-CONHU
regs[recipient_country == 'Multicountry Americas ORAS-CONHU',
     `:=`(c_1 = 'Argentina', c_2 = 'Belize', c_3 = 'Bolivia (Plurinational State of)',
          c_4 = 'Chile', c_5 = 'Colombia', c_6 = 'Costa Rica', c_7 = 'Cuba',
          c_8 = 'Dominican Republic', c_9 = 'Ecuador', c_10 = 'El Salvador', c_11 = 'Guatemala',
          c_12 = 'Guyana', c_13 = 'Haiti', c_14 = 'Honduras', c_15 = 'Mexico', c_16 = 'Nicaragua',
          c_17 = 'Panama', c_18 = 'Paraguay', c_19 = 'Peru', c_20 = 'Suriname', c_21 = 'Uruguay', c_22 = 'Venezuela (Bolivarian Republic of)', n = 22)] 
# Multicountry EECA PAS
regs[recipient_country == 'Multicountry EECA PAS',
     `:=`(c_1 = 'Armenia', c_2 = 'Azerbaijan', c_3 = 'Belarus',
          c_4 = 'Georgia', c_5 = 'Kazakhstan', c_6 = 'Kyrgyzstan', c_7 = 'Republic of Moldova',
          c_8 = 'Tajikistan', c_9 = 'Turkmenistan', c_10 = 'Ukraine', c_11 = 'Uzbekistan', n = 11)] 
# Multicountry HIV EECA APH
regs[recipient_country == 'Multicountry HIV EECA APH',
     `:=`(c_1 = 'Albania', c_2 = 'Armenia', c_3 = 'Azerbaijan', c_4 = 'Belarus', c_5 = 'Bosnia and Herzegovina',
          c_6 = 'Georgia', c_7 = 'Kazakhstan', c_8 = 'Kyrgyzstan', c_9 = 'Republic of Moldova', c_10 = 'Montenegro',
          c_11 = 'North Macedonia', c_12 = 'Russian Federation', c_13 = 'Serbia', 
          c_14 = 'Tajikistan', c_15 = 'Ukraine', c_16 = 'Uzbekistan', n = 16)] 
# Multicountry Southern Africa E8
regs[recipient_country == 'Multicountry Southern Africa E8',
     `:=`(c_1 = 'Angola', c_2 = 'Botswana', c_3 = 'Mozambique', c_4 = 'Namibia', 
          c_5 = 'South Africa', c_6 = 'Eswatini', c_7 = 'Zambia', c_8 = 'Zimbabwe', n = 8)] 
# Multicountry TB Asia TEAM
regs[recipient_country == 'Multicountry TB Asia TEAM',
     `:=`(c_1 = 'Cambodia', c_2 = "Lao People's Democratic Republic",
          c_3 = 'Myanmar', c_4 = 'Thailand', c_5 = 'Viet Nam', n = 5)] 
# Multicountry TB LAC PIH
regs[recipient_country == 'Multicountry TB LAC PIH',
     `:=`(c_1 = 'Bolivia (Plurinational State of)', c_2 = 'Colombia', c_3 = 'Dominican Republic', c_4 = 'El Salvador', 
          c_5 = 'Guatemala', c_6 = 'Haiti', c_7 = 'Mexico', c_8 = 'Peru', n = 8)] 
# Multicountry TB WC Africa NTP/SRL
regs[recipient_country == 'Multicountry TB WC Africa NTP/SRL',
     `:=`(c_1 = "Benin", c_2 = "Burkina Faso", c_3 = "Cabo Verde", c_4 = "Cameroon", 
          c_5 = "Central African Republic", c_6 = "Chad", c_7 = "Congo", c_8 = "Democratic Republic of the Congo",
          c_9 = "Côte d'Ivoire", c_10 = "Djibouti", c_11 = "Equatorial Guinea", c_12 = "Ethiopia", c_13 = "Gabon", 
          c_14 = "Gambia", c_15 = "Ghana", c_16 = "Guinea", c_17 = "Guinea-Bissau", c_18 = "Kenya", c_19 = "Liberia",
          c_20 = "Mali", c_21 = "Mauritania", c_22 = "Niger", c_23 = "Nigeria", c_24 = "Sao Tome and Principe", c_25 = "Senegal",
          c_26 = "Sierra Leone", c_27 = "Somalia", c_28 = "South Sudan", c_29 = "Togo", c_30 = "Uganda",
          n = 30)]
# East, Central, and Southern Africa Health Community
# https://ecsahc.org/ - look for member states
cntrys <- list("Eswatini", "Kenya", "Lesotho", "Malawi", "Mauritius", "Tanzania",
               "Uganda", "Zambia", "Zimbabwe")
regs[recipient_country == "Multicountry Africa ECSA-HC",
     (paste0("c_", seq_along(cntrys))) := cntrys]
regs[recipient_country == "Multicountry Africa ECSA-HC",
     n := length(cntrys)]

# Reshape long
regs <- melt.data.table(regs, measure.vars = names(regs)[names(regs) %like% 'c_'])

# Split projects
regs <- regs[!is.na(value), !c('variable')]
regs[, total_amt := total_amt / n]
regs <- regs[, !c('recipient_country', 'n', 'iso3_rc')]

# Merge iso codes + inc groups
regs[value == "Tanzania", value := "United Republic of Tanzania"]
isos <- setDT(fread(paste0(codes, 'fgh_location_set.csv')))[level == 3, c('location_name', 'ihme_loc_id', 'region_name')]
regs <- merge(regs, isos, by.x = 'value', by.y = 'location_name', all.x = T)
regs <- regs[, c('value', 'ihme_loc_id', 'total_amt')]
rm(isos)

regs <- get_ig(regs)
setnames(regs,
         c('ihme_loc_id', 'value', "income_group"),
         c('iso3_rc', 'recipient_country', "INC_GROUP"))

# Subset + collapse
regs <- regs[INC_GROUP != 'H',]
regs <- collapse(regs, 'sum', c('iso3_rc', 'recipient_country'), 'total_amt')
#---------------------------------# 

cat('  Bring regional grants back into data\n')
#----# Bring regional grants back into data #----# ####
cov <- cov[iso3_rc != '',]
cov <- rbind(cov, regs)

cov <- collapse(cov, 'sum', c('iso3_rc', 'recipient_country'), 'total_amt')
cov_copy <- copy(cov)
rm(regs, codes)
#------------------------------------------------# 

cat('  Attempt to adjust repurposed\n')
#----# Attempt to adjust repurposed #----# ####
year <- MAXYR
while (as.numeric(collapse(cov, 'sum', '', 'total_amt')) > 865574) {
  cat(paste0('----', year, '----\n'))
  # Subset full_adb
  adb <- copy(full_adb)[YEAR == year & INKIND == 0 & ELIM_CH == 0]
  # Merge datasets
  adj <- merge(adb, cov, by.x = 'ISO3_RC', by.y = 'iso3_rc', all.x = T)
  adj[is.na(DAH), DAH := 0]
  adj[is.na(total_amt), total_amt := 0]
  reallocated <- data.table()
  
  # Re-allocate what's possible by merging datasets
  for (country in unname(unique(adj[total_amt != 0, 'ISO3_RC'])$ISO3_RC)) {
    cat('  ')
    t <- adj[ISO3_RC == country, ]
    
    # Setup totals
    tot_amt <- as.numeric(t[1, 'total_amt'])
    tot_DAH <- as.numeric(collapse(t, 'sum', '', 'DAH'))
    t[, DAH_frct := DAH / tot_DAH]
    t[is.na(DAH_frct), DAH_frct := 0]
    t[, newDAH := as.numeric(NA)]
    
    # Generate HFA fractions
    for (hfa in names(t)[names(t) %like% '_DAH']) {
      t[, eval(paste0(hfa, '_prop')) := get(hfa) / DAH]
      t[is.na(get(paste0(hfa, '_prop'))), eval(paste0(hfa, '_prop')) := 0]
    }
    
    # Check if we're able to reallocate all repurposed money
    enough_to_reallocate <- ifelse(tot_DAH > tot_amt, T, F)
    # If not, reduce the total_amt so we don't introduce negatives
    if (!enough_to_reallocate) {
      cov[iso3_rc == country, total_amt := total_amt - tot_DAH]
      t[, total_amt := total_amt - tot_DAH]
      tot_amt <- tot_DAH
    }
    
    # Calculate new DAH amount
    t[, newDAH := pmax(0, DAH - (tot_amt * DAH_frct))]
    t[, `:=`(DAH_frct = NULL)]
    
    # If we were able to reallocate everything, zero our total_amt
    if (enough_to_reallocate) {
      cov[iso3_rc == country, total_amt := 0]
      t[, total_amt := 0]
    }
    
    # Print out country
    cat(paste0(country, ','))
    
    # Make sure we didn't calculate negative numbers
    if (any(t$newDAH < 0)) {
      stop(paste0('Introduced negative disbursement amount in ', country, ' ', year,'. Re-work calculation to ensure no negative values.'))
    }
    
    # re-allocate HFAs
    for (hfa in names(t)[names(t) %like% '_DAH' & !(names(t) %like% '_prop' | names(t) %like% 'new')]) {
      t[, eval(hfa) := get(paste0(hfa, '_prop')) * newDAH]
      t[is.na(get(hfa)), eval(hfa) := 0]
      t[, eval(paste0(hfa, '_prop')) := NULL]
    }
    
    # Append
    if (nrow(reallocated) == 0) {
      reallocated <- t
    } else {
      reallocated <- rbind(reallocated, t)
    }
    
    rm(t, tot_amt, country, hfa, enough_to_reallocate, tot_DAH)
  }
  
  # Subset remaining amounts to recalculate
  cov <- cov[total_amt > 0,]
  
  cat(paste0('\n\nStill $', red(format(collapse(cov, 'sum', '', 'total_amt'), big.mark = ',')), 
             ' to reallocate for ', year - 1, ' in: ', as.character(unique(cov[, 'recipient_country'])), '.\n'))
  
  # update reallocated
  if ('newDAH' %in% names(reallocated)) {
    reallocated[, `:=`(total_amt = NULL, recipient_country = NULL,
                       DAH = newDAH, newDAH = NULL)]
  }
  
  reallocated <- rbind(reallocated, adj[total_amt == 0, !c('recipient_country', 'total_amt')])
  
  full_adb <- full_adb[!(YEAR == year & INKIND == 0 & ELIM_CH == 0)]
  full_adb <- rbind(full_adb, reallocated)
  
  year <- year - 1
  cat('------------\n\n')
  rm(adb, adj, reallocated)
}
leftovers <- collapse(cov, 'sum', '', 'total_amt')$total_amt
frac_to_keep <- 1 - leftovers/ sum(full_adb[YEAR==MAXYR]$DAH)
cols <- colnames(full_adb)[grepl("DAH", colnames(full_adb))]
for (col in c(cols)) {
  full_adb[YEAR==MAXYR, eval(col) := frac_to_keep * get(col)]
}
rm(year)
##
## REMAINING TOTAL IN 'cov' SHOULD BE REASSIGNED TO ADJACENT COUNTRIES IN THE REGION
##
#----------------------------------------# 

cat('  Check to be sure amounts are even (to the nearest dollar)\n')
#----# Check to be sure amounts are even (to the nearest dollar) #----# ####
orig_amt <- as.numeric(collapse(orig_full_adb, 'sum', '', 'DAH'))
new_amt <- as.numeric(collapse(full_adb, 'sum', '', 'DAH'))
rep_amt <- as.numeric(collapse(cov_copy, 'sum', '', 'total_amt'))

if (round(orig_amt, 0) > round(rep_amt + new_amt, 0)) {
  stop('New amount is too low - algorithm is overcorrecting.')
} else if (round(orig_amt, 0) < round(rep_amt + new_amt, 0)) {
  stop('New amount is too high - algorithm is undercorrecting.')
} else {
  cat(green("    Sick algorithm, everything's perfect, you're great\n"))
}
rm(orig_amt, new_amt, rep_amt)
#---------------------------------------------------------------------# 

cat('  Re-save ADB_PDB\n')
#----# Re-save ADB_PDB #----# ####
save_dataset(full_adb, paste0('P_GFATM_ADB_PDB_FGH_precovid', dah.roots$report_year), 'GFATM', 'fin')
#---------------------------# 
#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Assign HFAs and Continue Formatting
#---------------------# 

#----# Environment Prep #----# ####
rm(list=ls())


if (!exists("code_repo"))  {
  code_repo <- unname(ifelse(Sys.info()['sysname'] == "Windows", "H:/repos/fgh/", paste0("/ihme/homes/", Sys.info()['user'][1], "/repos/fgh/")))
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr, haven, dplyr)

#----------------------------# 


cat('\n\n')
cat(green(' ##########################\n'))
cat(green(' #### ASSIGN GAVI HFAs ####\n'))
cat(green(' ##########################\n\n'))


cat('  Read Stage 1a Data\n')
#----# Read in Stage 1a Data #----# ####
dci <- fread(paste0(get_path('GAVI', 'int'), '1a_INTAKE.csv'))
#---------------------------------# 

cat('  Add HFAs\n')
#----# Add HFAs #----# ####
setnames(dci, "COVID", "oid_covid")
dci[, `:=`(ncd_other = 0, mnch_cnv = 0, hss = 0)]
dci[PROGRAM == 'HPV Demo' & PURPOSE != 'Health Sector Support', ncd_other := 1]
dci[PROGRAM == 'HPV Demo' & PURPOSE != 'Health Sector Support', ncd_other := 1]
dci[PROGRAM == 'HPV' & PURPOSE != 'Health Sector Support', ncd_other := 1]
dci[PURPOSE != 'Health Sector Support' & oid_covid != 1, mnch_cnv := 1]
dci[ncd_other == 1, mnch_cnv := 0]
dci[PURPOSE == 'Health Sector Support', hss := 1]
dci[RECIPIENT_COUNTRY == 'Congo (the Democratic Republic of the)', 
    c("RECIPIENT_COUNTRY", "ISO3_RC") := .("Democratic Republic of the Congo", "COD")]
dci[RECIPIENT_COUNTRY == "Côte d'Ivoire", 
    c("RECIPIENT_COUNTRY", "ISO3_RC") := .("Cote d'Ivoire", "CIV")]


#--------------------# 
cat('  Adding CRS Data\n') 

#----# Adding CRS data #----# ####
# Adding difference between CRS Total Disbursements & (Project Disbursements + Investment cases)
# GAVI Expenditure is considered the sum of 1.) Project Disbursements 2.) Investment Cases 3.) Admin + Workplan Costs 4.) Inkind
# The new GAVI disbursement data includes 1. & 2. so we need to calculate 3. to match what is reported to the CRS 
crs <- fread(get_path('CRS', 'fin', 'common_channels/M_CRS_GAVI_2.csv'))
crs[, longdescription := str_replace_all(longdescription, "[^[:alnum:]]", " ")]

# Calculating total unallocable reported in the CRS
crs[recipientname == 'Bilateral, unspecified', unallocable_disb_crs := sum(usd_disbursement, na.rm=T), by=year]
# Calculating total allocable reported in the CRS
crs[recipientname != 'Bilateral, unspecified', project_disb_crs := sum(usd_disbursement, na.rm=T), by=year]

# Calculating total investment cases reported in the CRS
crs[, investment := 0]
crs[recipientname == 'Bilateral, unspecified' & grepl('IFFIM', shortdescription), investment := 1]
crs[recipientname == 'Bilateral, unspecified' & grepl('INVESTMENT CASES', shortdescription), investment := 1]
crs[recipientname == 'Bilateral, unspecified' & grepl('ADVANCE MARKET', shortdescription), investment := 1]
crs[recipientname == 'Bilateral, unspecified' & grepl('IFFIm', longdescription), investment := 1]
crs[recipientname == 'Bilateral, unspecified' & grepl('Advance Market', longdescription), investment := 1]
crs[investment == 1, invest_disb_crs := sum(usd_disbursement, na.rm=T), by=year]

# Assume that everything that is not 1.) project disb or 2.) Investment cases in the CRS is Admin & Workplan
# most projects that do not have a specific recipient country and are not investment cases are Admin & Workplan

# Collapse to get 1 obs per year
crs <- crs %>% group_by(year) %>% summarize(usd_disbursement=sum(usd_disbursement, na.rm=T),
                                            unallocable_disb_crs=mean(unallocable_disb_crs, na.rm=T),
                                            project_disb_crs=mean(project_disb_crs, na.rm=T),
                                            invest_disb_crs=mean(invest_disb_crs, na.rm=T)) %>% setDT()
setnames(crs, 'usd_disbursement', 'disbursement_crs')
setnafill(crs,
          fill = 0,
          cols = c('disbursement_crs', 'unallocable_disb_crs', 'project_disb_crs', 'invest_disb_crs'))

# Calculate total project disbursement and investment cases reported in the CRS
crs[, proj_invest_crs := project_disb_crs + invest_disb_crs]
#---------------------------#  

cat('  Importing GAVI Database Data\n') 
#----# Importing GAVI DB Data #----# ####
# Import GAVI Database data (2000-2015)
admin_wp <- copy(dci)
admin_wp <- collapse(admin_wp, 'sum', 'YEAR', c('PROJ_INVEST_DISB', 'INVESTMENT', 'DISBURSEMENT_yr_paid'))
admin_wp[, `:=`(PROJ_INVEST_DISB = PROJ_INVEST_DISB / 1000000,
                INVESTMENT = INVESTMENT / 1000000,
                DISBURSEMENT_yr_paid = DISBURSEMENT_yr_paid / 1000000)]
setnames(admin_wp, 'YEAR', 'year')

# Merge in CRS dataset
admin_wp <- merge(admin_wp, crs, by='year', all=T)

# Calculate ratio of non-investment cases unallocable disb in the CRS / total disb 
# "non-investment cases unallocable disb in the CRS" is Administrative and Workplan costs
admin_wp[!is.na(unallocable_disb_crs), admin_wp := unallocable_disb_crs - invest_disb_crs]
admin_wp[!is.na(unallocable_disb_crs), admin_wp_ratio := admin_wp / proj_invest_crs]
admin_wp[, mean_admin_wp_ratio := mean(admin_wp_ratio, na.rm=T)]
admin_wp[is.na(admin_wp), admin_wp := mean_admin_wp_ratio * PROJ_INVEST_DISB]
admin_wp <- admin_wp[, c('year', 'admin_wp')]
setnames(admin_wp, 'year', 'YEAR')
admin_wp[, admin_wp := admin_wp * 1000000]
#----------------------------------# 

cat('  Merging together GAVI Database and Disbursement Data\n') 
#----# Merge DB and Disbursement #----# ####

# Merge datasets
dci <- merge(dci, admin_wp, by='YEAR', all.x=T)

# Recalculate + add some columns
dci[, TOTAL_DISB := sum(PROJ_INVEST_DISB, na.rm=T), by='YEAR']
dci[, PROJ_FRCT := PROJ_INVEST_DISB / TOTAL_DISB]
dci[, admin_wp := admin_wp * PROJ_FRCT]
setnames(dci, 'admin_wp', 'ADMIN_WRKPLAN')
dci[, DISBURSEMENT := PROJ_INVEST_DISB + ADMIN_WRKPLAN]
dci[, `:=`(TOTAL_DISB = NULL, PROJ_FRCT = NULL)]
rm(admin_wp, crs)
#-------------------------------------#  

cat('  Canceling out Negative Disbursement\n') 
#----# Canceling Negative Disbursement #----# ####
pre_loop <- copy(dci)
# Fill NAs, generate negative tags
pre_loop[PROGRAM == "", PROGRAM := NA]
pre_loop[DISBURSEMENT < 0 & !is.na(DISBURSEMENT), neg := 1]
pre_loop[, neg_proj := sum(neg, na.rm=T), by=c('RECIPIENT_COUNTRY', 'PROGRAM')]
pre_loop[, proj_n := 1:length(neg_proj), by=c('RECIPIENT_COUNTRY', 'PROGRAM')] 
pre_loop[, proj_N := length(neg_proj), by=c('RECIPIENT_COUNTRY', 'PROGRAM')] 
setnames(pre_loop, 'DISBURSEMENT', 'all_DAH')

# Create looping dataset & begin corrections
loop <- copy(pre_loop)[neg_proj >= 1]
loop[, all_fix_0 := all_DAH]
loop[is.na(all_fix_0), all_fix_0 := 0]

for (i in 1:max(loop$proj_N)) {
  j <- i - 1
  # New disbursement value
  loop[, eval(paste0('all_fix_', i)) := get(paste0('all_fix_', j))]
  loop[is.na(get(paste0('all_fix_', i))), eval(paste0('all_fix_', i)) := 0]
  # Isolate negative values
  loop[(get(paste0('all_fix_', i)) < 0) & (proj_n == proj_N - j), eval(paste0('all_neg_disb_', i)) := get(paste0('all_fix_', i))]
  loop[, eval(paste0('all_neg_proj_', i)) := sum(get(paste0('all_neg_disb_', i)), na.rm=T), by=c('RECIPIENT_COUNTRY', 'PROGRAM')]
  # Subtract the negative value from the previous disbursement
  loop[(get(paste0('all_neg_proj_', i)) != 0) & (proj_n == proj_N - i), eval(paste0('all_fix_', i)) := get(paste0('all_fix_', i)) + get(paste0('all_neg_proj_', i))]
  # Create dummy variable to indicate if the value was changed
  loop[(get(paste0('all_neg_proj_', i)) != 0) & (proj_n == proj_N - i) & (proj_N >= i + 1), eval(paste0('all_change_', i)) := 1]
  loop[, eval(paste0('all_pchange_', i)) := sum(get(paste0('all_change_', i)), na.rm=T), by=c('RECIPIENT_COUNTRY', 'PROGRAM')]
  # Change the negative values to zero
  loop[(get(paste0('all_neg_proj_', i)) != 0) & (proj_n == proj_N - j) & (get(paste0('all_pchange_', i)) == 1), eval(paste0('all_fix_', i)) := 0]
}
# Rename and drop calculation columns
setnames(loop, paste0('all_fix_', max(loop$proj_N)), 'all_noneg') 
toDrop <- c(names(loop)[grepl('all_fix|all_neg_disb|all_neg_proj|all_change|all_pchange', names(loop))])
loop[, eval(toDrop) := NULL]
loop[, all_DAH := all_noneg]
rm(i, j, toDrop)
#-------------------------------------------#  


cat('  Correct Remaining Negatives\n') 
#----# Correct Remaining Negatives #----# ####
negs <- loop[all_DAH < 0]
# check negs manually--countries appearing multiple times need to be added
# to list and have negative values fixed in custom code
customdeal <- negs[ISO3_RC %in% c("SSD", "QZA", "KOR", "ZWE")]
negs <- negs[ISO3_RC %ni% c("SSD", "QZA", "KOR", "ZWE")]

sub_neg <- function(year, rc, program, loop) {
  rc_country <- copy(loop)[ISO3_RC == rc]
  rc_country[YEAR == year & PROGRAM_TYPE == program, negval := all_DAH]
  rc_country[, negval2 := sum(negval, na.rm=T)]
  first_positive_row <- which((rc_country$all_DAH + rc_country$negval2) > 0)[1]
  print(rc)
  print(first_positive_row)
  rc_country[first_positive_row, all_DAH := all_DAH + negval2]
  rc_country[YEAR == year & PROGRAM_TYPE == program, all_DAH := 0]
  rc_country[, `:=`(negval = NULL, negval2 = NULL)]
}


result_list <- lapply(1:nrow(negs), function(i) {
  sub_neg(negs$YEAR[i], negs$ISO3_RC[i], negs$PROGRAM_TYPE[i], loop)
})

qza <- copy(loop)[ISO3_RC == 'QZA'] 

#fixing SSD
ssd <- copy(loop)[ISO3_RC == 'SSD']
ssd[YEAR == 2011, negval := all_DAH]
ssd[, negval2 := sum(negval, na.rm=T)]
ssd[, cnt := 1:nrow(ssd)]
ssd[cnt == 6, all_DAH := all_DAH + negval2]
ssd[YEAR == 2011, all_DAH := 0]
ssd[, `:=`(negval = NULL, negval2 = NULL)]
# now fix 2021
ssd[YEAR==2021 & PROGRAM == 'Measles', negval := all_DAH]
ssd[, negval2 := sum(negval, na.rm=T)]
ssd[cnt == 6, all_DAH := all_DAH + negval2]
ssd[YEAR == 2021 & PROGRAM == 'Measles', all_DAH := 0]
ssd[, `:=`(negval = NULL, negval2 = NULL)]
# fix 2023
ssd[YEAR==2023 & PROGRAM_TYPE == 'mena', negval := all_DAH]
ssd[, negval2 := sum(negval, na.rm=T)]
ssd[cnt == 6, all_DAH := all_DAH + negval2]
ssd[YEAR == 2023 & PROGRAM_TYPE == 'mena', all_DAH := 0]
ssd[, `:=`(negval = NULL, negval2 = NULL, cnt = NULL)]

#fixing ZWE
zwe <- copy(loop)[ISO3_RC == 'ZWE']
zwe[YEAR == 2022 & PROGRAM_TYPE == "m", negval := all_DAH]
zwe[, negval2 := sum(negval, na.rm=T)]
zwe[, cnt := 1:nrow(zwe)]
zwe[cnt == 3, all_DAH := all_DAH + negval2]
zwe[YEAR == 2022 & PROGRAM_TYPE == "m", all_DAH := 0]
zwe[, `:=`(negval = NULL, negval2 = NULL)]
zwe[YEAR == 2022 & PROGRAM_TYPE == "mr", negval := all_DAH]
zwe[, negval2 := sum(negval, na.rm=T)]
zwe[cnt == 3, all_DAH := all_DAH + negval2]
zwe[YEAR == 2022 & PROGRAM_TYPE == "mr", all_DAH := 0]
zwe[, `:=`(negval = NULL, negval2 = NULL, cnt = NULL)]

# KOR
kor <- copy(loop)[ISO3_RC == 'KOR']
kor[YEAR == 2021 & PROGRAM_TYPE == "covax", negval := all_DAH]
kor[, negval2 := sum(negval, na.rm=T)]
kor[, cnt := 1:nrow(kor)]
kor[cnt == 19, all_DAH := all_DAH + negval2]
kor[YEAR == 2021 & PROGRAM_TYPE == "covax", all_DAH := 0]
kor[, `:=`(negval = NULL, negval2 = NULL)]
kor[YEAR == 2021 & PROGRAM_TYPE == "nvs", negval := all_DAH]
kor[, negval2 := sum(negval, na.rm=T)]
kor[cnt == 19, all_DAH := all_DAH + negval2]
kor[YEAR == 2021 & PROGRAM_TYPE == "nvs", all_DAH := 0]
kor[, `:=`(negval = NULL, negval2 = NULL, cnt = NULL)]


loop <- loop[ISO3_RC %ni% c(unique(negs$ISO3_RC), unique(customdeal$ISO3_RC))]
loop <- rbind(loop, rbindlist(result_list), qza, kor, zwe, ssd, fill = T)
loop[, all_noneg := NULL]

# Negatives still exist for Global and unallocable (create proportion for 2021 and scale down all 2021 rows by that amount)
tot_2021 <- sum(loop[YEAR==2021]$all_DAH) # Get what the total should be including negatives
tot_2021_noneg <- sum(loop[YEAR==2021 & all_DAH >= 0, ]$all_DAH) # Get total without negatives
loop[YEAR==2021, prop := tot_2021 / tot_2021_noneg]
loop[YEAR==2021, all_DAH := all_DAH * prop] # scale down 2021 projects
loop[YEAR==2021 & ISO3_RC %in% c('QZA') & all_DAH < 0, all_DAH := 0] #make negatives 0
loop[, prop:= NULL]

# Add back to dci
pre_loop <- pre_loop[neg_proj < 1]
pre_loop <- rbind(pre_loop, loop, fill=T)
setnames(pre_loop, 'all_DAH', 'DISBURSEMENT')

noneg <- copy(pre_loop)
#---------------------------------------#  
cat('  Save Stage 2a Data\n')
#----# Save stage 2a #----# ####
save_dataset(noneg, paste0('2a_HFA_ASSIGN'), 'GAVI', 'int')

#-------------------------# 
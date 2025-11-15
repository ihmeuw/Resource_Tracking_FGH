########################################################################################
## UNAIDS Recipient CFix using CRS data
## Description: Aggregates UNAIDS data and then merges on CRS data for UNAIDS to fill in 
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

# Read in data
crs_raw <- setDT(fread(paste0(get_path('CRS', 'fin'), 'B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNAIDS.csv')))
hfas <- names(crs_raw)[names(crs_raw) %like% 'DAH']

# CRS recipient info
crs_recip <- copy(crs_raw)
crs_recip <- crs_recip[INKIND == 0, ]
crs_recip <- crs_recip[, lapply(.SD,sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                     'REPORTING_AGENCY'), .SDcols = names(crs_recip)[names(crs_recip) %like% 'DAH']]

setnames(crs_recip, 'ISO3_RC', 'ISO3_RC_new')

# Read in UNAIDS ADB_PDB (without DC)
unaids <- data.table(read.dta13(paste0(get_path('UNAIDS', 'fin'),
                                       "UNAIDS_ADB_PDB_FGH", dah.roots$report_year,
                                       "_noDC.dta")))
unaids_inkind <- copy(unaids)
unaids_inkind <- unaids_inkind[INKIND ==1, ]
unaids_inkind <- unaids_inkind[, lapply(.SD, sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                            'REPORTING_AGENCY', 'GOV', 'DONOR_COUNTRY'),
                 .SDcols = names(unaids_inkind)[names(unaids_inkind) %like% 'DAH']]

## Prepare CRS data for merging to remove unallocable amounts
crs <- copy(crs_raw)
crs <- crs[INKIND == 0, ]
crs <- crs[, lapply(.SD,sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE',
                                     'REPORTING_AGENCY'), .SDcols = names(crs)[names(crs) %like% 'DAH']]
setnames(crs, names(crs)[names(crs) %like% 'DAH'],
         gsub('DAH', 'DAH_crs', names(crs)[names(crs) %like% 'DAH']))

## Prepare UNAIDS data for merging
unaids <- unaids[INKIND == 0, ]

unaids <- unaids[, lapply(.SD, sum), by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'ISO3_RC', 'INCOME_SECTOR', 'INCOME_TYPE',
                                            'REPORTING_AGENCY', 'GOV', 'DONOR_COUNTRY'),
                 .SDcols = names(unaids)[names(unaids) %like% 'DAH']]
setnames(unaids, names(unaids)[names(unaids) %like% 'DAH'],
         gsub('DAH', 'DAH_un', names(unaids)[names(unaids) %like% 'DAH']))

## Merge together
dt <- merge(unaids, crs, by = c('YEAR', 'CHANNEL', 'ISO_CODE', 'INCOME_SECTOR', 'INCOME_TYPE'), all = T)
dt <- dt[!is.na(REPORTING_AGENCY.x), ] # removes CRS rows we don't want


# First separate out rows that merged with ones that do not have recipient data
dt_no_merge <- copy(dt)
dt_no_merge <- dt_no_merge[is.na(REPORTING_AGENCY.y)]

dt_merge <- copy(dt)
dt_merge <- dt_merge[REPORTING_AGENCY.y == 'CRS']

# Subtract CRS HFA values from Channel HFA values
hfa_crs <- c(  "nch_hss_hrh_DAH", "hiv_hss_hrh_DAH", "mal_diag_DAH", "mal_con_irs_DAH", "mal_con_oth_DAH", 
               "mal_treat_DAH", "mal_comm_con_DAH", "mal_amr_DAH", "mal_hss_other_DAH", "mal_hss_hrh_DAH",   
               "tb_hss_hrh_DAH", "oid_zika_DAH", "oid_amr_DAH", "oid_hss_other_DAH", "oid_hss_hrh_DAH",
               "ncd_hss_other_DAH", "ncd_hss_hrh_DAH","unalloc_DAH" )

for(hfa in hfa_crs) {
  dt_merge[, paste0(eval(hfa), '_un') := 0]
}

for (hfa in hfas) {
  dt_merge[, paste0(eval(hfa), '_diff') := get(paste0(hfa,'_un')) - get(paste0(hfa,'_crs'))]
}

fwrite(dt_merge, paste0(dah.roots$h, 'unaids_crs_diff.csv'))

# reallocate negatives in unallocable (un rows) - The diff will be the new un rows once CRS is added in
# Where we have more CRS money than UNAIDS total we are keeping all CRS and zeroing out the UNAIDS data
# Where there are nagatives in the new  unallocable row (_diff) we first try adding negatives to other and if 
# other is negative we scale down the posoitve hfas equally and make other zero

# Make UNAIDS unallocable rows 0 if DAH_diff is negative because that means CRS envelope is more
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
dt_unaids <- rbind(dt_merge, dt_no_merge, fill = T)
dt_unaids <- rbind(dt_unaids, unaids_inkind, fill = T) #add inkind back in
dt_unaids[, ISO3_RC := "QZA"]

## add in CRS rows
setnames(crs_recip, 'ISO3_RC_new', 'ISO3_RC')
dt <- rbind(dt_unaids, crs_recip, fill = T)

# replace Nas added where new columns were added for CRS that were not in UNAIDS
for (hfa in hfas) {
  dt[is.na(get(hfa)), eval(hfa) := 0]
}

## save 
save_dataset(dt, 'UNAIDS_ADB_PDB_FGH2020_noDC_recipient_fix', 'UNAIDS', 'fin')
save_dataset(dt, 'UNAIDS_ADB_PDB_FGH2020_noDC_recipient_fix', 'UNAIDS', 'fin', write_dta = T)


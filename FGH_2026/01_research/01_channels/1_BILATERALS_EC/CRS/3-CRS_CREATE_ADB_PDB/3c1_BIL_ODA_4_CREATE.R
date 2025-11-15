#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Prepare CRS BIL_ODA_4 dataset
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr)
#----------------------------# ####


cat('\n\n')
cat(green(' ###############################\n'))
cat(green(' #### CRS PREPARE BIL_ODA_4 ####\n'))
cat(green(' ###############################\n\n'))


cat('  Read in BIL_ODA_2 dataset\n')
#----# Read in BIL_ODA_2 dataset #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID.csv'))

# If a donor did not report a commitment to a specific recipient country during a specific year,
#   assume commitment = 0
# Do this for all HFAs and PAs except "Other", which is created again later
diseases <- names(dt)[names(dt) %like% '_commcurr' & names(dt) != 'other_commcurr']
diseases <- str_replace_all(diseases, '_commcurr', '')
for (disease in diseases) {
  dt[is.na(get(paste0(disease, '_commcurr'))), eval(paste0(disease, '_commcurr')) := 0]
  dt[is.na(get(paste0(disease, '_commcons'))), eval(paste0(disease, '_commcons')) := 0]
}
for (disease in diseases) {
  for (unit in c('commcurr', 'commcons', 'disbcurr', 'disbcons')) {
    dt[, eval(paste0(disease, '_', unit)) := get(paste0(disease, '_', unit)) * 1e6]
    setnames(dt, paste0(disease, '_', unit), paste0('ia_', disease, '_', unit))
  }
}
rm(disease, unit)

# Drop specific projects
# Canada project in 2005 that is unrealistically large and doesn't have any information on the CIDA website
dt <- dt[!(donor_name == 'Canada' & crs_id == '2005000296z'), ]
# Finland project in 1990 in Sri Lanka appears to be reported twice (once by the Finnish Gov, and once by the Ministry of Foreign Affairs)
dt <- dt[!(agency_name == "FINNISH GOVERNMENT" & crs_id == '1986000108z'), ]

# Drop country-years with all commitments and disbursements missing
dt[, total_comm := sum(ia_all_commcurr, na.rm=T), by=c('isocode', 'year')]
dt[, total_disb := sum(ia_all_disbcurr, na.rm=T), by=c('isocode', 'year')]
dt <- dt[!(total_comm == 0 & total_disb == 0), ] # drops Belgium 1992, which will be added in later
dt[, `:=`(total_comm = NULL, total_disb = NULL)]
#-------------------------------------# ####

cat('  Determine adjustment method\n')
#----# Determine adjustment method #----# ####
meth <- copy(dt)
meth <- collapse(meth, 'sum', c('isocode', 'year'), c('ia_all_commcurr', 'ia_all_disbcurr'))
meth[, disb_comm := ia_all_disbcurr / ia_all_commcurr]
meth[is.infinite(disb_comm), disb_comm := 1]
meth <- meth[year >= 1990, ]
meth[, method := 0]

# Method 1.) Use raw disbursements after the ratio goes above 50%, and doesn't go back below 30%
meth[disb_comm == 0 | disb_comm < 0.3, zero_yr := year]
meth[, zero_yr_iso := as.integer(max(zero_yr, na.rm=T)), by='isocode']
meth[is.na(zero_yr_iso), zero_yr_iso := 0] # Some -Inf vals generated as NA above so recoding as 0

meth[disb_comm >= 0.51 & !is.na(disb_comm) & zero_yr_iso < year, change_yr := year]
meth[, change_yr_iso := as.integer(min(change_yr, na.rm=T)), by='isocode']
meth[change_yr_iso <= year, method := 1]

# Method 2.) Use all adjusted commitments
meth[method == 0, method := 2]
meth[, max := max(disb_comm, na.rm=T), by='isocode']

cat('    Save quick dataset for methods annex graphs\n')
save_dataset(meth, 'methods_data_for_annex_graph', 'CRS', 'int')

dt <- merge(dt, meth, by=c('isocode', 'year'), all.x=T)
# Fix bad merge colnames
bad_merge <- names(dt)[substr(names(dt), nchar(names(dt)) - 1, nchar(names(dt))) == '.x']
bad_merge <- str_replace_all(bad_merge, '.x', '')
for (col in bad_merge) {
  dt[, eval(paste0(col, '.y')) := NULL]
  setnames(dt, paste0(col, '.x'), col)
}
rm(col, bad_merge)
#---------------------------------------# ####

cat('  Calculate adjustment commitments\n')
#----# Calculate adjustment commitments #----# ####
# Adjustment #1: DAC/CRS coverage ratio
dac <- fread(get_path('DAC', 'fin',
                      'B_DAC_[crs.update_mmyy]_COMM_COV_BYDONOR_BYYEAR_1990-[crs.data_year].csv'),
             select = c('year', 'isocode', 'cov_health', 'cov_total',
                        'health_dac_all_commcurr', 'health_dac_all_commcons'))
dt[, m_m := 1]
dac[, u_m := 2]
dt <- merge(dt, dac, by=c('year', 'isocode'), all=T)
dt[, merge := rowSums(dt[, c('u_m', 'm_m')], na.rm=T)]
# Method 3 will be applied to all country-years that have a DAC observation but $0 disb and comm in the CRS 
dt[merge == 2, method := 3]
dt <- dt[, !c('u_m', 'm_m', 'merge')]
rm(dac)

# Fill in 100% cov ratios for pre-1990 years (will need this data when we use lagged commitments)
dt[is.na(cov_health), cov_health := 1]

for (disease in diseases) {
  for (unit in c('commcurr', 'commcons', 'disbcurr', 'disbcons')) {
    dt[, eval(paste0(disease, '_', unit, '_dach')) := get(paste0('ia_', disease, '_', unit)) * cov_health]
  }
}
for (unit in c('commcurr', 'commcons')) {
  dt[, eval(paste0('health_dac_all_', unit)) := get(paste0('health_dac_all_', unit)) * 10^6]
  dt[method == 3, eval(paste0('all_', unit, '_dach')) := get(paste0('health_dac_all_', unit))]
}

# Fill donor name
dt[, donor_name := donor_name[!is.na(donor_name)[1L]], by='isocode']
dt[, donor_code := donor_code[!is.na(donor_code)[1L]], by='isocode']

# create dummy project variables
dt[method == 3,
   `:=`(recipient_name = "Bilateral, unspecified",
        recipient_code = 998,
        crs_id = paste0("DUMMY_PROJ_", isocode), 
        donor_agency = paste0(isocode, "_MISC"))]


# Adjustment #2: Disbursement schedules
sched <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_DISB_SCHEDULES_STD.csv'))
dt <- merge(dt, sched, by='isocode', all.x=TRUE)

# recreate unique project vars
dt <- dt[order(donor_agency, recipient_code, crs_id, year), ]
dt[, dummy := 1]
dt[, proj_N := sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, proj_n := 1:sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]

# creating estimated disbursements based on adjusted commitments using the disbursement schedules
data_new <- data.table()
for (i in 1:6) {
  if (i < 6) {
    t <- dt[proj_N == i, ]
  }
  if (i == 6) {
    t <- dt[proj_N >= i, ]
  }
  
  for (disease in diseases) {
    for (unit in c('curr', 'cons')) {
      for (p in 1:i) {
        j <- p - 1
        t[, eval(paste0(disease, '_disbyr', p, '_', unit)) :=
              shift(get(paste0(disease, '_comm', unit, '_dach')), n=j, type='lag')
              * get(paste0('MED_IMPRATE_', i, 'YR_YR_', p))]
      }
    }
  }
  data_new <- rbind(data_new, t, fill=T)
}
for (disease in diseases) {
  for (unit in c('curr', 'cons')) {
      data_new[, paste0(disease, "_disbest_", unit, "_dach") := rowSums(.SD, na.rm = TRUE),
               .SDcols = c(paste0(disease, '_disbyr1_', unit), paste0(disease, '_disbyr2_', unit),
                           paste0(disease, '_disbyr3_', unit), paste0(disease, '_disbyr4_', unit),
                           paste0(disease, '_disbyr5_', unit), paste0(disease, '_disbyr6_', unit))
      ]
  }
}

# generate a final disbursement variable with either adjusted commitments or raw disbursements	
data_new[, CRS_ESTIMATE := 0]
for (disease in diseases) {
  for (unit in c('curr', 'cons')) {
    # if method not in 2 or 3, use ia_disb
    data_new[, eval(paste0('final_', disease, '_disb_', unit)) :=
                 get(paste0('ia_', disease, '_disb', unit))]
    # else, use the adjusted commitments
    data_new[method %in% c(2,3),
             eval(paste0('final_', disease, '_disb_', unit)) :=
                 get(paste0(disease, '_disbest_', unit, '_dach'))]
    data_new[method %in% c(2,3), CRS_ESTIMATE := 1]
  }
}

# remove COVID funding from pre-2020 (generated by the last step)
setnafill(data_new,
          fill = 0.,
          cols = c('final_oid_covid_disb_curr', 'final_oid_covid_disb_cons'))
data_new[year < 2020,
         `:=`(
             final_all_disb_curr = final_all_disb_curr - final_oid_covid_disb_curr,
             final_all_disb_cons = final_all_disb_cons - final_oid_covid_disb_cons,
             final_oid_covid_disb_curr = 0,
             final_oid_covid_disb_cons = 0
         )]


#--------------------------------------------# ####

cat("  Calculate difference between DAC com'ts and CRS disbs\n")
#----# Calculate difference between DAC com'ts and CRS disbs #----# ####
t <- copy(data_new)
t <- t[, .(final_all_disb_curr = sum(final_all_disb_curr, na.rm = TRUE),
           final_all_disb_cons = sum(final_all_disb_cons, na.rm = TRUE)),
       by = c('isocode', 'year')]

t <- merge(t, meth, by=c('isocode', 'year'), all.x=T)

dac_adj <- fread(get_path('CRS', 'int', 'dac_adjusted_comm.csv'))
t <- merge(t, dac_adj, by=c('year', 'isocode'), all=T)
rm(dac_adj)


# above code takes time to run, but this script often needs data debugging, so save
save_dataset(t, "3c_before_adjustment", 
             channel = "crs",
             stage = "int")

# adjust DAC disbursements down by the disbursement/commitment ratio by country-year to reflect the fact that not all commitments will be disbursed
# if this ratio is greater than 1, just multiply by 1 so we don't inflate these amounts
t[method == 1, disbest_curr_dac := disbest_curr_dac * min(disb_comm, na.rm=T)]
t[method == 1, disbest_cons_dac := disbest_cons_dac * min(disb_comm, na.rm=T)]

t[, diff_curr := disbest_curr_dac - final_all_disb_curr]
t[, diff_cons := disbest_cons_dac - final_all_disb_cons]
t[, `:=`(final_all_disb_curr = NA_real_,
         final_all_disb_cons = NA_real_)]
t[(diff_curr > 0 & diff_cons > 0) & (!is.na(diff_curr) & !is.na(diff_cons)),
  final_all_disb_curr := diff_curr]
t[(diff_curr > 0 & diff_cons > 0) & (!is.na(diff_curr) & !is.na(diff_cons)),
  final_all_disb_cons := diff_cons]
t <- t[, c('isocode', 'year',
           names(t)[names(t) %like% 'final_all_disb_' | (names(t) %like% 'disbest_' & names(t) %like% '_dac')]),
       with=F]
t[, `:=`(crs_id = 'DUMMY', ISO3_RC = 'QZA', recipient_name = 'Bilateral, unspecified')]


data_new <- rbind(data_new, t, fill=T)
data_new[, donor_agency := donor_name]
data_new[donor_name == "Japan" & agency_name == "JAPANESE INTERNATIONAL CO-OPERATION AGENCY",
         donor_agency := "Japan_JICA"]

#-----------------------------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(data_new,
             'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_4',
             'CRS', 'int')
#------------------------# ####
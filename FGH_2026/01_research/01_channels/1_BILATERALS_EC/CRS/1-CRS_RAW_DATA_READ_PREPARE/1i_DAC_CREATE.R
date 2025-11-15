#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Prep DAC data - calculate coverage ratio of DAC commitments 
#           to CRS disbursements
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(crayon)
#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' #############################\n'))
cat(green(' #### CRS CREATE DAC DATA ####\n'))
cat(green(' #############################\n\n'))


cat('  Read in input data\n')
#----# Read in input data #----# ####
dac <- fread(get_path('DAC', 'int', "1a_DAC_CLEAN.csv"))
crs_comm <- fread(get_path('DAC', 'fin', 'B_CRS_[crs.update_mmyy]_BILODA_BYDONOR_BYSECTOR.csv'))
#---------------------------------------------------------------------#

cat('  Clean and combine input datasets\n')
#----# Clean and combine input datasets #----# ####
setnames(crs_comm,
         c("commitment_current", "disbursement_current", 
           "commitment_constant", "disbursement_constant"), 
         c("crs_oda_commcurr", "crs_oda_disbcurr",
           "crs_oda_commcons", "crs_oda_disbcons"))

crs_comm[sector_code %in% 111:119, sector_code := 110]    # all 110s belong to 110 education
crs_comm[sector_code %in% 121:129, sector_code := 120]    # all 120s belong to 120 health
crs_comm[sector_code %in% 151:159, sector_code := 150]    # all 150s belong to 150 Government & Civil Society
crs_comm[sector_code %in% c(210, 220, 231:236, 240, 250), sector_code := 200]   # all 200s belong to Economic Infrastructure & Services
crs_comm[sector_code %in% c(311:313, 321:323, 331, 332), sector_code := 300]    # all 300s belong to Production Sectors
crs_comm[sector_code %in% c(410,430), sector_code := 400]         # all 400s belong to Multi-Sector / Cross-Cutting
crs_comm[sector_code %in% c(510, 520, 530), sector_code := 500]   # all 500s belong to Commodity Aid / General Programme Assistance
crs_comm[sector_code %in% c(720, 730, 740), sector_code := 700]   # all 700s belong to Humanitarian Aid

crs_comm <- crs_comm[, lapply(.SD, sum, na.rm = TRUE),
                     by = .(year, isocode, sector_code),
                     .SDcols = grep("^crs", names(crs_comm))]

all <- merge(dac[, .(year, isocode, sector_code, dac_oda_commcurr, dac_oda_commcons)],
             crs_comm,
             by = c("year", "isocode", "sector_code"), all = TRUE)

#---------------------------------------------------------------------#

cat('  Generate subsetable data\n')
#----# Generate subsetable data #----# ####
cov_total <- all[, lapply(.SD, sum, na.rm = TRUE),
                 by = .(year, isocode),
                 .SDcols = grep("^dac|^crs", names(all))]

crs_cols <- grep("^crs", names(cov_total), value = TRUE)
setnames(cov_total, crs_cols, gsub('crs_oda_', 'total_crs_all_', crs_cols))

dac_cols <- grep("^dac", names(cov_total), value = TRUE)
setnames(cov_total, dac_cols, gsub('dac_oda_', 'total_dac_all_', dac_cols))

# compute ratio of the total DAC commitments to the total CRS disbursements
# - we expect the total commitments reported in the DAC data to be higher than
#   those obtained through the project-level CRS data
cov_total[, cov_total := total_dac_all_commcurr / total_crs_all_commcurr]
cov_total[cov_total < 1, cov_total := 1]
cov_total[cov_total == 0, cov_total := NA_real_]
cov_total[is.infinite(cov_total), cov_total := NA_real_]


#---
all <- all[sector_code %in% c(120,130), ] # Health and Population policies
all <- all[, lapply(.SD, sum, na.rm = TRUE),
           by = .(year, isocode),
           .SDcols = grep("^dac|^crs", names(all))]

crs_cols <- grep("^crs", names(all), value = TRUE)
setnames(all, crs_cols, gsub('crs_oda_', 'health_crs_all_', crs_cols))

dac_cols <- grep("^dac", names(all), value = TRUE)
setnames(all, dac_cols, gsub('dac_oda_', 'health_dac_all_', dac_cols))

all[, cov_health := health_dac_all_commcurr / health_crs_all_commcurr]
all[cov_health < 1, cov_health := 1]
all[cov_health == 0, cov_health := NA_real_]
all[is.infinite(cov_health), cov_health := NA_real_]

dt <- merge(all, cov_total, by = c('year', 'isocode'), all.x = TRUE)
rm(all, cov_total)
#---------------------------------------------------------------------#

cat('  Adjust France 1990\n')
#----# Adjust France 1990 #----# ####
setorder(dt, isocode, year)

dt[, cov_health_fix := ((1/2) * shift(cov_health, n=1, type='lead')) +
       ((1/3) * shift(cov_health, n=2, type='lead')) +
       ((1/6) * shift(cov_health, n=3, type='lead'))]
dt[, cov_total_fix := ((1/2) * shift(cov_total, n=1, type='lead')) +
       ((1/3) * shift(cov_total, n=2, type='lead')) +
       ((1/6) * shift(cov_total, n=3, type='lead'))]

dt[isocode == "FRA" & year == 1990,
   `:=`(cov_health = cov_health_fix, cov_total = cov_total_fix)]


cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'B_DAC_[crs.update_mmyy]_COMM_COV_BYDONOR_BYYEAR_1990-[crs.data_year]',
             'DAC', 'fin')
#---------------------------------------------------------------------#

cat('\n\n\t ###############################\n\t #### file  CRS 1i complete ####\n\t ###############################\n\n')

#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Save out CRS ADB_PDB subsets for UN channel and NGOs
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(dah.roots$k, 'FILEPATH/get_location_metadata.R'))
pacman::p_load(crayon)

#----------------------------# ####


cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### CRS FINALIZE ADB_PDB ####\n'))
cat(green(' ##############################\n\n'))


cat('  READ IN ADB_PDB data\n')
#----# Pull in CRS ADB PDB data #----# ####
dt <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_ADB_PDB_pre_USA19fix.csv'))
#---------------------------------------####

## read in data and fix columns
us_raw <- fread(get_path('CRS', 'raw', 'us_foreign_aid_dac_sector.csv'))
colnames(us_raw) <- c("International_Category_ID", "purposename",
                      "sectorcode","sectorname",
                      "fundingid","fundingtype",
                      "year","curr","const")
us_raw[, `:=` (const = as.numeric(const), curr = as.numeric(curr))]
## create annual aggregate 
us_raw <- us_raw[purposename == "Health and Population"]
us_raw[, sectorcode := 120]    # setting all sectorcodes to be 120 for sum
us <- dcast(us_raw[, .(year, sectorcode, fundingtype, curr, const)],
            formula = year + sectorcode ~ fundingtype,
            value.var = c('curr', 'const'),
            fun.aggregate = sum)
us[, isocode := 'USA']

usdt <- copy(dt)
usdt <- usdt[ELIM_CH != 1 &  ISO_CODE =="USA" & YEAR > 2000,]
usDAH <- names(usdt[,grep("DAH",names(usdt)), with=F])
usagg <- usdt[, lapply(.SD, sum, na.rm=T), by='YEAR', .SDcols = 'DAH']
colnames(usagg) <- c('year','all_DAH')

## creating a temporary dataset to check if median or mean are better for the estimate
# joining disbursements and DAH
temp <- merge(us[year <= 2020, .(year, curr_Disbursements)],
              usagg[year <= 2020, .(year, all_DAH)],
              by = 'year')
# making 2019 dah NA for mean median functions
temp[year != 2019, DAH := all_DAH]
# calculating ratios
temp[, dah_ratio := DAH/curr_Disbursements]
temp[year == 2019,
     dah_ratio := mean(c(temp[year==2018]$dah_ratio, temp[year==2020]$dah_ratio))]
temp[, calc_DAH := curr_Disbursements * dah_ratio]
temp[, dah_o_e_ratio :=  calc_DAH / all_DAH ]
temp[, test2 := dah_o_e_ratio * all_DAH]

ratio_dah_fa <- temp[year==2019]$dah_o_e_ratio

usdt[YEAR == 2019,
     (usDAH) := lapply(.SD, FUN = function(x) x * ratio_dah_fa),
     .SDcols = paste0(usDAH) ]

usdt <- usdt[YEAR==2019,]

dt <- dt[!(YEAR==2019 & ELIM_CH != 1 &  ISO_CODE =="USA")]
dt <- rbind(dt,usdt)
#--------------------------------------------# ####

# make sure PAs sum to DAH
hfacols <- grep("_DAH$", names(dt), value = TRUE)
dt[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = hfacols]
dt[tot == 0 & DAH > 0, unalloc_DAH := tot]
dt[, (hfacols) := lapply(.SD, \(x) DAH * (x / tot)), .SDcols = hfacols]
dt[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = hfacols]
stopifnot(nrow(dt[abs(tot - DAH) > 1]) == 0)
dt[, tot := NULL]


cat('  Save dataset\n')
#----# Save dataset #----# ####
# save climate version
save_dataset(dt, 'B_CRS_[crs.update_mmyy]_ADB_PDB_with_climate',
             'CRS', 'fin')

# integrate over climate
dah_cols <- grep("DAH", names(dt), value = TRUE)
fin <- dt[, lapply(.SD, sum, na.rm = TRUE),
          .SDcols = dah_cols,
          by = .(YEAR,
                 INCOME_SECTOR, INCOME_TYPE, DONOR_COUNTRY, DONOR_NAME, ISO_CODE,
                 REPORTING_AGENCY, CHANNEL,
                 ISO3_RC, INKIND, gov,
                 ELIM_CH, SOURCE_DOC)]

save_dataset(fin, 'B_CRS_[crs.update_mmyy]_ADB_PDB',
             'CRS', 'fin')
#------------------------# ####

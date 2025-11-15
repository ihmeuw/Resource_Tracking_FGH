########################################################################################################################
## AsDB COVID data prep
## Description: Produces COVID estimates from AsDB 2021 dataset
## Notes: Ensure "Amount for Health", "Health_fraction", and "disbursement_fraction" are filled in with numerical values 
## for all project rows. Add new regional splits in section 4 for any new data with multiple countries
########################################################################################################################
## 1. Set up Environment  ----------------------------------------------------------------------------------------# ####
rm(list = ls(all.names = TRUE))

library(data.table)
library(readxl)
library(dplyr)
library(readstata13)

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
asn_date <- format(Sys.time(), "%Y%m%d")

codes <- get_path("meta", "locs")

dir.create(get_path("asdb", "output"), showWarnings = FALSE)

## 2. Variable prep ----------------------------------------------------------------------------------------------# ####
cat('  Read in COVID data\n')
dt <- as.data.table(read_excel(paste0(dah.roots$j, 
                                      "FILEPATH/FGH_",
                                      2021,
                                      "/COVID_20220307.xlsx")))
names(dt)

dt <- dt[, .(`No.`, DMCs, Year, Health_fraction, `Amount for Health`, `Project Name`, `Project Description`, Modality, 
             disbursement_fraction)]

dt[, COMMITMENT := `Amount for Health` * Health_fraction * 1e6]
dt[, TOTAL_AMT := `Amount for Health` * Health_fraction * disbursement_fraction * 1e6]
## 3. Format column data types & clean iso codes------------------------------------------------------------------# ####
cat('  Format column data types\n')
## new money vs repurposed money
dt[, money_type := 'new_money']

## grant vs loan
dt[Modality %in% c('Grant', 'TA'), grant_loan := "grant"]
dt[Modality %like% 'loan|Loan', grant_loan := "loan"]
stopifnot(nrow(dt[is.na(grant_loan)]) == 0)
## -----------------------------------------------# ##
## AsDB used "alternative" codes to ISO3 - repair.
unique(dt$DMCs)
dt[, ihme_loc_id := DMCs]
dt[DMCs == 'BAN', ihme_loc_id := "BGD"]
dt[DMCs == 'BHU', ihme_loc_id := "BTN"]
dt[DMCs == 'PRC', ihme_loc_id := "CHN"]
dt[DMCs == 'INO', ihme_loc_id := "IDN"]
dt[DMCs == 'CAM', ihme_loc_id := "KHM"]
dt[DMCs == 'SRI', ihme_loc_id := "LKA"]
dt[DMCs == 'MAR', ihme_loc_id := "MHL"]
dt[DMCs == 'RMI', ihme_loc_id := "MHL"]
dt[DMCs == 'MYA', ihme_loc_id := "MMR"]
dt[DMCs == 'MON', ihme_loc_id := "MNG"]
dt[DMCs == 'MAL', ihme_loc_id := "MYS"]
dt[DMCs == 'NAU', ihme_loc_id := "NRU"]
dt[DMCs == 'NEP', ihme_loc_id := "NPL"]
dt[DMCs == 'PHI', ihme_loc_id := "PHL"]
dt[DMCs == 'REG', ihme_loc_id := "S6"]
dt[DMCs == 'SOLOMON ISLANDS', ihme_loc_id := "SLB"]
dt[DMCs == 'TAJ', ihme_loc_id := "TJK"]
dt[DMCs == 'VIE', ihme_loc_id := "VNM"]
dt[DMCs == 'VAN', ihme_loc_id := "VUT"]
dt[DMCs == 'SAM', ihme_loc_id := "WSM"]

unique(dt$ihme_loc_id)

## 4. Fix regional projects ------------------------------------------------------------------------------------- # ####
## Add any new splits which aren't already present here!

dt1 <- copy(dt[DMCs %like% "REG; BAN, BHU, IND, MAL, NEP, SRI", ])
dt1[, `:=`(TOTAL_AMT = TOTAL_AMT/2, COMMITMENT = COMMITMENT/2, ihme_loc_id = 'S5')] ## Super region
dt2 <- copy(dt1)[, ihme_loc_id := 'S6'] ## Super region

##
dta <- copy(dt)[DMCs %like% "REG; AFG, ARM, AZB, GEO, KAZ, KGZ, MON, PAK, CHN, TAJ, TUR, UZB", ]
dt3 <- copy(dta)[, `:=`(TOTAL_AMT = TOTAL_AMT * 9/12, COMMITMENT = COMMITMENT * 9/12, ihme_loc_id = 'S2')]
dt4 <- copy(dta)[, `:=`(TOTAL_AMT = TOTAL_AMT * 1/12, COMMITMENT = COMMITMENT * 1/12, ihme_loc_id = 'AFG')]
dt5 <- copy(dt4)[, ihme_loc_id := 'PAK']
dt6 <- copy(dt4)[, ihme_loc_id := 'CHN']
rm(dta)

##
dt7 <- copy(dt)[DMCs == "REG; BHU, MAL", ]
dt7 <- dt7[, `:=`(TOTAL_AMT = TOTAL_AMT/2, COMMITMENT = COMMITMENT/2, ihme_loc_id = 'BTN')]
dt8 <- copy(dt7)[, ihme_loc_id := 'MYS']

##
dtb <- copy(dt)[DMCs %like% "REG; ARM, CAM, COK, LAO, MON, MYA, NIU, PHL, WSM, THA, TLS, VIE", ]
dt9 <- copy(dtb)[, `:=`(TOTAL_AMT = TOTAL_AMT * 10/12, COMMITMENT = COMMITMENT * 10/12, ihme_loc_id = 'S6')]
dt10 <- copy(dtb)[, `:=`(TOTAL_AMT = TOTAL_AMT * 1/12, COMMITMENT = COMMITMENT* 1/12, ihme_loc_id = 'ARM')]
dt11 <- copy(dt10)[, ihme_loc_id := 'MNG']
rm(dtb)

dtc <- copy(dt)[DMCs %like% "REG; BAN, MON, NEP, PAK, PHL, SRI, UZB"]
dt12 <- copy(dtc)[, `:=`(TOTAL_AMT = TOTAL_AMT * 1/7, COMMITMENT = COMMITMENT * 1/7, ihme_loc_id = 'BGD')]
dt13 <- copy(dt12)[, ihme_loc_id := 'MNG']
dt14 <- copy(dt12)[, ihme_loc_id := 'NPL']
dt15 <- copy(dt12)[, ihme_loc_id := 'PAK']
dt16 <- copy(dt12)[, ihme_loc_id := 'PHL']
dt17 <- copy(dt12)[, ihme_loc_id := 'LKA']
dt18 <- copy(dt12)[, ihme_loc_id := 'UZB']
rm(dtc)

dtd <- copy(dt)[DMCs == "REG; MON; TAJ", ]
dt19 <- copy(dtd)[, `:=`(TOTAL_AMT = TOTAL_AMT / 2, COMMITMENT = COMMITMENT / 2, ihme_loc_id = 'MNG')]
dt20 <- copy(dt19)[, ihme_loc_id := 'TJK']
rm(dtd)

dte <- copy(dt)[DMCs == "REG; WSM, TON, TUV, VUT", ]
dt21 <- copy(dte)[,`:=`(TOTAL_AMT = TOTAL_AMT / 4, COMMITMENT = COMMITMENT / 4, ihme_loc_id = 'WSM')]
dt22 <- copy(dt21)[, ihme_loc_id := 'TON']
dt23 <- copy(dt21)[, ihme_loc_id := 'TUV']
dt24 <- copy(dt21)[, ihme_loc_id := 'VUT']
rm(dte)

dtf <- copy(dt)[DMCs == "REG; KHM, IDN, LAO, THA, TLS, VNM", ]
dt25 <- copy(dtf)[,`:=`(TOTAL_AMT = TOTAL_AMT / 6, COMMITMENT = COMMITMENT / 6, ihme_loc_id = 'KHM')]
dt26 <- copy(dt25)[, ihme_loc_id := 'IDN']
dt27 <- copy(dt25)[, ihme_loc_id := 'LAO']
dt28 <- copy(dt25)[, ihme_loc_id := 'THA']
dt29 <- copy(dt25)[, ihme_loc_id := 'TLS']
dt30 <- copy(dt25)[, ihme_loc_id := 'VNM']
rm(dtf)

dtg <- copy(dt)[DMCs == "REG; KHM, IDN, LAO, PHL, VNM", ]
dt31 <- copy(dtg)[,`:=`(TOTAL_AMT = TOTAL_AMT / 5, COMMITMENT = COMMITMENT / 5, ihme_loc_id = 'KHM')]
dt32 <- copy(dt31)[, ihme_loc_id := 'IDN']
dt33 <- copy(dt31)[, ihme_loc_id := 'LAO']
dt34 <- copy(dt31)[, ihme_loc_id := 'PHL']
dt35 <- copy(dt31)[, ihme_loc_id := 'VNM']
rm(dtg)

dt_short <- dt[!DMCs  %like% 'REG;', ]
dt_all <- rbindlist(list(dt_short, dt1, dt2, dt3, dt4, dt5, dt6, dt7, dt8, dt9, dt10, dt11, dt12, 
                         dt13, dt14, dt15, dt16, dt17, dt18, dt19, dt20, dt21, dt22, dt23, dt24, 
                         dt25, dt26, dt27, dt28, dt29, dt30, dt31, dt32, dt33, dt34, dt35))
rm(dt_short, dt1, dt2, dt3, dt4, dt5, dt6, dt7, dt8, dt9, dt10, dt11, dt12, 
   dt13, dt14, dt15, dt16, dt17, dt18, dt19,dt20 , dt21, dt22, dt23, dt24, 
   dt25, dt26, dt27, dt28, dt29, dt30, dt31, dt32, dt33, dt34, dt35)
stopifnot(sum(dt$TOTAL_AMT, na.rm = T) == sum(dt_all$TOTAL_AMT, na.rm = T))
stopifnot(sum(dt$COMMITMENT, na.rm = T) == sum(dt_all$COMMITMENT, na.rm = T))
rm(dt)

## 5. Add location ids and income groups ------------------------------------------------------------------------ # ####
# Recipient Location IDs
isos <- fread(paste0(codes, 'fgh_location_set.csv'))[, c('location_name', 'ihme_loc_id', 'super_region_name')]
dt <- merge(dt_all, isos, by = 'ihme_loc_id', all.x = T)
stopifnot(any(is.na(dt$location_name)) == FALSE)

setnames(dt, 'ihme_loc_id', 'iso3_rc')
# Income groups
ingr <- setDT(read.dta13(paste0(codes, 'wb_historical_incgrps.dta')))[
  YEAR == dah.roots$report_year, c('INC_GROUP', 'ISO3_RC')]

dt <- merge(dt, ingr, by.x = 'iso3_rc', by.y = 'ISO3_RC', all.x = T)
rm(ingr)

## 6. Produce vetting figures and format output column names -----------------------------------------------------# ####
cat('  Configure and launch keyword search\n')
dt2 <- covid_kws(dataset = dt, 
                keyword_search_colnames = c('Project Name', 'Project Description'), 
                keep_clean = T, keep_counts = T, languages = 'english')

# Some keyword search fixes (top 3 projects using manual kws)
dt2[No. == '55077-001', `:=` (vax_total = 2, vax_hr = 0, vax_ta = 1, vax_delivery = 0, vax_comm = 1, vax_hr_prop = 0, vax_ta_prop = 0.5, vax_delivery_prop = 0, vax_comm_prop = 0.5)]
dt2[No. == '54171-003; 4050, 8394', `:=` (vax_total = 6, vax_comm = 3, vax_waste = 1, vax_coord = 2, vax_comm_prop = 0.5, vax_waste_prop = (1/6), vax_coord_prop = (2/6))]
dt2[No. == '55013-001', `:=` (vax_total = 5, vax_comm = 3, vax_hr = 1, vax_coord = 1, vax_delivery = 0, vax_comm_prop = (3/5), vax_hr_prop = (1/5), vax_coord_prop = (1/5), vax_delivery_prop = 0)]

covid_stats_report(dataset = dt2, amount_colname = 'TOTAL_AMT', 
                   recipient_iso_colname = 'iso3_rc', save_plot = T, 
                   output_path = get_path('ASDB', 'output'))

## ----------------------------------------------- ##

formatting <- function(chan, channel_name) {
  hfas <- gsub('_prop', '', names(chan)[names(chan) %like% '_prop'])
  for (hfa in hfas) {
    chan[, eval(paste0(hfa, '_amt')) := TOTAL_AMT * get(paste0(hfa, '_prop'))]
    chan[, eval(paste0(hfa, '_prop')) := NULL]
  }
  
  # Check sum still holds
  chan <- rowtotal(chan, 'AMT_TEST', names(chan)[names(chan) %like% '_amt'])
  chan[round(TOTAL_AMT, 2) == round(AMT_TEST, 2), CHECK := 1]
  chan[, `:=`(AMT_TEST = NULL, CHECK = NULL)]
}


## ----------------------------------------------- ##

formatting(dt2, "AsDB")
names(dt2)

dt2 <- dt2[, .(ProjectNo = `No.`, CHANNEL = 'AsDB', iso3_rc, GBD_REGION = super_region_name, YEAR = Year, 
               INC_GROUP, money_type, grant_loan, COMMITMENT,
               `vax_r&d_amt`, vax_comm_amt, vax_mobil_amt, vax_sc_amt, vax_hygiene_amt, vax_waste_amt, vax_hr_amt,   
               vax_ta_amt, vax_coord_amt, vax_delivery_amt, vax_safety_amt, `vax_m&e_amt`, vax_other_amt, clc_amt, 
               rcce_amt, sri_amt, nl_amt, ipc_amt, cm_amt, scl_amt, 
               hss_amt,`r&d_amt`, ett_amt, other_amt, total_amt = COVID_total_amt)]

stopifnot(sum(dt_all$TOTAL_AMT, na.rm = T) == sum(dt2$total_amt, na.rm = T))

fwrite(dt2, paste0(get_path('ASDB', 'raw'), "COVID_pre_source_temp.csv"))

## 7. Use donor proportions from regular channel to split COVID data--------------------------------------------- # ####
## Although we read in this data from the AsDB ADB PDB, source data  for this stage ultimately comes from the 
## donor replenishment file located here:

## Source Regular data
dt_dah <- fread(get_path("asdb", "fin",
                         "AsDB_ADB_PDB_FGH[report_year].csv"))

## Create year-specific fractions
dt_dah2 <- copy(dt_dah)[YEAR >= 2020, .(YEAR, CHANNEL, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, DAH)]
dt_dah2 <- dt_dah2[, .(DAH = sum(DAH)), .(YEAR, CHANNEL, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE)]
dt_dah2 <- dt_dah2[DAH > 0]
dt_dah2[, `:=`(DAH_total = sum(DAH)), by = .(YEAR)]
dt_dah2[, DAH_prop := DAH / DAH_total]
dt_dah2 <- dt_dah2[, .(YEAR, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, DAH_prop)]

## Scale COVID values using Regular proportions
dt_source <- merge(dt2, dt_dah2, by = 'YEAR', allow.cartesian = T)
hfas <- gsub('_amt', '', names(dt_source)[names(dt_source) %like% '_amt'])
for (hfa in c(hfas)) {
  dt_source[, eval(paste0(hfa, '_amt')) := DAH_prop * get(paste0(hfa, '_amt'))]
}
dt_source[, COMMITMENT := DAH_prop * COMMITMENT]

##"total_amt" captured by hfa loop, COMMITMENT done separately.
stopifnot(round(sum(dt2$total_amt, na.rm = T),0) == round(sum(dt_source$total_amt, na.rm = T), 0))
stopifnot(round(sum(dt2$COMMITMENT, na.rm = T),0) == round(sum(dt_source$COMMITMENT, na.rm = T), 0))

dt_source[, DAH_prop := NULL]
dt_source[, ProjectNo := NULL]
dt_source[DONOR_NAME == 'other source', INCOME_TYPE := 'OTHER'] #make sure other sources do not get tagged as unallocable
## 8. Save out final results ------------------------------------------------------------------------------------ # ####
fwrite(dt_source, paste0(get_path('ASDB', 'fin'), 'COVID_prepped_2020_2021.csv'))

## End of Script ##


ggplot(dt_source) +
  geom_col(aes(x = YEAR, y = total_amt, fill = ISO_CODE), position = 'stack') +
  scale_x_continuous(breaks = c(2020, 2021)) +
  theme_bw()

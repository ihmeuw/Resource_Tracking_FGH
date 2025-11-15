########################################################################################
## 0_UN_double_counting_removal
## Project: FGH
## Description: Remove interagency double-counting prior to generating PREDS files
########################################################################################
## Notes:
## - To reuse script, need to manually check and replace file paths to load in
##   the proper dataset
## Clear environment
rm(list = ls(all.names = TRUE))

## Set filepaths
ifelse(Sys.info()[1] == "Linux", 
       h <- paste0("/ihme/homes/", Sys.info()[7], "/"), h <- "H:/")

code_repo <- 'FILEPATH'
report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)


data_yr     <- dah.roots$report_year - 1
dah_yr <- paste0("DAH_", dah.roots$abrv_year)
asn_date    <- format(Sys.time(), "%Y%m%d")

print(paste0("dah.roots$report_year: ", dah.roots$report_year))
print(paste0("dah.roots$prev_report_year: ", dah.roots$prev_report_year))
print(paste0("dah.roots$abrv_year: ", dah.roots$abrv_year))
print(paste0("asn_date: ", asn_date))

##--------------------------------------------------------------------------------------
## Folders and filepaths
in.dir <- paste0(dah.roots$j, "FILEPATH")
INT <- get_path('compiling', 'int')
FIN <- get_path('compiling', 'fin')
print(paste0("Intermediary output directory: ", INT))
print(paste0("FINAL output directory: ", FIN))


inkind_GFATM_dt   <- get_path('GFATM', 'raw', "P_GFATM_INKIND.csv",
                              check = TRUE)
inkind_GAVI_dt <- get_path("GAVI", "RAW", "P_GAVI_INKIND_[prev_report_year]update.csv",
                           check = TRUE)

GAVI_adb <- get_path("GAVI", "fin", "P_GAVI_ADB_PDB_FGH[report_year].csv",
                     check = TRUE)

PAHO_adb   <- get_path('PAHO', 'fin', "PAHO_ADB_PDB_FGH[report_year].dta",
                       check = TRUE)

UNAIDS_adb <- get_path("UNAIDS", "fin", "UNAIDS_ADB_PDB_FGH_[report_year]_includesDC_COVID_fix.csv",
                       check = TRUE)

UNFPA_adb  <- get_path('UNFPA', 'fin', "UNFPA_ADB_PDB_FGH[report_year]_ebola_fixed_includesDC_20250522.dta",
                       check = TRUE)

UNICEF_adb <- get_path("UNICEF", "fin", "UNICEF_ADB_PDB_FGH[report_year]_ebola_fixed_includesDC.dta",
                       check = TRUE)

WHO_adb    <- get_path('WHO', 'fin', "WHO_ADB_PDB_FGH[report_year]_ebola_fixed.dta",
                       check = TRUE)

##--------------------------------------------------------------------------------------
print('Read in and append UN agencies...')
GAVI_dt <- fread(GAVI_adb)[, REPORTING_AGENCY := 'GAVI']
## FGH 2024: we have report year data

PAHO_dt <- as.data.table(readstata13::read.dta13(PAHO_adb))[, REPORTING_AGENCY := "PAHO"]
PAHO_dt[ISO_CODE == 'UYU', ISO_CODE := 'URY']

UNAIDS_dt <- fread(UNAIDS_adb)[, REPORTING_AGENCY := 'UNAIDS']
UNAIDS_dt[ISO_CODE == 'ZAM', ISO_CODE := 'ZWE']
## FGH 2024 fix:
setnames(UNAIDS_dt, "oid_covid", "oid_covid_DAH")

UNFPA_dt <- as.data.table(read.dta13(UNFPA_adb))[, REPORTING_AGENCY := "UNFPA"]

UNICEF_dt <- as.data.table(read.dta13(UNICEF_adb))[, REPORTING_AGENCY := "UNICEF"]
UNICEF_dt[ISO_CODE == 'CPY', ISO_CODE := 'CYP']

WHO_dt <- as.data.table(readstata13::read.dta13(WHO_adb))[, REPORTING_AGENCY := "WHO"]
WHO_dt[ISO_CODE == 'XNI', ISO_CODE := 'GBR']

un_all <- rbind(GAVI_dt, PAHO_dt, UNAIDS_dt, UNFPA_dt, UNICEF_dt, WHO_dt, fill = TRUE)
rm(GAVI_dt, PAHO_dt, UNAIDS_dt, UNFPA_dt, UNICEF_dt, WHO_dt)

##--------------------------------------------------------------------------------------
# Do some additional cleaning missed at channel level
un_all[DONOR_NAME %in% c("AfDB", "AsDB", "GAVI", "GFATM", "PAHO",
                         "UNAIDS", "UNFPA", "UNICEF", "WB", "WB_IDA", "WB_IBRD",
                         "WHO"),
       SOURCE_CH := DONOR_NAME]

un_all[DONOR_NAME %in% c("AFDB",
                         "AFRICAN DEVELOPMENT FUND",
                         "AFRICAN DEVELOPMENT BANK GROUP"),
       SOURCE_CH := "AfDB"]

un_all[DONOR_NAME %in% c("ASIAN DEVELOPMENT BANK", "ASDB"), 
       SOURCE_CH := "AsDB"]


un_all[DONOR_NAME %in% c("GAVI,  THE VACCINE ALLIANCE",
                         "GAVI ALLIANCE",
                         "GAVI GLOBAL FUND FOR CHILDREN<92>S VACCINES",
                         "GAVI, THE VACCINE ALLIANCE",
                         "GAVI THE VACCINE ALLIANCE"), 
       SOURCE_CH := "GAVI"]

un_all[DONOR_NAME %in% c("GLOBAL FUND TO FIGHT AIDS TUBERCULOSIS AND MALARIA GFATM",
                         "THE GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)"), 
       SOURCE_CH := "GFATM"]

un_all[DONOR_NAME %in% c("UNAIDS USA", "JOINT UNITED NATIONS PROGRAM ON HIV AIDS UNAIDS"),
       SOURCE_CH := "UNAIDS"]

un_all[DONOR_NAME %in% c("UNITED NATIONS POPULATION FUND (UNFPA)"),
       SOURCE_CH := "UNFPA"]

un_all[DONOR_NAME %in% c("UNITED NATIONS CHILDREN'S FUND (UNICEF)"),
       SOURCE_CH := "UNICEF"]

un_all[DONOR_NAME %in% c("World Bank", "WORLD BANK"),
       SOURCE_CH := "WB"]

un_all[DONOR_NAME %in% c(
    "SHARE OF ADMINISTRATIVE COSTS FROM WHO",
    "SHARE OF ADMIN COST FROM WHO"
), SOURCE_CH := "WHO"]


un_all[CHANNEL == SOURCE_CH, SOURCE_CH := ""]


##--------------------------------------------------------------------------------------
dah.cols <- grep("DAH", colnames(un_all), value = TRUE)
setnafill(un_all, fill = 0, cols = dah.cols)

un_all[is.na(gov), gov := 0]
setnames(un_all, 'gov', "GOV")

## Subset and order columns
meta.cols <- c('YEAR', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY', 
               'REPORTING_AGENCY', 'ISO_CODE', 'ISO3_RC',
               'SOURCE_CH', 
               'INKIND', 'INKIND_RATIO', 'GOV', 'CHANNEL')
all_cols <- c(meta.cols, dah.cols)
un_all <- un_all[, ..all_cols]
setorderv(un_all, all_cols)

# if DAH is NA for all PAs, allocate to unalloc_DAH
pa.cols <- grep("_DAH", names(un_all), value = TRUE)
un_all[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = pa.cols]
un_all[, unalloc_DAH := fifelse(abs(tot) < 1, DAH, 0)]
pa.cols <- grep("_DAH", names(un_all), value = TRUE)

## Test to see if sum(pa columns) == DAH column
un_all[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = pa.cols]
un_all[, diff := DAH - tot] # make sure diff is only due to rounding
summary(un_all$diff, na.rm = T)

un_all[tot > 0 & DAH == 0, `:=`(
    DAH = tot, diff = 0
)]

# FGH 2024 fix - PAHO and UNAIDS are in violation
## preserve total envelope and rescale PAs to match
un_all[, (pa.cols) := lapply(.SD, \(x) DAH * x/tot), .SDcols = pa.cols]
un_all[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = pa.cols]
un_all[, diff := DAH - tot]

stopifnot(max(abs(un_all$diff)) < 100)
## if difference between DAH total column and summed column cannot be explained by 
## rounding, reach out to the person responsible for that channel for investigation
un_all[, c('diff', 'tot') := NULL]


##--------------------------------------------------------------------------------------
print('Export new ADB_PDBs from donor channels with double counting removed-------')

## For GFATM and GAVI, we also need to "add in" the administrative cost of these 
## double counted projects, so further scale by the SOURCE agency's in-kind ratio 
## Make sure these datasets are updated each year!

ik_GFATM <- fread(inkind_GFATM_dt)
ik_GFATM <- ik_GFATM[, c('YEAR', 'INKIND_RATIO')]
setnames(ik_GFATM, 'INKIND_RATIO', 'GFATM_IK')

ik_GAVI <- fread(inkind_GAVI_dt)
ik_GAVI <- ik_GAVI[, c('YEAR', 'INKIND_RATIO')]
setnames(ik_GAVI, 'INKIND_RATIO', 'GAVI_IK')

## Subset to rows were a source channel is reported. This is how we subtract from 
## an upstream donor agency 
un_all[is.na(SOURCE_CH), SOURCE_CH := ""]
to_subtract <- copy(un_all[SOURCE_CH != "", ])
to_subtract[, `:=`(ORIG_REPORTING_AGENCY = REPORTING_AGENCY,
                   ORIG_CHANNEL = CHANNEL)]
to_subtract[, REPORTING_AGENCY := SOURCE_CH]
to_subtract[, CHANNEL := SOURCE_CH]

## Now remove the in-kind amount of double counted projects because we only want to 
## subtract the project total, not administrative costs from the donating agency 

calcDoublecount <- function(x, y) -1 * (x * (1 - y))
to_subtract[, (dah.cols) := lapply(.SD, calcDoublecount, y = to_subtract[, INKIND_RATIO]), 
            .SDcols = dah.cols]

to_subtract <- merge(to_subtract, ik_GFATM, by = 'YEAR', all.x = TRUE)
to_subtract <- merge(to_subtract, ik_GAVI, by = 'YEAR', all.x = TRUE)

## Further remove the in-kind amount of funding from GFATM and GAVI since these channels
## are calculated by scaling up estimates using an in-kind ratio

calcG <- function(x, y) (x * (1 - y))
to_subtract[SOURCE_CH == 'GFATM',
            (dah.cols) := 
              lapply(.SD, calcG, y = to_subtract[SOURCE_CH == 'GFATM', GFATM_IK]), 
            .SDcols = dah.cols]
to_subtract[SOURCE_CH == 'GAVI', 
            (dah.cols) := 
              lapply(.SD, calcG, y = to_subtract[SOURCE_CH == 'GAVI', GAVI_IK]), 
            .SDcols = dah.cols]

print("Summary of values to subtract for inkind adjustment:")
print(summary(to_subtract$DAH))

# now we want to apply the DC subtraction to the source channel's total envelope
to_sub <- to_subtract[,
                      .(dah_subtract = sum(DAH, na.rm = TRUE)),
                      by = .(YEAR, CHANNEL)]

## convert UN flows to fractions of annual total
un_adjust <- copy(un_all)
un_adjust[, annual_total := sum(DAH, na.rm = TRUE), by = .(YEAR, CHANNEL)]
dah.cols <- grep("DAH", names(un_adjust), value = TRUE)
un_adjust[, (dah.cols) := lapply(.SD, \(x) x/annual_total), .SDcols = dah.cols]

## merge on the double-counted quantity to remove from total envelope
un_adjust <- merge(
    un_adjust, to_sub,
    by = c("YEAR", "CHANNEL"),
    all.x = TRUE
)
## (dah_subtract is NA if there is no subtraction needed for the CHANNEL-YEAR)
setnafill(un_adjust, fill = 0, cols = "dah_subtract")

## subtract double-counted quantity from source channel's total envelope
un_adjust[, annual_total_nodc := annual_total + dah_subtract] ## dah_subtract < 0

## convert DAH cols from fractions to dollar values using new total envelope
un_adjust[, (dah.cols) := lapply(.SD, \(x) x * annual_total_nodc), .SDcols = dah.cols]

## ensure re-allocation did not cause any issues in the HFA disaggregation
un_adjust[, tmp := rowSums(.SD, na.rm = TRUE),
          .SDcols = grep("_DAH", names(un_adjust), value = TRUE)]
stopifnot(un_adjust[abs(tmp - DAH) > 1, .N] == 0)
un_adjust[, tmp := NULL]

## clean up
un_adjust[, c("annual_total", "annual_total_nodc", "dah_subtract") := NULL]

## Only need to export for channels that predict 2020 values. 
## GFATM, AsDB, AfDB, and IDB are exempt
## The respective channels will then use these updated datasets to create PREDS datasets
##
## FGH 2024 note - didn't need predictions for GAVI, but they are kept in this 
##   group for consistency across reports
for (channel in c('GAVI', 'PAHO', 'UNAIDS', 'UNFPA', 'UNICEF', 'WHO')) {
    output <- un_adjust[REPORTING_AGENCY == channel, ]
    readstata13::save.dta13(
        output,
        paste0(get_path(channel, 'fin'), channel, "_ADB_PDB_FGH", dah.roots$report_year, "_noDC_", asn_date, ".dta")
    )
    readstata13::save.dta13(
        output,
        paste0(get_path(channel, 'fin'), channel, "_ADB_PDB_FGH", dah.roots$report_year, "_noDC.dta")
    )
    print(paste0(channel, " saved to:  ", get_path(channel, 'fin'), 
                 channel, "_ADB_PDB_FGH", dah.roots$report_year, "_noDC.dta"))
}

## For channels which have observed report-year data (not predictions), 
## we can apply subtractions directly in compiling
other_subtract <- to_subtract[!REPORTING_AGENCY %in% 
                                c('GAVI', 'PAHO', 'UNAIDS', 'UNFPA', 'UNICEF', 'WHO')]

save_dataset(other_subtract, paste0("nonUN_agency_double_counting_removal_", dah.roots$report_year), 'compiling','int')

print(paste0("Remaining double-counting saved to :", get_path('compiling', 'int'), 
             "nonUN_agency_double_counting_removal_", dah.roots$report_year, ".csv"))

## End of Script ##
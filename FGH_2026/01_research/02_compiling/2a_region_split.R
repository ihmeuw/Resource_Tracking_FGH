########################################################################################
## 03_region_split
## Project: FGH
## Description: Split region data, rewritten from Stata code
########################################################################################
#
# note, this should be run via 2_region_split_launch.R
print('1. Set up environment----------------------------------------------------------')
rm(list = ls(all.names = TRUE))

## Set filepaths
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

## Set constants
data_yr <- dah.roots$report_year - 1
dah_yr <- paste0("DAH_", dah.roots$abrv_year)
INT <- get_path('compiling', 'int')
FIN <- get_path('compiling', 'fin')

## Date passed from qsub script
args <- commandArgs(TRUE)
region_num <- args[1]
asn_date <- args[2]

if (interactive())
    region_num <- 1 ## for testing

print(paste0("report_year: ", dah.roots$report_year))
print(paste0("prev_report_year: ", dah.roots$prev_report_year))
print(paste0("dah.roots$abrv_year: ", dah.roots$abrv_year))
print(paste0("asn_date: ", asn_date))
print(paste0("region_num: ", region_num))
print(paste0("intermediary output directory: ", INT))

## -------------------------------------------------------------------------------------
print('2. Reshape data----------------------------------------------------------------')
## -------------------------------------------------------------------------------------
print("Start reshaping data")
print("Read in backcasted data")
## Create a file for each of the region types - fraction of DAH by country
alldata <- fread(paste0(INT, "region_data/DAH_compiling_1990_", dah.roots$report_year, "_backcasted.csv"))
print("Finished reading in data")


## Get DAH columns
dah.cols <- grep(dah_yr, colnames(alldata), value = TRUE)

## Rename the subregion for running code in parallel on cluster
setnames(alldata, 'iso3_subregion', 'iso3_region3', skip_absent = T)
setnames(alldata, 'ISO3_subregion', 'ISO3_region3', skip_absent = T)
setnames(alldata, 'n_subregion', 'n_region3')

## Regional data that needs to be disaggregated
regiondata <- alldata[(ISO3_RC %like% "^Q" & LEVEL == "REGIONAL") |
                        ISO3_RC %in% c('S2', 'S3', 'S4', 'S5', 'S6', 'S7'), ]
print("Regional codes: ") 
print(unique(regiondata$ISO3_RC))

## We only do the region split for years of observed data, excluding years of 
## preliminary estimate data
regiondata <- regiondata[YEAR <= data_yr, ]
regiondata[is.na(ELIM_CH), ELIM_CH := 0]
regiondata[is.na(ELIM_DONOR), ELIM_DONOR := 0]

sum_test <- sum(regiondata[, get(dah_yr)])
regiondata <- regiondata[, lapply(.SD, sum, na.rm = TRUE), 
                         by = c('YEAR', 'ISO3_RC', 'DONOR_NAME', 'ISO_CODE', 
                                'INCOME_SECTOR', 'INCOME_TYPE', 'REPORTING_AGENCY', 
                                'ELIM_CH', 'ELIM_DONOR', 'INKIND', 'CHANNEL', 'GOV'),
                        .SDcols = dah.cols]
print(paste0("Regional collapse successful: ", 
             round(sum(regiondata[, get(dah_yr)]), 0) == round(sum_test, 0)))

setnames(regiondata, 'ISO3_RC', 'iso')
print("Finished collapsing region-only data")

regiondata[, id := .I]

## -------------------------------------------------------------------------------------
## alldata
this_region <- paste0('ISO3_region', region_num)
print(paste0("Start preparing region ", this_region))

alldata <- alldata[LEVEL == "COUNTRY" & YEAR <= data_yr, ]
alldata <- alldata[get(this_region) != ""]

## ISO3_RC country all-year totals
sum_test <- sum(alldata[, get(dah_yr)])
alldata <- alldata[, .(dah = sum(get(dah_yr))), 
                         by = c('ISO3_RC', get('this_region'), 'gbd_region_name', 
                                'gbd_superregion_name', 'WB_REGION'),
                         .SDcols = dah.cols]
print(paste0("Country all-year collapse successful: ", 
             (round(sum(alldata[, dah]), 0) == round(sum_test, 0))))

alldata[, number := .N, .(get(this_region))]
print(unique(alldata[, .(get(this_region), number)]))
alldata[, iso := get(this_region)]

print("Compute country/region fractions for each project area")
alldata[, iso_total := sum(dah), by = .(iso)]
alldata[, recip_frct := dah / iso_total]
alldata <- alldata[, .(iso, ISO3_RC, recip_frct)]

tmp <- alldata[, .(frac = sum(recip_frct)), by = .(iso)]
stopifnot( tmp[abs(frac - 1) > 1e-6, .N] == 0)

## iso and ISO3_RC should be unique identifier
print(paste0("iso and ISO3_RC together make a unique ID: ",
             nrow(unique(alldata[, .(iso, ISO3_RC)])) == nrow(alldata)))


## -------------------------------------------------------------------------------------						
print("Create regional splits")

regiondata3 <- merge(regiondata, alldata, by = 'iso', all.x = T, allow.cartesian = T)


print("Multiply fraction times DAH")
print("Before multiplication: ")
print(summary(regiondata3[, get(dah_yr)]))
regiondata3[, (dah_yr) := recip_frct * get(dah_yr)]
print("After multiplication: ")
print(summary(regiondata3[, get(dah_yr)]))

print("Prepare project-area adjustment")
pas <- dah.roots$regular_pa_vars
pas <- paste0(pas, "_", dah_yr)
regiondata3[, (pas) := lapply(.SD, \(x) x * recip_frct), .SDcols = pas]

regiondata3 <- regiondata3[YEAR <= data_yr, ]

regiondata3[, tmp := rowSums(.SD, na.rm = TRUE),
            .SDcols = grep("_DAH_", names(regiondata3), value = TRUE)]
regiondata3[, diff := abs(DAH_24 - tmp)]
stopifnot(regiondata3[diff > 1e-3, .N] == 0)
regiondata3[, c('tmp', 'diff') := NULL]
regiondata3[, eval(dah_yr) := NULL]

dah.cols <- dah.cols[dah.cols != dah_yr]
regiondata3[, eval(dah_yr) := rowSums(.SD, na.rm = T), .SDcols = dah.cols]
regiondata3[, tag := paste0('add_file_', this_region)]
print("Final DAH summary: ")
print(summary(regiondata3[, get(dah_yr)]))
regiondata3 <- regiondata3[get(dah_yr) > 0, ]


print(paste0('Save final datasets to ', INT, 'region_data'))
save_dataset(regiondata3,
             paste0("add_file_", this_region),
             'compiling', 'int', 'region_data')
		
print('End of Script')

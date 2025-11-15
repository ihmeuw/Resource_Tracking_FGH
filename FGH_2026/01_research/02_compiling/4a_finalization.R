########################################################################################
## 4_finalization script
## Project: FGH
## Purpose: Compile all databases, rewritten from Stata code
########################################################################################
print('1. Set up environment----------------------------------------------------------')
## Clear environment
rm(list = ls(all.names = TRUE))

## Set filepaths
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
source(paste0(code_repo, "/FUNCTIONS/helper_functions.R"))

data_yr     <- dah.roots$report_year - 1
dah_yr <- paste0("DAH_", dah.roots$abrv_year)
## Date
args <- commandArgs(TRUE)
args <- c(format(Sys.time(), "%Y%m%d"))
asn_date <- args[1]

##Final data output filepath
INT <- get_path('compiling', 'int')
FIN <- get_path('compiling', 'fin')

print(paste0("report_year: ", dah.roots$report_year))
print(paste0("prev_report_year: ", dah.roots$prev_report_year))
print(paste0("dah.roots$abrv_year: ", dah.roots$abrv_year))
print(paste0("asn_date: ", asn_date))
print(paste0("final output directory: ", FIN))


REGION_ISOS <- c("QMA", "QNA", "QRA", "QSA", "QTA", 
                  "QNC", "QRD","QMD", "QME", "QNB", "QNE", "QRB", "QRC", "QRE", "QRS",
                  "S2", "S3", "S4", "S5", "S6", "S7")
## -------------------------------------------------------------------------------------
print('2. Read in data----------------------------------------------------------------')
## -------------------------------------------------------------------------------------
## Create a file for each of the region types - fraction of DAH by country
main_path <- paste0(INT, "region_data/DAH_compiling_1990_", dah.roots$report_year, "_backcasted.csv")

cat("Reading in:", main_path, "\n")
cat("    File last modified:\n    ",
    as.character.POSIXt(file.info(main_path)[, "mtime"]), "\n")

regional_fixed1 <- fread(main_path)

region1 <- fread(paste0(INT, "region_data/add_file_ISO3_region1.csv"))
region2 <- fread(paste0(INT, "region_data/add_file_ISO3_region2.csv"))
region3 <- fread(paste0(INT, "region_data/add_file_ISO3_region3.csv"))
region4 <- fread(paste0(INT, "region_data/add_file_ISO3_region4.csv"))

## -------------------------------------------------------------------------------------
print('3. Data cleaning------------------')
## -------------------------------------------------------------------------------------
regional_fixed <- copy(regional_fixed1)
setnames(regional_fixed, 'iso3_subregion', 'iso3_region3', skip_absent = T)
setnames(regional_fixed, 'ISO3_subregion', 'ISO3_region3', skip_absent = T)
setnames(regional_fixed, 'n_subregion', 'n_region3')

## Drop isocodes that get split out (will be replaced with split funding from files 
## output in region splitting)
regional_fixed <- regional_fixed[!(ISO3_RC %in% REGION_ISOS & YEAR <= data_yr), ]

regional_fixed <- rbind(regional_fixed, region1, region2, region3, region4, fill = T)

save_dataset(regional_fixed,
             "compiling_checkpoint2",
             channel = "compiling",
             stage = "int",
             folder = "region_data",
             format = "arrow")

## Drop high-income income_group spending
income_groups <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
regional_fixed <- merge(regional_fixed,
                        income_groups[, .(ISO3_RC, YEAR, INC_GROUP)],
                        by = c('ISO3_RC', 'YEAR'),
                        all.x = TRUE)
print(unique(regional_fixed[, .(INC_GROUP.x, INC_GROUP.y)]))
regional_fixed[, `:=`(INC_GROUP.x = NULL, REAL_INC_GROUP = INC_GROUP.y)]
regional_fixed[, INC_GROUP.y := NULL]
print("Removing high-income spending from the following channels and countries: ") 
# possible issue with Argentina and Ven
print(unique(regional_fixed[REAL_INC_GROUP == "H", .(ISO3_RC, YEARS = paste(sort(unique(YEAR)), collapse = ","))]))
print(paste0("Removing high-income spending totaling: $", regional_fixed[REAL_INC_GROUP == "H", round(sum(get(paste0('DAH_',dah.roots$abrv_year))) / 1e6, 2)], " million"))
regional_fixed[! REAL_INC_GROUP %in% c("L", "LM", "UM", "H"),
               REAL_INC_GROUP := NA_character_]
regional_fixed <- regional_fixed[REAL_INC_GROUP %in% c("L", "LM", "UM") | is.na(REAL_INC_GROUP), ]

## Now assign income group based on time-invariant estimation year categories
regional_fixed[, REAL_INC_GROUP := NULL]
setnames(regional_fixed, 'ISO3_RC', 'ihme_loc_id')
regional_fixed <- get_ig(regional_fixed)
setnames(regional_fixed, c('ihme_loc_id', 'income_group'), c('ISO3_RC', 'INC_GROUP'))
regional_fixed[INC_GROUP == '', INC_GROUP := NA]
table(regional_fixed$INC_GROUP)
cat("Locs with missing INC_GROUP: ",
    paste(regional_fixed[is.na(INC_GROUP), unique(ISO3_RC)], collapse = ", "), "\n")

regional_fixed[is.na(LEVEL), LEVEL := "COUNTRY_disaggregated"]
regional_fixed[ISO3_RC == "QZA", LEVEL := "UNALLOCABLE"]
regional_fixed[, `:=`(gbd_region_name = NULL, gbd_superregion_name = NULL,
                      WB_REGION = NULL,  WB_REGIONCODE = NULL, RECIPIENT_COUNTRY = NULL)]

## Make sure all country level observations have gbd and wb region info
regional_fixed[ISO3_RC == "WLD", ISO3_RC := "G"]
regional_fixed[ISO3_RC == "UNSP", ISO3_RC := "QZA"]
regional_fixed[ISO3_RC == "OTH", ISO3_RC := "QZA"]
regional_fixed[ISO3_RC == "KSV", ISO3_RC := "SRB"]
regional_fixed[ISO3_RC == "XKX", ISO3_RC := "SRB"]
regional_fixed[is.na(ISO3_RC) | ISO3_RC == "", ISO3_RC := "QZA"]

#
# finally, if we still have region level recipients, assign to unallocated
cat("Years with regional ISOs remaining:\n")
regional_fixed[ISO3_RC %in% REGION_ISOS, table(YEAR)]
regional_fixed[ISO3_RC %in% REGION_ISOS, ISO3_RC := "QZA"]

iso <- fread(paste0(FIN, "region_data/iso_data_region_test.csv"))
setdiff(unique(regional_fixed$ISO3_RC), iso$ISO3_RC)
regional_fixed <- merge(regional_fixed, iso, by = 'ISO3_RC', all.x = T)
regional_fixed[ISO3_RC == "QZA", WB_REGION := "Unallocable"]
regional_fixed[is.na(GOV), GOV := 0]

## The reason we have to do this here is because Slovenia isn't actually in the channel
## datasets, it is created during backcasting 
## Investigate the WB_REGION dataset and update!
regional_fixed[ISO3_RC == "SVN", WB_REGION := "Europe & Central Asia"]
regional_fixed[ISO3_RC == "GRC", WB_REGION := "Europe & Central Asia"]

regional_fixed[ISO3_RC == "QZA", `:=`(
    gbd_superregion_name = "Unallocable",
    gbd_region_name = "Unallocable"
)]


## these should no longer be relevant, since regions should be split for all
## years <= data_year
regional_fixed[ISO3_RC %in% c("QNA", "QNB", "QNC", "QNE", "R7") & is.na(gbd_superregion_name), 
               gbd_superregion_name := "Latin America and Caribbean"]
regional_fixed[ISO3_RC %in% c('QMA', 'QMD', 'QME') & is.na(gbd_superregion_name),
               gbd_superregion_name := "Sub-Saharan Africa"]
regional_fixed[ISO3_RC %in% c("QRA", "QRB","QRD", "QTA") & is.na(gbd_superregion_name), 
               gbd_superregion_name := "Southeast Asia, East Asia, and Oceania"]
regional_fixed[ISO3_RC %in% c("QSA", "QRS") & is.na(gbd_superregion_name), 
               gbd_superregion_name := "Central Europe, Eastern Europe, and Central Asia"]
regional_fixed[ISO3_RC == "QRC" & is.na(gbd_superregion_name), 
               gbd_superregion_name := "South Asia"]
regional_fixed[ISO3_RC == "QRE" & is.na(gbd_superregion_name), 
               gbd_superregion_name := "North Africa and Middle East"]
regional_fixed[ISO3_RC == "QMU" & is.na(gbd_superregion_name), 
               gbd_superregion_name := "Southeast Asia, East Asia, and Oceania"]
regional_fixed[ISO3_RC == 'S3',
               `:=`(RECIPIENT_COUNTRY = "Sub-Saharan Africa", gbd_superregion_name = "Sub-Saharan Africa")]
regional_fixed[ISO3_RC == 'S5', 
               `:=`(RECIPIENT_COUNTRY = "South Asia",  gbd_superregion_name = "South Asia")]
regional_fixed[ISO3_RC == 'S6', 
               `:=`(RECIPIENT_COUNTRY = "Southeast Asia, East Asia, and Oceania",  gbd_superregion_name = "Southeast Asia, East Asia, and Oceania")]

## For Global Fund ONLY, reassign blank superregions. 
regional_fixed[ISO3_RC %in% c("QPA", "QPB", "QMA") & is.na(gbd_superregion_name) & CHANNEL == "GFATM",
               gbd_superregion_name := "Sub-Saharan Africa"]
regional_fixed[ISO3_RC == "QMZ" & is.na(gbd_superregion_name) & CHANNEL == "GFATM", 
               gbd_superregion_name := "Central Europe, Eastern Europe, and Central Asia"]
regional_fixed[ISO3_RC == "QSD" & is.na(gbd_superregion_name) & CHANNEL == "GFATM",
               gbd_superregion_name := "South Asia"]
regional_fixed[ISO3_RC == "QSE" & is.na(gbd_superregion_name) & CHANNEL == "GFATM", 
               gbd_superregion_name := "Southeast Asia, East Asia, and Oceania"]
regional_fixed[ISO3_RC == "QMA" & is.na(gbd_superregion_name) & CHANNEL == "BMGF",
               gbd_superregion_name := "Sub-Saharan Africa"]

regional_fixed[ISO3_RC == "G" & (is.na(gbd_superregion_name) | gbd_superregion_name == ""),
               `:=`(gbd_region_name = 'Global' ,
                    gbd_superregion_name = "Global",
                    RECIPIENT_COUNTRY = 'Global')]


## Make sure this is = 0! There may still be some missings in prelim estimate years if 
## there are regional projects which are not split out.
stopifnot(nrow(regional_fixed[is.na(gbd_superregion_name) & YEAR <= data_yr, ]) == 0)

cols <- grep("DAH$", colnames(regional_fixed), value = TRUE)
regional_fixed[, c(cols, 'ISO3_region1', 'OECD_REGION',
                   'ISO3_region2', 'ISO3_region3', 'ISO3_region4',
                   'n_region1', 'n_region2', 'n_region3', 'n_region4',
                   'iso') := NULL]




## Create HFA-level totals -------------------------------------------------------------

## Calculate unallocable
hfa_pas <- paste0(dah.roots$regular_pa_vars, "_", dah_yr)
setnafill(regional_fixed, fill = 0, cols = hfa_pas)

regional_fixed[, tot := rowSums(.SD), .SDcols = hfa_pas]
regional_fixed[, diff := get(dah_yr) - tot]

if (nrow(regional_fixed[abs(diff) > 1]) > 0) {
    warning("Discrepancies between total DAH and its components. Please investigate.")
}
## note that at this point, there shouldn't be any major 'diff' for any of the
## channels since that should be resolved at the channel level before script 1.
## however, it can arise. If there is any diff, it should be investigated to
## determine why the compiling pipeline is causing it.
##
## in the meantime, modify the PAs accordingly:

## if the sum across components is greater than the total DAH envelope,
## diff < 0, so
## recalculate component fractions & apply to the total envelope
regional_fixed[, diff := round(diff)]
regional_fixed[diff < 0,
               (hfa_pas) := lapply(.SD, function(x) get(dah_yr) * x / tot),
               .SDcols = hfa_pas]
## if the sum across components is smaller than the total DAH envelope,
## diff > 0, so
## allocate the difference to unallocable
regional_fixed[diff > 0,
               paste0("unalloc_", dah_yr) := get(paste0("unalloc_", dah_yr)) + diff]

cols <- grep("DAH_", colnames(regional_fixed), value = TRUE)
setnafill(regional_fixed, fill = 0, cols = cols)

#
# re-test:
regional_fixed[, tot := rowSums(.SD), .SDcols = hfa_pas]
regional_fixed[, diff := get(dah_yr) - tot]

if (nrow(regional_fixed[diff > 0.1,]) > 0) {
    stop("There are still discrepancies in the HFA-level totals.")
}


#
# calculate HFAs (sum of PAs)
names <- c("rmh_", "nch_", "ncd_", "hiv_", "mal_", "tb_", "oid_")
for (name in names) {
    cols <- grep(paste0("^", name), colnames(regional_fixed), value = TRUE)
    regional_fixed[, eval(paste0(name, dah_yr)) := rowSums(.SD, na.rm = TRUE), .SDcols = cols]
}

swp.cols <- grep("^swap_", colnames(regional_fixed), value = TRUE)
regional_fixed[, eval(paste0('swap_hss_total_', dah_yr)) := rowSums(.SD, na.rm = TRUE),
               .SDcols = swp.cols]

#
# test:
regional_fixed[, tot := rowSums(.SD),
               .SDcols = paste0(dah.roots$regular_hfa_vars, "_", dah_yr)]

stopifnot(nrow(regional_fixed[abs(tot - get(dah_yr)) > 1 & get(dah_yr) > 0, ]) == 0)
regional_fixed[, c("tot", "diff") := NULL]


cols <- grep(paste0("_", dah_yr), colnames(regional_fixed), value = TRUE)
setnafill(regional_fixed, fill = 0, cols = cols)


#
# finalize elim_ch and elim_donor (double-counting indicators)
#
setnafill(
    regional_fixed, fill = 0, cols = c("ELIM_CH", "ELIM_DONOR")
)


## Order variables
setcolorder(regional_fixed,
            c('YEAR', 'DONOR_NAME', 'DONOR_COUNTRY', 'INCOME_SECTOR',
              'INCOME_TYPE', 'CHANNEL', 'REPORTING_AGENCY',
              'ELIM_CH', 'ELIM_DONOR', 'INKIND'))
regional_fixed <- regional_fixed[YEAR <= dah.roots$report_year & YEAR >= 1990, ]


#
# finalize channel
#
regional_fixed[CHANNEL == "INTLNGO", CHANNEL := "INTL_NGO"]

ch <- unique(regional_fixed$CHANNEL)
cat("Final channels:\n")
cat("Non-bilateral:\n",
    paste(sort(grep("BIL", ch, value = TRUE, invert = TRUE)), collapse = ", "),
    fill = TRUE)
cat("\nBilateral:\n",
    paste(sort(grep("BIL", ch, value = TRUE)), collapse = ", "), "\n",
    fill = TRUE)

cat("Channels that appear in non double-counted flows:\n")

cat(
    paste(sort(regional_fixed[ELIM_CH == 0 & ELIM_DONOR == 0, unique(CHANNEL)]),
          collapse = ", "),
    fill = TRUE)




#
# fix some donor names
#

## some wb donor names are ISO_CODES with a single number in front of them
regional_fixed[REPORTING_AGENCY == "WB" & grepl("^\\d+", DONOR_NAME) &
                   nchar(DONOR_NAME) <= 4,
               DONOR_NAME := substr(DONOR_NAME, 2, nchar(DONOR_NAME))]

## WHO fix:
regional_fixed <- regional_fixed[DONOR_NAME != "REFUNDS TO DONORS NOTE 3"]



# Fix missing donor country info
cc <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
#
# first, ensure all donor ISO codes are correct/finalized
#
regional_fixed[ISO_CODE == "", ISO_CODE := NA_character_]

regional_fixed[DONOR_COUNTRY %like% "D'IVOIRE",
               `:=` (DONOR_COUNTRY = "COTE D'IVOIRE", ISO_CODE = "CIV")]
regional_fixed[ISO_CODE == "EC", ISO_CODE := "QZA"]
regional_fixed[ISO_CODE %in% c("ADB", "INT_FOUND", "PRIVATE_INK", "US_FOUND"),
               ISO_CODE := "QZA"]
regional_fixed[ISO_CODE == "MCD", ISO_CODE := "MKD"] # macedonia
regional_fixed[ISO_CODE == "XCZ", ISO_CODE := "CZE"] # czech repub
regional_fixed <- merge(regional_fixed,
                        cc[, .(DONOR_COUNTRY = toupper(location_name), ihme_loc_id)],
                        by = "DONOR_COUNTRY", all.x = TRUE)

regional_fixed[is.na(ISO_CODE), ISO_CODE := ihme_loc_id]
## if the existing iso code is wrong, based on the donor country, correct it:
regional_fixed[!is.na(ISO_CODE) &
                   DONOR_COUNTRY != "UNALLOCABLE" & ## iso code will be used below
                   ISO_CODE != ihme_loc_id,
               ISO_CODE := ihme_loc_id]

## DEBT2HEALTH parsing
d2h <- unique(regional_fixed[DONOR_COUNTRY %ilike% "DEBT2HEALTH", .(DONOR_COUNTRY)])
d2h[, new := gsub("DEBT2HEALTH - ", "", toupper(DONOR_COUNTRY))]
d2h[, c("source", "dest") := tstrsplit(new, "-", fixed = TRUE)]
regional_fixed <- merge(
    regional_fixed,
    unique(d2h[, .(DONOR_COUNTRY, d2h = source)]),
    by = "DONOR_COUNTRY", all.x = TRUE
)
regional_fixed[!is.na(d2h), DONOR_COUNTRY := d2h]
regional_fixed[, d2h := NULL]
rm(d2h)

regional_fixed[, ISO_CODE := fcase(
    DONOR_COUNTRY == "BMGF", "BMGF",
    DONOR_COUNTRY == "BOSNIA & HERZEGOVINA", "BIH",
    DONOR_COUNTRY == "CZECH REPUBLIC", "CZE",
    DONOR_COUNTRY == "SUDANESE REPUBLIC", "SDN",
    DONOR_COUNTRY == "GUINEA BISSAU", "GNB",
    DONOR_COUNTRY == "JERSEY", "JEY",
    DONOR_COUNTRY == "OCCUPIED PALESTINIAN TERRITORIES", "PSE",
    DONOR_COUNTRY == "VIETNAM", "VNM",
    DONOR_COUNTRY == "AUSTRALIA", "AUS",
    DONOR_COUNTRY == "GERMANY", "DEU",
    DONOR_COUNTRY == "JORDAN", "JOR",
    DONOR_COUNTRY == "SPAIN", "ESP",
    DONOR_COUNTRY == "SERBIA & MONTENEGRO", "SRB",
    DONOR_COUNTRY == "SOUTH SUDAN", "SSD",
    DONOR_COUNTRY %in% c("COTE D'IVOIRE", "COTE D IVOIRE"), "CIV",
    DONOR_COUNTRY %ilike% "TAIWAN", "TWN",
    DONOR_COUNTRY %ilike% "VATICAN CITY", "VAT",
    DONOR_COUNTRY %ilike% "VENEZUELA", "VEN",
    DONOR_COUNTRY %ilike% "HONG KONG", "HKG",
    DONOR_COUNTRY %ilike% "UNITED STATES", "USA",
    DONOR_COUNTRY %ilike% "IRAN", "IRN",
    DONOR_COUNTRY %ilike% "NORTH KOREA", "PRK",
    DONOR_COUNTRY %ilike% "KOREA", "KOR",
    DONOR_COUNTRY %ilike% "RUSSIA", "RUS",
    DONOR_COUNTRY %ilike% "TANZANIA", "TZA",
    DONOR_COUNTRY %ilike% "GAMBIA", "GMB",
    DONOR_COUNTRY %ilike% "TURKEY", "TUR",
    DONOR_COUNTRY %ilike% "UNITED KINGDOM", "GBR",
    DONOR_COUNTRY %ilike% "MOLDOVA", "MDA",
    DONOR_COUNTRY %ilike% "LIBYA", "LBY",
    DONOR_COUNTRY %ilike% "MACAO", "CHN",
    ## some corps
    DONOR_COUNTRY == "ABSA GROUP LTD", "ZAF",
    DONOR_COUNTRY == "F.HOFFMANN-LA ROCHE", "CHE",
    DONOR_COUNTRY == "GLAXOSMITHKLINE PLC AND VIIV HEALTHCARE", "GBR",
    DONOR_COUNTRY == "MEDTRONIC LABS", "KEN",
    DONOR_COUNTRY == "PT. KALBE FARMA TBK", "IDN",
    rep_len(TRUE, .N), ISO_CODE
)]

regional_fixed[DONOR_NAME == "SOUTH SUDAN", ISO_CODE := "SSD"]


# check every year...
cat(crayon::yellow("  Donor countries with missing ISO codes:\n"))
print(regional_fixed[is.na(ISO_CODE), unique(DONOR_COUNTRY)])
cat(crayon::yellow("These donor countries will have ISO code set to 'QZA'.\n",
                   "Please fix if needed.\n"))

regional_fixed[, ihme_loc_id := NULL]

regional_fixed[is.na(ISO_CODE), ISO_CODE := "QZA"]
regional_fixed[ISO_CODE %in% c("OTH", "OTHER", "UNALL", "MULTI", "NGO", "INK",
                               "QTA", "Na", "CENTRAL", "PRIVATE", "WB", "PPP",
                               "PRIV_INK", "UN"),
               ISO_CODE := "QZA"]

#
# now use the ISO codes to get the correct donor country names
#
## but first, correct some potential mistakes
regional_fixed[ISO_CODE %in% c("UK", "GRB"), ISO_CODE := "GBR"]
regional_fixed[ISO_CODE == "VTN" & DONOR_COUNTRY %like% "VIET",
               ISO_CODE := "VNM"]

regional_fixed <- merge(regional_fixed,
                        unique(cc[, .(ISO_CODE = ihme_loc_id,
                                      location_name = toupper(location_name))]),
                        by = "ISO_CODE",
                        all.x = TRUE)
warn_isos <- regional_fixed[!is.na(ISO_CODE) & is.na(location_name), unique(ISO_CODE)]
if (length(warn_isos) > 0) {
    cat(crayon::yellow("The following ISO codes have no matching location name:\n"))
    print(warn_isos)
    cat(crayon::yellow("Correct if needed.\n"))
}


regional_fixed[, location_name := fcase(
    ISO_CODE == "BMGF", "BMGF",
    ISO_CODE == "JEY", "JERSEY",
    ISO_CODE %in% c("DEBT", "PRIVATE"), "",
    ISO_CODE == "YUG_FRMR", "YUGOSLAVIA",
    ISO_CODE == "CZE_FRMR", "CZECHOSLOVAKIA",
    ISO_CODE == "USSR_FRMR", "USSR",
    ISO_CODE == "FRO", "FAROE ISLANDS",
    rep_len(TRUE, .N), location_name
)]

cat(crayon::yellow("The following ISO codes have no matching donor country:\n"))
print(unique(regional_fixed[is.na(location_name), ISO_CODE]))
cat(crayon::yellow("These will have donor country set to 'UNALLOCABLE.'\n",
                   "Please fix if needed.\n"))

regional_fixed[is.na(location_name), `:=`(
    location_name = "UNALLOCABLE",
    ISO_CODE = "QZA"
    )]
regional_fixed[, `:=`(
    DONOR_COUNTRY = location_name,
    location_name = NULL
)]

regional_fixed[INCOME_SECTOR == "BMGF", `:=`(
    ISO_CODE = "BMGF",
    INCOME_TYPE = "FOUND",
    DONOR_NAME = "BMGF"
)]


regional_fixed[ISO3_RC == "G", ISO3_RC := "WLD"]
regional_fixed[RECIPIENT_COUNTRY == "", RECIPIENT_COUNTRY := NA_character_]
if(nrow(regional_fixed[is.na(RECIPIENT_COUNTRY) & ISO3_RC != "QZA"]))
    stop("There are missing recipient countries in the data.")
# ## These countries are being reassigned from the GBD superregion "High-income"
regional_fixed[ISO3_RC %in% c("ARG", "CHL", "URY"),
               gbd_superregion_name := "Latin America and Caribbean"]
regional_fixed[ISO3_RC == "KOR",
               gbd_superregion_name := "Southeast Asia, East Asia, and Oceania"]
regional_fixed[ISO3_RC == "MLT",
               gbd_superregion_name := "Central Europe, Eastern Europe, and Central Asia"]

drop.cols <- grep("_frct", colnames(regional_fixed), value = TRUE)
regional_fixed[, c(drop.cols, "id", "tag", "INKIND_RATIO") := NULL]

#
# Income Sector & Income Type Fixes ===========================================
#

## Fix these at channel level
## Fix in unicef, gavi
## gavi, unaids, unfpa, unicef, wb, who
regional_fixed[INCOME_SECTOR %in% c("CHN", "GBR","UK"), INCOME_SECTOR := "PUBLIC"]
## NGOs
regional_fixed[INCOME_SECTOR == "PUB_US", `:=`(ISO_CODE = "USA", INCOME_SECTOR = "PUBLIC", DONOR_COUNTRY = "UNITED STATES OF AMERICA")]
regional_fixed[INCOME_SECTOR == "OTHER_PRIV", INCOME_SECTOR := "PRIVATE"]
## UN agencies
regional_fixed[INCOME_SECTOR == "MULTI", INCOME_SECTOR := "OTHER"]
regional_fixed[INCOME_SECTOR %in%  c("NA", "UNALL", "UNSP"), INCOME_SECTOR := "UNALL"]
# GFATM, GAVI
regional_fixed[INCOME_SECTOR == "PRIVINK", INCOME_SECTOR := "PRIVATE"]
regional_fixed[INCOME_SECTOR == "PRIV_INK", INCOME_SECTOR := "PRIVATE"]

regional_fixed[INCOME_SECTOR == "PUBLIC" &
                   DONOR_NAME %like% "COMMITMENTS TO BE PERSONALLY SECURED",
               `:=`(
                   INCOME_SECTOR = "PRIVATE",
                   INCOME_TYPE = "INDIV"
               )]

regional_fixed[DONOR_NAME %in% c(
    "UNITED STATES OF AMERICA USAID",
    "UNITED STATES OF AMERICA CDC"
    ), `:=`(
        INCOME_SECTOR = "PUBLIC",
        INCOME_TYPE = "CENTRAL",
        ISO_CODE = "USA",
        DONOR_COUNTRY = "UNITED STATES OF AMERICA"
)]



# ad hoc fixes - update as needed, rm if fixed at channel level
regional_fixed[INCOME_TYPE == "DEVBANKS", INCOME_TYPE := "DEVBANK"]
regional_fixed[INCOME_TYPE == "FOUNDATION", INCOME_TYPE := "FOUND"]
regional_fixed[INCOME_TYPE == "SOC", INCOME_TYPE := "OTHER"]
regional_fixed[INCOME_TYPE == "UNSP", INCOME_TYPE := "UNALL"]
regional_fixed[DONOR_NAME == "WHO" & CHANNEL == "WHO", `:=`(
    INCOME_SECTOR = "UNALL",
    INCOME_TYPE = "UNALL",
    DONOR_NAME = "UNALLOCABLE"
)]

## validate/clean source assignments - based on:
## https://hub.ihme.washington.edu/spaces/RT/pages/123852351/Source+Assignments
regional_fixed[INCOME_TYPE == "CENTRAL" & INCOME_SECTOR == "UNALL",
               INCOME_SECTOR := "PUBLIC"]

regional_fixed[DONOR_NAME %in% c(
    "F.HOFFMANN-LA ROCHE", "PT. KALBE FARMA TBK", "MEDTRONIC LABS", "ABSA GROUP LTD"
), `:=`(
    INCOME_SECTOR = "INK",
    INCOME_TYPE = "CORP"
)]
regional_fixed[DONOR_NAME %in% c("PRINCIPAL REPAYMENTS"), `:=`(
    INCOME_SECTOR = "DEBT",
    INCOME_TYPE = "OTHER"
)]
regional_fixed[DONOR_NAME %in% c("WB_IBRD"), `:=`(
    INCOME_SECTOR = "OTHER",
    INCOME_TYPE = "DEVBANK"
)]
regional_fixed[DONOR_NAME %in% c(
    "THE PACIFIC COMMUNITY (FORMER SOUTH PACIFIC COMMISSION)",
    "INSTITUTE OF NUTRITION OF CENTRAL AMERICA AND PANAMA"
), `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "OTHER"
)]
regional_fixed[DONOR_NAME == "YEMEN HUMANITARIAN FUND" & ISO_CODE == "QZA", `:=`(
    INCOME_SECTOR = "OTHER",
    INCOME_TYPE = "UN",
    DONOR_COUNTRY = "UNALLOCABLE"
)]
regional_fixed[DONOR_NAME == "TÜRKIYE", `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "CENTRAL"
)]
regional_fixed[INCOME_SECTOR == "CENTRAL", `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "CENTRAL"
)]
regional_fixed[INCOME_SECTOR == "OTHER" & INCOME_TYPE == "CENTRAL", `:=`(
    INCOME_TYPE = ""
)]

regional_fixed[INCOME_SECTOR == "INK", INCOME_TYPE := "CORP"]
stopifnot(regional_fixed[INCOME_TYPE == "CORP" & INCOME_SECTOR != "INK", .N] == 0)

regional_fixed[INCOME_SECTOR == "PRIVATE" & INCOME_TYPE == "",
               INCOME_TYPE := "UNALL"]
regional_fixed[INCOME_TYPE == "FOUND" & INCOME_SECTOR != "BMGF",
               INCOME_SECTOR := "PRIVATE"]
regional_fixed[INCOME_TYPE == "NGO",
               INCOME_SECTOR := "OTHER"]
regional_fixed[INCOME_TYPE %in% c("UN", "DEVBANK", "PPP"),
               INCOME_SECTOR := "OTHER"]

regional_fixed[INCOME_TYPE == "" & INCOME_SECTOR == "PUBLIC" & ISO_CODE != "QZA",
               INCOME_TYPE := "CENTRAL"]
regional_fixed[INCOME_TYPE == "" & INCOME_SECTOR == "PUBLIC" & ISO_CODE == "QZA",
               INCOME_TYPE := "UNALL"]

regional_fixed[INCOME_TYPE == "" & INCOME_SECTOR == "DEBT",
               INCOME_TYPE := "OTHER"]
regional_fixed[INCOME_TYPE == "" & INCOME_SECTOR == "OTHER",
               INCOME_TYPE := "OTHER"]
regional_fixed[INCOME_TYPE == "" & INCOME_SECTOR == "UNALL",
               INCOME_TYPE := "UNALL"]
regional_fixed[INCOME_SECTOR == "PPP", `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "PPP"
)]
regional_fixed[CHANNEL == "WB_IBRD" & DONOR_NAME == "BOND ISSUANCE", `:=` (
    INCOME_SECTOR = "DEBT", INCOME_TYPE = "OTHER"
)]
regional_fixed[DONOR_NAME == "UNSPECIFIED" & DONOR_COUNTRY == "UNALLOCABLE", `:=`(
    INCOME_SECTOR = "UNALL", INCOME_TYPE = "UNALL"
)]

cat(crayon::yellow("The following DONOR_NAMEs will have INCOME_TYPE set to UNALL:\n"))
cat(paste(regional_fixed[INCOME_TYPE == "", unique(DONOR_NAME)], collapse = " | "),
    fill = TRUE)
regional_fixed[INCOME_TYPE == "", INCOME_TYPE := "UNALL"]



# Finally, we need to make sure that anything assigned to INCOME_SECTOR "OTHER"
# can't be reassigned to a more meaningful category

regional_fixed[CHANNEL == "WHO" & DONOR_NAME %in% c(
    "WHO COLLABORATING CENTRE FOR RESEARCH AND TRAINING IN MENTAL HEALTH"        
    ,"WHO COLLABORATING CENTER FOR RESEARCH AND TRAINING IN MENTAL HEALTH"        
    ,"AID-WHO"                                                                    
    ,"OTHER INCOME FOR OTHER WHO FUNDS"                                           
    ,"WHO STAFF ASSOCIATION"                                                      
    ,"WHO STAFF ASSOCIATION VOLUNTARY EMERGENCY RELIEF FUND"                      
    ,"OTHER INCOME FROM OTHER WHO FUNDS"                                          
    ,"UNSPECIFIED VOLUNTARY CONTRIBUTIONS TO TRUST FUNDS (WHO ACTIVITIES)"        
    ,"WHO REGIONAL OFFICE FOR EUROPE (RGO)"                                       
    ,"WHO REGIONAL OFFICE FOR EUROPE"                                             
    ,"REGIONAL OFFICE FOR AFRICA WHO HEALTH EMERGENCIES PROGRAM"                  
    ,"VOLUNTARY ASSESSED CONTRIBUTIONS – WHO FCTC CONFERENCE OF THE PARTIES (COP)"
    ,"VOLUNTARY ASSESSED CONTRIBUTIONS – WHO FCTC MEETING OF THE PARTIES (MOP)"   
    ,"WHO CONTINGENCY FUND FOR EMERGENCIES"                                       
    ,"WHO FOUNDATION"                                                             
), `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "OTHER"
)]


regional_fixed[SOURCE_CH %in% c("AfDB", "AsDB", "IDB", "WB", "WB_IDA", "WB_IBRD"), `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "DEVBANK"
)]

regional_fixed[SOURCE_CH %in% c("UNICEF", "WHO", "PAHO", "UNFPA", "UNAIDS"), `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN"
)]

regional_fixed[SOURCE_CH %in% c("GFATM", "GAVI"), `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "PPP"
)]

regional_fixed[DONOR_NAME %like% "UNITED NATIONS" &
                   INCOME_SECTOR == "OTHER" & INCOME_TYPE != "UN", `:=`(
                       INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN"
                   )]
regional_fixed[INCOME_TYPE == "UN" & DONOR_NAME %like% "ARAB GULF PROGRAMME",
               `:=`(INCOME_TYPE = "OTHER")]


## (a creditor nation foregoes repayment of a loan if the debtor nation invests
##  all or part of the freed-up resources into a Global Fund-supported program)
regional_fixed[DONOR_NAME %like% "DEBT2HEALTH", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "OTHER"
)]

regional_fixed[DONOR_NAME == "HIS HIGHNESS SHEIKH MOHAMED BIN ZAYED AL NAHYAN", `:=`(
    INCOME_SECTOR = "PRIVATE", INCOME_TYPE = "INDIV"
)]

regional_fixed[DONOR_NAME == "NORAD/MALAWI", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL",
    ISO_CODE = "NOR", DONOR_COUNTRY = "NORWAY"
)]

regional_fixed[DONOR_NAME %in% c(
    "COUNCIL OF EUROPE DEVELOPMENT BANK",
    "DEVELOPMENT BANK OF LATIN AMERICA",
    "ISLAMIC DEVELOPMENT BANK",
    "IDB"
), `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "DEVBANK"
)]


regional_fixed[DONOR_NAME %in% c(
    "FOOD AND AGRICULTURE ORGANIZATION (FAO)",
    "UN HABITAT",
    "UNSP_UN"
), `:=`(INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN")]

regional_fixed[DONOR_NAME == "WORLD HEALTH ORGANIZATION-UNITAID", `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN", DONOR_NAME = "WHO"
)]

regional_fixed[DONOR_NAME == "EC", `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "EC"
)]

regional_fixed[CHANNEL == "BMGF" | INCOME_SECTOR == "BMGF" | DONOR_NAME == "BMGF",
               `:=`(
                   INCOME_SECTOR = "BMGF", INCOME_TYPE = "BMGF"
                   )]


regional_fixed[INCOME_SECTOR == "OTHER" & INCOME_TYPE == "OTHER" & DONOR_NAME %in% c(
    "ANONYMOUS",
    "MISSING AGENCY NAME",
    "UNSPECIFIED VOLUNTARY CONTRIBUTIONS TO TRUST FUNDS (WHO ACTIVITIES)",
    "UNSPECIFIED PUBLIC VOLUNTARY CONTRIBUTIONS TO VFHP (ESTIMATE)",
    "INCOME FOR JUNIOR PROFESSIONAL OFFICERS (UNSPECIFIED CONTRIBUTIONS)",
    "MISCELLANEOUS INCOME",
    "MISCELLANEAOUS INCOME",
    "MISCELLANEAOUS",
    "OTHER AND MISCELLANEOUS RECEIPTS"
), `:=`(
    INCOME_SECTOR = "UNALL", INCOME_TYPE = "UNALL"
)]


#
# Final cleaning, aggregate, and save
#

# Check that all rows have CHANNEL value
regional_fixed[REPORTING_AGENCY == "UNFPA" & CHANNEL == "",
               CHANNEL := "UNFPA"]
stopifnot(nrow(regional_fixed[is.na(CHANNEL) | CHANNEL == "", ]) == 0)

## clean other vars
regional_fixed[is.na(SOURCE_CH), SOURCE_CH := ""]

regional_fixed[LEVEL == "COUNTRY_disaggregated",
               LEVEL := "COUNTRY_split"]


setnafill(regional_fixed, fill = 0, cols = "prelim_est")

## aggregate by id cols
final <- regional_fixed[, lapply(.SD, sum, na.rm = TRUE),
                        .SDcols = grep("DAH_", names(regional_fixed), value = TRUE),
                        by = c(
                            "YEAR",
                            "ELIM_CH", "ELIM_DONOR",
                            "DONOR_NAME", "ISO_CODE", "DONOR_COUNTRY",
                            "INCOME_SECTOR", "INCOME_TYPE",
                            "CHANNEL", "REPORTING_AGENCY", "SOURCE_CH", "GOV",
                            "ISO3_RC", "RECIPIENT_COUNTRY", "LEVEL",
                            "INC_GROUP", "WB_REGION", "WB_REGIONCODE",
                            "gbd_region_name", "gbd_superregion_name",
                            "INKIND",
                            "prelim_est"
                        )]
final <- final[get(dah_yr) != 0]


setorder(final, YEAR)
save_dataset(
    final, "adb_pre_disagg",
    channel = "compiling",
    stage = "int"
)


cat("* Done\n")

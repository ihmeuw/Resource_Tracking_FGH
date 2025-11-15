#######################################################################################
## 01_compiling
## Project: FGH
## Description: Compile all databases, rewritten from Stata code
#######################################################################################

#~~~~~~~~~~~    NOTES:     ~~~~~~~~~~~~
#

## Clear environment
rm(list = ls(all.names = TRUE))
options(warn = 1) ## print warnings as they occur

## Set filepaths
h <- ifelse(Sys.info()["sysname"] == "Linux", 
            paste0("/ihme/homes/", Sys.info()["user"], "/"),
            "H:/")
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))

data_yr     <- get_dah_param('CRS', 'data_year')
dah_yr <- paste0("DAH_", dah.roots$abrv_year)
asn_date    <- format(Sys.time(), "%Y%m%d")
prev_report_year <- dah.roots$prev_report_year

print(paste0("dah.roots$report_year: ", dah.roots$report_year))
print(paste0("dah.roots$prev_report_year: ", dah.roots$prev_report_year))
print(paste0("dah.roots$abrv_year: ", dah.roots$abrv_year))
print(paste0("asn_date: ", asn_date))

##--------------------------------------------------------------------------------------
## Folders and filepaths
DAH_RESEARCH_DIR <- file.path(dah.roots$j, "FILEPATH/")
INT <- get_path('compiling', 'int')
FIN <- get_path('compiling', 'fin')
print(paste0("Intermediary output directory: ", INT))
print(paste0("FINAL output directory: ", FIN))

##
## Channels
##

# Dev Banks ==== 
#
.AFDB_adb <- get_path('AFDB', 'fin', "M_AfDB_ADB_PDB_FGH[report_year].dta",
                     check = TRUE)
.ASDB_adb <- get_path('ASDB', 'fin', "AsDB_ADB_PDB_FGH[report_year].csv",
                     check = TRUE)
.IDB_adb <- get_path('IDB', 'fin', "IDB_ADB_PDB_FGH[report_year].dta",
                    check = TRUE)

# note - in FGH 2024, we did not need preds for the first time
.WB_adb <- get_path("WB", "fin", "WB_ADB_PDB_FGH[report_year].csv",
                   check = TRUE)

dev_banks <- list(
    AFDB_adb = .AFDB_adb,
    ASDB_adb = .ASDB_adb,
    IDB_adb = .IDB_adb,
    WB_adb = .WB_adb
)


# Foundations ====
#
.BMGF_adb <- get_path("BMGF", "fin", "GATES_ADB_PDB_FGH[report_year].csv",
                      check = TRUE)
.BMGF_pred <- get_path("BMGF", "fin", "BMGF_PREDS_DAH_1990_[report_year]_BY_HFA.csv",
                       check = TRUE)

.USFOUND_adb <- get_path("US_FOUNDS", "FIN",  "F_USA_ADB_PDB_FGH[report_year]_20250429.dta",
                         check = TRUE)
.USFOUND_pred <- get_path("US_FOUNDS", "fin",
                          "P_FOUNDATIONS_EXP_ASSETS_PREDS_FINAL_FGH[report_year]_20250429.dta",
                          check = TRUE)

foundations <- list(
    BMGF_adb = .BMGF_adb,
    BMGF_pred = .BMGF_pred,
    USFOUND_adb = .USFOUND_adb,
    USFOUND_pred = .USFOUND_pred
)


# Bilaterals ====
#
final_crs_update <- "0425"
.CRS_adb <- get_path("CRS", "fin", "B_CRS_[crs.update_mmyy]_ADB_PDB.csv",
                     check = TRUE,
                     crs.update_mmyy = final_crs_update)
.CRS_pred <- get_path("BILAT_PREDICTIONS", "fin",
                      "Bilateral_predictions_[report_year]_BY_HFA_[crs.update_mmyy].csv",
                      check = TRUE,
                      crs.update_mmyy = final_crs_update)
.EC_pred <- get_path("BILAT_EC", "fin",
                     "EC_PREDS_BY_HFA_SOURCE_1990_[report_year]_[crs.update_mmyy].csv",
                     check = TRUE,
                     crs.update_mmyy = final_crs_update)

.CHINA_adb <- get_path("CHINA", "fin", "BIL_CHINA_ADBPDB_1990_[report_year].csv",
                      check = TRUE)

.EEA_adb   <- get_path('EEA', 'fin', "EEA_ADB_PDB_1025.dta",
                      check = TRUE)

bilaterals <- list(
    CRS_adb = .CRS_adb,
    CRS_pred = .CRS_pred,
    EC_pred = .EC_pred,
    CHINA_adb = .CHINA_adb,
    EEA_adb = .EEA_adb
)



# Public-Private Partnerships ====
#
.CEPI_adb <- get_path("CEPI", "fin", "CEPI_ADB_PDB_[report_year].csv",
                     check = TRUE)

.GAVI_adb <- get_path("GAVI", "fin", "GAVI_ADB_PDB_FGH[report_year]_noDC.dta",
                     check = TRUE)

## FGH 2024: predictions not needed for GAVI this report-year

.GFATM_adb <- get_path("GFATM", "fin", "P_GFATM_ADB_PDB_FGH_[report_year].csv",
                       check = TRUE)

ppp <- list(
    CEPI_adb = .CEPI_adb,
    GAVI_adb = .GAVI_adb,
    GFATM_adb = .GFATM_adb
)

# NGOs ====
#
.NGO_adb <- get_path("NGO", "fin", "NGO_ADB_PDB_FGH2024_2025_05_01.csv",
                     check = TRUE)

ngos <- list(
    NGO_adb = .NGO_adb
)


# UN Agencies ====
#
.PAHO_adb <- get_path("PAHO", "fin", "PAHO_ADB_PDB_FGH[report_year]_noDC.dta",
                     check = TRUE)
.PAHO_pred <- get_path("PAHO", "fin",
                      "PAHO_PREDS_DAH_BY_SOURCE_1990_[report_year]_noDC.dta",
                      check = TRUE)

.UNAIDS_adb <- get_path("UNAIDS", "fin", "UNAIDS_ADB_PDB_FGH[report_year]_noDC.dta",
                       check = TRUE)
.UNAIDS_pred <- get_path("UNAIDS", "fin",
                        "UNAIDS_PREDS_BY_SOURCE_DAH_1996_[report_year].dta",
                        check = TRUE)

.UNFPA_adb <- get_path("UNFPA", "fin", "UNFPA_ADB_PDB_FGH[report_year]_noDC.dta",
                      check = TRUE)
.UNFPA_pred <- get_path("UNFPA", "fin",
                       "UNFPA_PREDS_DAH_BY_SOURCE_1990_[report_year]_ebola_fixed.dta",
                       check = TRUE)

.UNICEF_adb <- get_path("UNICEF", "fin", "UNICEF_ADB_PDB_FGH[report_year]_noDC.dta",
                       check = TRUE)
.UNICEF_pred <- get_path("UNICEF", "fin",
                        "UNICEF_PREDS_DAH_BY_SOURCE_1990_[report_year].dta",
                        check = TRUE)

.UNITAID_adb <- get_path("UNITAID", "fin", "UNITAID_ADBPDB_FGH[report_year].csv",
                        check = TRUE)


.WHO_adb <- get_path("WHO", "fin", "WHO_ADB_PDB_FGH[report_year]_noDC.dta",
                     check = TRUE)
.WHO_pred <- get_path("WHO", "fin",
                     "WHO_PREDS_DAH_BY_SOURCE_1990_[report_year]_noDC.dta",
                     check = TRUE)

un_agencies <- list(
    PAHO_adb = .PAHO_adb,
    PAHO_pred = .PAHO_pred,
    UNAIDS_adb = .UNAIDS_adb,
    UNAIDS_pred = .UNAIDS_pred,
    UNFPA_adb = .UNFPA_adb,
    UNFPA_pred = .UNFPA_pred,
    UNICEF_adb = .UNICEF_adb,
    UNICEF_pred = .UNICEF_pred,
    UNITAID_adb = .UNITAID_adb,
    WHO_adb = .WHO_adb,
    WHO_pred = .WHO_pred
)




all_paths <- c(
    dev_banks,
    foundations,
    bilaterals,
    ppp,
    ngos,
    un_agencies
)


# check coverage of ADBPDBs and PREDs
adb <- data.table()
for (ch in ls(all_paths, pattern = "*_adb")) {
    fp <- get(ch, all_paths)
    d <- if (grepl(".dta", fp, fixed = TRUE)) {
        data.table(read.dta13(fp, select.cols = c("YEAR", "DAH")))
    } else {
        fread(fp, select = c("YEAR", "DAH"))
    }
    d <- d[, .(tot = sum(DAH, na.rm = TRUE)), by = .(YEAR)]
    adb <- rbind(adb,
                 data.table(channel = gsub("_adb", "", ch),
                            adb_min_yr = min(d[tot > 0, YEAR]),
                            adb_max_yr = max(d[tot > 0, YEAR])))
}

pred <- data.table()
for (ch in ls(all_paths, pattern = "*_pred")) {
    fp <- get(ch, all_paths)
    d <- if (grepl(".dta", fp, fixed = TRUE)) {
        data.table(read.dta13(fp))
    } else {
        fread(fp)
    }
    names(d) <- toupper(names(d))
    if (ch == "UNAIDS_pred")
        d[, DAH_YY := rowSums(.SD, na.rm = TRUE), .SDcols = grep("_DAH_", names(d))]
    if ("DAH_FRCT" %in% names(d)) d[, DAH_FRCT := NULL]
    dahcol <- grep("^DAH_|^OUTFLOW_|^DISBURSEMENT_|^FINAL_DAH_", names(d), value = TRUE)
    if (any(grepl("DAH_", dahcol)) && any(grepl("OUTFLOW_", dahcol)))
        dahcol <- dahcol[grep("DAH_", dahcol)] # prefer dah to outflow
    d <- d[, .(tot = sum(get(dahcol), na.rm = TRUE)), by = .(YEAR)]
    pred <- rbind(pred,
                  data.table(channel = gsub("_pred", "", ch),
                             pred_max_yr = max(d[tot > 0, YEAR]),
                             pred_name = dahcol))
}

coverage <- merge(adb, pred, by = "channel", all = TRUE)
coverage[, pred_yrs := ""]
coverage[!is.na(pred_max_yr) & !is.na(adb_max_yr) & adb_max_yr != pred_max_yr,
         pred_yrs := paste(seq(adb_max_yr + 1, pred_max_yr), collapse = ","),
         by = channel]
coverage[is.na(adb_max_yr), pred_yrs := pred_max_yr]
coverage[, check := ifelse(adb_max_yr != dah.roots$report_year | is.na(adb_max_yr),
                           "check pred", "")]
coverage[, warn := ifelse(!is.na(pred_max_yr) & pred_max_yr != dah.roots$report_year,
                          "pred_max_yr != report-year", "")]
coverage[adb_max_yr != dah.roots$report_year & is.na(pred_max_yr),
         warn := "adb_max_yr != report-year"]
coverage[adb_max_yr >= pred_max_yr, warn := "adb_max_yr >= pred_max_yr"]

cat(crayon::yellow("\tInspect the coverage of the channel datasets:\n"))
print(tidyr::tibble(coverage), n = Inf)
flush.console()

nwarn <- coverage[warn != "", .N]
warning(paste0("There are ", nwarn, " warnings in the coverage of the channel datasets. Inspect carefully."))

rm(adb, pred)


## Other input files
country_codes_path <- get_path("meta", "locs", "countrycodes_official.csv",
                               check = TRUE)
wb_ig_path <- get_path("meta", "locs", "wb_historical_incgrps.csv",
                       check = TRUE)
deflators_path <- get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv",
                           check = TRUE)
oecd_countries_path <- get_path("meta", "locs", "oecd_regions.csv",
                                check = TRUE)

usfound_names_path  <- paste0(DAH_RESEARCH_DIR, "FILEPATH/Cleaning Raw Names.xlsx")
stopifnot(file.exists(usfound_names_path))



# HELPERS ---------------------------------------------------------------------
.__hfas <- dah.roots$regular_hfa_vars[! dah.roots$regular_hfa_vars %in% c("other", "unalloc")]
standardized_names <- function(x) {
    if (inherits(x, "data.frame")) {
        x <- names(x)
    } else if (!is.character(x)) {
        stop("x must be a character vector of names or a data.frame like object")
    }
    dahcolidx <- grep("dah", x, ignore.case = TRUE)
    x[dahcolidx] <- gsub("dah", "DAH", tolower(x[dahcolidx]))
    x[-dahcolidx] <- toupper(x[-dahcolidx])
    chk <- vapply(
        c(paste0(.__hfas, "_dah"), paste0("pr_", .__hfas, "_dah_", dah.roots$abrv_year)),
        function(nm) nm %in% tolower(x),
        logical(1))
    if (any(chk)) 
        warning("HFA columns exist, when only PA columns are expected.", call. = FALSE)
    x <- gsub("mal_con_other", "mal_con_oth", x, fixed = TRUE)
    if (interactive()) print(x)
    return(x)
}


##--------------------------------------------------------------------------------------
print('1. Append and prepare estimates data------------------')
##--------------------------------------------------------------------------------------
#
# ensure inputs have a total DAH column and Program Area (i.e., sub-HFA) columns
# but NO aggregated HFA columns.
# Also make sure oid_covid_DAH is removed & dropped
#

print('UN agencies...')
UNAIDS_dt <- as.data.table(readstata13::read.dta13(all_paths$UNAIDS_adb))
names(UNAIDS_dt) <- standardized_names(UNAIDS_dt)

UNFPA_dt  <- as.data.table(readstata13::read.dta13(all_paths$UNFPA_adb))
names(UNFPA_dt) <- standardized_names(UNFPA_dt)

UNICEF_dt <- as.data.table(readstata13::read.dta13(all_paths$UNICEF_adb))
names(UNICEF_dt) <- standardized_names(UNICEF_dt)

PAHO_dt   <- as.data.table(readstata13::read.dta13(all_paths$PAHO_adb))
names(PAHO_dt) <- standardized_names(PAHO_dt)

WHO_dt    <- as.data.table(readstata13::read.dta13(all_paths$WHO_adb))
names(WHO_dt) <- standardized_names(WHO_dt)
WHO_dt <- WHO_dt[YEAR < dah.roots$report_year]

UNITAID_dt <- fread(all_paths$UNITAID_adb)
UNITAID_dt[, REPORTING_AGENCY := "UNITAID"]
names(UNITAID_dt) <- standardized_names(UNITAID_dt)
## Should be fixed at the channel level
colnames(UNITAID_dt) <- sub("_hss_DAH", "_hss_other_DAH", colnames(UNITAID_dt))
colnames(UNITAID_dt) <- sub("tb_hss$", "tb_hss_other", colnames(UNITAID_dt))
colnames(UNITAID_dt) <- sub("mal_irs_", "mal_con_irs_", colnames(UNITAID_dt))
colnames(UNITAID_dt) <- sub("swap_other_", "swap_hss_other_", colnames(UNITAID_dt))
colnames(UNITAID_dt) <- sub("mal_bednets_", "mal_con_nets_", colnames(UNITAID_dt))

un_all <- rbind(UNAIDS_dt, UNFPA_dt, UNICEF_dt, PAHO_dt, WHO_dt, UNITAID_dt, fill = TRUE)
rm(UNAIDS_dt, UNFPA_dt, UNICEF_dt, PAHO_dt, WHO_dt, UNITAID_dt)

##-------------------------------------------------------------------------------
print('Banks...')
AFDB_dt <- data.table(read.dta13(all_paths$AFDB_adb))
AFDB_dt[, REPORTING_AGENCY := 'AfDB']
names(AFDB_dt) <- standardized_names(AFDB_dt)
## for consistency with earlier submissions, don't use new income assignments
AFDB_dt[YEAR >= 2021 & INCOME_SECTOR == "PUBLIC",
        `:=`(
            INCOME_SECTOR = "UNALL",
            INCOME_TYPE = "UNALL",
            ISO_CODE = "",
            DONOR_NAME = "UNSP"
        )]




WB_dt <- fread(all_paths$WB_adb)
WB_dt[, REPORTING_AGENCY := 'WB']
names(WB_dt) <- standardized_names(WB_dt)

IDB_dt <- data.table(read.dta13(all_paths$IDB_adb))
IDB_dt[, REPORTING_AGENCY := 'IDB']
names(IDB_dt) <- standardized_names(IDB_dt)

ASDB_dt <- fread(all_paths$ASDB_adb)
ASDB_dt[, REPORTING_AGENCY := 'AsDB']
names(ASDB_dt) <- standardized_names(ASDB_dt)

banks_all <- rbind(AFDB_dt, WB_dt, IDB_dt, ASDB_dt, fill = TRUE)
rm(AFDB_dt, WB_dt, IDB_dt, ASDB_dt)

##-------------------------------------------------------------------------------
print('Foundations...')
USFOUND_dt <- as.data.table(read.dta13(all_paths$USFOUND_adb))
USFOUND_dt[, REPORTING_AGENCY := 'US_FOUND']
names(USFOUND_dt) <- standardized_names(USFOUND_dt)

BMGF_dt <- fread(all_paths$BMGF_adb)
BMGF_dt[, REPORTING_AGENCY := 'BMGF']
BMGF_dt <- BMGF_dt[YEAR < dah.roots$report_year]
names(BMGF_dt) <- standardized_names(BMGF_dt)

foundations_all <- rbind(USFOUND_dt, BMGF_dt, fill = TRUE)
rm(USFOUND_dt, BMGF_dt)

##-------------------------------------------------------------------------------
print('PPP...')
GAVI_dt <- as.data.table(read.dta13(all_paths$GAVI_adb))
GAVI_dt[, REPORTING_AGENCY := 'GAVI']
names(GAVI_dt) <- standardized_names(GAVI_dt)
## FGH 2024: no preds needed, we have report-year data
## FHG 2024: Ad-hoc fix: funding to south korea should have been assigned to north korea
GAVI_dt[ISO3_RC == "KOR", ISO3_RC := "PRK"]

GFATM_dt <- fread(all_paths$GFATM_adb)
GFATM_dt[, REPORTING_AGENCY := 'GFATM']
names(GFATM_dt) <- standardized_names(GFATM_dt)

CEPI_dt <- fread(all_paths$CEPI_adb)
CEPI_dt[, REPORTING_AGENCY := 'CEPI']
names(CEPI_dt) <- standardized_names(CEPI_dt)

ppp_all <- rbind(GAVI_dt, GFATM_dt, CEPI_dt, fill = TRUE)
rm(GAVI_dt, GFATM_dt, CEPI_dt)

##--------------------------------------------------------------------------------------
print('Bilaterals, EC and NGOs...\n')
CRS_dt <- fread(all_paths$CRS_adb) ## bilat and EC
CRS_dt[, REPORTING_AGENCY := "CRS"]
names(CRS_dt) <- standardized_names(CRS_dt)
CRS_dt <- CRS_dt[YEAR >= 1990, ]

##NGO estimates and prelim estimates are both in one file, all in nominal dollars	
NGO_dt <- fread(all_paths$NGO_adb)
NGO_dt <- NGO_dt[YEAR <= report_year]
NGO_dt[, REPORTING_AGENCY := "NGO"]
names(NGO_dt) <- standardized_names(NGO_dt)
NGO_dt[, GOV := 2]
# TMP
# No update for NGO channel, instead propagate 2018 values forward over time.
NGO_dt <- NGO_dt[YEAR <= 2018]
tmp <- NGO_dt[YEAR == 2018, -"YEAR"]
tmp <- tmp[, .(YEAR = 2019:2024),
           by = names(tmp)]
NGO_dt <- rbind(NGO_dt, tmp)
rm(tmp)
# /TMP
#


CHINA_dt <- fread(all_paths$CHINA_adb)
CHINA_dt[, REPORTING_AGENCY := "BIL_CHINA"]
names(CHINA_dt) <- standardized_names(CHINA_dt)

EEA_dt <- as.data.table(read.dta13(all_paths$EEA_adb))
EEA_dt[, REPORTING_AGENCY := "EEA"]
names(EEA_dt) <- standardized_names(EEA_dt)

bilat_all <- rbind(CRS_dt, NGO_dt, CHINA_dt, EEA_dt, fill = TRUE)
rm(CRS_dt, NGO_dt, CHINA_dt, EEA_dt)


# TMP
## For FGH2024 initial submission, we are copying NGO DAH for all years
## after 2018, based on 2018 data. Thus, we still incorporate the
## separate COVID estimates for NGOs from FGH2023.

oid_covid_dt <- fread(get_path("compiling", "fin", "COVID_data.csv",
                               report_year = prev_report_year))
# standardize reporting agency to make verifications simpler
oid_covid_dt[, REPORTING_AGENCY := fcase(
    REPORTING_AGENCY == "AFDB", "AfDB",
    REPORTING_AGENCY == "ASDB", "AsDB",
    REPORTING_AGENCY == "CHINA", "BIL_CHINA",
    REPORTING_AGENCY == "BILAT", "CRS",
    rep_len(TRUE, .N), REPORTING_AGENCY
)]
covid_agg <- oid_covid_dt[, .(
    oid_covid_DAH = sum(oid_covid_DAH, na.rm = TRUE)
    ), by = .(REPORTING_AGENCY,
              YEAR,
              DONOR_COUNTRY, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE,
              CHANNEL,
              ISO3_RC,
              ELIM_CH, ELIM_DONOR)]

covid_agg <- covid_agg[REPORTING_AGENCY %in% c(
    "NGO"
)]
covid_agg[, DAH := oid_covid_DAH]

bilat_all <- rbind(bilat_all, covid_agg, fill = TRUE)
# /TMP
#






##--------------------------------------------------------------------------------------
print('Append all into one dataframe and adjust')
append_all <- rbind(bilat_all, foundations_all, un_all, banks_all, ppp_all,
                    fill = TRUE)

append_all[, tot := rowSums(.SD, na.rm = TRUE),
           .SDcols = grep("_DAH", names(append_all), value = TRUE)]

bad_hfa_ch <- append_all[abs(tot - DAH) > 100, unique(REPORTING_AGENCY)]
if (length(bad_hfa_ch) > 0) {
    warning(paste0("Following channels have PAs that don't sum to total DAH:  ",
                   paste(bad_hfa_ch, collapse = ", ")))
}

dah.cols <- grep("DAH", colnames(append_all), value = TRUE)
setnafill(append_all, fill = 0, cols = dah.cols)

for (col in dah.cols[dah.cols != "DAH"]){
    if (! col %in% paste0(dah.roots$regular_pa_vars, "_DAH"))
        warning(col, " is in `append_all` but is not a regular PA variable")
}

stopifnot( length(dah.cols[dah.cols != "DAH"]) == length(dah.roots$regular_pa_vars) )
rm(bilat_all, foundations_all, un_all, banks_all, ppp_all)

append_all[is.na(GOV), GOV := 0]

## Subset and order columns
meta.cols <- c('YEAR', 'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'DONOR_COUNTRY', 
               'REPORTING_AGENCY', 'ISO_CODE', 'ISO3_RC', 'ELIM_CH', 'SOURCE_CH', 
               'INKIND', 'INKIND_RATIO', 'GOV', 'CHANNEL')
all_cols <- c(meta.cols, dah.cols)
append_all <- append_all[, ..all_cols]

## Test to see if sum(pa columns) == DAH column
pa.cols <- dah.cols[dah.cols != 'DAH']
append_all[, tot := rowSums(.SD, na.rm = TRUE),
           .SDcols = pa.cols]

if (any(append_all$tot < 0)){
    warning("Negative disbursements found in channel-level data. Please investigate channels: ",
            paste(append_all[tot < 0, unique(REPORTING_AGENCY)], collapse = ", "))
}
append_all <- append_all[tot >= 0]

append_all[tot == 0 & DAH != 0, `:=`(
    unalloc_DAH = DAH,
    tot = DAH
)]



#
# Double-Counting Removal ====================================================
#
print('1b. Double-Counting Adjustment------------------')
# Apply double-counting subtraction to total envelopes of CHANNEL-YEARs
#
## From the previous 0_un_doublecounting_fix.R script, 
## non-UN agencies with double counting for removal
nonUN_noDC <- fread(get_path("compiling",
                             "int",
                             "nonUN_agency_double_counting_removal_[report_year].csv"))
to_sub <- nonUN_noDC[, .(dah_subtract = sum(DAH, na.rm = TRUE)),
                     by = .(CHANNEL, YEAR)]

## WB DC fix - WHO receives voluntary contributions from "World Bank", but we don't
##   know really if it should be counted as IDA or IBRD, but it needs to be
##   subtracted from one of our actual channels (i.e., not "WB"). So we split it
##   evenly between IDA and IBRD:
wbdc <- to_sub[CHANNEL == "WB"]
wbdc <- wbdc[, .(dah_subtract = dah_subtract / 2,
                 CHANNEL = c("WB_IDA", "WB_IBRD")),
             by = .(YEAR)]
to_sub <- to_sub[CHANNEL != "WB"]
to_sub <- rbind(to_sub, wbdc)
rm(wbdc)

## re-aggregate the double-counting removals by channel-year
to_sub <- to_sub[, .(dah_subtract = sum(dah_subtract, na.rm = TRUE)),
                 by = .(CHANNEL, YEAR)]

## convert DAH flows to fractions of total channel envelope
append_all[, annual_total := sum(DAH, na.rm = TRUE), by = .(YEAR, CHANNEL)]
dah.cols <- grep("DAH", colnames(append_all), value = TRUE)
append_all[, (dah.cols) := lapply(.SD, \(x) x / annual_total),
           .SDcols = dah.cols]

## merge on the double-counted quantity to remove from total channel envelope
append_all <- merge(append_all, to_sub,
                     by = c("CHANNEL", "YEAR"),
                     all.x = TRUE)
## (dah_subtract is NA if there is no subtraction needed for the CHANNEL-YEAR)
setnafill(append_all, fill = 0, cols = "dah_subtract")

## subtract double-counted quantity from source channel's total envelope
append_all[, annual_total_nodc := annual_total + dah_subtract] ## dah_subtract < 0

## convert DAH cols from fractions to dollar values using new total envelope
append_all[, (dah.cols) := lapply(.SD, \(x) x * annual_total_nodc),
           .SDcols = dah.cols]

## ensure re--alocation did not cause any issues in the HFA disaggregation
append_all[, tot := rowSums(.SD, na.rm = TRUE),
           .SDcols = grep("_DAH", names(append_all), value = TRUE)]
stopifnot(all(
    append_all[abs(tot - DAH) > 100, unique(REPORTING_AGENCY)] %in% bad_hfa_ch
))

append_all[, c("annual_total", "annual_total_nodc", "dah_subtract") := NULL]

# Add eliminated quantities into DB for tracking purposes
#   but flag with "ELIM_DONOR" column
dah_cols <- grep("DAH", names(nonUN_noDC), value = TRUE)
### convert quantities from negative to positive, since now they represent
### transfers, not eliminations
nonUN_noDC[, (dah_cols) := lapply(.SD, \(x) -1 * x), .SDcols = dah_cols]
nonUN_noDC[, `:=`(
    DONOR_NAME = SOURCE_CH,
    REPORTING_AGENCY = ORIG_REPORTING_AGENCY,
    CHANNEL = ORIG_CHANNEL,
    ORIG_REPORTING_AGENCY = NULL,
    ORIG_CHANNEL = NULL
)]

append_all[, ELIM_DONOR := 0]
nonUN_noDC[, ELIM_DONOR := 1]
append_all <- rbind(append_all, nonUN_noDC, fill = TRUE)


#
# should not be differences between summed columns and DAH column
## if difference between DAH total column and summed column cannot be explained by 
## rounding, reach out to the person responsible for that channel for investigation
append_all[, diff := DAH - tot] # make sure diff is only due to rounding
summary(append_all$diff, na.rm = T)
# stopifnot(max(abs(append_all$diff)) < 100)

append_all[, c('diff', 'tot') := NULL]

#
# At this point, need to ensure all PAs sum to total DAH
#
pa_cols <- grep("_DAH", names(append_all), value = TRUE)
append_all[, dah_sum := rowSums(.SD, na.rm = TRUE), .SDcols = pa_cols]
append_all[, (pa_cols) := lapply(.SD, function(x) {
    DAH * x / dah_sum
}), .SDcols = pa_cols]

## test
append_all[, dah_sum := rowSums(.SD, na.rm = TRUE), .SDcols = pa_cols]
if (append_all[abs(dah_sum - DAH) > 1e-3, .N] > 0)
    stop("PA sums do not match total DAH")
append_all[, dah_sum := NULL]


save_dataset(append_all,
             paste0("compiling_checkpoint1"),
             channel = "compiling",
             stage = "int",
             folder = "region_data",
             format = "arrow")

##--------------------------------------------------------------------------------------
print('1c. Verify UN Double-counting fix removed all double counting-------')
to_subtract <- copy(append_all[SOURCE_CH != "" & SOURCE_CH != REPORTING_AGENCY])
stopifnot(unique(to_subtract$REPORTING_AGENCY) %in% 
            c("GAVI", "PAHO", "UNAIDS", "UNFPA", "UNICEF", "WHO"))
rm(to_subtract)

## When REPORTING AGENCY AND SOURCE_CH are different, these are rows which were tagged
## with a SOURCE_CH at the channel level. These had double-counting removed in a 
## previous step. However, if there are REPORTING_AGENCIES other than these 6 - 
## ("GAVI", "PAHO", "UNAIDS", "UNFPA", "UNICEF", "WHO") - present in to_subtract, 
## then you need to consult the person responsible for that channel, and possibly
## rerun the 0_un_doublecounting_fix.R file with that agency included.

##--------------------------------------------------------------------------------------
print('1d. Clean retrospective data------------------')

## Keep 1990 onward - seems to be dropping one value for no reason
append_1990 <- copy(append_all[YEAR >= 1990, ])

## Clean donor name
append_1990[, DONOR_NAME := toupper(DONOR_NAME)]
append_1990[, DONOR_COUNTRY := toupper(DONOR_COUNTRY)]
append_1990[DONOR_NAME == '', DONOR_NAME := NA]

append_1990[ISO_CODE %in% c('', 'UNSP', 'UNSP_UN'), ISO_CODE := NA]
append_1990[ISO_CODE == 'HK', ISO_CODE := 'HKG']
append_1990[ISO_CODE == 'CZECH', ISO_CODE := 'CZE_FRMR']
append_1990[ISO_CODE == 'XYG', ISO_CODE := 'YUG_FRMR']
append_1990[DONOR_NAME == 'ELIMINATION', DONOR_NAME := 'ELIMINATIONS']
append_1990[DONOR_NAME == 'AFRICA DEVELOPMENT FUND', 
            DONOR_NAME := 'AFRICAN DEVELOPMENT FUND']
append_1990[DONOR_NAME == "AGENCE DE COOPERATION CULTURELLE ET TECHNIQUE (AGENCY FOR CULTURAL AND TECHNICAL COOPERATION)",
            DONOR_NAME := "AGENCE DE COOPERATION CULTURELLE ET TECHNIQUE"]

## IFFIM & WORLD BANK INCOME SECTORS
append_1990[CHANNEL == 'WB_IBRD', INCOME_SECTOR := "DEBT"]
append_1990[(DONOR_NAME %in% c("IBRD TRANSFERS", "PRINCIPAL REPAYMENTS") 
             & CHANNEL == "WB_IDA"), INCOME_SECTOR := "DEBT"]

## EC
append_1990[DONOR_NAME %in% c("COMMISSION OF THE EUROPEAN COMMUNITIES (CEC)", 
                              "COMMSSION OF THE EUROPEAN COMMUNITIES (CEC)",
                              "COMMISSION OF THE EUROPEAN COMMUNITIES CEC",
                              "EUROPEAN UNION",
                              "COMMISSION OF THE EUROPEAN COMMUNITIES",
                              "EUROPEAN COMMUNITY",
                              "EUROPEAN ECONOMIC COMMUNITY"),
            DONOR_NAME := "EC"]
append_1990[DONOR_NAME %like% "EUROPEAN COMMISSION", DONOR_NAME := "EC"]

## PAHO
append_1990[DONOR_NAME == "PAN AMERICAN HEALTH AND EDUCATION FOUNDATION", 
            DONOR_NAME := "PAHO"] #old name for PAHO
append_1990[DONOR_NAME %like% "PAN AMERICAN HEALTH ORGANIZATION",
            DONOR_NAME := "PAHO"]

## WHO
append_1990[DONOR_NAME %in% c("WHO (EXTRABUDGETARY FUNDS)",
                              "WHO (REGULAR BUDGET)",
                              "WHO (ROLL BACK MALARIA)"), 
            DONOR_NAME := "WHO"]

## UN Agencies
append_1990[DONOR_NAME %in% c("JOINT UNITED NAITONS PROGRAM ON HIV AND AIDS",
                              "JOINT UNITED NATIONS PROGRAM ON HIV AIDS UNAIDS"),
            DONOR_NAME := "UNAIDS"]

append_1990[DONOR_NAME %in% c("UNFP", "UNFPA PROGRAMME SUPPORT SERVICES"), 
            DONOR_NAME := "UNFPA"]
append_1990[DONOR_NAME %like% "UNITED NATIONS POPULATION FUND",
            DONOR_NAME := "UNFPA"]

append_1990[DONOR_NAME %in% c(
    "UNICEF",
    "UNITED NATIONS CHILDREN'S FUND (UNICEF)"
), DONOR_NAME := "UNICEF"]




## AFDB
append_1990[DONOR_NAME %in% c("AFDB-ABIDJAN", "AFRICAN DEVELOPMENT FUND",
                              "AFRICAN DEVELOPMENT BANK GROUP"), 
            DONOR_NAME := "AFDB"]

## ASDB
append_1990[DONOR_NAME %in% c("ASIAN DEVELOPMENT BANK", "ADB"), DONOR_NAME := "ASDB"]

## GF
append_1990[DONOR_NAME %in% c("BILL & MELINDA GATES FOUNDATION",
                              "BILL AND MELINDA GATES FOUNDATION",
                              "BILL MELINDA GATES FOUNDATION"),
            DONOR_NAME := "GF"] 
append_1990[DONOR_NAME == "GF", `:=`(
    INCOME_SECTOR = "GF"
)]
append_1990[INCOME_SECTOR == "BMGF", INCOME_TYPE := "FOUND"]


## GLOBAL FUND
append_1990[DONOR_NAME %in% c("GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA",
                              "GLOBAL FUND"), 
            DONOR_NAME := "GFATM"]
append_1990[DONOR_NAME %like% "GLOBAL FUND TO FIGHT AIDS",
            DONOR_NAME := "GFATM"]

## World Bank
append_1990[DONOR_NAME %in% c("IBRD", "IBRD TRANSFERS", 
                              "IBRD - ANGOLA", "IBRD- ANGOLA", "IBRD-ANGOLA", 
                              "INTERNATIONAL BANK FOR RECONSTRUCTION AND DEVELOPMENT"),
            DONOR_NAME := "WB_IBRD"]

append_1990[DONOR_NAME == "INTERNATIONAL DEVELOPMENT ASSOCIATION", 
            DONOR_NAME := "WB_IDA"]

## UNITAID
append_1990[DONOR_NAME %in% c("UNITAID - INTERNATIONAL DRUG PURCHASE FACILITY",
                              "INTERNATIONAL DRUG PURCHASE FACILITY (UNITAID)",
                              "UNITAID – INTERNATIONAL DRUG PURCHASE FACILITY"), 
            DONOR_NAME := "UNITAID"]


## US FOUNDATIONS
## This spreadsheet was created for a data request for disaggregated US Foundations 
## donor data. Please add and update with cleaned donor names as needed, it is far from
## a complete list!
clean_names <- read_excel(usfound_names_path)
setnames(clean_names, 'donor_name', 'DONOR_NAME')

append_1990b <- merge(append_1990, clean_names, 
                      by = 'DONOR_NAME', all.x = TRUE)
append_1990b[!is.na(clean_name), DONOR_NAME := clean_name]
append_1990b[, clean_name := NULL]

## clean INCOME_TYPE
append_1990b[INCOME_TYPE %in% c("ADJUSTMENT", "ADJUSTMENTS", "BANK/TRUST FUND", 
                                "REFUNDS", "INTEREST", "COST"), 
             INCOME_TYPE := "OTHER"]
append_1990b[INCOME_TYPE == "IND", INCOME_TYPE := "INDIV"]
append_1990b[(DONOR_NAME == "UNAIDS" & INCOME_TYPE == "EC"), INCOME_TYPE := "UN"]

## merge in recipient country names and regions
append_1990b[, ISO3_RC := trimws(ISO3_RC)]
append_1990b[ISO3_RC %in% c("KOS", "XKX"), ISO3_RC := "KSV"]
append_1990b[ISO3_RC %in% c(NA, 'N/A', 'NA', 'UNSP', ''), ISO3_RC := "QZA"]
## redundant with call on line 292
append_1990b[ISO3_RC %in% c("YUG_FRMR", "YUG"), ISO3_RC := "XYG"]

## replace isocodes of NGOs and Foundations that are based in US or UK with "QZA"
append_1990b[ISO3_RC == 'USA', ISO3_RC := "QZA"] #NGO projects
append_1990b[ISO3_RC %in% c("G", "GLOBAL"), ISO3_RC := "WLD"]
append_1990b[ISO3_RC == "AL_", ISO3_RC := "QZA"] # oddity in Gavi data in FGH2024

## Country codes
cc <- fread(country_codes_path)
cc <- unique(cc[!is.na(iso3) & iso3 != "", 
                .(ISO3_RC = iso3, WB_REGION, WB_REGIONCODE, countryname_ihme)])
cc <- cc[! (ISO3_RC == "MMR" & WB_REGION == "")] # MMR is duplicated

append_1990c <- merge(append_1990b, cc, by = 'ISO3_RC', all.x = TRUE)
setnames(append_1990c, 'countryname_ihme', 'RECIPIENT_COUNTRY')
cat("ISO3_RC without matching country names:\n") 
cat("- (Make sure this is just UNSP, regional codes, and WLD)\n")
print(unique(append_1990c[is.na(RECIPIENT_COUNTRY), ISO3_RC]))


append_1990c[ISO3_RC == "CXR", WB_REGION := "East Asia & Pacific"] #Christmas Island 
cat("ISO3_RC without matching WB region names:\n") 
cat("- (Make sure this is just UNSP, regional codes, and WLD)\n")
print(unique(append_1990c[is.na(WB_REGION), ISO3_RC]))

## Merge with World Bank Income Groups
wb <- fread(wb_ig_path)
append_1990c <- merge(append_1990c, wb[, .(ISO3_RC, YEAR,INC_GROUP)], 
                      by = c("ISO3_RC", "YEAR"), all.x = TRUE)

cat("High income countries receiving DAH include: \n") 
print(unique(append_1990c[INC_GROUP == 'H', RECIPIENT_COUNTRY])) # South Korea
append_1990c[INC_GROUP == 'H', `:=`(ISO3_RC = 'WLD', RECIPIENT_COUNTRY = NA)] # no obs
stopifnot(nrow(append_1990c[INC_GROUP == 'H' & ISO3_RC != 'WLD', ]) == 0) # no obs

## look into this - doesn't seem to be in data anymore
append_1990c <- append_1990c[!(ISO3_RC %in% c("JEY", "GGY")), ] # high income but not in WB inc groups dataset

## Merge in GBD regions
gbd_regions <- get_location_metadata(location_set_id = get_dah_param("location_set_id"),
                                     release_id = get_dah_param("gbd_release_id"))
setnames(gbd_regions, 
         old = c('ihme_loc_id', 'super_region_name', 'region_name'),
         new = c('ISO3_RC', 'gbd_superregion_name', 'gbd_region_name'))
gbd_regions <- gbd_regions[, .(ISO3_RC, gbd_superregion_name, gbd_region_name)]
gbd_regions <- gbd_regions[!ISO3_RC == "NULL", ]

append_1990c <- merge(append_1990c, gbd_regions, by = 'ISO3_RC', all.x = TRUE)

## The 11 small countries not included in GBD data, plus former USSR states
append_1990c[ISO3_RC %in% c("AIA", "MSR", "ANT", "MAF", "TCA", "SHN"), 
             gbd_region_name := "Caribbean"]
append_1990c[ISO3_RC %in% c("CXR", "MNP", "WLF"), 
             gbd_region_name := "Oceania"]
append_1990c[ISO3_RC %in% c("MYT", "GAB"), 
             gbd_region_name := "Western Sub-Saharan Africa"]
append_1990c[ISO3_RC %in% c("KSV", "XYG", "USSR_FRMR"), 
             gbd_region_name := "Eastern Europe"]
append_1990c[ISO3_RC %in%  c("AIA", "MSR", "ANT", "MAF", "TCA", "SHN"),
             gbd_superregion_name := "Latin America and Caribbean"]
append_1990c[ISO3_RC %in% c("CXR", "MNP", "WLF"),
             gbd_superregion_name := "Southeast Asia, East Asia, and Oceania"]
append_1990c[ISO3_RC %in% c("MYT", "GAB"), 
             gbd_superregion_name := "Sub-Saharan Africa"]
append_1990c[ISO3_RC %in% c("KSV", "XYG", "USSR_FRMR"), 
             gbd_superregion_name := "Central Europe, Eastern Europe, and Central Asia"]

## Deflate all observed estimates (should be in nominal dollars)
defl <- fread(deflators_path)
defl_name <- paste0('GDP_deflator_20', dah.roots$abrv_year)
defl_cols <- c('YEAR', defl_name)
dah_est <- merge(append_1990c, defl[, ..defl_cols], by = 'YEAR', all.x = TRUE)

dah_est[, (dah.cols) := lapply(.SD, \(x) x / get(defl_name)), 
        .SDcols = dah.cols]

dah_est[CHANNEL == "WB-IBRD", CHANNEL := "WB_IBRD"]
colnames(dah_est) <- gsub("DAH", dah_yr, colnames(dah_est))


##--------------------------------------------------------------------------------------
print('2. Append predictions estimates------------------')
##--------------------------------------------------------------------------------------
print('Bilaterals...') #prev_report_year-report_yr in December, or only report_yr in January
## check years against the `coverage` dataset created earlier in script
## includes inkind, excludes double counting
bilat_pred <- fread(all_paths$CRS_pred)[YEAR > data_yr, ]
names(bilat_pred) <- standardized_names(bilat_pred)

pr.cols <- grep("pr_", colnames(bilat_pred), value = TRUE)
bilat_pred_cols <- c('CHANNEL', 'YEAR', dah_yr, pr.cols, 'ISO_CODE', 'INCOME_SECTOR')
bilat_pred <- bilat_pred[, ..bilat_pred_cols]
bilat_pred[, REPORTING_AGENCY := "CRS"]

print('EC...') #prev_report_year-report_year in December, or only report_year in January 
## includes inkind, excludes double counting
ec_pred <- fread(all_paths$EC_pred)[YEAR > data_yr & YEAR <= dah.roots$report_year,]
names(ec_pred) <- standardized_names(ec_pred)

outflow.yr <- paste0("OUTFLOW_", dah.roots$abrv_year)
ec_pred[, (dah_yr) := get(outflow.yr)]
ec.pr.cols <- grep("pr_", colnames(ec_pred), value = TRUE)
ec_pred_cols <- c('CHANNEL', 'YEAR', 'ISO_CODE', dah_yr, ec.pr.cols, 'INCOME_SECTOR')
ec_pred <- ec_pred[, ..ec_pred_cols]
ec_pred[, REPORTING_AGENCY := "EC"]
ec_pred[INCOME_SECTOR != "PUBLIC", INCOME_SECTOR := "PUBLIC"]

print('BMGF...') #report_year - includes inkind, excludes double counting
bmgf_pred <- fread(all_paths$BMGF_pred)[YEAR == dah.roots$report_year,]
bmgf_pred <- bmgf_pred[, `:=`(CHANNEL = "BMGF", REPORTING_AGENCY = "BMGF")]
bmgf_pred[, `:=`(INCOME_SECTOR = "PRIVATE", DONOR_NAME = "BMGF", ISO_CODE = NA)]

bmgf.pr.cols <- grep("pr_", colnames(bmgf_pred), value = TRUE)
bmgf_pred_cols <- c('CHANNEL', 'YEAR', 'ISO_CODE', dah_yr, bmgf.pr.cols,
                    'REPORTING_AGENCY', 'INCOME_SECTOR', 'DONOR_NAME')
bmgf_pred <- bmgf_pred[, ..bmgf_pred_cols]
names(bmgf_pred) <- standardized_names(bmgf_pred)

## -------------------------------------------------------------------------------------
un_agency_reshape <- function(df,
                              str_name,
                              dah_colname=dah_yr,
                              years=dah.roots$report_year) {
  print(str_name)
  if (is.character(df)) {
    df <- as.data.table(readstata13::read.dta13(df))
  }
  names(df) <- standardized_names(df)

  df <- df[(YEAR %in% years & CHANNEL == str_name), ]
  df[, `:=`(REPORTING_AGENCY = str_name, DONOR_NAME = NA)]
  if (!('SOURCE_CH' %in% colnames(df))) {
    df[, SOURCE_CH := NA]
  }

  pr.cols <- grep("pr_", colnames(df), value = TRUE)
  pred_cols <- c('CHANNEL', 'YEAR', 'ISO_CODE', dah_colname, pr.cols, 
                 'REPORTING_AGENCY', 'INCOME_SECTOR', 'DONOR_NAME', 'SOURCE_CH')
  df <- df[, c(pred_cols[pred_cols %in% names(df)]), with = FALSE]
  return(df)
}

## Global Fund (no predictions)- Starting in FGH 2015, we had GFATM data up to 
## `dah.roots$report_year, so no predictions dataset is necessary
## Also no need to add PREDS for BIL_CHINA, CEPI, Wellcome Trust, EEA, AsDB, AfDB, IDB,
## UNITAID, GFF

print('UNAIDS, UNFPA, UNICEF, WHO, and PAHO...') #prev_report_year-report_year - no inkind, excludes double counting 
## (assume inkind is included in UN agency total)	
unaids_pred <- un_agency_reshape(all_paths$UNAIDS_pred, 'UNAIDS',
                                 dah_colname = dah_yr,
                                 years = c(dah.roots$report_year))

unfpa_pred <- un_agency_reshape(all_paths$UNFPA_pred, 'UNFPA',
                                dah_colname = dah_yr,
                                years = c(dah.roots$report_year))

unicef_pred <- un_agency_reshape(all_paths$UNICEF_pred, 'UNICEF',
                                 dah_colname = dah_yr,
                                 years = dah.roots$report_year)

who_pred <- un_agency_reshape(all_paths$WHO_pred, 'WHO',
                              dah_colname = dah_yr,
                              years = dah.roots$report_year)

paho_pred <- un_agency_reshape(all_paths$PAHO_pred, 'PAHO',
                               dah_colname = dah_yr,
                               years = dah.roots$report_year)

## -------------------------------------------------------------------------------------
print('US Foundations...') 
## includes inkind, excludes double counting

## 4 prediction years: 1990-1991, report_year-1 - report_year
usfound_pred <- as.data.table(readstata13::read.dta13(all_paths$USFOUND_pred))
usfound_pred <- usfound_pred[(YEAR == dah.roots$prev_report_year |
                                  YEAR == dah.roots$report_year |
                                  YEAR <= 1991), ]
usfound_pred[, `:=`(REPORTING_AGENCY = "US_FOUND", INCOME_SECTOR = "PRIVATE", 
                    ISO_CODE = NA)]

## Create list of column names which contain the desired string
cols.to.rename <- colnames(usfound_pred)[grepl(paste0("_DAH_", dah.roots$abrv_year), colnames(usfound_pred))]
## Paste prefix to beginning of every item in that list
pr.cols <- paste("pr_", cols.to.rename, sep = '')
## Use the two lists to rename columns in data.table
setnames(usfound_pred, cols.to.rename, pr.cols)

pred_cols <- c('CHANNEL', 'YEAR', 'ISO_CODE', dah_yr, pr.cols, 'REPORTING_AGENCY', 'INCOME_SECTOR')
usfound_pred <- usfound_pred[, ..pred_cols]

## -------------------------------------------------------------------------------------
print('Append all prediction files together')
pred_append <- rbind(bilat_pred, ec_pred, 
                     bmgf_pred,
                     # ibrd_pred, ida_pred,
                     # gavi_pred,
                     unaids_pred, unfpa_pred, unicef_pred, who_pred, paho_pred,
                     usfound_pred, fill = TRUE)

stopifnot(pred_append$YEAR %in% c(1990, 1991, dah.roots$report_year - 1, dah.roots$report_year))

rm(bilat_pred, ec_pred,
   bmgf_pred, 
   unaids_pred, 
   unfpa_pred, unicef_pred, who_pred, paho_pred, usfound_pred)

## fix income sector for governments (explore this - this seems weird)
pred_append[INCOME_SECTOR %in% c("AUS", "AUT", "BEL", "BRA", "CAN", "CHE", "DEU", "DNK", "ESP", 
                                 "FIN", "FRA", "GRC", "IND", "IRL", "ITA", "JPN", "KOR", "KWT", 
                                 "LUX", "MCO", "NLD", "NOR", "NZL", "OMN", "PRT", "QAT", "RUS", 
                                 "SAU", "SWE",  "UK", "USA", "ZAF", 'GBR', 'CHN'), 
            INCOME_SECTOR := "PUBLIC"]
pred_append[is.na(INCOME_SECTOR), INCOME_SECTOR := "UNALL"]


## Replace NA values with zeroes
pred_cols <- c(dah_yr, grep("pr_", colnames(pred_append), value = TRUE))
setnafill(pred_append, fill = 0, cols = pred_cols)

## check that the HFAs add up to the total envelope
pred_append[, tot := rowSums(.SD, na.rm = T), 
            .SDcols = grep("pr_", colnames(pred_append), value = TRUE)]
## if sum across HFAs is 0 but the total prediction is != 0, assign to unalloc
pred_append[tot == 0 & get(dah_yr) != 0,
            paste0("pr_unalloc_", dah_yr) := get(dah_yr)]
pred_append[tot == 0 & get(dah_yr) != 0, 
            tot := get(dah_yr)] # for the sake of the below checks

pred_append[, `:=`(diff = get(dah_yr) - tot,
                   pct_diff = (get(dah_yr) - tot)/tot)]
setnafill(pred_append, fill = 0, cols = c("diff", "pct_diff"))
## Make sure the values of diff are all attributable to rounding
print(paste0("Greatest absolute difference is: $", 
             max(round(pred_append$diff, 2))))
print(paste0("Is the greatest percent difference less than 1%? ", 
             max(pred_append$pct_diff) < 0.01))

stopifnot(max(pred_append$pct_diff) < 0.01)

pred_append[, (dah_yr) := tot]
pred_append[, `:=`(diff = NULL, pct_diff = NULL, tot = NULL)]

##  generate prelim_est variable to indicate the DAH is a preliminary estimate
pred_append[, prelim_est := 1]
pred_append[, INKIND := 2]
colnames(pred_append) <- gsub("pr_", "", colnames(pred_append))
stopifnot(
    length(grep("_DAH", names(pred_append))) == length(dah.roots$regular_pa_vars)
)

## append with the estimates data
stopifnot(
    length(grep("_DAH", names(dah_est))) == length(dah.roots$regular_pa_vars)
)

cat(crayon::yellow("  Check that estimates and predictions do not overlap:\n"))

cmp <- merge(
    dah_est[get(dah_yr) != 0,
            .(est_end = max(YEAR)), by = REPORTING_AGENCY],
    pred_append[, .(pred_start = min(YEAR[which(YEAR > 2010)]),
                    pred_end = max(YEAR)),
                by = REPORTING_AGENCY],
    all = TRUE
)
print(tidyr::tibble(cmp), n = Inf)

if (
    nrow(cmp[est_end >= pred_start]) > 0 ||
    nrow(cmp[!is.na(pred_end) & pred_end != dah.roots$report_year]) ||
    nrow(cmp[!is.na(pred_start - est_end) & pred_start - est_end != 1])
) {
    stop("Estimates and predictions do not have proper time coverage")
}


prelim_est <- rbind(dah_est, pred_append, fill = TRUE)
prelim_est[is.na(prelim_est), prelim_est := 0] # prelim_est is a flag column

## Make sure no missing values
cat('Are all INKIND values present?\n')
prelim_est[is.na(INKIND) & abs(get(paste0("oid_covid_DAH_",dah.roots$abrv_year))) > 0,
           INKIND := 0]
prelim_est[is.na(INKIND) & get(dah_yr) == 0, INKIND := 0]

stopifnot(length(prelim_est[is.na(INKIND), INKIND]) == 0)



## --------------------------------------------------------------------------------------
print('3. Format data & Fix Bilateral Transfers to NGOs------------------')
## --------------------------------------------------------------------------------------
prengofix <- copy(prelim_est)
## Distinguish WLD and QZA (WLD = global, QZA = unallocable)
prengofix[is.na(ISO3_RC), ISO3_RC := "QZA"]
prengofix[ISO3_RC == "QZA", `:=`(WB_REGION = "Unallocable", WB_REGIONCODE = "NA")]
prengofix[ISO3_RC == "G", ISO3_RC := "WLD"]
prengofix[ISO3_RC == "WLD", `:=`(WB_REGION = "Global", WB_REGIONCODE = "WLD")]

## Create recipient level variable
prengofix[, LEVEL := "COUNTRY"]
prengofix[, ISO3_RC := fcase(
    ISO3_RC == "S5", "QRA",
    ISO3_RC == "S6", "QTA",
    rep_len(TRUE, .N), ISO3_RC
)]
prengofix[ISO3_RC %in% c("QMA", "QMD", "QME", "QNA", "QNB", "QNC", "QNE", "QRA", "QRB",
                         "QRC", "QRD", "QRE", "QRS", "QSA", "QTA"),
          LEVEL := "REGIONAL"]
prengofix[ISO3_RC %in% c("QZA", "WLD"), LEVEL := "GLOBAL"]
## In Part 4 of compiling we replace LEVEL = "UNALLOCABLE" if ISO3_RC=="QZA"

## Categorize in-kind costs that are to double counted channels as DAH from the 
## reporting agency 
prengofix[(REPORTING_AGENCY == "BMGF" & CHANNEL != "BMGF" & INKIND == 1), 
          CHANNEL := "BMGF"]
prengofix[(REPORTING_AGENCY == "US_FOUND" & CHANNEL != "US_FOUND" & INKIND == 1),
          CHANNEL := "US_FOUND"]
#prengofix[(REPORTING_AGENCY == "WELLCOME" & CHANNEL!="WELLCOME" & INKIND == 1),
#          CHANNEL := "WELLCOME"]

#TODO (some day) - determine why we do this?
## Subtract bilateral transfers to NGOs in the CRS 
## subtract CRS NGO transfers from NGO public income
# new_ngo <- prengofix[REPORTING_AGENCY == "NGO" | (CHANNEL %in% c("NGO", "INTLNGO", "INTL_NGO") &
#                      REPORTING_AGENCY == "CRS"), ]
# ## Error in Stata code dropping all CHANNELs except NGO/INTLNGO - but above is correct
# ## Replicating stata error here
new_ngo <- prengofix[REPORTING_AGENCY %in% c("NGO", "CRS") &
                         CHANNEL %in% c("NGO", "INTLNGO", "INTL_NGO"), ]

dah_cols <- grep(dah_yr, colnames(prengofix), value = TRUE)
new_ngo[REPORTING_AGENCY == 'CRS', 
        (dah_cols) := lapply(.SD, function(x) x * -1), .SDcols = dah_cols]

new_ngo[(DONOR_NAME == "UNITED STATES" & ELIM_CH == 1), INCOME_SECTOR := "PUB_US"]
new_ngo[(DONOR_NAME != "UNITED STATES" & ELIM_CH == 1), INCOME_SECTOR := "PUB_NONUS"]
new_ngo <- new_ngo[, lapply(.SD, sum, na.rm = TRUE),
                   by = .(YEAR, CHANNEL, INCOME_SECTOR, ISO3_RC),
                   .SDcols = dah_cols]

new_ngo[, tmp := rowSums(.SD, na.rm = TRUE),
        .SDcols = grep("_DAH", names(new_ngo), value = TRUE)]
stopifnot( new_ngo[abs(tmp - get(dah_yr)) > 0.1, .N] == 0 )
new_ngo[, tmp := NULL]

##---
## Drop ngo public income from the volag and replace with newly calculated income
prengofix <- prengofix[!(REPORTING_AGENCY == "NGO" &
                             CHANNEL %in% c("NGO", "INTLNGO", "INTL_NGO")), ]
prengofix <- rbind(prengofix, new_ngo, fill = T)

prengofix[(CHANNEL %in% c("NGO", "INTLNGO") & ELIM_CH == 1 & REPORTING_AGENCY == "CRS"), 
          ELIM_CH := 0]
prengofix[is.na(ISO3_RC), ISO3_RC := "QZA"]
prengofix[(CHANNEL %in% c("NGO", "INTLNGO") & is.na(REPORTING_AGENCY)), 
          REPORTING_AGENCY := "NGO"]
prengofix[REPORTING_AGENCY == "NGO", ELIM_CH := 0]
prengofix[(INCOME_SECTOR == "INK" & REPORTING_AGENCY == "NGO"), INKIND := 1]
prengofix[REPORTING_AGENCY == "NGO", 
          `:=`(WB_REGION = "Unallocable", WB_REGIONCODE = "NA", LEVEL = "GLOBAL")]

## Adjust NGO sources for VolAg/Guidestar data
## Funds from bilaterals to NGOs will be added as SOURCE = bilateral country, 
## CHANNEL = NGO or INTLNGO.
## Then funds are subtracted from the total CHANNEL = BIL_countryname observation
## This way, source and channel will each sum correctly and NGO source will be 
## disaggregated properly

## calculate total expenditure from public non-US sources
pred_cols <- grep(dah_yr, colnames(prengofix), value = TRUE)
pub_nonus <- prengofix[INCOME_SECTOR == 'PUB_NONUS', ]
nonus_cols <- c('YEAR', 'INCOME_SECTOR', 'CHANNEL', pred_cols)
pub_nonus <- pub_nonus[, ..nonus_cols]
pub_nonus <- pub_nonus[YEAR <= dah.roots$report_year]
pub_nonus <- dcast.data.table(pub_nonus, YEAR + INCOME_SECTOR ~ CHANNEL, value.var = pred_cols, fun = sum)
pub_nonus[, INCOME_SECTOR := NULL]

## Disaggregate NGO DAH by source based on relative porportions of contributions 
## by donors
## WHY is this line commented out?
##pub_nonus <- pub_nonus[CHANNEL %like% "BIL" & CHANNEL != "BIL_USA" & CHANNEL != "BIL_CHN", ]
nonus_ngos <- prengofix[(REPORTING_AGENCY == "CRS" & CHANNEL %like% "NGO" & 
                           ISO_CODE != "USA"), ]
nonus_ngos <- nonus_ngos[, lapply(.SD, sum), by = .(YEAR, ISO_CODE), 
                         .SDcols = pred_cols]

nonus_ngos[, total_nonus_bilateral := sum(get(dah_yr)), .(YEAR)]		
nonus_ngos[, frct_nonus_bilateral := get(dah_yr) / total_nonus_bilateral]

nonus_ngos <- merge(nonus_ngos, pub_nonus, by = "YEAR", all = T)
setnames(nonus_ngos, paste0(dah_yr, "_INTLNGO"), paste0("all_", dah_yr, "_INTLNGO"))
setnames(nonus_ngos, paste0(dah_yr, "_NGO"), paste0("all_", dah_yr, "_NGO"))
setnames(nonus_ngos, dah_yr, paste0("all_", dah_yr))


pas <- gsub(paste0("_", dah_yr), "", grep("_DAH_", names(prengofix), value = TRUE))
pas <- c("all", pas)

for (hfa in pas) {
  nonus_ngos[, eval(paste0('ngo_', hfa)) := 
               frct_nonus_bilateral * get(paste0(hfa, '_', dah_yr, '_NGO'))]
  nonus_ngos[, eval(paste0('ingo_', hfa)) := 
               frct_nonus_bilateral * get(paste0(hfa, '_', dah_yr, '_INTLNGO'))]
}

ngo.cols <- c('YEAR', "ISO_CODE", grep("^ngo_", colnames(nonus_ngos), value = TRUE))
ingo.cols <- c('YEAR', "ISO_CODE", grep("^ingo_", colnames(nonus_ngos), value = TRUE))
nonus_ngos_ngo <- nonus_ngos[, ..ngo.cols]
nonus_ngos_ingo <- nonus_ngos[, ..ingo.cols]
names(nonus_ngos_ngo) <- gsub("^ngo_",  '', names(nonus_ngos_ngo))
nonus_ngos_ngo[, CHANNEL := "NGO"]
names(nonus_ngos_ingo) <- gsub("^ingo_",  '', names(nonus_ngos_ingo))
nonus_ngos_ingo[, CHANNEL := "INTLNGO"]
nonus_ngos <- rbind(nonus_ngos_ngo, nonus_ngos_ingo)

setnafill(nonus_ngos, fill = 0, cols = pas)
setnames(nonus_ngos, pas, paste0(pas, "_", dah_yr))
setnames(nonus_ngos, paste0("all_", dah_yr), dah_yr)

## Format data
nonus_ngos[, `:=`(INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", 
                  REPORTING_AGENCY = "NGO", ISO3_RC = "QZA", ELIM_CH = 0, 
                  INKIND = 0, WB_REGION = "Unallocable", WB_REGIONCODE = "NA")]

nobolafixed <- rbind(prengofix, nonus_ngos, fill = T)
nobolafixed <- nobolafixed[INCOME_SECTOR != "PUB_NONUS", ]

## Rename NGOS from CRS
nobolafixed[(CHANNEL == "DONORCOUNTRYNGOS" & ISO_CODE == "USA"), CHANNEL := "NGO"]
nobolafixed[(CHANNEL == "DONORCOUNTRYNGOS" & ISO_CODE != "USA"), CHANNEL := "INTLNGO"]
nobolafixed[CHANNEL == "INTERNATIONALNGOS", CHANNEL := "INTLNGO"]
nobolafixed[(REPORTING_AGENCY == "NGO" & is.na(INKIND) & INCOME_SECTOR != "INK"), INKIND := 0]


## these should be dropped by the US_FOUND channel because Gates has its own
##   pipeline
nobolafixed[CHANNEL == "US_FOUND" & INCOME_SECTOR == "BMGF", ELIM_DONOR := 1]

#
# test total
dah.cols <- grep(dah_yr, colnames(nobolafixed), value = TRUE)
pa.cols <- dah.cols[dah.cols != dah_yr]
nobolafixed[, tot := rowSums(.SD, na.rm = TRUE),
            .SDcols = pa.cols]
nobolafixed[, diff := abs(tot - get(dah_yr))]

if (nrow(nobolafixed[diff > 100, ]) > 0) {
    warning("NGO DAH disaggregation error: PAs do not sum to total DAH, please fix.")
}
nobolafixed[, c("tot", "diff") := NULL]

## -------------------------------------------------------------------------------------
print('4. Location updates------------------')
## -------------------------------------------------------------------------------------
## Make sure all country level obs have gbd and wb region info
## Manual GBD region fixes
iso <- copy(nobolafixed)
stopifnot( iso[is.na(gbd_region_name) & LEVEL == "COUNTRY", .N] == 0 )

iso[(is.na(gbd_region_name) & LEVEL == "COUNTRY"),  ISO3_RC := "QZA"]
iso[ISO3_RC == "WLD", RECIPIENT_COUNTRY := "GLOBAL"]
iso <- iso[!(ISO3_RC %in% c('SSD', 'SYR') & WB_REGIONCODE == 'NA')] ## COVID bug, please fix properly
iso <- unique(iso[, .(gbd_region_name, gbd_superregion_name, WB_REGION, WB_REGIONCODE,
                      RECIPIENT_COUNTRY, ISO3_RC)])	
iso <- iso[RECIPIENT_COUNTRY != ""] ## drops regional isocodes
iso <- iso[ISO3_RC != "WLD"]
iso <- rbind(iso,
             data.table(ISO3_RC = "WLD",
                        RECIPIENT_COUNTRY = "GLOBAL", 
                        WB_REGION = "Global", WB_REGIONCODE = "WLD"),
             fill = TRUE)
## probably not necessary with new location hierarchy
##creates "filled" loation data and merges it back onto dataframe, i don't think
## resulting in any changes. overly complicated
dir.create(get_path("compiling", "fin", "region_data"),
            showWarnings = FALSE, recursive = TRUE)
fwrite(iso, paste0(FIN, "region_data/iso_data_region_test.csv"))

nobolafixed[, c('RECIPIENT_COUNTRY', 'gbd_region_name', 'gbd_superregion_name',
                'WB_REGION', 'WB_REGIONCODE') := NULL]
nobolafixed <- merge(nobolafixed, iso, by = 'ISO3_RC', all.x = T)

## Fix regional projects (use country list from oecd)
## See "Compiling" Hub page for documentation on this file.
oecdregion <- fread(oecd_countries_path)
nobolafixed <- merge(nobolafixed, 
                     oecdregion[, .(RECIPIENT_COUNTRY, OECD_REGION, ISO3_region1,
                                    ISO3_region2, ISO3_subregion, ISO3_region4)],
                     by = 'RECIPIENT_COUNTRY', all.x = T)

nobolafixed[, dummy_N := 1]
nobolafixed[, n_region1 := sum(dummy_N), .(ISO3_region1, YEAR)]
nobolafixed[, n_region2 := sum(dummy_N), .(ISO3_region2, YEAR)]
nobolafixed[, n_subregion := sum(dummy_N), .(ISO3_subregion, YEAR)]
nobolafixed[, n_region4 := sum(dummy_N), .(ISO3_region4, YEAR)]
nobolafixed[, dummy_N := NULL]

## They are preliminary estimations, we don't tag DAHG DAHNG for preliminary estimation,
## put 0 here to get rid of missing values
nobolafixed[is.na(GOV), GOV := 0] 
nobolafixed[REPORTING_AGENCY == "NGO", GOV := 2] 
nobolafixed <- nobolafixed[!(CHANNEL == "" & get(dah_yr) == 0), ]

nobolafixed[INCOME_SECTOR=="" & REPORTING_AGENCY=='BMGF',
            `:=`(INCOME_SECTOR='BMGF', INCOME_TYPE='FOUND')]
nobolafixed[CHANNEL %like% "BIL", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL"
)]
nobolafixed[INCOME_SECTOR == "" & DONOR_NAME %like% "UNITED STATES", `:=`(
    ISO_CODE = "USA", DONOR_COUNTRY = "UNITED STATES",
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL"
)]
nobolafixed[INCOME_SECTOR == "Corporation", INCOME_TYPE := "OTHER"]
nobolafixed[INCOME_SECTOR == "Corporation" & INKIND == 0,
            INCOME_SECTOR := "PRIVATE"]
nobolafixed[INCOME_SECTOR == "Corporation" & INKIND == 1,
            INCOME_SECTOR := "PRIVINK"]
nobolafixed[INCOME_SECTOR == "PRIVATE_INK",
            INCOME_SECTOR := "PRIVINK"]
nobolafixed[INCOME_SECTOR == "",`:=`(INCOME_SECTOR='PRIVATE', INCOME_TYPE='OTHER')]
nobolafixed[INCOME_SECTOR == "PRIVATE",`:=` (
    INCOME_TYPE = "OTHER",
    DONOR_NAME = "",
    DONOR_COUNTRY = "")]
nobolafixed[INCOME_SECTOR == "UN", `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN"
)]
nobolafixed[INCOME_SECTOR == "WHO", `:=`(
    INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN", DONOR_NAME = "WHO"
)]

nobolafixed[DONOR_COUNTRY == "UNITED STATS", DONOR_COUNTRY := "UNITED STATES"]


#
# ensure gdp deflator is consistent
defl <- fread(deflators_path)
defl_name <- paste0('GDP_deflator_20', dah.roots$abrv_year)
nobolafixed[, eval(defl_name) := NULL]
nobolafixed <- merge(nobolafixed,
                     defl[, c('YEAR', defl_name), with = FALSE],
                     by = 'YEAR', all.x = TRUE)

setnafill(nobolafixed, fill = 0, cols = c("ELIM_CH", "ELIM_DONOR"))

#
# Again, need to ensure all PAs sum to total DAH
#
pa_cols <- grep("_DAH_", names(nobolafixed), value = TRUE)
nobolafixed[, dah_sum := rowSums(.SD, na.rm = TRUE), .SDcols = pa_cols]

if( nobolafixed[abs(dah_sum - get(dah_yr)) > 0.1, .N] > 0 )
    warning("PA sums do not match total DAH at end of script 1, check above steps")

## normalize by sum anyways to reduce small differences
nobolafixed[, (pa_cols) := lapply(.SD, function(x) {
    get(dah_yr) * x / dah_sum
}), .SDcols = pa_cols]

## final test
nobolafixed[, dah_sum := rowSums(.SD, na.rm = TRUE), .SDcols = pa_cols]
if (nobolafixed[abs(dah_sum - get(dah_yr)) > 1e-3, .N] > 0)
    stop("PA sums do not match total DAH")
nobolafixed[, dah_sum := NULL]


print(paste0('5. Write DAH_Compiling file to ', INT, 'region_data----------------'))

save_dataset(nobolafixed,
             paste0("DAH_compiling_1990_", dah.roots$report_year),
             channel = "compiling",
             stage = "int",
             folder = "region_data")


print('End of Script')
## Stata code is 853 lines, try to beat that! 1000 at longest
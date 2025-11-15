#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize China ADBPDB
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)


#
# Load outputs from normal pipeline
#
adb <- fread(get_path("china", "int",
                      "BIL_CHINA_ADBPDB_1990_[report_year]_HFA.csv"))
adb[, ISO3_RC := "QZA"]


adb[, tmp := rowSums(.SD, na.rm = TRUE), .SDcols = grep("_DAH", names(adb), value = TRUE)]
stopifnot(adb[abs(tmp - DAH) > 1e-3, .N] == 0)
adb[, tmp := NULL]


#
# Load outputs from covid pipeline
#
covid <- fread(get_path("china", "fin", "COVID_prepped.csv"))

covid[, c("ISO3", "DONOR_COUNTRY", "INCOME_TYPE", "RECIPIENT_COUNTRY") := NULL]
setnames(covid, "AMOUNT", "oid_covid_DAH")
covid[, `:=`(
    inkind = 0,
    ISO_CODE = "CHN",
    CHANNEL = "BIL_CHN",
    DAH = oid_covid_DAH
)]
covid[, DONOR_NAME := fcase(
    DONOR_NAME == "China, Government of - National Health Commission",
    "CHN_NHC",
    DONOR_NAME == "China, Government of - China International Development Cooperation Agency",
    "CHN_CIDCA",
    DONOR_NAME == "China, Government of",
    "CHN_MOFCOM", # assume minisitry of finance and commerce for general China
    rep_len(TRUE, .N), "CHN_UNALLOC"
)]

# add inkind
inkind <- fread(get_path('CHINA', 'int',
                         'CHINA_INKIND_ESTIMATE_1990_[report_year].csv'))
covid <- merge(covid, inkind, by = "YEAR", all.x = TRUE)
covid[, inkind := 0]
## create inkind rows
covid_inkind <- copy(covid)
covid_inkind[, `:=`(oid_covid_DAH = oid_covid_DAH * inkind_ratio,
                    inkind = 1)]
## append inkind and non-inkind data
covid <- rbind(covid, covid_inkind)
covid[, DAH := oid_covid_DAH]


# combine non-covid with covid
adb <- rbind(adb, covid, fill = TRUE)

dah_cols <- grep("DAH", names(adb), value = TRUE)
setnafill(adb, fill = 0, cols = dah_cols)


adb[, tmp := rowSums(.SD, na.rm = TRUE),
    .SDcols = grep("_DAH", names(adb), value = TRUE)]
stopifnot(adb[abs(tmp - DAH) > 1e-3, .N] == 0)
adb[, tmp := NULL]
adb[, inkind_ratio := NULL]



setcolorder(adb,
            c("YEAR", "ISO_CODE", "INCOME_SECTOR", "DONOR_NAME", "CHANNEL",
              "ISO3_RC", "inkind", dah_cols))

save_dataset(adb, "BIL_CHINA_ADBPDB_1990_[report_year]",
             channel = "china",
             stage = "fin")


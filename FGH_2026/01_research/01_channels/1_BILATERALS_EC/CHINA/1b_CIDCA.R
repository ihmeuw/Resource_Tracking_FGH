#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Format CHINA CIDCA Data
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

# Filepath prep
DEFL <- get_path("meta", "defl")
XRATES <- get_path("meta", "rates")
#----------------------------# ####


cat('\n\n')
cat(green(' ###########################################\n'))
cat(green(' #### BEGIN CHINA CIDCA DATA FORMATTING ####\n'))
cat(green(' ###########################################\n\n'))


cat('  Read in CIDCA Data\n')
#----# Read CIDCA data #----# ####
# Import CIDCA data
# Keep 2018 as start year.  Department was established in 2018 to support MOFCOM, so 2018 is the correct year
cidca <- setDT(
    read_excel(get_path('CHINA', 'raw', "CHINA_CIDCA_ESTIMATE_2018_[report_year].xlsx"))
    )[, -c("Note", "Document_Publish_Date")]

#---------------------------# ####

cat('  Scale disbursement\n')
#----# Scale Disbursement #----# ####
# Scale down disbursement
# in 2021 it was suggested to use MOFCOM fraction values as MOFCOM is the larger entity
hospital_pre_2013 <- 80/580
hospital_post_2013 <- 58/423


# Replace CIDCA_DISB_ODA_MLN_CNY with calculated proportion
cidca[, disb_proportion := ifelse(YEAR < 2013,
                                  CIDCA_DISB_ODA_MLN_CNY * hospital_pre_2013,
                                  CIDCA_DISB_ODA_MLN_CNY * hospital_post_2013)]
cidca[, CIDCA_DISB_ODA_MLN_CNY := NULL]
setnames(cidca, "disb_proportion", "CIDCA_DISB_ODA_MLN_CNY")

# Generate the disbursement fraction
cidca[, BUDGET_FRCT := CIDCA_DISB_ODA_MLN_CNY / CIDCA_BUDGET_ODA_MLN_CNY]

# Generate weighted fraction with three-year weighted average
cidca[, shift1 := data.table::shift(BUDGET_FRCT, 0L, type = "lag")]
cidca[, shift2 := data.table::shift(BUDGET_FRCT, 1L, type = "lag")]
cidca[, shift3 := data.table::shift(BUDGET_FRCT, 2L, type = "lag")]
cidca[, WGT_AVG_FRCT := rowSums(.SD, na.rm = T)/3, .SDcols = c("shift1","shift2","shift3")]
cidca[,`:=` (shift1=NULL, shift2=NULL, shift3=NULL)]
# Replace report_yr budget number with predicted weighted disbursement
cidca[is.na(CIDCA_DISB_ODA_MLN_CNY), CIDCA_DISB_ODA_MLN_CNY := CIDCA_BUDGET_ODA_MLN_CNY * WGT_AVG_FRCT]
#------------------------------# ####

cat('  Adjust Inkind\n')
#----# Inkind Adjustment #----# ####
inkind <- fread(get_path('CHINA', 'int', 'CHINA_INKIND_ESTIMATE_1990_[report_year].csv'))
# Inkind adjustment
cidca <- merge(cidca, inkind, by="YEAR", all.x=TRUE)
cidca[, inkind := 0]

cidca_inkind <- copy(cidca)
cidca_inkind[, `:=`(CIDCA_DISB_ODA_MLN_CNY = CIDCA_DISB_ODA_MLN_CNY * inkind_ratio,
                    inkind = 1)]

# Append inkind and non-inkind data
cidca <- setDT(rbind(cidca, cidca_inkind))
#-----------------------------# ####

cat('  Merge exchange rates and deflate\n')
#----# Merge exchange rates & deflate #----# ####
exch_rates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"))
exch_rates <- exch_rates[LOCATION == "CHN" & TIME >= 1990, .(TIME, Value)]
setnames(exch_rates, c("TIME", "Value"), c("YEAR", "XRATE"))

deflators <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
deflators <- deflators[, c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)),
                       with = FALSE]

# Merge in exchange rates & currency convert
cidca <- merge(cidca, exch_rates, by="YEAR", all.x = TRUE)
setnames(cidca, "CIDCA_DISB_ODA_MLN_CNY", "CIDCA_DAH")
cidca[, CIDCA_DAH_USD := CIDCA_DAH / XRATE]

cidca <- merge(cidca, deflators, by="YEAR", all.x = TRUE)
cidca[, paste0("CIDCA_DAH_", dah.roots$abrv_year) := CIDCA_DAH_USD / get(paste0("GDP_deflator_", dah.roots$report_year))]
#------------------------------------------# ####

cat('  Save Stage 1b Dataset\n')
#----# Save Stage 1b #----# ####
save_dataset(cidca, paste0('1b_CIDCA'), 'CHINA', 'int')
#-------------------------# ####
#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Merge China Agencies and output datasets
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

#----------------------------# ####


cat('\n\n')
cat(green(' ###################################\n'))
cat(green(' #### BEGIN CHINA DATA ASSEMBLY ####\n'))
cat(green(' ###################################\n\n'))


cat('  Read in Stage 2 datasets\n')
#----# Read in datasets #----# ####
CHINA_MOFCOM <- fread(get_path('CHINA', 'int', '2a_MOFCOM.csv'))
CHINA_CIDCA <- fread(get_path('CHINA', 'int', '2b_CIDCA.csv'))
CHINA_NHC <- fread(get_path('CHINA', 'int', '2c_NHC.csv'))
CHINA_MOE <- fread(get_path('CHINA', 'int', '2d_MOE.csv'))
CHINA_EXIM <- fread(get_path('CHINA', 'int', '2e_EXIM_BANK.csv'))
#----------------------------# ####

cat('  Adjust MOFCOM\n')
#----# Adjust MOFCOM #----# ####
# MOFCOM
CHINA_MOFCOM[, `:=`(CHANNEL = "BIL_CHN",
                    DONOR_NAME = "CHN_MOFCOM")]
names(CHINA_MOFCOM)[names(CHINA_MOFCOM) == "E_MOFCOM_DAH"] <- "DAH_CNY"
names(CHINA_MOFCOM)[names(CHINA_MOFCOM) == "swap_hss_other_DAH"] <- "swap_hss_other_DAH_CNY"
names(CHINA_MOFCOM)[names(CHINA_MOFCOM) == "E_MOFCOM_DAH_USD"] <- "DAH"
names(CHINA_MOFCOM)[names(CHINA_MOFCOM) == "swap_hss_other_DAH_USD"] <- "swap_hss_other_DAH"
names(CHINA_MOFCOM)[names(CHINA_MOFCOM) == paste0("E_MOFCOM_DAH_", dah.roots$abrv_year)] <- paste0("DAH_", dah.roots$abrv_year)
toKeep <- c("YEAR", "CHANNEL", "DONOR_NAME", "DAH_CNY", "DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_other_DAH", paste0("swap_hss_other_DAH_", dah.roots$abrv_year), "inkind", "inkind_ratio", paste0("GDP_deflator_", dah.roots$report_year), "XRATE" )
CHINA_MOFCOM <- CHINA_MOFCOM[, toKeep, with = F]
#-------------------------# ####

cat('  Adjust CIDCA\n')
#----# Adjust CIDCA #----# ####
# CIDCA
CHINA_CIDCA[, `:=`(CHANNEL = "BIL_CHN",
                   DONOR_NAME = "CHN_CIDCA")]
names(CHINA_CIDCA)[names(CHINA_CIDCA) == "CIDCA_DAH"] <- "DAH_CNY"
names(CHINA_CIDCA)[names(CHINA_CIDCA) == "CIDCA_DAH_USD"] <- "DAH"
names(CHINA_CIDCA)[names(CHINA_CIDCA) == paste0("CIDCA_DAH_", dah.roots$abrv_year)] <- paste0("DAH_", dah.roots$abrv_year)
toKeep <- c("YEAR", "CHANNEL", "DONOR_NAME", "DAH_CNY", "DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_other_DAH", paste0("swap_hss_other_DAH_", dah.roots$abrv_year), 
           "inkind", "inkind_ratio", paste0("GDP_deflator_", dah.roots$report_year), "XRATE")
CHINA_CIDCA <- CHINA_CIDCA[, toKeep, with = F]
#------------------------# ####

cat('  Adjust NHC\n')
#----# Adjust NHC #----# ####
# NHC
CHINA_NHC[, `:=`(CHANNEL = "BIL_CHN",
                 DONOR_NAME = "CHN_NHC",
                 swap_hss_other_DAH = NA)]
CHINA_NHC[, eval(paste0("swap_hss_other_DAH_", dah.roots$abrv_year)) := NA]
names(CHINA_NHC)[names(CHINA_NHC) == "E_NHC_DAH"] <- "DAH_CNY"
names(CHINA_NHC)[names(CHINA_NHC) == "swap_hss_hrh_DAH"] <- "swap_hss_hrh_DAH_CNY"
names(CHINA_NHC)[names(CHINA_NHC) == "other_DAH"] <- "other_DAH_CNY"
names(CHINA_NHC)[names(CHINA_NHC) == "E_NHC_DAH_USD"] <- "DAH"
names(CHINA_NHC)[names(CHINA_NHC) == "swap_hss_hrh_DAH_USD"] <- "swap_hss_hrh_DAH"
names(CHINA_NHC)[names(CHINA_NHC) == "other_DAH_USD"] <- "other_DAH"
names(CHINA_NHC)[names(CHINA_NHC) == paste0("E_NHC_DAH_", dah.roots$abrv_year)] <- paste0("DAH_", dah.roots$abrv_year)
toKeep <- c("YEAR", "CHANNEL", "DONOR_NAME", "DAH_CNY", "DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_hrh_DAH", paste0("swap_hss_hrh_DAH_", dah.roots$abrv_year), "swap_hss_other_DAH", paste0("swap_hss_other_DAH_", dah.roots$abrv_year), "other_DAH", paste0("other_DAH_", dah.roots$abrv_year), "inkind", "inkind_ratio", paste0("GDP_deflator_", dah.roots$report_year), "XRATE" )
CHINA_NHC <- CHINA_NHC[, toKeep, with = F]
#----------------------# ####

cat('  Adjust MOE\n')
#----# Adjust MOE #----# ####
# MOE
CHINA_MOE[, `:=`(CHANNEL = "BIL_CHN",
                 DONOR_NAME = "CHN_MOE")]
names(CHINA_MOE)[names(CHINA_MOE) == "E_MOE_DAH"] <- "DAH_CNY"
names(CHINA_MOE)[names(CHINA_MOE) == "swap_hss_hrh_DAH"] <- "swap_hss_hrh_DAH_CNY"
names(CHINA_MOE)[names(CHINA_MOE) == "E_MOE_DAH_USD"] <- "DAH"
names(CHINA_MOE)[names(CHINA_MOE) == "swap_hss_hrh_DAH_USD"] <- "swap_hss_hrh_DAH"
names(CHINA_MOE)[names(CHINA_MOE) == paste0("E_MOE_DAH_", dah.roots$abrv_year)] <- paste0("DAH_", dah.roots$abrv_year)
toKeep <- c("YEAR", "CHANNEL", "DONOR_NAME", "DAH_CNY", "DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_hrh_DAH", paste0("swap_hss_hrh_DAH_", dah.roots$abrv_year), "inkind", "inkind_ratio", paste0("GDP_deflator_", dah.roots$report_year), "XRATE")
CHINA_MOE <- CHINA_MOE[, toKeep, with = F]
#----------------------# ####

cat('  Adjust EXIM BANK\n')
#----# Adjust EXIM Bank #----# ####
#EXIM
CHINA_EXIM[, `:=`(CHANNEL = "BIL_CHN",
                  DONOR_NAME = "CHN_EXIM")]
names(CHINA_EXIM)[names(CHINA_EXIM) == "E_EXIM_DAH"] <- "DAH_CNY"
names(CHINA_EXIM)[names(CHINA_EXIM) == "swap_hss_other_DAH"] <- "swap_hss_other_DAH_CNY"
names(CHINA_EXIM)[names(CHINA_EXIM) == "E_EXIM_DAH_USD"] <- "DAH"
names(CHINA_EXIM)[names(CHINA_EXIM) == "swap_hss_other_DAH_USD"] <- "swap_hss_other_DAH"
names(CHINA_EXIM)[names(CHINA_EXIM) == paste0("E_EXIM_DAH_", dah.roots$abrv_year)] <- paste0("DAH_", dah.roots$abrv_year)
toKeep <- c("YEAR", "CHANNEL", "DONOR_NAME", "DAH_CNY", "DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_other_DAH", paste0("swap_hss_other_DAH_", dah.roots$abrv_year), "inkind", "inkind_ratio", paste0("GDP_deflator_", dah.roots$report_year), "XRATE")
CHINA_EXIM <- CHINA_EXIM[, toKeep, with = F]
#----------------------------# ####

cat('  Compile Datasets\n')
#----# Compile Datasets #----# ####
COMPILED <- rbind(
    CHINA_NHC, CHINA_MOFCOM, CHINA_MOE, CHINA_EXIM, CHINA_CIDCA,
    fill = TRUE
)

COMPILED <- COMPILED[, -c("DAH_CNY"), with = F]

cols <- c("DAH", paste0("DAH_", dah.roots$abrv_year), "swap_hss_hrh_DAH", paste0("swap_hss_hrh_DAH_", dah.roots$abrv_year), "swap_hss_other_DAH", paste0("swap_hss_other_DAH_", dah.roots$abrv_year), 
          "other_DAH", paste0("other_DAH_", dah.roots$abrv_year))
COMPILED[, (cols) := lapply(.SD, function(x) x * 1e6), .SDcols = cols]

COMPILED[, `:=`(ISO_CODE = "CHN",
                INCOME_SECTOR = "PUBLIC")]

COMPILED[is.na(inkind), inkind := 0]
#----------------------------# ####

# drop deflated columns
COMPILED[, grep("DAH_\\d{2}$", names(COMPILED), value = TRUE) := NULL]
COMPILED[, grep("GDP_deflator_\\d{4}$", names(COMPILED), value = TRUE) := NULL]
COMPILED[, c("XRATE") := NULL]

hfas <- grep("_DAH$", names(COMPILED), value = TRUE)
COMPILED <- COMPILED[, lapply(.SD, sum, na.rm = TRUE),
                     .SDcols = c("DAH", hfas),
                     by = .(YEAR, ISO_CODE, INCOME_SECTOR, CHANNEL, DONOR_NAME,
                            inkind, inkind_ratio)]


# ensure PAs sum to total DAH
COMPILED[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = hfas]
COMPILED[tot == 0 & DAH > 0, unalloc_DAH := tot] # ideally no rows like this, check
COMPILED[, (hfas) := lapply(.SD, \(x) DAH * (x / tot)), .SDcols = hfas]
COMPILED[, tot := rowSums(.SD, na.rm = TRUE), .SDcols = hfas]
# check
stopifnot(nrow(COMPILED[abs(tot - DAH) > 100]) == 0)
COMPILED[, tot := NULL]

setnafill(COMPILED, fill = 0, cols = grep("DAH", names(COMPILED), value = TRUE))


cat('  Save ADBPDB Dataset\n')
#----# Save ADB PDB Dataset #----# ####
save_dataset(COMPILED,
             'BIL_CHINA_ADBPDB_1990_[report_year]_preCOVID',
             'CHINA', 'int')
#--------------------------------# ####
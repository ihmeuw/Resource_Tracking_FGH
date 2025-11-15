#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Calculate INKIND
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)

## Local CONSTANTS
COUNTRY_FEATURES <- get_path("meta", "locs")
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ################################\n"))
cat(green(" #### GFATM INKIND CALCULATE ####\n"))
cat(green(" ################################\n\n"))

#### #----#                     INCOME_ALL data                      #----# ####
cat("  Prep INCOME_ALL data\n")
## Read in data from previous script
donor_data <- fread(paste0(get_path("GFATM", "raw"),
                           "P_GFATM_INCOME_ALL.csv"))
# Fill NAs
donor_data[is.na(INCOME_REG_PAID), INCOME_REG_PAID := 0]
donor_data[is.na(INCOME_REG_PLEDGED), INCOME_REG_PLEDGED := 0]
donor_data <- donor_data[!(INCOME_REG_PAID == 0 & INCOME_REG_PLEDGED == 0),]
# INCOME_REG and INCOME_ALL are now simply INCOME_REG without the NAs
donor_data[, INCOME_REG := INCOME_REG_PAID]
donor_data[, INCOME_ALL := INCOME_REG]
# Calculate total annual income for each YEAR
donor_data[, INCOME_TOTAL_YR := sum(INCOME_ALL, na.rm=T), by="YEAR"]
# Calculate the proportion of each line item income to the total for that year
# across ALL locs, sources, etc.
donor_data[, INCOME_ALL_SHARE := INCOME_ALL / INCOME_TOTAL_YR]
# Take sum of share of annual income by year, sectorm income type, 
# donor name/country/iso, channel
donor_data <- collapse(donor_data, "sum", 
                       c("YEAR", "INCOME_SECTOR", "INCOME_TYPE", "DONOR_NAME",
                         "DONOR_COUNTRY", "ISO3", "CHANNEL"),
                       "INCOME_ALL_SHARE")
donor_data[, n := seq_len(.N), by = "YEAR"]

# Save number of donors data
annual_donors <- copy(donor_data)
annual_donors[, N := .N, by = "YEAR"]
annual_donors <- dplyr::distinct(annual_donors[, c("YEAR", "N")])
#------------------------------------------------------------------------------#

#### #----#              Duplicate observations N times              #----# ####
cat("  Duplicate observations N times\n")
post_split <- fread(paste0(get_path("GFATM", "int"),
                           "post_split.csv"))
post_split <- merge(post_split, annual_donors, by="YEAR", all=T)
expand <- data.frame()
for (i in min(annual_donors$N):max(annual_donors$N)) {
  t <- post_split[N == i, ]
  expand <- setDT(rbind(expand, lapply(t, rep, i)))
  if (i == min(annual_donors$N)) {
    expand <- setDT(rbind(expand, lapply(t, rep, i)))
  }
  rm(t)
}
# Multiply DAH by donor fraction
expand[, dummy := 1]
expand[, n := 1:sum(dummy), by=c("YEAR", "ISO3_RC", "RECIPIENT_AGENCY",
                                 "ELIM_CH", "gov")]
expand[, dummy := NULL]
expand <- merge(expand, donor_data, by=c("YEAR", "n"), all = T)
to_calc <- c(names(expand)[names(expand) %like% "_DAH"], "DAH")
for (col in to_calc) {
  expand[, eval(col) := get(col) * INCOME_ALL_SHARE]
}
expand[, `:=`(n = NULL, N = NULL)]
rm(donor_data, annual_donors, post_split, col, i, to_calc)
#------------------------------------------------------------------------------#

#### #----#                 Bring in income sectors                  #----# ####
cat("  Bring in income sectors\n")
isos <- setDT(read.dta13(paste0(
  COUNTRY_FEATURES, "wb_historical_incgrps.dta"))
  )[, c("ISO3_RC", "YEAR", "INC_GROUP")]

myr <- max(isos$YEAR)
if (myr < dah.roots$report_year) {
    warning("ISO Code metadata needs updatings. Reusing latest income groups.")
    for (yr in (myr+1):(dah.roots$report_year)) {
        iso_t <- isos[YEAR == myr]
        iso_t[, YEAR := yr]
        isos <- rbind(isos, iso_t)
    }
}

isos[, u_m := 2]
expand[, m_m := 1]
expand <- merge(expand, isos, by=c("ISO3_RC", "YEAR"), all=T)
expand[, merge := rowSums(expand[, c("u_m", "m_m")], na.rm=T)]
expand <- expand[merge %in% c(1,3) & INC_GROUP != "H", 
                 !c("u_m", "m_m", "merge")]
rm(isos)
#------------------------------------------------------------------------------#

#### #----#                     Calculate inkind                     #----# ####
cat("  Calculate inkind\n")
# Read in inkind & merge
inkind <- setDT(fread(paste0(get_path("GFATM", "raw"), "P_GFATM_INKIND.csv")))
inkind[, u_m := 2]
expand[, m_m := 1]
inkind_final <- merge(expand, inkind, by="YEAR", all=T)
inkind[, u_m := NULL]
expand[, m_m := NULL]
inkind_final[, merge := rowSums(inkind_final[, c("u_m", "m_m")], na.rm=T)]
inkind_final <- inkind_final[merge == 3, !c("u_m", "m_m", "merge")]
inkind_final[, `:=`(CHANNEL.x = NULL, CHANNEL.y = NULL, CHANNEL = "GFATM")]
# Calculate DAH
to_calc <- c(names(inkind_final)[names(inkind_final) %like% "_DAH"], "DAH")
for (col in to_calc) {
  inkind_final[!is.na(INKIND_RATIO), eval(col) := get(col) * INKIND_RATIO]
}
inkind_final[, `:=`(EXP_GRANTS = NULL, EXP_OPER = NULL, SOURCE = NULL, 
                    INKIND = 1, INCOME_ALL_SHARE = 1)]
# Append & rename
adbpdb <- rbind(expand, inkind_final, fill=T)
adbpdb[is.na(INKIND), INKIND := 0]
adbpdb[INKIND == 1, ELIM_CH := 0]
adbpdb <- adbpdb[!is.na(DAH) & YEAR <= dah.roots$report_year, ]
setnames(adbpdb, "ISO3", "ISO_CODE")
rm(expand, inkind, inkind_final, col, to_calc)
#------------------------------------------------------------------------------#

#### #----#                      Save database                       #----# ####
cat("  Save database\n")
save_dataset(adbpdb, paste0("P_GFATM_ADB_PDB_FGH", dah.roots$report_year, 
                            "_unreallocated"), "GFATM", "fin")
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

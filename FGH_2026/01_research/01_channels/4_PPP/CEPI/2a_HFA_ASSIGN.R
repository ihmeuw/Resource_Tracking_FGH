#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Assign CEPI HFAs
#------------------------------------------------------------------------------#

#####-------------------------# enviro setup #------------------------------####

# system prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

# source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))

#####---------------------------# read in data #----------------------------####

cat('  Read in Stage 1a Data\n')

# from stage 1a
cepi_inc_exp <- setDT(fread(paste0(get_path('CEPI', 'int'), '1a_INTAKE_CLEAN.csv')))

# inkind
cepi_inkind <- fread(paste0(get_path('CEPI', 'raw'), "CEPI_INKIND_",dah.roots$report_year,".csv"), header = TRUE, sep=',')

#####---------------------------# assign hfas #-----------------------------####

cat('  Assign HFAs\n')

# for inkind, keep only necessary columns
cepi_inkind <- cepi_inkind[, .(YEAR, INKIND_RATIO)]

# merge inkind and income_expenditure
cepi_inc_exp <- merge(cepi_inc_exp, cepi_inkind, by = "YEAR")
cepi_inc_exp[, INKIND := 0]

# make duplicate inkind_expenditure and multiply budget fractions with inkind ratios
cepi_inc_exp_inkind <- setDT(copy(cepi_inc_exp))
hfas <- c("oid_hss_other", "oid_other", "swap_hss_pp", "oid_ebz", "oid_covid")
cepi_inc_exp_inkind[,
                    paste0(hfas, '_disb_frct') := lapply(.SD, \(x) x * INKIND_RATIO),
                    .SDcols = paste0(hfas, "_disb_frct")]
cepi_inc_exp_inkind[, INKIND := 1]

# append income_expenditure_inkind to income_expenditure
cepi_inc_exp <- rbind(cepi_inc_exp, cepi_inc_exp_inkind)

# generate two additional columns
cepi_inc_exp[, `:=` (CHANNEL = 'CEPI',
                     gov = 2)]

#fix more country codes
# Fix some donor country info
cepi_inc_exp[, DONOR_NAME := toupper(DONOR_NAME)]
cepi_inc_exp[, DONOR_COUNTRY := toupper(DONOR_COUNTRY)]
cepi_inc_exp[is.na(DONOR_COUNTRY), DONOR_COUNTRY := ""]
cepi_inc_exp[, DONOR_NAME := sub(", GOVERNMENT OF$", "", DONOR_NAME)]
cepi_inc_exp[DONOR_NAME == "EUROPEAN COMMISSION", DONOR_NAME := "EC"]
cepi_inc_exp[DONOR_NAME == "THE UNITED STATES", DONOR_NAME := "UNITED STATES OF AMERICA"]
cepi_inc_exp[DONOR_NAME == "THE UNITED KINGDOM", DONOR_NAME := "UNITED KINGDOM"]
cepi_inc_exp[DONOR_NAME == "KINGDOM OF SAUDI ARABIA", DONOR_NAME := "SAUDI ARABIA"]
cepi_inc_exp[, DONOR_COUNTRY := sub(", GOVERNMENT OF$", "", DONOR_COUNTRY)]
cepi_inc_exp[, DONOR_COUNTRY := ifelse(DONOR_COUNTRY == "",
                                       unique(DONOR_COUNTRY[DONOR_COUNTRY != ""])[1], DONOR_COUNTRY),
             by = DONOR_NAME]
cepi_inc_exp[DONOR_NAME == "BMGF", `:=` (
  INCOME_SECTOR = "BMGF",
  DONOR_COUNTRY = "UNITED STATES OF AMERICA",
  DONOR_NAME = "BILL AND MELINDA GATES FOUNDATION"
)]
cepi_inc_exp[DONOR_NAME == "FEDERAL GOVERNMENT OF GERMANY", DONOR_NAME := "GERMANY"]
cepi_inc_exp[DONOR_NAME == "ANONYMOUS DONOR", DONOR_COUNTRY := ""]
cepi_inc_exp[DONOR_NAME == "PORTUGAL", DONOR_COUNTRY := "PORTUGAL"]
cepi_inc_exp[DONOR_NAME == "ICELAND", DONOR_COUNTRY := "ICELAND"]
cepi_inc_exp[DONOR_NAME == "PHILIPPINES", DONOR_COUNTRY := "PHILIPPINES"]
cepi_inc_exp[DONOR_NAME == "NIGEL BLACKWELL", DONOR_COUNTRY := ""]
cepi_inc_exp[DONOR_NAME == "UN FOUNDATION", DONOR_COUNTRY := ""]
cepi_inc_exp[DONOR_NAME == "BANCO COMERCIAL PORTUGUESE", DONOR_COUNTRY := "PORTUGAL"]
cepi_inc_exp[DONOR_NAME == "BANCO SANTANDER TOTTA", DONOR_COUNTRY := "PORTUGAL"]
cepi_inc_exp[DONOR_NAME == "CALOUSTE GULBENKIAN FOUNDATION", DONOR_COUNTRY := "PORTUGAL"]
cepi_inc_exp[DONOR_NAME == "CITI BANK", DONOR_COUNTRY := "UNITED STATES OF AMERICA"]
cepi_inc_exp[DONOR_NAME == "FUNDACION BANCO SANTANDER", DONOR_COUNTRY := "PORTUGAL"]
cepi_inc_exp[DONOR_NAME == "UNALLOCABLE DIFFERENCE", DONOR_COUNTRY := ""]
cepi_inc_exp[DONOR_NAME == "NORSTAT DENMARK", DONOR_COUNTRY := "DENMARK"]
cepi_inc_exp[DONOR_NAME == "SALESFORCE.COM INC.", DONOR_COUNTRY := "UNITED STATES OF AMERICA"]
cepi_inc_exp[DONOR_NAME == "WAKING UP FOUNDATION", DONOR_COUNTRY := "UNITED STATES OF AMERICA"]
check <- cepi_inc_exp[is.na(DONOR_COUNTRY)]

# generate DAH column
cepi_inc_exp[, DAH := 0]
hfa_new <- paste0(hfas, '_disb_frct')
cepi_inc_exp[, DAH := rowSums(.SD, na.rm = TRUE), .SDcols = hfa_new]

names(cepi_inc_exp) <- toupper(gsub("_disb_frct", "_dah", names(cepi_inc_exp)))


#####--------------------------# save dataset #-----------------------------####

cat('  Save Stage 2a Data\n')

# write csv file
save_dataset(cepi_inc_exp, paste0('2a_HFA_ASSIGN'), 'CEPI', 'int')

#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
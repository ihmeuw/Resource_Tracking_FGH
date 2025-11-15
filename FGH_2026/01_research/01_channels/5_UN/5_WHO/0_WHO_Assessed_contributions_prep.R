#-----#    !---- DOCSTRING ----!    ####
# Cleans Assessed contributions dataset
#--------------------------------------------------------------------------------------#
rm(list=ls(all.names = TRUE))
library(data.table)
library(openxlsx)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

data_yr <- report_year - 1


# note: the input excel files must be created/updated before running this script,
#  see the HUB for instructions

#
# audited financial statement
#
exp_consts <- as.data.table(openxlsx::read.xlsx(
    get_path("who", "raw", "WHO_[prev_abrv_year]_expenses.xlsx")
))
exp_reg <- exp_consts[item == "EXP_REG", as.numeric(value)]
exp_eb_vfh <- exp_consts[item == "EXP_EB_VFHP", as.numeric(value)]
exp_other <- exp_consts[item == "EXP_OTHER", as.numeric(value)]


#
# assessed contributions
#
dt <- as.data.table(openxlsx::read.xlsx(
    get_path("WHO","raw", "WHO_[prev_abrv_year]_Assessed_contributions_raw.xlsx"),
    sep.names = " "
))

# parse:
# finding Row and Column values for start and end of dataframe
x <- as.matrix(dt)
a_str <- min(grep("^A$", x)) # first occurance of the string since that is the colnames
c_str <- min(grep("^C$",x))
i_str <- min(grep("^I$",x))

rowa_str <- 1 + (a_str - 1) %% nrow(x)   #
cola_str <- 1 + (a_str - 1) %/% nrow(x)  # number of full columns before position i
rowc_str <- 1 + (c_str - 1) %% nrow(x)   # number of positions outside full columns
colc_str <- 1 + (c_str - 1) %/% nrow(x)  # number of full columns before position i
rowi_str <- 1 + (i_str - 1) %% nrow(x)   # number of positions outside full columns
coli_str <- 1 + (i_str - 1) %/% nrow(x)  # number of full columns before position i
# drop all rows before the first row with "YYYY (EUR million)" for header purposes
dt <- dt[`Members and Associate Members` != "" & `Members and Associate Members` != "Total"]
dt <- dt[, c(1, cola_str, colc_str, coli_str), with = F]

colnames(dt) <- c("DONOR_NAME",
                  paste0("REG_NA_", data_yr),
                  paste0("REG_COLL_", data_yr),
                  "REG_COLL_PRIOR")


#
# finalize
#
val_cols <- grep("REG", names(dt), value = TRUE)
dt[, (val_cols) := lapply(.SD, \(x) {
    # gsub non numeric characters except minus sign or period
    x <- gsub("[^0-9.-]", "", x)
    x[x == "-"] <- NA
    as.numeric(x)
}), .SDcols = val_cols]


dt[, `:=`(CHANNEL = "WHO",
          INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL",
          DONOR_COUNTRY = DONOR_NAME,
          EXP_REG = exp_reg*1000,
          EXP_EB_VFHP = exp_eb_vfh*1000,
          EXP_OTHER = exp_other*1000,
          SOURCE_DOC = paste0("WHO Status of collection of assessed constributions as at 31 December ", data_yr,
                              " and Financial Report and Audited Financial Statements"))
   ]

col_final_order <- c("CHANNEL", "INCOME_SECTOR", "INCOME_TYPE", "DONOR_COUNTRY","DONOR_NAME", 
                      paste0("REG_NA_", data_yr), paste0("REG_COLL_", data_yr), "REG_COLL_PRIOR", 
                      "EXP_REG", "EXP_EB_VFHP", "EXP_OTHER", "SOURCE_DOC")
setcolorder(dt, col_final_order)
save_dataset(dt,
             "WHO_[prev_abrv_year]_Assessed_contributions",
             "WHO", "raw")

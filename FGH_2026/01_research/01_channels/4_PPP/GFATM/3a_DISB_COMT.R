#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Compile Commitment & Disbursement Data
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

# if report_year input data is not yet complete, scale up disbursements
SCALE_DISBURSEMENTS <- FALSE

## Local CONSTANTS
# Named list of disbursements_in_detail dataset downloaded dates for past 
# (at least past 4) years including current report year
DISBURSEMENTS_DOWNLOADED_DATES <- list(
  `2019` = "2020-01-14",
  `2020` = "2021-02-16",
  `2021` = "2022-01-14",
  `2022` = "2023-11-01",
  `2023` = "2024-02-05",
  `2024` = get_dah_param("GFATM", "disbursements_date")
)
# Previous report years used different dates formatting in filenames
DISBURSEMENTS_IN_DETAIL_DOWNLOADED_DATE_STRINGS <- format(as.Date(
  unlist(unname(DISBURSEMENTS_DOWNLOADED_DATES))), "%d%m%y")
# Previous report years of interest to use for estimating missing current
# year data
PREVIOUS_YEARS_OF_INTEREST <- as.character(
  seq(dah.roots$report_year - 3, dah.roots$report_year - 1)
)
# GFATM RAW datasets path base string
GFATM_RAW_BASE <- paste0(
  dah.roots$j,
  "FILEPATH"
)
# GFATM final outputs path base string
GFATM_FIN_BASE <- paste0(
  dah.roots$j,
  "FILEPATH"
)
# List of Disbursements_in_Detail dataset paths for each previous year of 
# interest
PREV_DISBURSEMENTS_IN_DETAIL <- setNames(
  paste0(GFATM_RAW_BASE, PREVIOUS_YEARS_OF_INTEREST, "/P_GFATM_Disbursements_in_Detail_",
         DISBURSEMENTS_IN_DETAIL_DOWNLOADED_DATE_STRINGS[1:3], ".csv"),
  PREVIOUS_YEARS_OF_INTEREST
)
PREV_ADB_PDB_PATHS <- setNames(
  paste0(GFATM_FIN_BASE, PREVIOUS_YEARS_OF_INTEREST, "/P_GFATM_ADB_PDB_FGH",
    PREVIOUS_YEARS_OF_INTEREST, ".csv"),
  PREVIOUS_YEARS_OF_INTEREST
)

NGO_DESCRIPTIVE_VARS <- paste0(
  dah.roots$j,
  "FILEPATH"
)

NGO_DESCRIPTIVE_VARS <- fread(get_path("NGO", "INT", "NGO_PROGRAM_DESCRIPTIONS.csv", 
                                       report_year = 2023))

COUNTRY_FEATURES <- get_path("meta", "locs")

# Columns to retain for disbursements_in_detail dataset
DISBURSEMENT_COLS <- c("GeographicAreaName", "GeographicAreaCode_ISO3",
                       "GeographicAreaLevelName", "MultiCountryName",
                       "ComponentName", "GrantAgreementNumber",
                       "GrantAgreementStatusTypeName", "DisbursementYear",
                       "IsActive", "GrantAgreementTitle", "ProgramStartDate",
                       "ProgramEndDate", "PrincipalRecipientName",
                       "ImplementationPeriodName", "ImplementationPeriodNumber",
                       "ImplementationPeriodStartDate",
                       "ImplementationPeriodEndDate", "DisbursementDate",
                       "DisbursementAmount")
#------------------------------------------------------------------------------#


######################## #----# HELPER FUNCTIONS #----# ########################
#' @title days_to_end_of_year
#' @description Gets N days until 12/31 of the same year, given a valid data.
#' 
#' @param update_date [character/datetime] Valid date-like string or date object
#' @return Returns an integer number of days until the end of the calendar year
days_to_end_of_year <- function (update_date) {
  return (
    as.numeric(
      as.Date(paste0(format(as.Date(update_date), "%Y"),
                     "-12", "-31")) - 
        as.Date(update_date)
    )
  )
}
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ####################################\n"))
cat(green(" #### GFATM DISB & COM'T COMPILE ####\n"))
cat(green(" ####################################\n\n"))

#### #----#                Import Disbursement Data                  #----# ####
cat("  Import Disbursement Data\n")
## Read in the dataset downloaded directly from 
## https://data-service.theglobalfund.org/downloads
gf_disb <- fread(
  get_path("GFATM", "raw", 
           paste0("P_GFATM_Disbursements_in_Detail_", 
                  get_dah_param("GFATM", "disbursements_date"), ".csv")))

# Keep pre-defined set of columns only
selected_columns <- DISBURSEMENT_COLS[DISBURSEMENT_COLS %in% colnames(gf_disb)]
gf_disb <- gf_disb[, .SD, .SDcols = selected_columns]
setnames(gf_disb, 
         c("GrantAgreementNumber", "DisbursementDate", "DisbursementYear", 
           "GeographicAreaCode_ISO3", "DisbursementAmount"),
         c("PROJECT_ID", "disb_date", "disb_year", "countrycd", "disbursement"))
#------------------------------------------------------------------------------#

#### #----#                 Clean disbursement data                  #----# ####
cat("  Clean Disbursement Data\n")
# Convert disb_date from datetime-like character to Date class
gf_disb[, disb_date := as.Date(disb_date, "%m/%d/%Y")]
# Number rows as a column
gf_disb[, obs := 1:nrow(gf_disb)]
gf_disb[, disb_day := tolower(format(
  x = as.Date(disb_date, format = "%m/%d/%Y"), 
  format = "%d%b%Y"
))]
# Reorder data by PROJECT_ID, disbursement date ascending
gf_disb <- gf_disb[order(PROJECT_ID, disb_date)]
# Count of disbursement observations per project
gf_disb[, total_disb_no := .N, by = "PROJECT_ID"]
# Give each project's disbursements observation a rank within that project
gf_disb[, disb_no := 1:max(total_disb_no), by = "PROJECT_ID"]
# Tag final disbursement, disbursement date, and disbursement year
# Note - this will be NA for these columns everywhere else
gf_disb[disb_no == total_disb_no, `:=`(fin_disb = disbursement,
                                       fin_disb_day = disb_day,
                                       fin_disb_year = disb_year)]
# Oh. I hate this solution, though technically works.. leaving for now
gf_disb[, `:=`(final_disb = mean(fin_disb, na.rm=T),
               final_disb_day = max(fin_disb_day, na.rm=T),
               final_disb_year = max(fin_disb_year, na.rm=T)), by="PROJECT_ID"]
gf_disb[, `:=`(fin_disb = NULL, fin_disb_day = NULL, fin_disb_year = NULL)]
# Take sum of disbursement
gf_disb[, DISBURSEMENT_TOTAL := sum(disbursement, na.rm=T), by = "PROJECT_ID"]
# Collapse sum/mean
# Get sum of disbursements
ds <- gf_disb[, lapply(.SD, sum, na.rm=T), 
              by=c("PROJECT_ID", "disb_year", "final_disb_day", 
                   "final_disb_year", "countrycd"), 
              .SDcols=c("disbursement")]
# Get mean of disbursements
dm <- gf_disb[, lapply(.SD, mean, na.rm=T), 
              by=c("PROJECT_ID", "disb_year", "final_disb_day", 
                   "final_disb_year", "countrycd"), 
              .SDcols=c("DISBURSEMENT_TOTAL", "final_disb")]
# Merge sums and means
gf_disb_summary <- merge(ds, dm, 
                 by=c("PROJECT_ID", "disb_year", "final_disb_day", 
                      "final_disb_year", "countrycd"))
rm(ds, dm)
#------------------------------------------------------------------------------#
if (SCALE_DISBURSEMENTS) {
  #### #----#         Initial submission disbursement scaling          #----# ####
  cat("  Initial submission disbursement scaling\n")
  #' The following section is for use only for initial submission when data for 
  #' the full report year is not available. Because we don't have a full year of 
  #' data we need to scale up disbursement as if we do have a full year. To do 
  #' this we look at the pledges/contributions from the past few years (as many 
  #' as we have mid-year and final pledges/contributions datasets) and determine 
  #' the difference between what individual donors have given between this month 
  #' and January. We turn the difference into a proportion and multiply 
  #' disbursement amounts in the current report year to scale up to the full year.
  #' An attempt was made in 2019 to automate this process but name matching 
  #' proved to be nearly impossible year-to-year, so sadly this is manual.
  #
  # Calculations performed:
      # 3-year lagged calculation (we"ll use 2016 for simplicity)
          # ( ((total 2016 amount from ADB_PDB / amount [current month] 2016 that 
          # we reported was disbursed) - 1) * ((365 - [current month] 2019 
          # download date)/(365-[current month] 2016 download date)) ) + 1
      # 2-year lagged calculation (we"ll use 2017 for simplicity)
          # ( ((total 2017 amount from ADB_PDB / amount [current month] 2017 that 
          # we reported was disbursed) - 1) * ((365 - [current month] 2019 
          # download date)/(365-[current month] 2017 download date)) ) + 1
      # 1-year lagged calculation (we"ll use 2018 for simplicity)
          # ( ((total 2018 amount from ADB_PDB / amount [current month] 2018 that 
          # we reported was disbursed) - 1) * ((365 - sept 2019 download date) /
          # (365-june 2018 download date))) + 1
  #
  # For each of the three years prior to the current report year, you will need 
  # to:
    # 1. Calculate the total DAH in that year (i.e. total DAH in 2016)
    # 2. Sum the total amount reported from the "Contributions" column in that 
    # year's midyear Pledges/Contributions dataset
        # 2.a. Calculate only the report year and make sure the data you're 
        # looking at is from the mid-year update (most likely in September-ish)
    # 3. Calculate the number of days between January 1st in that year and the 
        # day the mid-year Pledges/Contributions dataset was downloaded
        # This website can be helpful (or just use R???): 
        # https://www.thecalculatorsite.com/time/days-between-dates.php
        # Be sure you're using the right year (i.e. Number of days between Jan 1, 
        # 2016 and Sept 4, 2016 for example)
    # 4. Update the locals declared below with the information from each of these 
        # findings
  cat(red("  WARNING!! PERFORMING DISBURSEMENT SCALING!! IF YOU HAVE NOT ",
          "UPDATED THIS SECTION WITH NEW DATES & VALUES YOU MUST IMMEDIATELY ",
          "ADDRESS THIS WARNING!!\n"))
  #----# Variable Collection #----#
  # Current report year days until 12/31
  days_to_eoy_current <- days_to_end_of_year(
    DISBURSEMENTS_DOWNLOADED_DATES[[as.character(dah.roots$report_year)]]
  )
  # Initialize empty list and counter
  lagged_props_list <- list()
  i <- 1
  # For each of the 3 years prior to current report year...
  for (year in PREVIOUS_YEARS_OF_INTEREST) {
    download_date <- DISBURSEMENTS_DOWNLOADED_DATES[[year]]
    days_to_eoy <- days_to_end_of_year(download_date)
    # Total from final ADB_PDB for this report year
    adb_pdb_year <- fread(PREV_ADB_PDB_PATHS[[year]])
    year_total <- adb_pdb_year[YEAR == year, sum(DAH)]
    gf_disb_year <- fread(PREV_DISBURSEMENTS_IN_DETAIL[[year]])
    gf_disb_year[, DisbursementDate := as.Date(DisbursementDate, "%m/%d/%Y")]
    year_to_download_date_disbursement <- gf_disb_year[
      DisbursementDate >= paste0(year, "-01", "-01") & 
        DisbursementDate <= download_date, 
      sum(DisbursementAmount)
    ]
    # Calculate the lagged proportion for this report year
    lagged_proportion <- (year_total / year_to_download_date_disbursement) * 
      (days_to_eoy_current / days_to_eoy) + 1
    
    # Add to list
    lagged_props_list[i] <- lagged_proportion
    i <- i + 1
}
# Calculate our final 3-year weighted proportion
proportion <- sum(unlist(lagged_props_list)) / 3
#-------------------------------#

#----# Recalculation #----#
gf_disb[disb_year == dah.roots$report_year, 
        `:=` (DISBURSEMENT_TOTAL = DISBURSEMENT_TOTAL * proportion,
              final_disb = final_disb * proportion,
              disbursement = disbursement * proportion)]
#-------------------------#
#------------------------------------------------------------------------------#
}

#### #----#                     Prep NGOs data                       #----# ####
cat("  Prep NGOs data\n")
# US NGOs
us_ngo <- setDT(read.dta13(paste0(NGO_DESCRIPTIVE_VARS, "Agency_ID_FF2017.dta")))[, !c("n")]
us_ngo[, RECIPIENT_AGENCY := toupper(agency)]
us_ngo[, dummy := 1]
us_ngo[, n := 1:sum(dummy), by=c("id", "RECIPIENT_AGENCY")]
us_ngo <- distinct(us_ngo)
us_ngo <- us_ngo[n == 1, ]
us_ngo[, `:=`(n = NULL, dummy = NULL)]
us_ngo <- setDT(us_ngo)
# String cleaning
us_ngo[RECIPIENT_AGENCY == "DUMAS M. SIM<E9>US FOUNDATION", `:=` (RECIPIENT_AGENCY = "DUMAS M. SIMEUS FOUNDATION", agency = tolower("DUMAS M. SIMEUS FOUNDATION"))]
us_ngo[RECIPIENT_AGENCY == "DUMAS M. SIM<E9>US FOUNDATION INC", `:=` (RECIPIENT_AGENCY = "DUMAS M. SIMEUS FOUNDATION INC", agency = tolower("DUMAS M. SIMEUS FOUNDATION INC"))]
us_ngo[RECIPIENT_AGENCY == "DUMAS M. SIM<E9>US FOUNDATION, INC.", `:=` (RECIPIENT_AGENCY = "DUMAS M. SIMEUS FOUNDATION, INC.", agency = tolower("DUMAS M. SIMEUS FOUNDATION, INC."))]
us_ngo[RECIPIENT_AGENCY == "LITHUANIAN CHILDREN<92>S RELIEF", `:=` (RECIPIENT_AGENCY = "LITHUANIAN CHILDREN'S RELIEF", agency = tolower("LITHUANIAN CHILDREN'S RELIEF"))]
us_ngo[RECIPIENT_AGENCY == "LITHUANIAN CHILDREN<92>S RELIEF INC", `:=` (RECIPIENT_AGENCY = "LITHUANIAN CHILDREN'S RELIEF INC", agency = tolower("LITHUANIAN CHILDREN'S RELIEF INC"))]
us_ngo[RECIPIENT_AGENCY == "LITHUANIAN CHILDREN<92>S RELIEF, INC.", `:=` (RECIPIENT_AGENCY = "LITHUANIAN CHILDREN'S RELIEF, INC.", agency = tolower("LITHUANIAN CHILDREN'S RELIEF, INC."))]
us_ngo[RECIPIENT_AGENCY == "M<E9>DECINS SANS FRONTI<E8>RES U.S.A.", `:=` (RECIPIENT_AGENCY = "MEDECINS SANS FRONTIERES U.S.A.", agency = tolower("MEDECINS SANS FRONTIERES U.S.A."))]
us_ngo[RECIPIENT_AGENCY == "M<E9>DECINS SANS FRONTI<E8>RES U.S.A. INC", `:=` (RECIPIENT_AGENCY = "MEDECINS SANS FRONTIERES U.S.A. INC", agency = tolower("MEDECINS SANS FRONTIERES U.S.A. INC"))]
us_ngo[RECIPIENT_AGENCY == "M<E9>DECINS SANS FRONTI<E8>RES U.S.A., INC.", `:=` (RECIPIENT_AGENCY = "MEDECINS SANS FRONTIERES U.S.A., INC.", agency = tolower("MEDECINS SANS FRONTIERES U.S.A., INC."))]
us_ngo[RECIPIENT_AGENCY == "SOV<E9> LAVI", `:=` (RECIPIENT_AGENCY = "SOVE LAVI", agency = tolower("SOVE LAVI"))]

#INT'L NGOs
intl_ngo <- setDT(read.dta13(paste0(NGO_DESCRIPTIVE_VARS, "Intl_Agency_ID_FF2017.dta"), encoding = "UTF-8"))
intl_ngo[, RECIPIENT_AGENCY := toupper(agency)]
intl_ngo[, dummy := 1]
intl_ngo[, n := 1:sum(dummy), by="RECIPIENT_AGENCY"]
intl_ngo <- intl_ngo[n == 1, ]
intl_ngo[, `:=`(n = NULL, dummy = NULL)]
setnames(intl_ngo, "id", "ingo_id")
intl_ngo <- setDT(intl_ngo)
# String cleaning
intl_ngo[RECIPIENT_AGENCY == "AGENCE D' AIDE À LA COOPÉRATION TECHNIQUE ET AU DÉVELOPPEMENT", RECIPIENT_AGENCY := "AGENCE D' AIDE A LA COOPERATION TECHNIQUE ET AU DEVELOPPEMENT"]
intl_ngo[RECIPIENT_AGENCY == "AGRONOMES ET VÉTÉRINAIRES SANS FRONTIÈRES", RECIPIENT_AGENCY := "AGRONOMES ET VETERINAIRES SANS FRONTIERES"]
intl_ngo[RECIPIENT_AGENCY == "CAFÉDIRECT PRODUCERS' FOUNDATION", RECIPIENT_AGENCY := "CAFEDIRECT PRODUCERS' FOUNDATION"]
intl_ngo[RECIPIENT_AGENCY == "CENTRE CANADIEN D’ÉTUDE ET DE COOPÉRATION INTERNATIONALE", RECIPIENT_AGENCY := "CENTRE CANADIEN D'ETUDE ET DE COOPERATION INTERNATIONALE"]
intl_ngo[RECIPIENT_AGENCY == "COMITÉ D'AIDE MÉDICALE", RECIPIENT_AGENCY := "COMITE D'AIDE MEDICALE"]
intl_ngo[RECIPIENT_AGENCY == "FUNDACI¢N ACCI¢N CONTRA EL HAMBRE", RECIPIENT_AGENCY := "FUNDACION ACCION CONTRA EL HAMBRE"]
intl_ngo[RECIPIENT_AGENCY == "MÉDECINS DU MONDE", RECIPIENT_AGENCY := "MEDECINS DU MONDE"]
intl_ngo[RECIPIENT_AGENCY == "MÈDECINS SANS FRONTIËRES", RECIPIENT_AGENCY := "MEDECINS SANS FRONTIERES"]
intl_ngo[RECIPIENT_AGENCY == "MÉDICOS DEL MUNDO ESPAÑA", RECIPIENT_AGENCY := "MEDICOS DEL MUNDO ESPANA"]
intl_ngo[RECIPIENT_AGENCY == "MISSION ØST", RECIPIENT_AGENCY := "MISSION OST"]
intl_ngo[RECIPIENT_AGENCY == "PHARMACIENS SANS FRONTIÈRES", RECIPIENT_AGENCY := "PHARMACIENS SANS FRONTIERES"]
intl_ngo[RECIPIENT_AGENCY == "SOCIÉTÉ DE COOPÉRATION POUR LE DÉVELOPPEMENT INTERNATIONAL", RECIPIENT_AGENCY := "SOCIETE DE COOPERATION POUR LE DEVELOPPEMENT INTERNATIONAL"]
intl_ngo[RECIPIENT_AGENCY == "TRIANGLE GÉNÉRATION HUMANITAIRE", RECIPIENT_AGENCY := "TRIANGLE GENERATION HUMANITAIRE"]
intl_ngo[RECIPIENT_AGENCY == "VÉTÉRINAIRES SANS FRONTIÈRES - BELGIUM", RECIPIENT_AGENCY := "VETERINAIRES SANS FRONTIERES - BELGIUM"]
intl_ngo[RECIPIENT_AGENCY == "VÉTÉRINAIRES SANS FRONTIÈRES - CENTRE INTERNATIONAL", RECIPIENT_AGENCY := "VETERINAIRES SANS FRONTIERES - CENTRE INTERNATIONAL"]
intl_ngo[RECIPIENT_AGENCY == "VÉTÉRINAIRES SANS FRONTIÈRES - GERMANY", RECIPIENT_AGENCY := "VETERINAIRES SANS FRONTIERES - GERMANY"]
intl_ngo[RECIPIENT_AGENCY == "VÉTÉRINAIRES SANS FRONTIÈRES - SWITZERLAND", RECIPIENT_AGENCY := "VETERINAIRES SANS FRONTIERES - SWITZERLAND"]
#------------------------------------------------------------------------------#

#### #----#                 Import commitment data                   #----# ####
cat("  Import Commitment Data\n")
# Read in data & setnames
comt <- setDT(fread(paste0(get_path("GFATM", "raw"), "P_GFATM_PDB_GRANTS_DETAIL_", get_dah_param("GFATM", "pledges_contributions_date"), ".csv")))
names(comt) <- tolower(names(comt))
setnames(comt, c("geographicareaname", "componentname", "principalrecipientclassificationname", "principalrecipientname", "grantagreementstatustypename", "localfundagentname",
                 "grantagreementnumber", "programstartdate", "programenddate", "totalsignedamount", "totalcommittedamount", "totaldisbursedamount", "geographicareacode_iso3"),
         c("country", "component", "principalrecipienttype", "principalrecipient", "grantstatus1", "localfundagent", "grantnumber", "implementationperiodstartdate", 
           "implementationperiodenddate", "grantamount", "confirmedcommitmentamount", "disbursed", "countrycd"),
         skip_absent = TRUE)
# Fill component column
comt <- comt[country != "", ]
comt[component == "" & shift(x = component, n = 1, type = "lag") != "", 
     component := shift(x = component, n = 1, type = "lag")]
setnames(comt, "grantnumber", "PROJECT_ID")
# Fill in other columns
for (i in 1:nrow(comt)) {
  if (i > 1) {
    c_row <- comt[i, ]
    p_row <- comt[i-1, ]
    # Fill principalrecipienttype
    if (c_row$principalrecipienttype == "" & p_row$principalrecipienttype != "") {
      comt[i, "principalrecipienttype"] <- p_row$principalrecipienttype
    }
    # Fill principalrecipient
    if (c_row$principalrecipient == "" & p_row$principalrecipient != "") {
      comt[i, "principalrecipient"] <- p_row$principalrecipient
    }
    # Fill component
    if (c_row$component == "" & p_row$component != "") {
      comt[i, "component"] <- p_row$component
    }
    # Fill grantstatus1
    if (c_row$grantstatus1 == "" & p_row$grantstatus1 != "") {
      comt[i, "grantstatus1"] <- p_row$grantstatus1
    }
    # Fill project_id
    if (c_row$PROJECT_ID == "" & p_row$PROJECT_ID != "") {
      comt[i, "PROJECT_ID"] <- p_row$PROJECT_ID
    }
    rm(c_row, p_row)
  }
}
# Collapse sum
comt <- comt[, lapply(.SD, sum, na.rm=T), by=c("country", "component", "principalrecipienttype", "principalrecipient", "PROJECT_ID"),
             .SDcols=c("confirmedcommitmentamount")]
#------------------------------------------------------------------------------#

#### #----#            Merge commitments & disbursements             #----# ####
cat("  Merge commitments & disbursements\n")
comt[, m_m := 1]
gf_disb[, u_m := 2]
mer <- merge(comt, gf_disb, by="PROJECT_ID", all = T)
comt[, m_m := NULL]
gf_disb[, u_m := NULL]
mer[, merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[merge %in% c(2,3), !c("u_m", "m_m")]

mer[merge == 3, DATA_SOURCE := "GFATM Disbursements PDF & Grants in Detail XLS"]
mer[merge == 2, DATA_SOURCE := "GFATM Disbursements PDF"]
mer[merge == 1, DATA_SOURCE := "GFATM Grants in Detail XLS"]
mer[, merge := NULL]

# String cleaning
mer[principalrecipient == "Minist\xe8re de la Sant\xe9 et du D\xe9veloppement Social de la R\xe9publique du Mali",
        principalrecipient := "Ministre de la Sant et du Dveloppement Social de la Republique du Mali"]
mer[principalrecipient ==  "Initiative Priv\xe9e et Communautaire pour la Sant\xe9 et la Riposte au VIH/SIDA  au Burkina Faso (IPC/BF)",
        principalrecipient:= "Initiative Priv et Communautaire pour la Sant et la Riposte au VIH/SIDA  au Burkina Faso (IPC/BF)"]
mer[principalrecipient == "Alliance Nationale des Communaut<e9>s pour la Sant<e9> - S<e9>n<e9>gal",
        principalrecipient := "Alliance Nationale des Communautes pour la Sante- Senegal"]
mer[principalrecipient == "Alter Vida - Centro de Estudios y Formaci<f3>n para el Ecodesarrollo",
        principalrecipient := "Alter Vida - Centro de Estudios y Formacion para el Ecodesarrollo"]
mer[principalrecipient == "Asociaci<f3>n Dominicana Pro-Bienestar de la Familia (PROFAMILIA)",
        principalrecipient := "Asociacion Dominicana Pro-Bienestar de la Familia (PROFAMILIA)"]
mer[principalrecipient == "Asociaci<f3>n Protecci<f3>n a la Salud",
        principalrecipient := "Asociacion Proteccion a la Salud"]
mer[principalrecipient == "Association Comorienne pour le Bien-E?tre de la Famille",
        principalrecipient := "Association Comorienne pour le Bien-Etre de la Famille"]
mer[principalrecipient == "Caritas C<f4>te d'Ivoire",
        principalrecipient := "Caritas Cote d'Ivoire"]
mer[principalrecipient == "Cellule Nationale de Coordination Technique de la Riposte Nationale au Sida et aux H<e9>patites",
        principalrecipient := "Cellule Nationale de Coordination Technique de la Riposte Nationale au Sida et aux Hepatites"]
mer[principalrecipient == "Central Board of <91>Aisyiyah",
        principalrecipient := "Central Board of 'Aisyiyah"]
mer[principalrecipient == "Centrale d<92>Achat des M<e9>dicaments Essentiels et de Mat<e9>riel M<e9>dical de Madagascar (SALAMA)",
        principalrecipient := "Centrale d'Achat des Medicaments Essentiels et de Materiel Medical de Madagascar (SALAMA)"]
mer[principalrecipient == "Centre de Recherches M<e9>dicales de Lambar<e9>n<e9>",
        principalrecipient := "Centre de Recherches Medicales de Lambarene"]
mer[principalrecipient == "Centro de Colabora<e7><e3>o em Sa<fa>de",
        principalrecipient := "Centro de Colaboracao em Saude"]
mer[principalrecipient == "Centro de Informaci<f3>n y Recursos para el Desarrollo",
        principalrecipient := "Centro de Informacion y Recursos para el Desarrollo"]
mer[principalrecipient == "Centro de Investigaci<f3>n, Educaci<f3>n y Servicios (CIES)",
        principalrecipient := "Centro de Investigacion, Educacion y Servicios (CIES)"]
mer[principalrecipient == "Conseil National de Lutte contre le SIDA de la R<e9>publique du S<e9>n<e9>gal",
        principalrecipient := "Conseil National de Lutte contre le SIDA de la Republique du Senegal"]
mer[principalrecipient == "Conseil National de Lutte contre le VIH/SIDA, la Tuberculose, le Paludisme, les H<e9>patites, les Infections sexuellement transmissibles et les Epid<e9>mies",
        principalrecipient := "Conseil National de Lutte contre le VIH/SIDA, la Tuberculose, le Paludisme, les Hepatites, les Infections sexuellement transmissibles et les Epidemies"]
mer[principalrecipient == "Consejo de las Am<e9>ricas",
        principalrecipient := "Consejo de las Americas"]
mer[principalrecipient == "Corporaci<f3>n Kimirina",
        principalrecipient := "Corporacion Kimirina"]
mer[principalrecipient == "Direction Ex<e9>cutive du Conseil National de lutte contre le Sida, les IST et les Epid<e9>mies",
        principalrecipient := "Direction Executive du Conseil National de lutte contre le Sida, les IST et les Epidemies"]
mer[principalrecipient == "Federaci<f3>n Red NICASALUD",
        principalrecipient := "Federacion Red NICASALUD"]
mer[principalrecipient == "Fonds de soutien aux activit<e9>s en mati<e8>re de population et de lutte contre le Sida de la R<e9>publique du Tchad",
        principalrecipient := "Fonds de soutien aux activites en matiere de population et de lutte contre le Sida de la Republique du Tchad"]
mer[principalrecipient == "Funda<e7><e3>o Ataulpho de Paiva",
        principalrecipient := "Fundacao Ataulpho de Paiva"]
mer[principalrecipient == "Funda<e7><e3>o de Medicina Tropical do Amazonas (FMT-AM)",
        principalrecipient := "Fundacao de Medicina Tropical do Amazonas (FMT-AM)"]
mer[principalrecipient == "Funda<e7><e3>o Faculdade de Medicina (FFM)",
        principalrecipient := "Fundacao Faculdade de Medicina (FFM)"]
mer[principalrecipient == "Funda<e7><e3>o Para O Desenvolvimento Cient<ed>fico E Tecnol<f3>gico Em Sa<fa>de (FIOTEC)",
        principalrecipient := "Fundacao Para O Desenvolvimento Cientifico E Tecnologico Em Saude (FIOTEC)"]
mer[principalrecipient == "Funda<e7><e3>o para o Desenvolvimento da Comunidade",
        principalrecipient := "Fundacao para o Desenvolvimento da Comunidade"]
mer[principalrecipient == "Fundaci<f3>n Universidad de Antioquia",
        principalrecipient := "Fundaciun Universidad de Antioquia"]
mer[principalrecipient == "Groupe Pivot Sant<e9> Population",
        principalrecipient := "Groupe Pivot Sante Population"]
mer[principalrecipient == "Initiative Priv<e9>e et Communautaire contre le VIH/SIDA au Burkina Faso",
        principalrecipient := "Initiative Privee et Communautaire contre le VIH/SIDA au Burkina Faso"]
mer[principalrecipient == "Instituto de Nutrici<f3>n de Centro Am<e9>rica y Panam<e1> (INCAP)",
        principalrecipient := "Instituto de Nutricion de Centro America y Panama (INCAP)"]
mer[principalrecipient == "Instituto Dermatol<f3>gico y Cirug<ed>a de Piel <91>Dr. Huberto Bogaert D<ed>az<92>",
        principalrecipient := "Instituto Dermatologico y Cirugia de Piel 'Dr. Huberto Bogaert Diaz'"]
mer[principalrecipient == "INSTITUTO NACIONAL DE SALUD P<da>BLICA (INSP)",
        principalrecipient := "INSTITUTO NACIONAL DE SALUD PUBLICA (INSP)"]
mer[principalrecipient == "Instituto Nicarag<fc>ense de Seguridad Social",
        principalrecipient := "Instituto Nicaraguense de Seguridad Social"]
mer[principalrecipient == "International Charitable Foundation <93>Alliance for Public Health<94>",
        principalrecipient := "International Charitable Foundation 'Alliance for Public Health'"]
mer[principalrecipient == "La Croix-Rouge fran<e7>aise",
        principalrecipient := "La Croix-Rouge francaise"]
mer[principalrecipient == "Ministry of Health and Public Hygiene of the Republic of C<f4>te d'Ivoire - PNLP",
        principalrecipient := "Ministry of Health and Public Hygiene of the Republic of Cote d'Ivoire - PNLP"]
mer[principalrecipient == "Ministry of Health and Public Hygiene of the Republic of C<f4>te d'Ivoire - PNLS",
        principalrecipient := "Ministry of Health and Public Hygiene of the Republic of Cote d'Ivoire - PNLS"]
mer[principalrecipient == "Ministry of Health and Public Hygiene of the Republic of C<f4>te d'Ivoire - PNLT",
        principalrecipient := "Ministry of Health and Public Hygiene of the Republic of Cote d'Ivoire - PNLT"]
mer[principalrecipient == "Office National de la Famille et de la Population de la R<e9>publique de Tunisie",
        principalrecipient := "Office National de la Famille et de la Population de la Republique de Tunisie"]
mer[principalrecipient == "Organismo Andino de Salud - Convenio Hip<f3>lito Unanue",
        principalrecipient := "Organismo Andino de Salud - Convenio Hipolito Unanue"]
mer[principalrecipient == "Persatuan Karya Dharma Kesehatan Indonesia (also known as <93>PERDHAKI<94>, Association of Voluntary Health Services of Indonesia)",
        principalrecipient := "Persatuan Karya Dharma Kesehatan Indonesia (also known as 'PERDHAKI', Association of Voluntary Health Services of Indonesia)"]
mer[principalrecipient == "Pr<e9>vention Information Lutte contre le Sida",
        principalrecipient := "Prevention Information Lutte contre le Sida"]
mer[principalrecipient == "Primature de la R<e9>publique Togolaise",
        principalrecipient := "Primature de la Republique Togolaise"]
mer[principalrecipient == "Programme d'Appui au D<e9>veloppement Sanitaire du Burkina Faso",
        principalrecipient := "Programme d'Appui au Developpement Sanitaire du Burkina Faso"]
mer[principalrecipient == "Programme National contre la Tuberculose de la R<e9>publique du B<e9>nin",
        principalrecipient := "Programme National contre la Tuberculose de la Republique du Benin"]
mer[principalrecipient == "Programme National de Lutte contre le Paludisme de la R<e9>publique du B<e9>nin",
        principalrecipient := "Programme National de Lutte contre le Paludisme de la Republique du Benin"]
mer[principalrecipient == "Programme National de Lutte contre le SIDA de la R<e9>publique de C<f4>te d'Ivoire",
        principalrecipient := "Programme National de Lutte contre le SIDA de la Republique de Cote d'Ivoire"]
mer[principalrecipient == "Programme sant<e9> de lutte contre le Sida",
        principalrecipient := "Programme sante de lutte contre le Sida"]
mer[principalrecipient == "R<e9>seau Burundais des Personnes Vivant avec le VIH/SIDA",
        principalrecipient := "Reseau Burundais des Personnes Vivant avec le VIH/SIDA"]
mer[principalrecipient == "RSE on REU <93>National Scientific Center of Phthisiopulmonology of the Republic of  Kazakhstan<94> of the Ministry of Health of the Republic of  Kazakhstan",
        principalrecipient := "RSE on REU 'National Scientific Center of Phthisiopulmonology of the Republic of  Kazakhstan' of the Ministry of Health of the Republic of  Kazakhstan"]
mer[principalrecipient == "S<e9>cr<e9>tariat Ex<e9>cutif du Comit<e9> National de Lutte Contre le VIH/SIDA de la R<e9>publique de Madagascar",
        principalrecipient := "Secretariat Executif du Comite National de Lutte Contre le VIH/SIDA de la Republique de Madagascar"]
mer[principalrecipient == "Secr<e9>tariat Ex<e9>cutif du Comit<e9> National de Lutte contre le Sida de la R<e9>publique de Guin<e9>e",
        principalrecipient := "Secretariat Executif du Comite National de Lutte contre le Sida de la Republique de Guinee"]
mer[principalrecipient == "Secr<e9>tariat Ex<e9>cutif National de Lutte contre le SIDA de la R<e9>publique Islamique de Mauritanie",
        principalrecipient := "Secretariat Executif National de Lutte contre le SIDA de la Republique Islamique de Mauritanie"]
mer[principalrecipient == "Secr<e9>tariat Ex<e9>cutif Permanent du Conseil National de Lutte contre le SIDA",
        principalrecipient := "Secretariat Executif Permanent du Conseil National de Lutte contre le SIDA"]
mer[principalrecipient == "Secr<e9>tariat Permanent du Conseil National de Lutte contre le Sida et les IST du Burkina Faso",
        principalrecipient := "Secretariat Permanent du Conseil National de Lutte contre le Sida et les IST du Burkina Faso"]
mer[principalrecipient == "Secretar<ed>a de la Integraci<f3>n Social Centroamericana",
        principalrecipient := "Secretaria de la Integracion Social Centroamericana"]
mer[principalrecipient == "Soci<e9>t<e9> d<92>Electricit<e9> Industrielle et de B<e2>timents (SEIBsa)",
        principalrecipient := "Societe d'Electricite Industrielle et de Batiments (SEIBsa)"]
mer[principalrecipient == "Soci<e9>t<e9> Tunisienne des Maladies Respiratoires et d<92>Allergologie",
        principalrecipient := "Societe Tunisienne des Maladies Respiratoires et d'Allergologie"]
mer[principalrecipient == "The Consejo T<e9>cnico de Asistencia M<e9>dico Social (CTAMS) of the Government of the Republic of Costa Rica",
        principalrecipient := "The Consejo Tecnico de Asistencia Medico Social (CTAMS) of the Government of the Republic of Costa Rica"]
mer[principalrecipient == "Unit<e9> de Gestion des Projets d'Appui au Secteur Sant<e9> de la R<e9>publique de Madagascar",
        principalrecipient := "Unite de Gestion des Projets d'Appui au Secteur Sante de la Republique de Madagascar"]
mer[principalrecipient == "Alliance Nationale pour la Sant<e9> et le D<e9>veloppement en C<f4>te D'Ivoire",
    principalrecipient := "Alliance Nationale pour la Sante et le Developpement en Cote D'Ivoire"]
mer[principalrecipient == "Secr\xe9tariat Permanent du Conseil National de Lutte contre le Sida et les Infections Sexuellement Transmissibles (SP/CNLS-IST)",
    principalrecipient := "Secretariat Permanent du Conseil National de Lutte contre le Sida et les Infections Sexuellement Transmissibles (SP/CNLS-IST)"]
mer[principalrecipient == "Programme d'Appui au D<e9>veloppement Sanitaire (PADS)",
    principalrecipient := "Programme d'Appui au Developpement Sanitaire (PADS)"]
mer[principalrecipient == "Minist<e8>re de Sant<e9> Publique",
    principalrecipient := "Ministere de Sante Publique"]
mer[principalrecipient == "Ministry of Health and Public Hygiene of the Republic of C<f4>te d'Ivoire",
    principalrecipient := "Ministry of Health and Public Hygiene of the Republic of Cote d'Ivoire	"]
rm(comt, gf_disb)
#------------------------------------------------------------------------------#

#### #----#                Add ISO and Region Codes                  #----# ####
cat("  Add ISO and Region Codes\n")
mer[country == "Indonesia", countrycd := "IDN"]
mer[countrycd == "DZA", country := "Algeria"]
mer[country == "Bolivia (Plurinational State)", country := "Bolivia (Plurinational State of)"]
mer[country == "Congo (Democratic Republic)", country := "Democratic Republic of the Congo"]
mer[country == "Iran (Islamic Republic)", country := "Iran (Islamic Republic of)"]
mer[country == "Korea (Democratic Peoples Republic)", country := "Democratic People's Republic of Korea"]
mer[country == "Lao (Peoples Democratic Republic)", country := "Lao People's Democratic Republic"]
mer[country == "Macedonia (Former Yugoslav Republic)", country := "North Macedonia"]
mer[country == "Tanzania (United Republic)", country := "United Republic of Tanzania"]
mer[country == "South Sudan" & disb_year < 2011, country := "Sudan"]
mer[country%in% c("CCM Zanzibar", "Zanzibar"), country := "United Republic of Tanzania"]
mer[country=="Moldova", country := "Republic of Moldova"]
setnames(mer, "country", "country_lc")
mer[country_lc == "C\xf4te d'Ivoire", country_lc := "Côte d'Ivoire"]

# Merge ISO codes
isos <- setDT(fread(paste0(COUNTRY_FEATURES, "fgh_location_set.csv")))
isos <- isos[, c("ihme_loc_id", "location_name", "region_name")]
colnames(isos) <- c("iso3", "countryname_ihme", "gbd_region")
isos[, country_lc := countryname_ihme]
isos[, u_m := 2]

mer[, m_m := 1]
mer <- merge(mer, isos, by="country_lc", all=T)
mer[, merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[merge %in% c(1,3), !c("merge", "u_m", "m_m")]
# Rename
setnames(mer, c("countryname_ihme", "iso3", "localfundagent", "component", "confirmedcommitmentamount", "disbursement", "grantstatus1", "principalrecipient",
                "principalrecipienttype"),
         c("RECIPIENT_COUNTRY", "ISO3_RC", "LFA", "DISEASE_COMPONENT", "COMMITMENT", "DAH", "GRANT_STATUS", "PR_NAME", "PR_TYPE"),
         skip_absent = T)
mer[RECIPIENT_COUNTRY == "" | is.na(RECIPIENT_COUNTRY), RECIPIENT_COUNTRY := country_lc]
mer[ISO3_RC == "" | is.na(ISO3_RC), ISO3_RC := countrycd]
mer[RECIPIENT_COUNTRY == "Kosovo", ISO3_RC := "KSV"]
mer[RECIPIENT_COUNTRY == "Türkiye", `:=` (RECIPIENT_COUNTRY = "Turkey",
                                          ISO3_RC = "TUR",
                                          gbd_region = "North Africa and Middle East")]

mer[RECIPIENT_COUNTRY == "Venezuela", `:=` (RECIPIENT_COUNTRY = "Venezuela (Bolivarian Republic of)",
                                            ISO3_RC = "VEN",
                                            gbd_region = "Central Latin America")]
# Fix GBD Regions
mer[ISO3_RC %in% c("TZA","QNB", "QPA","QPB"), gbd_region := "Eastern Sub-Saharan Africa"]
mer[ISO3_RC %in% c("CIV", "QPF", "COD"), gbd_region := "Central Sub-Saharan Africa"]
mer[ISO3_RC %in% c("QMZ", "KSV"), gbd_region := "Central Europe"]
mer[ISO3_RC %in% c("QRA"), gbd_region := "Caribbean"]
mer[ISO3_RC %in% c("QRD", "BOL"), gbd_region := "Andean Latin America"]
mer[ISO3_RC %in% c("QSE", "LAO"), gbd_region := "Southeast Asia"]
mer[ISO3_RC %in% c("QRC", "QSD", "QSA"), gbd_region := "South Asia"]
mer[ISO3_RC %in% c("QSF", "IRN", "PSE"), gbd_region := "North Africa and Middle East"]
mer[ISO3_RC %in% c("MDA"), gbd_region := "Eastern Europe"]
mer[ISO3_RC %in% c("PRK"), gbd_region := "East Asia"]
mer[ISO3_RC %in% c("SWZ"), gbd_region := "Southern Sub-Saharan Africa"]
mer[ISO3_RC %in% c("QTD"), gbd_region := "Southern Europe"]

# Final cleaning
mer[, `:=`(YEAR = disb_year,
           country_lc = NULL,
           countrycd = NULL)]
rm(isos)
#------------------------------------------------------------------------------#

#### #----#                   Tag double counting                    #----# ####
cat("  Tag double counting\n")
mer[PR_NAME == "Minist\xe8re de la Sant\xe9 Publique et de la Population (Unit\xe9 de Gestion des Projets)",
    PR_NAME := "Governmental Ministre de la Sante Publique et de la Population (Unite de Gestion des Projets)"]
mer[PR_NAME == "Association pour R\xe9silience des Communaut\xe9s vers l\x92Acc\xe8s au D\xe9veloppement et \xe0 la Sant\xe9 Plus",
    PR_NAME := "Association pour Resilience des Communautes vers les au Developpement et la Sante Plus"]
mer[PR_NAME == "Ministry of Health, Public Hygiene and Universal Health Coverage of the Republic of C\xf4te d\x92Ivoire",
    PR_NAME := "Ministry of Health, Public Hygiene and Universal Health Coverage of the Republic of Cote d Ivoire"]
mer[PR_NAME == "St. Petersburg charitable fund programs \x93Humanitarian action\x94",
    PR_NAME := "St. Petersburg charitable fund programs Humanitarian action"]

mer[, `:=`(RECIPIENT_AGENCY = toupper(PR_NAME),
           ELIM_CH = 0)]
mer[grepl("UNITED NATIONS CHILDREN", RECIPIENT_AGENCY), ELIM_CH := 1]
# If UNDP is ever added as a channel:
# mer[grepl("UNITED NATIONS DEVELOPMENT PROGRAMME", RECIPIENT_AGENCY), ELIM_CH := 1]
mer[RECIPIENT_AGENCY == "CATHOLIC RELIEF SERVICES, UNITED STATES CONFERENCE OF CATHOLIC BISHOPS (USCCB)", RECIPIENT_AGENCY := "CATHOLIC RELIEF SERVICES - UNITED STATES CONFERENCE OF CATHOLIC BISHOPS"]
mer[RECIPIENT_AGENCY == "CATHOLIC RELIEF SERVICES - MADAGASCAR", RECIPIENT_AGENCY := "CATHOLIC RELIEF SERVICES - UNITED STATES CONFERENCE OF CATHOLIC BISHOPS"]
mer[RECIPIENT_AGENCY == "POPULATION SERVICE INTERNATIONAL,", RECIPIENT_AGENCY := "POPULATION SERVICES INTERNATIONAL"]
mer[RECIPIENT_AGENCY == "SAVE THE CHILDREN", RECIPIENT_AGENCY := "SAVE THE CHILDREN FEDERATION, INC."]
# Merge us_ngos
mer[, m_m := 1]
us_ngo[, u_m := 2]
mer <- merge(mer, us_ngo, by="RECIPIENT_AGENCY", all=T)
mer[, merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[merge %in% c(1,3), ]
mer[merge == 3, ELIM_CH := 1]
mer[, `:=`(u_m = NULL, m_m = NULL, merge = NULL)]
# Merge intl_ngos
intl_ngo[, u_m := 2]
mer[, m_m := 1]
mer <- merge(mer, intl_ngo, by=c("RECIPIENT_AGENCY"), all=T)
mer[, merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[merge %in% c(1,3), ]
mer[merge == 3, ELIM_CH := 1]
mer[, `:=`(u_m = NULL, m_m = NULL, merge = NULL)]
mer[is.na(agency.x) & !is.na(agency.y), agency.x := agency.y]
mer[, agency.y := NULL]
setnames(mer, "agency.x", "agency")
rm(intl_ngo, us_ngo)
#------------------------------------------------------------------------------#

#### #----#                     Add CRS HFA data                     #----# ####
cat("  Add CRS HFA data\n")
# Read in CRS data & Clean
crs <- setDT(fread(paste0(get_path("CRS", "fin"), "common_channels/M_CRS_GFATM_2.csv")))
crs[, dummy := 1]
crs[, n := 1:sum(dummy), by="projectnumber"]
crs <- crs[n == 1, ]
crs[, `:=`(n = NULL, dummy = NULL)]
setnames(crs, c("projectnumber", "year"), c("PROJECT_ID", "YEAR"))
# Fix a few `mer` items
mer[, PROJECT_ID := str_replace_all(PROJECT_ID, "-00", "")]
# Merge
crs[, u_m := 2]
mer[, m_m := 1]
mer <- merge(mer, crs[, c("PROJECT_ID", "projecttitle", "shortdescription", "longdescription", "u_m")], by="PROJECT_ID", all=T)
mer[, crs_merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[crs_merge %in% c(1,3), !c("u_m", "m_m")]

# Add more CRS data
crs_extra <- mer[crs_merge == 1, c("PROJECT_ID", "projecttitle", "longdescription")]
crs_extra[, dummy := 1]
crs_extra[, n := 1:sum(dummy), by="PROJECT_ID"]
crs_extra <- crs_extra[n == 1, ]
setnames(crs_extra, c("projecttitle", "longdescription"), c("projecttitle_new", "longdescription_new"))
crs_extra[, dummy := NULL]
# Merge again
crs_extra[, u_m := 2]
mer[, m_m := 1]
mer <- merge(mer, crs_extra, by="PROJECT_ID", all=T)
mer[, merge := rowSums(mer[, c("u_m", "m_m")], na.rm=T)]
mer <- mer[merge %in% c(1,3), !c("u_m", "m_m")]
mer[merge == 3, `:=`(projecttitle = projecttitle_new, longdescription = longdescription_new)]
mer[, `:=`(projecttitle_new = NULL, longdescription_new = NULL, merge = NULL)]
rm(crs, crs_extra, NGO_DESCRIPTIVE_VARS)
#------------------------------------------------------------------------------#

#### #----#                     Pre-DEX Cleaning                     #----# ####
cat("  Pre-DEX Cleaning\n")
databeforehfas <- copy(mer)
databeforehfas[is.na(COMMITMENT), COMMITMENT := DISBURSEMENT_TOTAL]
# Tagging
databeforehfas[, `:=`(hiv = 0, mal = 0, tb = 0, swap_hss = 0)]
databeforehfas[DISEASE_COMPONENT == "HIV", hiv := 1]
databeforehfas[DISEASE_COMPONENT == "Malaria", mal := 1]
databeforehfas[DISEASE_COMPONENT == "Tuberculosis", tb := 1]
databeforehfas[DISEASE_COMPONENT == "TB/HIV", `:=`(tb = 0.5, hiv = 0.5)]
databeforehfas[DISEASE_COMPONENT == "Multicomponent", `:=`(tb = (1/3), hiv = (1/3), mal = (1/3))]
databeforehfas[DISEASE_COMPONENT == "RSSH", swap_hss := 1]
#------------------------------------------------------------------------------#

#### #----#                       Save Dataset                       #----# ####
cat("  Save dataset\n")
save_dataset(databeforehfas, paste0("data_before_hfas"), "GFATM", "int")
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

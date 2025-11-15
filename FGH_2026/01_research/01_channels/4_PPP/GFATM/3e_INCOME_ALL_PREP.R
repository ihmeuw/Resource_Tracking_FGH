#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Prepare INCOME_ALL dataset
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

## Local CONSTANTS
FGH_BASE <- paste0(
  dah.roots$j,
  "FILEPATH/FGH_",
  dah.roots$report_year,
  "/"
)
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ###############################\n"))
cat(green(" #### GFATM INCOME_ALL PREP ####\n"))
cat(green(" ###############################\n\n"))

#### #----#                     Import deflators                     #----# ####
cat("  Import deflators\n")
# Import, collapse, and rename
xrates <- fread(get_path(
    "meta", "rates",
    paste0("OECD_XRATES_NattoUSD_1950_", dah.roots$report_year, ".csv")
))
xrates[, YEAR := as.numeric(substr(TIME, 1, 4))]
xrates <- collapse(xrates, "mean", c("YEAR", "LOCATION", "Country"), "Value")
setnames(xrates, c("Value", "LOCATION"), c("EXCHRATE", "ISO3"))
xrates[ISO3 == "EA19", ISO3 := "FRA"]
# Generate currency column
xrates[ISO3 == "AUS", CURRENCY := "AUD"]
xrates[ISO3 == "CAN", CURRENCY := "CAD"]
xrates[ISO3 == "CHE", CURRENCY := "CHF"]
xrates[ISO3 == "DNK", CURRENCY := "DKK"]
xrates[ISO3 == "FRA", CURRENCY := "EUR"]
xrates[ISO3 == "GBR", CURRENCY := "GBP"]
xrates[ISO3 == "JPN", CURRENCY := "JPY"]
xrates[ISO3 == "NOR", CURRENCY := "NOK"]
xrates[ISO3 == "SWE", CURRENCY := "SEK"]
xrates[ISO3 == "NZL", CURRENCY := "NZD"]
xrates[ISO3 == "USA", CURRENCY := "USD"]
xrates[ISO3 == "IDN", CURRENCY := "IDR"]
xrates[ISO3 == "KOR", CURRENCY := "KRW"]
xrates <- xrates[CURRENCY != "" & YEAR > 2013, ]
#------------------------------------------------------------------------------#

#### #----#             Read in pledges + contributions              #----# ####
cat("  Read in pledges + contributions\n")
pc <- fread(paste0(
  get_path("GFATM", "raw"),
  "pledges_contributions_reference_rate_dataset_202526.csv"
), fill = TRUE)

pc <- pc %>%
  mutate(GeographyName = ifelse(DonorType1 == "Debt2Health", 
                                str_replace(DonorName, "Debt2Health - ", ""), GeographyName)) %>%
  mutate(GeographyName = ifelse(DonorType1 == "Debt2Health", 
                                str_replace(GeographyName, "-.*", ""), GeographyName)) %>%
  mutate(GeographyName = ifelse(DonorName == "Debt2Health - Jordan-Germany", 
                          "Germany", DonorName)) %>%
  mutate(GeographyName = ifelse(DonorName == "Debt2Health - Germany - El Salvador", 
                                "Germany", DonorName))

source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 1, release_id = 16)
locs <- locs[location_type == "admin0"]
locs <- select(locs, location_name, ihme_loc_id) %>%
  rename(GeographyName = location_name) %>%
  mutate(GeographyName = ifelse(GeographyName == "United States of America", "United States", GeographyName)) %>%
  mutate(GeographyName = ifelse(GeographyName == "Republic of Korea", "Korea (Republic)", GeographyName)) %>%
  mutate(GeographyName = ifelse(GeographyName == "Democratic Republic of the Congo", "Congo (Democratic Republic)", GeographyName)) %>%
  mutate(GeographyName = ifelse(GeographyName == "United Republic of Tanzania", "Tanzania (United Republic)", GeographyName)) %>%
  add_row(GeographyName = "Liechtenstein", ihme_loc_id = "LIE")
  

pc <- left_join(pc, locs, by = "GeographyName")

pc <- pc[IndicatorName != "", !c("ReplenishmentPeriod", "Month", "DonorType1")]
setnames(pc, c("DonorName", "ihme_loc_id", "GeographyName",
               "DonorType", "Currency_ReferenceRate", "Year", "IndicatorName", "Amount_ReferenceRate"),
         c("DONOR_NAME", "ISO3", "DONOR_COUNTRY", "INCOME_SECTOR", "CURRENCY",
           "YEAR", "Indicator", "Amount"))
# String cleaning
pc[DONOR_NAME == "United Arab<a0>Emirates",
   DONOR_NAME := "United Arab Emirates"]
pc[DONOR_NAME == "C<f4>te d'Ivoire", `:=`(DONOR_NAME = "Cote d'Ivoire",
                                          DONOR_COUNTRY = "Cote d'Ivoire")]
pc[DONOR_NAME == "Debt2Health - Germany-C<f4>te d'Ivoire",
   DONOR_NAME := "Debt2Health - Germany-Cote d'Ivoire"]
#------------------------------------------------------------------------------#

#### #----#          Clean donors, ISOs, and income sectors          #----# ####
cat("  Clean donors, ISOs, and income sectors\n")
# Remove AMFm from donor name
pc[, DONOR_NAME := gsub("AMFm ", "", DONOR_NAME)]
# Recode donor countries
pc[DONOR_NAME %in% c("Bill & Melinda Gates Foundation",
                     "Catholic Relief Services", "Chevron Corporation",
                     "Idol Gives Back", "LMI (Lutheran Malaria Initiative)",
                     "Product (RED)", "United Nations Foundation", "Vale",
                     "United Methodist Church", "Rockefeller Foundation",
                     "YMCA and Y's Men International"), 
   DONOR_COUNTRY := "United States"]
pc[DONOR_NAME %in% c("Anglo American plc", "Comic Relief", "United Kingdom",
                     "Human Crescent", "Children's Investment Fund Foundation",
                     "Co-Impact"), 
   DONOR_COUNTRY := "United Kingdom"]
pc[DONOR_NAME %in% c("BHP Billiton Sustainable Communities", "Duet Group",
                     "Rotary Australia World Community Service and Rotarians Against Malaria"), 
   DONOR_COUNTRY := "Australia"]
pc[DONOR_NAME %in% c("Canada", "M∙A∙C AIDS Fund", 
                     "Plan International and Plan Canada"),
   DONOR_COUNTRY := "Canada"]
pc[DONOR_NAME %in% c("Gift from Africa", "Goodbye Malaria - Sonhos Social 
                     Capital / Relate Trust ZA", "Standard Bank"),
   DONOR_COUNTRY := "South Africa"]
pc[DONOR_NAME %in% c("Takeda Pharmaceutical", 
                     "Hottokenai Campaign (G-CAP Coalition Japan)"),
   DONOR_COUNTRY := "Japan"]
pc[DONOR_NAME %in% c("Communitas Foundation"),
   DONOR_COUNTRY := "Bulgaria"]
pc[DONOR_NAME %in% c("Ecobank"),
   DONOR_COUNTRY := "Togo"]
pc[DONOR_NAME %in% c("Fullerton Health"),
   DONOR_COUNTRY := "Singapore"]
pc[DONOR_NAME %in% c("Munich Re"),
   DONOR_COUNTRY := "Germany"]
pc[DONOR_NAME %in% c("Nationale Postcode Loterij N.V.", "Cordaid"),
   DONOR_COUNTRY := "Netherlands"]
pc[DONOR_NAME %in% c("Tahir Foundation"),
   DONOR_COUNTRY := "Indonesia"]
pc[DONOR_NAME %in% c("Le Nu Thuy Dong"),
   DONOR_COUNTRY := "Viet Nam"]
pc[DONOR_NAME %in% c("FIFA Foundation"),
   DONOR_COUNTRY := "France"]
# Recode iso codes
pc[DONOR_NAME %in% c("Bill & Melinda Gates Foundation",
                     "Catholic Relief Services", "Chevron Corporation",
                     "Idol Gives Back", "LMI (Lutheran Malaria Initiative)",
                     "Product (RED)", "United Nations Foundation", "Vale",
                     "United Methodist Church",
                     "Hottokenai Campaign (G-CAP Coalition Japan)",
                     "Rockefeller Foundation",
                     "YMCA and Y's Men International"), 
   ISO3 := "USA"]
pc[DONOR_NAME %in% c("Anglo American plc", "Comic Relief", "United Kingdom",
                     "Human Crescent", "Children's Investment Fund Foundation",
                     "Co-Impact")
   , ISO3 := "GBR"]
pc[DONOR_NAME %in% c("BHP Billiton Sustainable Communities", "Duet Group",
                     "Rotary Australia World Community Service and Rotarians 
                     Against Malaria"),
   ISO3 := "AUS"]
pc[DONOR_NAME %in% c("Canada", "M?A?C AIDS Fund", 
                     "Plan International and Plan Canada"),
   ISO3 := "CAN"]
pc[DONOR_NAME %in% c("Gift from Africa", "Goodbye Malaria - Sonhos Social 
                     Capital / Relate Trust ZA", "Standard Bank"),
   ISO3 := "ZAF"]
pc[DONOR_NAME %in% c("Takeda Pharmaceutical"),
   ISO3 := "JPN"]
pc[DONOR_NAME %in% c("Communitas Foundation"),
   ISO3 := "BGR"]
pc[DONOR_NAME %in% c("Ecobank"),
   ISO3 := "TGO"]
pc[DONOR_NAME %in% c("Fullerton Health"),
   ISO3 := "SGP"]
pc[DONOR_NAME %in% c("Munich Re"),
   ISO3 := "DEU"]
pc[DONOR_NAME %in% c("Nationale Postcode Loterij N.V.", "Cordaid"),
   ISO3 := "NLD"]
pc[DONOR_NAME %in% c("Tahir Foundation"),
   ISO3 := "IDN"]
pc[DONOR_NAME %in% c("Le Nu Thuy Dong"),
   ISO3 := "VNM"]
pc[DONOR_NAME %in% c("FIFA Foundation"),
   ISO3 := "FRA"]
# Income sectors
pc[DONOR_NAME %in% c("Andorra", "Austria", "Barbados", "Burkina Faso",
                     "Georgia", "Kenya", "Kuwait", "Latvia", "Malawi",
                     "Malaysia", "Monaco", "Namibia", "Qatar", "Rwanda",
                     "Senegal", "Togo", "Tunisia", "Zimbabwe",
                     "Australia", "Belgium", "Brazil", "Brunei Darussalam",
                     "Cameroon", "Canada", "China", "Denmark", "Finland",
                     "France", "Germany", "Greece", "Hungary", "Iceland",
                     "India", "Ireland", "Italy", "Japan",
                     "Korea (Republic)", "Liechtenstein", "Luxembourg",
                     "Mexico", "Netherlands", "New Zealand", "Nigeria",
                     "Norway", "Poland", "Portugal", "Romania",
                     "Russian Federation", "Saudi Arabia", "Singapore",
                     "Slovenia", "South Africa", "Spain", "Benin",
                     "Sweden", "Switzerland", "Thailand", "Uganda",
                     "United Kingdom", "United States", "Other Countries",
                     "Estonia", "Zambia") |
     ISO3 == "CIV",
   INCOME_SECTOR := "PUBLIC"]
pc[DONOR_NAME %in% c("Anglo American plc",
                     "BHP Billiton Sustainable Communities",
                     "Bill & Melinda Gates Foundation",
                     "Catholic Relief Services", "Chevron Corporation",
                     "Comic Relief", "Communitas Foundation", "Duet Group",
                     "Ecobank", "Fullerton Health", "Gift from Africa",
                     "Goodbye Malaria - Sonhos Social Capital / 
                     Relate Trust ZA",
                     "Hottokenai Campaign (G-CAP Coalition Japan)",
                     "Idol Gives Back", "LMI (Lutheran Malaria Initiative)",
                     "M?A?C AIDS Fund", "Munich Re",
                     "Nationale Postcode Loterij N.V.", "Other Private Sector",
                     "Product (RED)", "Standard Bank", "Tahir Foundation",
                     "Takeda Pharmaceutical", "United Methodist Church",
                     "United Nations Foundation", "Vale"), 
    INCOME_SECTOR := "PRIVATE"]
pc[grepl("Debt2Health - ", DONOR_NAME), INCOME_SECTOR := "OTHER"]
pc[DONOR_NAME %in% c("European Commission", 
                     "World Health Organization-Unitaid"), 
   INCOME_SECTOR := "MULTI"]
pc[INCOME_SECTOR %in% c("Foundation", "Private Sector & Nongovernment"), 
   INCOME_SECTOR := "PRIVATE"]
pc[grepl("Public Sector", INCOME_SECTOR), INCOME_SECTOR := "PUBLIC"]
# Income types
pc[DONOR_NAME %in% c("Andorra", "Australia", "Austria", "Barbados", "Belgium",
                     "Benin", "Brazil", "Brunei Darussalam", "Burkina Faso",
                     "Cameroon", "Canada", "Chad", "China", "Denmark",
                     "Estonia", "Finland", "France", "Georgia", "Germany",
                     "Greece", "Hungary", "Iceland", "India", "Ireland",
                     "Italy", "Japan", "Kenya", "Korea (Republic)",
                     "Kuwait", "Latvia", "Liechtenstein", "Luxembourg",
                     "Mali", "Malawi", "Malaysia", "Mexico", "Monaco",
                     "Namibia", "Netherlands", "New Zealand", "Niger",
                     "Nigeria", "Norway", "Poland", "Portugal", "Qatar",
                     "Romania", "Russian Federation", "Rwanda", "Saudi Arabia",
                     "Senegal", "Singapore", "Slovenia", "South Africa",
                     "Spain", "Sweden", "Switzerland", "Thailand", "Togo",
                     "Tunisia", "Uganda", "United Arab Emirates",
                     "United Kingdom", "United States", "Zambia", "Zimbabwe",
                     "Congo (Democratic Republic)", "Armenia", "Burundi",
                     "Equatorial Guinea", "Azerbaijan", "Congo", "Madagascar",
                     "Eswatini","Ukraine", "Central African Republic", "Malta"),
   INCOME_TYPE := "CENTRAL"]
pc[ISO3 == "CIV", INCOME_TYPE := "CENTRAL"]
pc[DONOR_NAME %in% c("Anglo American plc",
                     "BHP Billiton Sustainable Communities",
                     "Bill & Melinda Gates Foundation", "Duet Group",
                     "Ecobank", "Gift from Africa", "Munich Re",
                     "Product (RED)", "Standard Bank", "Takeda Pharmaceutical",
                     "Vale", "Human Crescent", "Co-Impact", "Cordaid",
                     "Chevron Corporation"),
   INCOME_TYPE := "CORP"]
pc[DONOR_NAME == "Chevron Corporation" & YEAR %in% c(2010, 2011, 2012, 2013,
                                                     2014, 2015, 2016), 
   INCOME_TYPE := "CORP"]
pc[DONOR_NAME == "Tahir Foundation" & YEAR <= 2016,
   INCOME_TYPE := "CORP"]
pc[DONOR_NAME %in% c("Catholic Relief Services", "Communitas Foundation",
                     "Fullerton Health", "Goodbye Malaria - Sonhos Social 
                     Capital / Relate Trust ZA",
                     "Hottokenai Campaign (G-CAP Coalition Japan)",
                     "LMI (Lutheran Malaria Initiative)", "M?A?C AIDS Fund",
                     "Nationale Postcode Loterij N.V.",
                     "United Methodist Church",
                     "United Nations Foundation",
                     "Children's Investment Fund Foundation",
                     "Rotary Australia World Community Service and Rotarians 
                     Against Malaria",
                     "Rockefeller Foundation", "YMCA and Y's Men International",
                     "Plan International and Plan Canada", "FIFA Foundation"), 
   INCOME_TYPE := "FOUND"]
pc[DONOR_NAME == "Comic Relief" & YEAR %in% c(2014:dah.roots$report_year),
   INCOME_TYPE := "FOUND"]
pc[DONOR_NAME == "Tahir Foundation" & YEAR >= 2017, INCOME_TYPE := "FOUND"]
pc[(DONOR_NAME == "Chevron Corporation" & YEAR %in% c(2008, 2009)) |
     (DONOR_NAME == "Comic Relief" & YEAR %in% c(2009, 2010)) |
     DONOR_NAME == "Idol Gives Back", 
   INCOME_TYPE := "UNSP"]
pc[DONOR_NAME %in% c("Other Private Sector", "Le Nu Thuy Dong", "Other Public",
                     "Commitments to be personally secured by Bill Gates and 
                     Bono with the active support of France for the period 
                     2020-2022"), 
   INCOME_TYPE := "OTHER"]
pc[grepl("Debt2Health - ", DONOR_NAME), INCOME_TYPE := "OTHER"]
pc[DONOR_NAME %in% c("European Commission", "UNITAID", "Unitaid"),
   INCOME_TYPE := "EC"]
pc[DONOR_NAME %in% c("World Health Organization-Unitaid") & YEAR <= 2009,
   INCOME_TYPE := "UN"]
pc[DONOR_NAME %in% c("World Health Organization-Unitaid") & YEAR >= 2010,
   INCOME_TYPE := "OTHER"]

# Collapse sum
pc <- collapse(
  pc,
  "sum",
  c("DONOR_NAME", "ISO3", "DONOR_COUNTRY", "INCOME_SECTOR", "INCOME_TYPE", 
    "Indicator", "CURRENCY", "YEAR"),
  "Amount"
)
#------------------------------------------------------------------------------#

#### #----#                     Merge deflators                      #----# ####
cat("  Merge deflators\n")
xrates[, u_m := 2]
pc[, m_m := 1]
pc <- merge(pc, xrates, by = c("CURRENCY", "YEAR"), all = T)
pc[, merge := rowSums(pc[, c("u_m", "m_m")], na.rm = T)]
pc[, ISO3.y := NULL]
setnames(pc, "ISO3.x", "ISO3")
pc[ISO3 == "SGP" & YEAR >= 2017, EXCHRATE := 1.36]
pc[CURRENCY == "USD", EXCHRATE := 1]
pc <- pc[merge %in% c(1,3), !c("merge", "u_m", "m_m")]
rm(xrates)
#------------------------------------------------------------------------------#

#### #----#               Calculate amount & reshape                 #----# ####
cat("  Calculate Amount & Reshape\n")
pc[YEAR >= 2013 & CURRENCY != "" & !is.na(EXCHRATE), 
   Amount := Amount / EXCHRATE]
# Reshape
pc <- dcast(
  pc, 
  formula = "DONOR_NAME + ISO3 + DONOR_COUNTRY + INCOME_SECTOR + INCOME_TYPE + 
  CURRENCY + YEAR + Country + EXCHRATE ~ Indicator", 
  value.var = "Amount"
)
setnames(pc, c("Contribution - Reference Rate", "Pledge - Reference Rate"), 
         c("INCOME_REG_PAID", "INCOME_REG_PLEDGED"))
pc[, CHANNEL := "GFATM"]
#------------------------------------------------------------------------------#

#### #----#                       Save Dataset                       #----# ####
cat("  Save dataset\n")
save_dataset(pc, "P_GFATM_INCOME_ALL", "GFATM", "raw")
#------------------------------------------------------------------------------#
check <- fread(get_path("GFATM", "raw", "P_GFATM_INCOME_ALL.csv", report_year = 2023))


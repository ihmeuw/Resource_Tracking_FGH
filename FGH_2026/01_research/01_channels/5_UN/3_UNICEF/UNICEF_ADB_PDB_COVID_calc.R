## Purpose: Starting from FGH 2024, since covid data is no longer tracked 
## the funding amount should be subtracted from the total envelope.
##                                                                
##    1. Implemented a new pause line in the middle of 1_UNICEF_INC_EXP stata code
##       that saves aout a prep file.
##    2. Run this code which loads in that prep file. It should do the necessary
##       calculations, then save out the prep file in the same name
##    3. Resume running the stata code, and proceed with the rest of the pipeline
##                                                                            

# Load in the packages
rm(list = ls(all.names = TRUE))
library(data.table)
library(readxl)
library(dplyr)
library(tidyr)
library(readstata13)
library(haven)
library(stringr)
library(stringdist)
library(tidyverse)
library(crayon)
library(purrr)

# Set base paths based on the operating system
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH/"
  h <- paste0("/homes/", Sys.getenv("USER"), "/")
  k <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "J:/"
  h <- "H:/"
  k <- "K:/"
  l <- "L:/"
}

code_repo <- 'FILEPATH'
report_year <- 2024
data_year <- report_year - 1
source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

# set file paths. The FGH_<year> can be changed for FGH_2023 and beyond.
path_24 <- paste0(j, "FILEPATH")
WD <- paste0(j, "FILEPATH")
income_sector_type <- data.frame(read_csv(paste0(WD, "FILEPATH/income_sector_and_type_assignments.csv")))

# pull in the data. Note the ADB_PDB data names will change every year
covid_prepped <- data.table(read.csv(paste0(path_24, "COVID_prepped.csv")))
ADB_PDB <- data.table(read.dta13(paste0(path_24, "UNICEF_ADB_PDB_FGH", report_year, "_prep.dta")))

ADB_PDB[DONOR_NAME %like% "OTHER" & ! DONOR_NAME %like% "PRIVATE", unique(DONOR_NAME)]
ADB_PDB[DONOR_NAME %in% c("OTHER INCOME", "OTHERS, RR", "OTHERS, REGULAR RESOURCES", "OTHER REVENUE", "OTHER") &
            EBOLA == 0,
        `:=`(
            DONOR_NAME = "OTHER REVENUE",
            DONOR_COUNTRY = NA_character_,
            ISO_CODE = NA_character_,
            INCOME_SECTOR = "OTHER",
            INCOME_TYPE = "OTHER"
        )]
ADB_PDB <- ADB_PDB[, .(
    OUTFLOW = sum(OUTFLOW, na.rm = TRUE),
    oid_ebz_DAH = sum(oid_ebz_DAH, na.rm = TRUE)
), by = .(
    YEAR, CHANNEL, INCOME_SECTOR, INCOME_TYPE, DONOR_NAME, DONOR_COUNTRY, ISO_CODE,
    SOURCE_CH, REPORTING_AGENCY, EBOLA
)]

covid_prepped <- covid_prepped %>%
  select(year, donor_name, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, total_amt) %>%
  mutate(ISO_CODE = case_when(
    ISO_CODE == "" ~ "NA",
    TRUE ~ ISO_CODE
  ))

covid_prepped <- covid_prepped %>%
    group_by(year, donor_name, INCOME_SECTOR, INCOME_TYPE, ISO_CODE) %>%
    summarise(total_amt = sum(total_amt, na.rm = TRUE),
              .groups = "drop")

covid_prepped <- covid_prepped %>%
  mutate(donor_name = case_when(
    donor_name ==  "European Commission's Humanitarian Aid and Civil Protection Department" ~ "EUROPEAN COMMISSION HUMANITARIAN AID CIVIL PROTECTION",
    # donor_name == "UNICEF National Committee/Netherlands"                                 
    donor_name == "UNICEF National Committee/Spain" ~ "UNICEF NATIONAL COMMITTEE SPAIN",                                       
    donor_name == "United States Fund for UNICEF" ~ "UNITED STATES FUND FOR UNICEF",                                          
    donor_name == "African Development Bank (as secondary donors)" ~ "AFRICAN DEVELOPMENT BANK AS SECONDARY DONORS",                        
    donor_name == "Asian Development Bank" ~ "ASIAN DEVELOPMENT BANK",                                                
    donor_name == "Burkina Faso" ~ "BURKINA FASO",                                                          
    donor_name == "Dominican Republic"   ~ "DOMINICAN REPUBLIC",                                                 
    donor_name == "GAVI The Vaccine Alliance" ~ "GAVI THE VACCINE ALLIANCE",                                             
    donor_name == "Global Partnership for Education" ~ "GLOBAL PARTNERSHIP FOR EDUCATION",                                      
    donor_name == "Liechtenstein" ~ "LIECHTENSTEIN",                                                         
    donor_name == "Private Sector" ~ "PRIVATE SECTOR",                                                       
    donor_name == "South Korea" ~ "SOUTH KOREA",                                                           
    donor_name == "United Arab Emirates" ~ "UNITED ARAB EMIRATES",                                                 
    donor_name == "United Nations Multi Partner Trust\xa0" ~ "UNITED NATIONS MULTI PARTNER TRUST",                                
    donor_name == "United Nations Secretariat" ~ "UN SECRETARIAT",                                            
    donor_name == "WFP - Italy" ~ "WFP ITALY",                                                           
    donor_name == "World Bank (as secondary donor)" ~ "WORLD BANK AS SECONDARY DONOR",                                       
    donor_name == "Bulgaria" ~ "BULGARIA",                                                              
    donor_name == "Central Emergency Response Fund" ~ "CENTRAL EMERGENCY RESPONSE FUND",                                       
    donor_name == "China" ~ "CHINA",                                                                 
    donor_name == "COVID-19 Humanitarian Thematic Fund" ~ "COVID 19 HUMANITARIAN THEMATIC FUND",                                   
    donor_name == "COVID-19 Solidarity Response Fund" ~ "COVID 19 SOLIDARITY RESPONSE FUND",                                     
    donor_name == "Ethiopia Humanitarian Fund" ~ "ETHIOPIA HUMANITARIAN FUND",                                            
    donor_name == "GAVI Alliance" ~ "GAVI ALLIANCE",                                                         
    donor_name == "Korea, Republic of" ~ "KOREA REPUBLIC OF",                                                    
    donor_name == "occupied Palestinian territory Humanitarian Fund" ~ "OCCUPIED PALESTINIAN TERRITORY HUMANITARIAN FUND",                      
    donor_name == "Poland" ~ "POLAND",                                                               
    donor_name == "Private (individuals & organizations)"  ~ "PRIVATE INDIVIDUALS ORGANIZATIONS",                               
    donor_name == "The Pacific Community (former South Pacific Commission)" ~ "THE PACIFIC COMMUNITY FORMER SOUTH PACIFIC COMMISSION",               
    donor_name == "Ukraine Humanitarian Fund" ~ "UKRAINE HUMANITARIAN FUND",                                             
    donor_name == "UN COVID-19 Response and Recovery Fund" ~ "UN COVID 19 RESPONSE AND RECOVERY FUND",                                
    donor_name == "UNICEF National Committee/Australia" ~ "UNICEF NATIONAL COMMITTEE AUSTRALIA",                                   
    donor_name == "UNICEF National Committee/Canada" ~ "UNICEF NATIONAL COMMITTEE CANADA",                                      
    donor_name == "UNICEF National Committee/Denmark" ~ "UNICEF NATIONAL COMMITTEE DENMARK",                                     
    donor_name == "UNICEF National Committee/France" ~ "UNICEF NATIONAL COMMITTEE FRANCE",                                      
    donor_name == "UNICEF National Committee/Germany" ~ "UNICEF NATIONAL COMMITTEE GERMANY",                                     
    donor_name == "UNICEF National Committee/Hong Kong" ~ "UNICEF NATIONAL COMMITTEE HONG KONG",                                   
    # donor_name == "UNICEF National Committee/Italy" ~ ""                                       
    donor_name == "UNICEF National Committee/Japan" ~ "UNICEF NATIONAL COMMITTEE JAPAN",                                       
    donor_name == "UNICEF National Committee/Luxembourg" ~ "UNICEF NATIONAL COMMITTEE LUXEMBOURG",                                  
    # donor_name == "UNICEF National Committee/Norway" ~ ""                                      
    # donor_name == "UNICEF National Committee/Portugal"                                    
    donor_name == "UNICEF National Committee/Saudi Arabia" ~ "UNICEF NATIONAL COMMITTEE SAUDI ARABIA",                                
    donor_name == "UNICEF National Committee/United Kingdom" ~ "UNICEF NATIONAL COMMITTEE UNITED KINGDOM",                              
    donor_name == "United Nations Children's Fund" ~ "UNITED NATIONS CHILDREN S FUND",                                        
    donor_name == "United Nations Educational, Scientific and Cultural Organization" ~ "UNITED NATIONS EDUCATIONAL SCIENTIFIC AND CULTURAL ORGANIZATION",      
    donor_name == "World Bank" ~ "WORLD BANK",                                                            
    donor_name == "World Health Organization" ~ "WORLD HEALTH ORGANIZATION",                                             
    donor_name == "Yemen Humanitarian Fund" ~ "YEMEN HUMANITARIAN FUND",
    TRUE ~ donor_name
  ))

setDT(covid_prepped)
covid_prepped <- covid_prepped[, .(oid_covid_OUTFLOW = sum(total_amt, na.rm = TRUE)),
                               by = .(year, donor_name, ISO_CODE,
                                      INCOME_SECTOR, INCOME_TYPE)]

setDT(ADB_PDB)
nrow(ADB_PDB)
nrow(unique(ADB_PDB[, .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE)]))

# find which donor names are close, but not exact matches
covid_years <- ADB_PDB %>%
  filter(YEAR %in% c(2020, 2021, 2022))


# Assuming covid_years$DONOR_NAME and covid_prepped$donor_name are character vectors
donor_names <- unique(covid_years$DONOR_NAME)
prepped_names <- unique(covid_prepped$donor_name)

# Compute the distance matrix using the Jaro-Winkler method
dist_matrix <- stringdistmatrix(donor_names, prepped_names, method = "jw")

# Set a threshold for what you consider a close match, e.g., distance less than 0.2
threshold <- 0.2

# Find indices of the dist_matrix that have a distance below the threshold
close_matches_indices <- which(dist_matrix < threshold, arr.ind = TRUE)

# Extract the matching names
close_matches <- lapply(seq_len(nrow(close_matches_indices)), function(i) {
  list(donor_name = donor_names[close_matches_indices[i, 1]],
       prepped_name = prepped_names[close_matches_indices[i, 2]],
       distance = dist_matrix[close_matches_indices[i, 1], close_matches_indices[i, 2]])
})

# Optionally, convert the list of close matches to a data frame
close_matches_df <- do.call(rbind, lapply(close_matches, function(x) data.frame(t(unlist(x)))))

covid_prepped[, donor_name := toupper(donor_name)]
#now fix names to merge on to the prep file
covid_prepped <- covid_prepped %>%
  mutate(donor_name = case_when(
    donor_name == "ASIAN DEVELOPMENT BANK" ~ "AsDB",
    donor_name == "United States" ~ "UNITED STATES",
    donor_name == "European Commission" ~ "EUROPEAN COMMISSION",
    donor_name == "UNITED NATIONS EDUCATIONAL SCIENTIFIC AND CULTURAL ORGANIZATION" ~ "UNITED NATIONS EDUCATIONAL, SCIENTIFIC AND CULTURAL ORGANIZATION (UNESCO)",    
    donor_name == "GAVI THE VACCINE ALLIANCE" ~ "GAVI, THE VACCINE ALLIANCE",
    donor_name == "UNITED NATIONS MULTI PARTNER TRUST" ~ "UNITED NATIONS MULTI PARTNER TRUST FUND",
    ISO_CODE == "CHN" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM CHINA",
    ISO_CODE == "ARE" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM UNITED ARAB EMIRATES",
    ISO_CODE == "IRL" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM IRELAND",
    ISO_CODE == "MEX" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM MEXICO",
    ISO_CODE == "NA" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM MISCELLANEOUS",
    ISO_CODE == "QAT" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM QATAR",
    ISO_CODE == "SGP" & donor_name == "PRIVATE INDIVIDUALS ORGANIZATIONS" ~ "OTHER PRIVATE DONOR FROM SINGAPORE",
    ISO_CODE == "NA" & donor_name == "PRIVATE SECTOR" ~ "PRIVATE SECTOR DIVISION",
    donor_name == "PRIVATE SECTOR" ~ "World Bank",
    donor_name == "UNOPS - NEW YORK" ~ "UNITED NATIONS OFFICE FOR PROJECT SERVICES (UNOPS)",
    donor_name == "UNESCO" & year == 2021 ~ "THE UNITED NATIONS EDUCATIONAL, SCIENTIFIC AND CULTURAL ORGANIZATION (UNESCO)",
    donor_name == "UNDP USA" ~ "UNITED NATIONS DEVELOPMENT PROGRAMME (UNDP)",
    donor_name %like% "HUMANITARIAN THEMATIC FUND|HUMANITARIAN FUND|RESPONSE FUND|RECOVERY FUND" ~ "UNITED NATIONS MULTI PARTNER TRUST FUND",
    donor_name == "KOREA REPUBLIC OF" ~ "REPUBLIC OF KOREA",
    donor_name %ilike% "UNITED STATES" ~ "UNITED STATES",
    donor_name %like% "CANADA" ~ "CANADA",
    donor_name %like% "DENMARK" ~ "DENMARK",
    donor_name %like% "FRANCE" ~ "FRANCE",
    donor_name %like% "GERMANY" ~ "GERMANY",
    donor_name %like% "HONG KONG" ~ "OTHER PRIVATE DONOR FROM HONG KONG, CHINA",
    donor_name %like% "JAPAN" ~ "JAPAN",
    donor_name %like% "LUXEMBOURG" ~ "LUXEMBOURG",
    donor_name %like% "SAUDI ARABIA" ~ "SAUDI ARABIA",
    donor_name %like% "SPAIN" ~ "SPAIN",
    donor_name %like% "UNITED KINGDOM" ~ "UNITED KINGDOM",
    donor_name %like% "ITALY" ~ "ITALY",
    donor_name %like% "PORTUGAL" ~ "PORTUGAL",
    donor_name %like% "NETHERLANDS" ~ "NETHERLANDS",
    donor_name %like% "NORWAY" ~ "NORWAY",
    donor_name %like% "EUROPEAN COMMISSION" ~ "EUROPEAN COMMISSION",
    donor_name %like% "AUSTRALIA" ~ "AUSTRALIA",
    donor_name %like% "SOUTH KOREA" ~ "REPUBLIC OF KOREA",
    donor_name %like% "GAVI" ~ "GAVI, THE VACCINE ALLIANCE",
    donor_name %like% "WORLD HEALTH ORG" ~ "WHO",
    donor_name == "OTHER PRIVATE DONOR FROM MISCELLANEOUS" ~ "OTHER REVENUE",
    donor_name == "PRIVATE SECTOR DIVISION" ~ "OTHER REVENUE",
    donor_name == "UNOCHA" ~ "OFFICE FOR THE COORDINATION OF HUMANITARIAN AFFAIRS (OCHA)",
    donor_name == "UN SECRETARIAT" ~ "UNITED NATIONS OFFICE FOR PROJECT SERVICES (UNOPS)",
    donor_name == "BURKINA FASO" & year == 2021 ~ "OTHER REVENUE",
    TRUE ~ donor_name
  ))

covid_prepped[donor_name == "OTHER PRIVATE DONOR FROM MISCELLANEOUS", donor_name := "OTHER REVENUE"]
covid_prepped[donor_name == "PRIVATE SECTOR DIVISION", donor_name := "OTHER REVENUE"]


## after all this, there are a few donors that have no matches in the ADBPDB data
setDT(covid_prepped)
covid_prepped[! donor_name %in% unique(ADB_PDB$DONOR_NAME), unique(donor_name)]
covid_prepped[! donor_name %in% unique(ADB_PDB$DONOR_NAME),
              donor_name := "OTHER REVENUE"]

covid_prepped <- covid_prepped[, .(oid_covid_OUTFLOW = sum(oid_covid_OUTFLOW)),
                               by = .(year, donor_name)]


ADB_PDB_2 <- merge(
  ADB_PDB, covid_prepped,
  by.x = c("YEAR", "DONOR_NAME"),
  by.y = c("year", "donor_name"),
  all.x = TRUE  ## left-join the covid values onto the main data set
)

nrow(ADB_PDB_2) == nrow(ADB_PDB)  # should be TRUE, since merge vars uniquely ID rows
ADB_PDB_2[!is.na(oid_covid_OUTFLOW), .N] == nrow(covid_prepped)  # should be TRUE, since all covid observations need to merge

matched_donors <- ADB_PDB_2[!is.na(oid_covid_OUTFLOW), unique(DONOR_NAME)]
covid_prepped[! donor_name %in% matched_donors]


# This prints out both the MATCHED cases AND  THE UNMATCHED CASES! use it to
# determine whether
#   a. Attempt 100% match again
#   b. Work with data as it is, and drop the unmmatched ones
#   c. Do a full merge, and add to the top of total envelope
covid_matched <- ADB_PDB_2 %>%
  filter(YEAR %in% c(2020, 2021, 2022) & !is.na(oid_covid_OUTFLOW))

covid_unmatched <- ADB_PDB_2 %>%
  filter(YEAR %in% c(2020, 2021, 2022) & is.na(oid_covid_OUTFLOW))

setnafill(ADB_PDB_2, fill = 0, cols = "oid_covid_OUTFLOW")
ADB_PDB_2[, diff := OUTFLOW - oid_covid_OUTFLOW]
ADB_PDB_2[diff < 0 & oid_covid_OUTFLOW > 0,
          oid_covid_OUTFLOW := OUTFLOW]
ADB_PDB_2[, diff := NULL]


ADB_PDB_2[DONOR_NAME == "NETHERLANDS", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", ISO_CODE = "NLD"
)]
ADB_PDB_2[DONOR_NAME == "ISRAEL", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", ISO_CODE = "ISR"
)]
ADB_PDB_2[DONOR_NAME == "GERMANY", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", ISO_CODE = "DEU"
)]
ADB_PDB_2[DONOR_NAME == "SPAIN", `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", ISO_CODE = "ESP"
)]


ADB_PDB_2 <- ADB_PDB_2[, .(
    OUTFLOW = sum(OUTFLOW, na.rm = TRUE),
    oid_ebz_DAH = sum(oid_ebz_DAH, na.rm = TRUE),
    oid_covid_OUTFLOW = sum(oid_covid_OUTFLOW, na.rm = TRUE)
), by = .(
    YEAR, CHANNEL, INCOME_SECTOR, INCOME_TYPE, DONOR_NAME, DONOR_COUNTRY, ISO_CODE,
    SOURCE_CH, REPORTING_AGENCY, EBOLA
)]

write_dta(ADB_PDB_2, paste0(get_path("UNICEF", "FIN"), "UNICEF_ADB_PDB_FGH_prepfix.dta"))
# Return to stata


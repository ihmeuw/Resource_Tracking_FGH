#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Reformat Commitment & Disbursement Data
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions
report_year <- 2024
source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
library(lubridate)

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

#### #----#                Import Disbursement Data                  #----# ####
cat("  Import Disbursement Data\n")
## Read in the dataset downloaded directly from 
## https://data-service.theglobalfund.org/downloads
prev_disb <- fread(
  get_path("GFATM", "raw", 
           paste0("P_GFATM_Disbursements_in_Detail_", "2024-02-05", ".csv"),
           report_year = 2023))

disbkey <- distinct(prev_disb, GeographicAreaName, GeographicAreaCode_ISO3, GeographicAreaLevelName)
multkey <- prev_disb %>%
  filter(MultiCountryName != "") %>%
  distinct(MultiCountryName, GeographicAreaName, GeographicAreaCode_ISO3, GeographicAreaLevelName) %>%
  filter(!(MultiCountryName == "Multicountry Eastern Africa IGAD" & GeographicAreaName == "Africa")) %>%
  filter(!(MultiCountryName == "Multicountry HIV SEA AFAO" & GeographicAreaName == "Asia"))
titlekey <- prev_disb %>%
  filter(GrantAgreementTitle != "") %>%
  distinct(GrantAgreementNumber, GrantAgreementTitle) %>%
  group_by(GrantAgreementNumber) %>%
  slice(1)
datekey <- prev_disb %>%
  distinct(GrantAgreementNumber, ProgramStartDate, ProgramEndDate)

new_disb <- fread(
  get_path("GFATM", "raw", 
           paste0("P_GFATM_Disbursements_in_Detail_preformat_", 
                  get_dah_param("GFATM", "disbursements_date"), ".csv")))

colnames_fixed <- new_disb %>%
  rename(GeographicAreaName = GeographyName1) %>%
  rename(ComponentName = GrantComponent1) %>%
  rename(GrantAgreementNumber = Grant1) %>%
  rename(GrantAgreementStatusTypeName = Status1) %>%
  mutate(DisbursementDate = dmy(DisbursementDate), DisbursementYear = year(DisbursementDate)) %>%
  rename(PrincipalRecipientName = PrincipalRecipient1) %>%
  mutate(ImplementationPeriodName = "Implementation Period") %>%
  mutate(ImplementationPeriodNumber = str_trim(str_replace(GrantCycleName, "Grant Cycle", ""))) %>%
  mutate(ImplementationPeriodStartDate = ImplementationPeriodStartDate1) %>%
  mutate(ImplementationPeriodEndDate = ImplementationPeriodEndDate1) %>%
  mutate(DisbursementAmount = DisbursementAmount_ReferenceRate) %>%
  select(all_of(selected_columns))

# Merges area location on from past dataset
disb_areacodes <- colnames_fixed %>%
  mutate(GeographicAreaName = ifelse(
    GeographicAreaName == "Venezuela (Bolivarian Republic)", "Venezuela", GeographicAreaName)) %>%
  left_join(disbkey, by = "GeographicAreaName")

# above process doesn't catch multicountry--correct rows for multicountry areas
multicountry <- disb_areacodes %>%
  filter(is.na(GeographicAreaCode_ISO3)) %>%
  rename(MultiCountryName = GeographicAreaName) %>%
  select(-GeographicAreaCode_ISO3, -GeographicAreaLevelName) %>%
  left_join(multkey, by = "MultiCountryName")

# Adds correct rows back onto disb_areacodes
final_dt <- disb_areacodes %>%
  filter(!is.na(GeographicAreaCode_ISO3)) %>%
  rbind(multicountry, fill = T) %>%
  mutate(MultiCountryName = replace_na(MultiCountryName, "")) %>%
  left_join(datekey, by = "GrantAgreementNumber") %>%
  left_join(titlekey, by = "GrantAgreementNumber")

save_dataset(final_dt, 
             paste0("P_GFATM_Disbursements_in_Detail_", get_dah_param("GFATM", "disbursements_date"), ".csv"),
             "GFATM", "raw")
rm(multicountry, multkey, datekey, disbkey, new_disb, newdisb_area, titlekey, prev_disb,
   disb_areacodes, final_dt, colnames_fixed)
#### #----#                Import Disbursement Data                  #----# ####
cat("  Import Disbursement Data\n")
## Read in the dataset downloaded directly from 
## https://data-service.theglobalfund.org/downloads
prev_grants <- fread(
  get_path("GFATM", "raw", 
           paste0("P_GFATM_PDB_GRANTS_DETAIL_", "2024-02-05", ".csv"),
           report_year = 2023))

areakey <- distinct(prev_grants, GeographicAreaName, GeographicAreaCode_ISO3, GeographicAreaLevelName)
multkey <- prev_grants %>%
  filter(MultiCountryName != "") %>%
  distinct(MultiCountryName, GeographicAreaName, GeographicAreaCode_ISO3, GeographicAreaLevelName) %>%
  filter(!(MultiCountryName == "Multicountry Eastern Africa IGAD" & GeographicAreaName == "Africa")) %>%
  filter(!(MultiCountryName == "Multicountry HIV SEA AFAO" & GeographicAreaName == "Asia"))
titlekey <- prev_grants %>%
  filter(GrantAgreementTitle != "") %>%
  distinct(GrantAgreementNumber, GrantAgreementTitle) %>%
  group_by(GrantAgreementNumber) %>%
  slice(1)
datekey <- prev_grants %>%
  distinct(GrantAgreementNumber, ProgramStartDate, ProgramEndDate)

new_grants <- fread(
  get_path("GFATM", "raw", 
           paste0("P_GFATM_PDB_GRANTS_DETAIL_preformat_2025-01-15.csv")))

agg <- new_grants %>%
  rename(GeographicAreaName = GeographyName1) %>%
  rename(ComponentName = GrantComponent1) %>%
  rename(GrantAgreementNumber = Grant1) %>%
  rename(GrantAgreementStatusTypeName = Status1) %>%
  rename(PrincipalRecipientName = PrincipalRecipient1) %>%
  rename(PrincipalRecipientClassificationName = PrincipalRecipientType1) %>%
  rename(PrincipalRecipientSubClassificationName = PrincipalRecipientSubType1) %>%
  rename(Currency = Currency_ReferenceRate) %>%
  group_by(GrantAgreementNumber, ComponentName,  GeographicAreaName, PrincipalRecipientName,
           PrincipalRecipientClassificationName, PrincipalRecipientSubClassificationName) %>%
  summarise(TotalCommittedAmount = sum(CommitmentAmount_ReferenceRate)) %>%
  mutate(GeographicAreaName = ifelse(
    GeographicAreaName == "Venezuela (Bolivarian Republic)", "Venezuela", GeographicAreaName)) %>%
  left_join(areakey, by = "GeographicAreaName")
  
multifix <- agg %>%
  filter(is.na(GeographicAreaCode_ISO3)) %>%
  rename(MultiCountryName = GeographicAreaName) %>%
  select(-GeographicAreaCode_ISO3, -GeographicAreaLevelName) %>%
  left_join(multkey, by = "MultiCountryName")

final_dt <- agg %>%
  filter(!is.na(GeographicAreaCode_ISO3)) %>%
  rbind(multifix) %>%
  left_join(datekey, by = "GrantAgreementNumber") %>%
  left_join(titlekey, by = "GrantAgreementNumber")

#Use this to fill in regional grant projects csv
check <- final_dt %>%
  filter(GeographicAreaLevelName %in% c("Subnational", "Continent", "Sub-continent", "World")) %>%
  filter(GrantAgreementNumber %ni% prev_grants$GrantAgreementNumber)

save_dataset(final_dt, 
             paste0("P_GFATM_PDB_GRANTS_DETAIL_", get_dah_param("GFATM", "disbursements_date"), ".csv"),
             "GFATM", "raw")

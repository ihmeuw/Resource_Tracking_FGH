## Purpose: Starting from FGH 2024, since covid data is no longer tracked 
## the funding amount should be subtracted from the total envelope.
##                                                                
##    1. I've implemented a new pause line in the middle of 1_PAHO_INC_EXP stata code
##       that saves aout a prep file.
##    2. Run this code which loads in that prep file. It should do the necessary
##       calculations, then save out the prep file in the same name
##    3. Resume running the stata code, and proceed with the rest of the pipeline
##                                                                            


# Load in the packages
rm(list = ls())
library(data.table)
library(readxl)
library(dplyr)
library(tidyr)
library(readstata13)
library(haven)
library(stringr)
library(tidyverse)
library(crayon)
library(purrr)

code_repo <- 'FILEPATH'
report_year <- 2024
data_year <- report_year - 1
source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
income_sector_type <- fread(get_path("meta", "donor", "income_sector_and_type_assignments.csv"))

# pull in the data. Note the ADB_PDB data names will change every year
covid_prepped <- fread(get_path("PAHO", "fin", "COVID_prepped.csv"))
INC_EXP <- read.dta13(get_path("PAHO", "fin", paste0("PAHO_INC_EXP_1990_", data_year, ".dta")))

covid_prepped <- covid_prepped %>%
  select(YEAR, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, TOTAL_AMT) %>%
  mutate(ISO_CODE = case_when(
    ISO_CODE == "" ~ "NA",
    TRUE ~ ISO_CODE
  ))

covid_prepped <- covid_prepped %>%
  mutate(DONOR_NAME = case_when(
    DONOR_NAME == "FOUNDATION YAMUNI TABUSH" ~ "FUNDACION YAMUNI TABUSH",
    TRUE ~ DONOR_NAME
  ))

covid_prepped <- covid_prepped %>%
  left_join(income_sector_type, by = c("DONOR_NAME", "ISO_CODE")) %>%
  mutate(
    INCOME_SECTOR = if_else(is.na(INCOME_SECTOR.y), INCOME_SECTOR.x, INCOME_SECTOR.y),
    INCOME_TYPE = if_else(is.na(INCOME_TYPE.y), INCOME_TYPE.x, INCOME_TYPE.y)
  ) %>%
  select(-c(INCOME_SECTOR.x, INCOME_TYPE.x, INCOME_SECTOR.y, INCOME_TYPE.y, IHME_NAME))

setDT(INC_EXP); setDT(covid_prepped)
nrow(INC_EXP)
nrow(unique(INC_EXP[, .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE)]))

INC_EXP[ISO_CODE == "", ISO_CODE := NA]

## aggregate so that now the rows are uniquely identified by the grouping variables
covid_prepped <- covid_prepped %>%
  mutate(DONOR_NAME = case_when(
    DONOR_NAME == "BELIZE" ~ "Belize",
    DONOR_NAME == "CANADA" ~ "Canada",
    DONOR_NAME == "COLOMBIA" ~ "Colombia",
    DONOR_NAME == "UNITED KINGDOM" ~ "United Kingdom",
    DONOR_NAME == "UNITED STATES OF AMERICA" ~ "United States",
    DONOR_NAME == "WHO FOUNDATION" ~ "WHO",
    TRUE ~ "NA"
  ),
  ISO_CODE = case_when(
    DONOR_NAME == "NA" ~  "NA",
    TRUE ~ ISO_CODE
  ),
  INCOME_SECTOR = case_when(
    DONOR_NAME == "NA" ~ "OTHER",
    is.na(INCOME_SECTOR) ~ "OTHER",
    TRUE ~ INCOME_SECTOR
  ),
  INCOME_TYPE = case_when(
    DONOR_NAME == "NA" ~ "NA",
    is.na(INCOME_TYPE) ~ "NA",
    TRUE ~ INCOME_TYPE
  ))


covid_prepped <- covid_prepped[, .(oid_covid_DAH = sum(TOTAL_AMT, na.rm = TRUE)),
                               by = .(YEAR, DONOR_NAME, ISO_CODE,
                                      INCOME_SECTOR, INCOME_TYPE)]

covid_prepped <- covid_prepped %>%
  mutate(ISO_CODE = case_when(
    YEAR == 2020 & DONOR_NAME == "NA" ~ NA,
    YEAR == 2021 & DONOR_NAME == "NA" ~ NA,
    YEAR == 2020 & DONOR_NAME == "WHO" ~ NA,
    YEAR == 2021 & DONOR_NAME == "WHO" ~ NA,
    TRUE ~ ISO_CODE
  ))

INC_EXP_2 <- merge(
  INC_EXP, covid_prepped,
  by = c("YEAR", "DONOR_NAME", "ISO_CODE",
         "INCOME_SECTOR", "INCOME_TYPE"),
  all.x = TRUE  ## left-join the covid values onto the main data set
)
nrow(INC_EXP_2) == nrow(INC_EXP)  # should be TRUE, since merge vars uniquely ID rows
INC_EXP_2[!is.na(oid_covid_DAH), .N] == nrow(covid_prepped)  # should be TRUE, since all covid observations need to merge

# now proceed with covid subtraction
setnafill(INC_EXP_2, fill = 0, cols = "oid_covid_DAH")
INC_EXP_3 <- INC_EXP_2 %>%
  mutate(OUTFLOW = ifelse(OUTFLOW < oid_covid_DAH, oid_covid_DAH, OUTFLOW)) %>%
    mutate(OUTFLOW = OUTFLOW - oid_covid_DAH)

save_dataset(INC_EXP_2, "PAHO_INC_EXP_1990_2023_fix.dta", "PAHO", "FIN", format = "dta")

# Return to stata, run until ADB_PDB prep data gets created, then pull that in here for further cleaning
ADB_PDB <- read_dta(get_path("PAHO", "FIN", paste0("PAHO_ADB_PDB_FGH", report_year, "_prep.dta")))
setnafill(ADB_PDB, fill = 0, cols = "oid_covid_DAH")
ADB_PDB_2 <- ADB_PDB %>%
  mutate(DAH = DAH + oid_covid_DAH)

write_dta(ADB_PDB_2, paste0(get_path("PAHO", "FIN"), "PAHO_ADB_PDB_FGH", report_year, "_prepfix.dta"))


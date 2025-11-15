#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Rescales data
#------------------------#

#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}
source(paste0(code_repo, 'FGH_2024/utils.R'))
#----------------------------# ####
prev_yr <- read_dta("FILEPATH")

prev_covid <- read_dta("FILEPATH")

old_data <- read_dta("FILEPATH")
mid_data <- read_dta("FILEPATH")
new_data <- read_dta("FILEPATH")

prev_sum <- prev_yr %>%
  bind_rows(prev_covid) %>%
  group_by(YEAR) %>%
  summarize(prevtotal = sum(amount_split))
# add covid onto here!

new_sum <- new_data %>%
  group_by(YEAR) %>%
  summarize(newtotal = sum(amount_split))

bothmethods <- merge(prev_sum, new_sum, by= "YEAR") %>%
  mutate(ratio = newtotal/prevtotal)

scaler <- mean(bothmethods[bothmethods$YEAR %in% c(2020:2021),]$ratio)

old_revised <- old_data %>%
  mutate(gm_name = toupper(gm_name)) %>%
  mutate(scale = scaler) %>%
  mutate(across(matches(".*_DAH$|amount_split"), ~ .x * scale)) %>%
  select(-scale)

mid_revised <- mid_data %>%
  mutate(gm_name = toupper(gm_name)) %>%
  mutate(scale = scaler) %>%
  mutate(across(matches(".*_DAH$|amount_split"), ~ .x * scale)) %>%
  select(-scale)


new_revised <- new_data %>%
  mutate(gm_name = toupper(gm_name)) %>%
  filter(YEAR < 2020) %>%
  mutate(scale = scaler) %>%
  mutate(across(matches(".*_DAH$|amount_split"), ~ .x * scale)) %>%
  select(-scale)


new2022 <- filter(new_data, YEAR == 2022) %>%
  mutate(gm_name = toupper(gm_name)) %>%
  mutate(gm_name = gsub("[,.]", "", gm_name),
         gm_name = gsub("^THE ", "", gm_name)) %>%
  group_by(gm_name) %>%
  summarize(tot22 = sum(amount_split)) %>%
  select(gm_name, tot22)
new2021 <- filter(new_data, YEAR == 2021) %>%
  mutate(gm_name = toupper(gm_name)) %>%
  mutate(gm_name = gsub("[,.]", "", gm_name),
         gm_name = gsub("^THE ", "", gm_name)) %>%
  group_by(gm_name) %>%
  summarize(tot21 = sum(amount_split)) %>%
  select(gm_name, tot21)
matched <- merge(new2022, new2021, by = "gm_name")

scaler22 <- sum(matched$tot22)/sum(matched$tot21)
addrows <- new_data %>%
  filter(YEAR == 2021) %>%
  mutate(gm_name = gsub("[,.]", "", gm_name),
         gm_name = gsub("^THE ", "", gm_name)) %>%
  mutate(gm_name = toupper(gm_name)) %>%
  filter(gm_name %ni% unique(matched$gm_name)) %>%
  mutate(scale = scaler22) %>%
  mutate(across(matches(".*_DAH$|amount_split"), ~ .x * scale)) %>%
  mutate(YEAR = 2022)

new_22 <- new_data %>%
  filter(YEAR >= 2020) %>%
  bind_rows(new_revised, addrows)



save_dataset(old_revised, "1992_2012_foundation_data_FGH_2024_scaled.csv", "US_FOUNDS", "int")
save_dataset(mid_revised, "2002_2012_foundation_data_FGH_2024_scaled.csv", "US_FOUNDS", "int")
save_dataset(new_22, "2013_21_foundation_data_FGH_2024_scaled.csv", "US_FOUNDS", "int")


fin <- read_dta("FILEPATH")

byy <- fin %>%
  mutate(DONOR_NAME = gsub("[,.]", "", DONOR_NAME),
         DONOR_NAME = gsub("^THE ", "", DONOR_NAME)) %>%
  mutate(DONOR_NAME = toupper(DONOR_NAME)) %>%
  group_by(DONOR_NAME, YEAR) %>%
  summarize(TOTAL = sum(DAH)) %>%
  arrange(-YEAR, -TOTAL)

years <- unique(byy$YEAR)  # Extract unique years
wb <- createWorkbook()
for (yr in years) {
  addWorksheet(wb, sheetName = as.character(yr))  # Create a sheet for each year
  writeData(wb, sheet = as.character(yr), byy[byy$YEAR == yr, ])  # Write filtered data
}

saveWorkbook(wb, "FILEPATH")




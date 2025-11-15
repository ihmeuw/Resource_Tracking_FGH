###################################################
### Purpose: Create GNI forecasts for use in DAH forecasting
###################################################

rm(list = ls())

library(data.table)
require(ggplot2)

dah_d_folder <- "ID"
root_fold <- 'FILEPATH'

if(!dir.exists(root_fold)) {
  dir.create(root_fold)
}

###########################
### (1) Get GDP per capita
###########################

## Get best GDP per capita
gdppc <- fread('FILEPATH')
gdppc <- gdppc[scenario == "4.5", .(iso3, year, gdppc = mean, ln_gdppc = log(mean))]


###########################
### (2) Get GNI per cap from WB WDI
### GNIpc HIC cutoff at current USD : $13,935 (source: https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups)
###########################
wb_hic_cutoff <- 13935

library("wbstats", lib.loc = 'FILEPATH')
source = "WB_WDI_GDP"
webpage <- "https://databank.worldbank.org/reports.aspx?source=world-development-indicators"

indids <- c("NY.GNP.PCAP.CD")
gni_pc <- data.table(wb_data(indicator = indids))
setnames(gni_pc, old = indids[1], new = "value")
gni_pc <- gni_pc[, .(year = date, iso3 = iso3c, gnipc = value, country, log_gni_pc = log(value))]

dropctrys <- c("Arab World","Caribbean small states","Central Europe and the Baltics",
               "Early-demographic dividend","East Asia & Pacific (excluding high income)","East Asia & Pacific (IDA & IBRD countries)",
               "Euro area","Europe & Central Asia (excluding high income)","Europe & Central Asia (IDA & IBRD countries)",
               "European Union","Fragile and conflict affected situations","IBRD only",
               "IDA & IBRD total","IDA blend","IDA only", "East Asia & Pacific",
               "IDA total","Late-demographic dividend","Latin America & Caribbean (excluding high income)",
               "Latin America & the Caribbean (IDA & IBRD countries)", "Middle East & North Africa (excluding high income)",
               "Middle East & North Africa (IDA & IBRD countries)",   
               "North America","OECD members","Other small states",
               "Pacific island small states","Post-demographic dividend","Pre-demographic dividend",                            
               "Small states","South Asia (IDA & IBRD)","Sub-Saharan Africa (excluding high income)",       
               "Sub-Saharan Africa (IDA & IBRD countries)", "Europe & Central Asia", "Heavily indebted poor countries (HIPC)",      
               "High income", "Latin America & Caribbean", "Least developed countries: UN classification",
               "Low & middle income", "Low income", "Lower middle income", "Middle East & North Africa",                  
               "Middle income", "South Asia", "Sub-Saharan Africa", "Upper middle income", "World",
               "Africa Eastern and Southern", "Africa Western and Central")

gni_pc <- gni_pc[!country %in% dropctrys]
gni_pc[, country := NULL]
gni_pc[, year := as.numeric(year)]
gni_pc <- gni_pc[which(rowSums(is.na(gni_pc[,.(iso3,date,gnipc)])) == 0)]
gni_pc <- gni_pc[gnipc != 0]

###########################
### (3) Merge with GDP per capita and regress GDP on GNI
###########################

## Merge
merged_data <- merge(gni_pc, gdppc, c('iso3', 'year'), all.y=T)
merged_data[, log_gnipc:= log(gnipc)]

## Regression
gni_gdp_model <- lm(data = merged_data[year %in% c(1995:2023)],
                    formula = log_gnipc ~ 1 + log(gdppc)) 

## Predict and calculate intercept shift
merged_data[, ln_gnipc_pred := predict(gni_gdp_model, newdata = merged_data)]
merged_data[, intshift := log_gni_pc - ln_gnipc_pred]

## Intercept shift
merged_data[!(is.na(log_gni_pc)), maxyr := max(year), by="iso3"]
merged_data[, maxyr := mean(maxyr, na.rm = T), by = "iso3"]
merged_data[year == maxyr, intshift_maxyr := intshift, by = "iso3"]
merged_data[, intshift_maxyr := mean(intshift_maxyr, na.rm = T), by = "iso3"]
merged_data[year >= maxyr, ln_gnipc_pred_final := ln_gnipc_pred]
merged_data[year >= maxyr, ln_gnipc_pred_final := ln_gnipc_pred_final + intshift_maxyr]

merged_data[!(is.na(log_gni_pc)), minyr := min(year), by="iso3"]
merged_data[, minyr := mean(minyr, na.rm = T), by = "iso3"]
merged_data[year == minyr, intshift_minyr := intshift, by = "iso3"]
merged_data[, intshift_minyr := mean(intshift_minyr, na.rm = T), by = "iso3"]
merged_data[year <= minyr, ln_gnipc_pred_final := ln_gnipc_pred]
merged_data[year <= minyr, ln_gnipc_pred_final := ln_gnipc_pred_final + intshift_minyr]

## Exponentiate
merged_data[, gnipc_pred := exp(ln_gnipc_pred_final)]

## Keep retrospective estimates
merged_data[year <= maxyr & year >= minyr, final_gnipc := gnipc]
merged_data[year > maxyr | year < minyr, final_gnipc := gnipc_pred]
merged_data <- merged_data[year <= 2100 & year >= 1990]

# Use GDP estimates for countries with no GNI estimates for all years
no_gni_countries <- merged_data[is.na(final_gnipc), unique(iso3)]
no_gni_countries <- no_gni_countries[no_gni_countries != "SWZ"]
merged_data[iso3 %in% no_gni_countries, final_gnipc := gdppc]

# Convert from nominal to real USD, and GNI per capita to total GNI

final_data <- copy(merged_data[,.(iso3, year, final_gnipc, gdppc)])
exclude_countries <- c("ZWE")
final_data_x <- final_data[iso3 %in% exclude_countries]
final_data[, year2 := year]
final_data[year2 > 2028, year2 := 2028]

## Source currency conversion function
source('FILEPATH')
final_data <- currency_conversion(data = final_data[!(iso3 %in% exclude_countries)],
                                  col.loc = 'iso3',
                                  col.value = "final_gnipc", 
                                  currency = 'usd', col.currency.year = "year2",
                                  base.unit = 'usd', base.year = .default_base_year)

final_data <- rbind(final_data, final_data_x)

## Get populations and calculate total GNI
pops <- fread('FILEPATH')
final_data <- merge(final_data, pops, by = c("year", "iso3"), all.x = T)
final_data[, final_gni := final_gnipc * population]
final_data <- final_data[!is.na(final_gni)]

# Create a column for which year a country becomes high-income, depending on if it surpasses the high-income threshold
final_data[year >= 2024, high_income := ifelse(final_gnipc >= wb_hic_cutoff, 1, 0), by = "iso3"]
final_data[high_income == 1, year_high_income := year]
final_data[, year_high_income := min(year_high_income, na.rm = T), by = "iso3"]
final_data[is.na(year_high_income), year_high_income := 2101] # for countries that never become high-income before 2100, assign year to 2101

## Save out GNI per capita forecasts
fwrite(final_data[,.(iso3, year, final_gni, year_high_income)], paste0(root_fold, "/gnipc_forecast.csv"))


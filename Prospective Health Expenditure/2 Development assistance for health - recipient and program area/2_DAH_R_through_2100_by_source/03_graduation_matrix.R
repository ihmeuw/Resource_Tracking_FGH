###################################################
### Author: USERNAME
### Purpose: Create graduation matrix for DAH recipients based off of GNI
###################################################

rm(list = ls())

library(AFModel)

## Open the parser and parse the version
args <- commandArgs(trailingOnly = TRUE)
date <- args[1]
project <- args[2]
end_FC <- as.integer(args[3])
grads_path <- args[4]
last_retro_DAH_year <- as.integer(args[5])

###########################
### (1) Get GDP per capita
###########################

## Get best GDP per capita draws and stats
gdppc <- fread('FILEPATH')
gdppc <- gdppc[scenario == 4.5, .(iso3, year, gdppc = mean, ln_gdppc = log(mean))]

gdppc_draws <- data.table(read_feather('FILEPATH'))
gdppc_draws <- gdppc_draws[scenario == 4.5]

###########################
### (2) Get GNI per capita from WB WDI
### GNIpc HIC cutoff at current USD : $14005 (source: https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups)
###########################

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
### (3) Merge with GDP per capita and regression GDP on GNI
###########################

merged_data <- merge(gni_pc, gdppc, c('iso3', 'year'), all.y=T)
merged_data[, log_gnipc:= log(gnipc)]
merged_data[, log_gnipc_sq:= log(gnipc)*log(gnipc)]

gni_gdp_model <- lm(data = merged_data[year %in% c(1995:last_retro_DAH_year)],
                    formula = log(gdppc) ~ 1 + log_gnipc) 

## High-income cutoff for graduation GDPpc value for GNIpc = 14,005
## See https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups
grad_cutoff <- exp(coef(gni_gdp_model)[1] + coef(gni_gdp_model)[2]*log(14005))


###########################
### (4) Convert the GDP per capita draws dataframe into a 0-1 matrix
##      where 0 = grad and 1 = no grad
###########################

grad_draws <- gdppc_draws[scenario == 4.5]
grad_draws <- melt(grad_draws, c('iso3', 'scenario', 'year'))
grad_draws[year <= last_retro_DAH_year, value:= 1]
grad_draws[value > grad_cutoff & year > last_retro_DAH_year, value:= 0]
grad_draws[value <= grad_cutoff & value != 0 & year >= last_retro_DAH_year, value:= 1]
grad_draws[, value := cumprod(value), by = c('iso3', 'scenario', 'variable')]

grad_draws[, scenario:= NULL]
grad_draws <- dcast(grad_draws, iso3 + year ~ variable, value.var = 'value')

# Update CHN, ROU, and TKL grad matrix
bad <- grad_draws[iso3 == "ROU" | iso3 == "CHN" | iso3 == "TKL"]
bad <- melt(bad, id.vars = c("year", "iso3"))
bad <- bad[, .(tot = sum(value)), by = c("year", "iso3")] 

# Hold grad matrix the same from the last year where there are ~50 nonzero draws
bad_rou <- max(bad[iso3=="ROU" & tot > 50]$year) + 1
bad_chn <- max(bad[iso3=="CHN" & tot > 50]$year) + 1
bad_tkl <- max(bad[iso3=="TKL" & tot > 50]$year) + 1

rou_fix <- grad_draws[iso3 == "ROU" & year == bad_rou]
chn_fix <- grad_draws[iso3 == "CHN" & year == bad_chn]
tkl_fix <- grad_draws[iso3 == "TKL" & year == bad_tkl]

grad_draws <- grad_draws[!(iso3 == "ROU" & year > bad_rou) & !(iso3 == "CHN" & year > bad_chn) & !(iso3 == "TKL" & year > bad_tkl)]
for(i in 1:eval(2100-bad_rou)) {
  grad_draws <- rbind(grad_draws, copy(rou_fix)[, year := eval(i + bad_rou)])
}
for(i in 1:eval(2100-bad_chn)) {
  grad_draws <- rbind(grad_draws, copy(chn_fix)[, year := eval(i + bad_chn)])
}
for(i in 1:eval(2100-bad_tkl)) {
  grad_draws <- rbind(grad_draws, copy(tkl_fix)[, year := eval(i + bad_tkl)])
}

setorder(grad_draws, iso3, year)

write_feather(grad_draws[year <= end_FC], grads_path)

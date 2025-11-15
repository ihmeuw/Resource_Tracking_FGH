#----# Docstring #----# ####
# Project:  FGH
# Purpose:  CRS data by country prep
#---------------------# ####


#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(stringr, crayon, readstata13, readxl)
# Variable prep
rates <- get_path("meta", "rates")
defl <- get_path("meta", "defl")
#----------------------------# ####


cat('\n\n')
cat(green(' ##################################\n'))
cat(green(' #### CRS DATA BY COUNTRY PREP ####\n'))
cat(green(' ##################################\n\n'))


cat('  Read in OECD exchange rates\n')
#----# Read in OECD exchange rates #----# ####
# - update if new DAC members using the euro are added
eurozone <- c('AUT', 'BEL', 'DEU', 'EST', 'ESP', 'FIN', 'FRA', 'GRC', 'IRL',
              'ITA', 'LTU', 'LUX', 'NLD', 'PRT', 'SVK', 'SVN') 

xrates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"),
                select = c("LOCATION", "TIME", "Value"))
setnames(xrates, c('LOCATION', 'TIME'), c('ISO3', 'YEAR'))
xrates <- dcast.data.table(xrates, formula = 'ISO3 ~ YEAR', value.var = 'Value')
xrates[ISO3 == 'EA19', ISO3 := 'EUR']
xrates[, maxn := nrow(xrates)]
# Duplicate the EUR obs `length(eurozone)` more times
xrates <- rbind(xrates,
                do.call("rbind",
                        replicate(length(eurozone), xrates[ISO3=='EUR', ], simplify = FALSE)))
xrates[, n2 := .I]

# break EU into the OECD countries from Europe
t <- 1
for (c in eurozone) {
  xrates[ISO3=="EUR" & n2 == maxn + t, ISO3 := c]
  t <- t + 1
}
xrates[, `:=`(maxn = NULL, n2 = NULL)]

yr_cols <- grep("ISO", names(xrates), value = TRUE, invert = TRUE)
setnames(xrates, yr_cols, paste0("Value", yr_cols))
#---------------------------------------# ####

#
# prep retrospective
retro <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_ADB_PDB.csv'))
retro <- retro[ELIM_CH == 0, ]
##  drop COVID-19 funding
setnafill(retro, fill = 0, cols = "oid_covid_DAH")
retro[, DAH_nocov := DAH - oid_covid_DAH]
retro[, oid_covid_DAH := NULL]
retro <- retro[DAH_nocov > 0]
### first, convert HFAs to fractions
dah_cols <- grep("_DAH", names(retro), value = TRUE)
retro[, (dah_cols) := lapply(.SD, \(x) x/DAH_nocov), .SDcols = dah_cols]
### then modify DAH to get counterfactual trend - if no COVID
retro[YEAR %in% 2020:2022,
      DAH := DAH_nocov]
### re-distribute
dah_cols <- grep("_DAH", names(retro), value = TRUE)
retro[, (dah_cols) := lapply(.SD, \(x) x * DAH), .SDcols = dah_cols]

setorder(retro, YEAR)
save_dataset(retro, "adb_for_preds",
             channel = "crs",
             stage = "int")


retro <- retro[, lapply(.SD, sum, na.rm = TRUE),
               by = .(ISO_CODE, YEAR),
               .SDcols = grep("DAH", names(retro))]
setnames(retro, c('DAH', 'ISO_CODE'), c('all_DAH', 'isocode'))

rts <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
rts <- rts[, c('YEAR', paste0('GDP_deflator_', dah.roots$report_year)), with=FALSE]


cat('  Add CRS data by country\n')
#----# Add CRS data by country #----# ####
# Step 1: Add data by country (includes in-kind and has double counting removed)
# Step 2: Convert budgeted ODA or DAH to nominal LCU to real LCU, then exchange to real USD
bil_donors <- names(dah_cfg$crs$donors)
bil_donors <- bil_donors[bil_donors != "EC"]

dt <- data.table()

for (loc in bil_donors) {
  # Read in country data
  t <- read_excel(paste0(get_path('BILAT_PREDICTIONS', 'raw'),
                         loc, '_BUDGET_', dah.roots$report_year, '.xlsx'),
                  sheet = 'use')
  setDT(t)
  if (! "DAH" %in% names(t)) {
    t[, DAH := NA_real_]
  }
  t <- t[!is.na(YEAR) & (!is.na(ODA) | !is.na(DAH)) &
             between(YEAR, 1990, dah_cfg$report_year),]
  
  curr <- toupper(unique(t$CURRENCY))
  if (length(curr) != 1 || is.na(curr)) {
      stop("Currency column is missing or invalid for ", c, "")
  }
  if (max(t$YEAR) != dah_cfg$report_year) {
    stop("Data for ", c, " does not continue to report-year, ", dah_cfg$report_year)
  }
  
  # Combine with aggregated CRS data
  t <- merge(
      t,
      retro[isocode == loc & between(YEAR, 1990, dah_cfg$report_year)],
      by = c('YEAR'),
      all = TRUE
  )
  t[, isocode := NULL]
  
  # Format xrates
  xr <- melt(xrates[ISO3 == loc, ], id.vars = "ISO3")
  xr[, variable := as.numeric(gsub("Value", "", variable))]
  setnames(xr, c('variable', 'value'), c('YEAR', 'exchange_rate'))
  if (nrow(xr) == 0 && loc != "USA") {
    stop("No exchange rate data for ", loc)
  }
  
  t <- merge(t, xr, by='YEAR', all.x=T)
  rm(xr)
  
  if (loc == 'AUS') {
    t[, SOURCE := 'Australia International Development Assistance Budget']
  } else if (loc == 'AUT') {
    t[, SOURCE := 'Austria Federal Ministry of Finance Budget']
  } else if (loc == 'BEL') {
    t[, SOURCE := 'Project General Budget']
  } else if (loc == 'CAN') {
    t[, SOURCE := 'Canadian International Development Agency (CIDA) Report on Plans and Priorities']
  } else if (loc == 'CHE') {
    t[, SOURCE := 'Foreign Affairs Budget']
  } else if (loc == "CZE") {
    t[, SOURCE := 'Activity Plan of Czech Development Agency']
  } else if (loc == 'DEU') {
    t[, SOURCE := 'Plan of the Federal Budget']
  } else if (loc == 'DNK') {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == "EST") {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == 'FIN') {
    t[, SOURCE := 'Document Assembly in budget years 1998-2013']
    t[YEAR < 2002, ODA := NA] # 1998-2001 are in the former native currency rather than euro
  } else if (loc == 'ESP') {
    t[, SOURCE := 'Annual Plan of International Cooperation']
  } else if (loc == 'FRA') {
    t[, SOURCE := 'Finance Bills, General Budget']
  } else if (loc == 'GRC') {
    t[, SOURCE := 'Ministry of Finance Budget']
  } else if (loc == 'HUN') {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == 'IRL') {
    t[, SOURCE := 'Department of Finance Budget']
  } else if (loc == "ISL") {
    t[, SOURCE := 'Iceland International Development Cooperation Budget']
  } else if (loc == 'ITA') {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == 'JPN') {
    t[, SOURCE := 'Highlights of the Budget']
  } else if (loc == 'KOR') {
    t[, SOURCE := 'ODA Korea Comprehensive Implementation Plan']
  } else if (loc == "LTU") {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == 'LUX') {
    t[, SOURCE := 'Gazette Grand Duchy of Luxembourg']
  } else if (loc == 'NLD') {
    t[, SOURCE := 'Netherlands International Cooperation Budget']
  } else if (loc == 'NOR') {
    t[, SOURCE := "Norway's National Budget"]
  } else if (loc == 'NZL') {
    t[, SOURCE := 'VOTE Official Development Assistance']
  } else if (loc == 'POL') {
    t[, SOURCE := 'Development Cooperation Plan']
  } else if (loc == 'PRT') {
    t[, SOURCE := 'Ministry of Finance and Public Administration State Budget']
  } else if (loc == "SVK") {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == "SVN") {
    t[, SOURCE := 'Adopted budget plan']
  } else if (loc == 'SWE') {
    t[, SOURCE := 'Ministry of Foreign Affairs Budget']
  } else if (loc == 'GBR') {
    t[, SOURCE := 'DFID Budget']
  } else if (loc == 'ARE') {
    t[, SOURCE := 'Ministry of Foreign Affairs and International Cooperation']
  } else if (loc == 'USA') {
    t[, SOURCE := 'Foreign Assistance Dashboard, Budget of the US Gov']
    t[YEAR == 2005, DAH := NA] # incomplete data for 2005
  } 

  # values already in USD - don't need exchange rate
  cat(loc, ": Currency is", curr, "\n")
  if (curr == "USD") {
    t[, exchange_rate := 1]
  }
  t[, `:=`(ODA_USD = ODA / exchange_rate,
           DAH_USD = DAH / exchange_rate)]
  
  # Merge deflators + deflate
  t <- merge(t, rts, by='YEAR', all.x=T)
  for (col in c('ODA_USD', 'DAH_USD', names(t)[names(t) %like% '_DAH'])) {
    t[, eval(paste0(col, '_', dah.roots$abrv_year))
              := get(col) / get(paste0('GDP_deflator_', dah.roots$report_year))]
  }
  
  # prediction years - set any observed DAH to 0 (should already be the case)
  t[YEAR > get_dah_param('CRS', 'data_year'), all_DAH := 0]
  setorder(t, YEAR, ISO3)
  t[, ISO3 := loc]
  
  dt <- rbind(dt, t, fill=T)
}
# #-----------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'oda_all_[report_year]',
             'BILAT_PREDICTIONS', 'fin')
#------------------------# ####
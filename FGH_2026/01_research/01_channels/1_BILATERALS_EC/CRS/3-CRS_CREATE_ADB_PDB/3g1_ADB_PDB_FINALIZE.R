#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Finalize CRS ADB PDB
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(dah.roots$k, 'FILEPATH/get_location_metadata.R'))
pacman::p_load(readxl, crayon, readstata13)

#----------------------------# ####


cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### CRS FINALIZE ADB_PDB ####\n'))
cat(green(' ##############################\n\n'))


cat('  Pull location metadata\n')
#----# Pull location metadata #----# ####
iso_codes <- get_location_metadata(location_set_id = get_dah_param("location_set_id"),
                                   release_id = get_dah_param("gbd_release_id")
                                   )[level == 3, c('ihme_loc_id', 'location_name')]
setnames(iso_codes, 'ihme_loc_id', 'ISO3_RC')
#----------------------------------# ####

cat('  Pull in UAE data\n')
#----# Pull in UAE data #----# ####
# As of FGH 2017, we starting tracking United Arab Emirates funding reported in CRS. However, they only reported in CRS from 2009 onwards, 
# so we contacted UAE MOFAIC and received 1990-2008 annual DAH through correspondence.
dt <- fread(
    paste0(dah.roots$j, 'FILEPATH/UAE_DAH_1990_2008.csv')
)[, !c('Grand Total')]
colnames(dt) <- gsub(" ", "", tolower(colnames(dt)))

for (col in names(dt)[names(dt) %like% 'dah']) {
  dt[get(col) == '-', eval(col) := NA]
  dt[, eval(col) := gsub(',', '', get(col))]
  dt[, eval(col) := as.numeric(get(col))]
}

dt <- dt[!(subsector %like% 'Total' & country == ''), ]
dt <- dt[subsector != 'Emergency health', ]

dt[country == "Gambia", country := "The Gambia"]
dt[country == "United States of America", country := "United States"]
dt[country == "DR Congo", country := "Democratic Republic of the Congo"]
setnames(dt, 'country', 'location_name')
dt <- merge(dt, iso_codes, by='location_name', all.x=T)
dt[location_name == "Iran", ISO3_RC := "IRN"]
dt[location_name == "Syria", ISO3_RC := "SYR"]
dt[location_name == "Tanzania", ISO3_RC := "TZA"]
dt[location_name == "The Gambia", ISO3_RC := "GMB"]
dt[location_name == "United States", ISO3_RC := "USA"]
dt[location_name == "Multi-country (Global)", ISO3_RC := "QZA"]
if (dt[is.na(ISO3_RC), .N] != 0)
    stop("Processed UAE data is missing ISO3_RC for some countries. Please check cleaning code.")

dt <- melt(dt, measure.vars = names(dt)[names(dt) %like% 'dah'])
dt[, variable := as.numeric(gsub("dah", "", variable))]
dt <- dt[!is.na(value), ]
setnames(dt, c('variable', 'value'), c('YEAR', 'DAH'))
rm(col, iso_codes)
#----------------------------# ####

cat('  Eliminate high-income funding\n')
#----# Eliminate high-income funding #----# ####
incs <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"),
              select = c('ISO3_RC', 'YEAR', 'INC_GROUP'))
dt <- merge(dt, incs, by=c('ISO3_RC', 'YEAR'), all.x=T)
dt <- dt[!INC_GROUP == 'H' | is.na(INC_GROUP), !c('INC_GROUP')]

dt[subsector == "Basic health care" | subsector == "Medical services" | subsector == "Medical research", 
   other_DAH := DAH]
dt[subsector == "Basic health infrastructure", swap_hss_other_DAH := DAH]
dt[subsector == "Health policy and administration" | subsector == "Medical education and training", swap_hss_hrh_DAH := DAH]
dt[subsector == "Infectious disease control", oid_other_DAH := DAH]
dt[, subsector := NULL]

dt[, `:=`(ISO_CODE = "ARE", DONOR_COUNTRY = "United Arab Emirates", DONOR_NAME = "United Arab Emirates",
          INCOME_TYPE = "CENTRAL", INCOME_SECTOR = "PUBLIC", REPORTING_AGENCY = "CRS",
          SOURCE_DOC = "Correspondence", gov = 0)]
dt[, location_name := NULL]

dt[, `:=`(CHANNEL = 'BIL_ARE', ELIM_CH = 0)]
UAEdata <- copy(dt)
rm(dt, incs)
#-----------------------------------------# ####

cat('  Add inkind data\n')
#----# Add inkind data #----# ####
ink <- read_excel(get_path('CRS', 'raw', 'inkind_ratios_bychannel_1990_[prev_report_year].xlsx'), sheet='stata')
setDT(ink)
setnames(ink, 'CHANNEL', 'DONOR_NAME')
ink <- merge(ink, UAEdata, by=c('DONOR_NAME', 'YEAR'), all.y=T)

for (col in names(ink)[names(ink) %like% 'DAH']) {
  ink[, eval(col) := get(col) * INKIND_RATIO]
}
ink[, INKIND := 1]

dt <- rbind(UAEdata, ink, fill=T)
dt[is.na(INKIND), INKIND := 0]
for (col in names(dt)[names(dt) %like% 'DAH']) {
  dt[is.na(get(col)), eval(col) := 0]
}

dah_cols <- grep("DAH", names(dt), value = TRUE)
dt <- dt[, lapply(.SD, sum, na.rm = TRUE),
         by = c('YEAR', 'ISO3_RC', 'gov', 'CHANNEL', 'ISO_CODE', 'INKIND',
                'DONOR_COUNTRY', 'SOURCE_DOC', 'ELIM_CH',
                'INCOME_SECTOR', 'INCOME_TYPE', 'DONOR_NAME', 'REPORTING_AGENCY'),
         .SDcols = dah_cols]
addUAE <- copy(dt)
rm(dt, ink, UAEdata, col)
#---------------------------# ####

cat('   Append UAE data to final dataset\n')
#----# Append UAE data to final dataset #----# ####
dt <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_ADB_PDB_PMI_beforeUAE.csv'))
dt <- rbind(dt, addUAE, fill=T)

to_fill <- names(dt)[names(dt) %like% '_DAH']
setnafill(dt, fill=0, cols=to_fill)
dt[is.na(is_climate), is_climate := FALSE]
#--------------------------------------------# ####


dt <- dt[YEAR <= get_dah_param("crs", "data_year")]

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'B_CRS_[crs.update_mmyy]_ADB_PDB_pre_USA19fix',
             'CRS', 'fin')
#------------------------# ####
#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Create WB INT PDB database
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, dplyr)
#----------------------------# ####


cat('\n\n')
cat(green(' ##########################\n'))
cat(green(' #### WB INTPDB CREATE ####\n'))
cat(green(' ##########################\n\n'))


cat('  Read in datasets\n')
#----# Read in dataset #----# ####
dt <- fread(get_path('WB', 'int', 'cleaned_pkws.csv'))
iati <- fread(get_path("wb", "int", "iati_post_kws_cleaned.csv"))
setnames(iati,
         c("proj_id", "iso3",    "recip",       "agreement_type", "finance_type", "title_narr", "proj_name"),
         c("projid",  "ISO3_RC", "countryname", "agreementtype",  "lendinginstr", "projname",   "project_name"))

## covid DAH for years 2020-2021
covid <- fread(get_path("wb", "fin", "COVID_prepped.csv", report_year = 2023))
covid[, projid := paste0("covid_prepped", .I)]
covid[, agreementtype := gsub("WB_", "", CHANNEL)]
covid <- covid[YEAR %in% 2020:2021]
covid <- covid[, .(
    oid_covid_DAH = sum(total_amt, na.rm = TRUE)
), by = .(
    YEAR,
    ISO3_RC,
    agreementtype 
)]
covid[, DAH := oid_covid_DAH]
covid[, projid := paste0("covid_prepped_", .GRP), by = .(agreementtype, ISO3_RC)]


dt <- dt[YEAR <= 2021]
iati <- iati[YEAR >= 2022]
dt <- rbind(dt, iati, covid, fill = TRUE)
setnafill(dt, fill = 0, cols = grep("DAH", names(dt), value = TRUE))


dt[!is.na(DAH) & DAH < 0, neg := 1]
dt[, neg_proj := sum(neg, na.rm=T), by='projid']
dt <- dt[order(projid, YEAR), ]
dt[, dummy := 1]
dt[, `:=`(proj_n = 1:sum(dummy),
          proj_N = sum(dummy)), by='projid']
dt[, dummy := NULL]
#---------------------------# ####

cat('  Adjust negative disbursements\n')
#----# Adjust negative disbursements #----# ####
neg_fix <- copy(dt[neg_proj >= 1])
hfas <- names(neg_fix)[names(neg_fix) %like% '_DAH' & names(neg_fix) != paste0('DAH_', dah.roots$abrv_year)]
## convert HFA cols into fractions
neg_fix[, (hfas) := lapply(.SD, \(x) x/DAH), .SDcols = hfas]
setnafill(neg_fix, fill = 0, cols = hfas)

dah_col <- "DAH"

neg_fix[, DAH_fix_0 := fifelse(is.na(DAH), 0, DAH)]

for (i in 1:max(neg_fix$proj_N)) {
   cat(paste0(i, ' '))
   j <- i - 1
   # Generate new disbursement variable
   neg_fix[, eval(paste0(dah_col, '_fix_', i)) := get(paste0(dah_col, '_fix_', j))]
   neg_fix[is.na(get(paste0(dah_col, '_fix_', i))), eval(paste0(dah_col, '_fix_', i)) := 0]
   # Isolate negative values
   neg_fix[get(paste0(dah_col, '_fix_', i)) < 0 & proj_n == (proj_N - j),
           eval(paste0(dah_col, '_neg_disb_', i)) := get(paste0(dah_col, '_fix_', i))]
   neg_fix[, eval(paste0(dah_col, '_neg_proj_', i)) :=
               sum(get(paste0(dah_col, '_neg_disb_', i)), na.rm=T),
           by='projid']
   # Subtract negative value from previous disbursement
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & proj_n == (proj_N - i),
           eval(paste0(dah_col, '_fix_', i)) := get(paste0(dah_col, '_fix_', i)) + get(paste0(dah_col, '_neg_proj_', i))]
   # Track whether value was changed
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & (proj_n == proj_N - i) & (proj_N >= (i + 1)) & get(paste0(dah_col, '_fix_', i)) != 0,
           eval(paste0(dah_col, '_change', i)) := 1]
   neg_fix[, eval(paste0(dah_col, '_pchange', i)) := sum(get(paste0(dah_col, '_change', i)), na.rm=T), by='projid']
   # Reset negative value to 0
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & (proj_n == (proj_N - j)) & get(paste0(dah_col, '_pchange', i)) == 1,
           eval(paste0(dah_col, '_fix_', i)) := 0]
}

setnames(neg_fix,
         paste0(dah_col, '_fix_', max(neg_fix$proj_N)),
         paste0(dah_col, '_noneg'))

to_drop <- names(neg_fix)[names(neg_fix) %like% paste0(dah_col, '_fix') |
                            names(neg_fix) %like% paste0(dah_col, '_neg_disb_') |
                            names(neg_fix) %like% paste0(dah_col, '_neg_proj_') |
                            names(neg_fix) %like% paste0(dah_col, '_change') |
                            names(neg_fix) %like% paste0(dah_col, '_pchange')]
neg_fix[, eval(to_drop) := NULL]
neg_fix[, (dah_col) := get(paste0(dah_col, '_noneg'))]
neg_fix[, eval(paste0(dah_col, '_noneg')) := NULL]

## convert HFAs back into values now that negative disbursements have been adjusted
neg_fix[, (hfas) := lapply(.SD, \(x) x * DAH), .SDcols = hfas]

# Append
dt <- dt[neg_proj < 1]
dt <- rbind(dt, neg_fix)


# if we've reached the end of the project and still have negatives, then we want
#   to subtract forward in time
dt[, c("proj_n", "proj_N", "neg", "neg_proj") := NULL]
dt[!is.na(DAH) & DAH < 0, neg := 1]
dt[, neg_proj := sum(neg, na.rm=T), by='projid']
dt <- dt[order(projid, -YEAR), ]
dt[, dummy := 1]
dt[, `:=`(proj_n = 1:sum(dummy),
          proj_N = sum(dummy)), by='projid']
dt[, dummy := NULL]

neg_fix <- copy(dt[neg_proj >= 1 & !is.na(neg_proj)])
hfas <- names(neg_fix)[names(neg_fix) %like% '_DAH' & names(neg_fix) != paste0('DAH_', dah.roots$abrv_year)]
## convert HFA cols into fractions
neg_fix[, (hfas) := lapply(.SD, \(x) x/DAH), .SDcols = hfas]
setnafill(neg_fix, fill = 0, cols = hfas)

dah_col <- "DAH"

neg_fix <- neg_fix[projid == "P003302"]

neg_fix[, DAH_fix_0 := fifelse(is.na(DAH), 0, DAH)]

for (i in 1:max(neg_fix$proj_N)) {
   cat(paste0(i, ' '))
   j <- i - 1
   # Generate new disbursement variable
   neg_fix[, eval(paste0(dah_col, '_fix_', i)) := get(paste0(dah_col, '_fix_', j))]
   neg_fix[is.na(get(paste0(dah_col, '_fix_', i))), eval(paste0(dah_col, '_fix_', i)) := 0]
   # Isolate negative values
   neg_fix[get(paste0(dah_col, '_fix_', i)) < 0 & proj_n == (proj_N - j),
           eval(paste0(dah_col, '_neg_disb_', i)) := get(paste0(dah_col, '_fix_', i))]
   neg_fix[, eval(paste0(dah_col, '_neg_proj_', i)) :=
               sum(get(paste0(dah_col, '_neg_disb_', i)), na.rm=T),
           by='projid'] # calculating different sums here but the projid/ISO3
                        # aggregated totals match Stata output
   # Subtract negative value from previous disbursement
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & proj_n == (proj_N - i),
           eval(paste0(dah_col, '_fix_', i)) :=
               get(paste0(dah_col, '_fix_', i)) + get(paste0(dah_col, '_neg_proj_', i))]
   # Track whether value was changed
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & (proj_n == proj_N - i) & (proj_N >= (i + 1)) & get(paste0(dah_col, '_fix_', i)) != 0,
           eval(paste0(dah_col, '_change', i)) := 1]
   neg_fix[, eval(paste0(dah_col, '_pchange', i)) := sum(get(paste0(dah_col, '_change', i)), na.rm=T), by='projid']
   # Reset negative value to 0
   neg_fix[get(paste0(dah_col, '_neg_proj_', i)) != 0 & (proj_n == (proj_N - j)) & get(paste0(dah_col, '_pchange', i)) == 1,
           eval(paste0(dah_col, '_fix_', i)) := 0]
}

setnames(neg_fix,
         paste0(dah_col, '_fix_', max(neg_fix$proj_N)),
         paste0(dah_col, '_noneg'))

to_drop <- names(neg_fix)[names(neg_fix) %like% paste0(dah_col, '_fix') |
                            names(neg_fix) %like% paste0(dah_col, '_neg_disb_') |
                            names(neg_fix) %like% paste0(dah_col, '_neg_proj_') |
                            names(neg_fix) %like% paste0(dah_col, '_change') |
                            names(neg_fix) %like% paste0(dah_col, '_pchange')]
neg_fix[, eval(to_drop) := NULL]
neg_fix[, (dah_col) := get(paste0(dah_col, '_noneg'))]
neg_fix[, eval(paste0(dah_col, '_noneg')) := NULL]

## convert HFAs back into values now that negative disbursements have been adjusted
neg_fix[, (hfas) := lapply(.SD, \(x) x * DAH), .SDcols = hfas]


## ensure all negatives have been accounted for
stopifnot(neg_fix[DAH < 0, .N] == 0)



#-----------------------------------------# ####

cat('  Append & check negative adjustments\n')
#----# Append & check negative adjustments #----# ####
# Append
dt <- dt[neg_proj < 1]
data <- rbind(dt, neg_fix)


# ZWE fix
data[DAH < 0 & ISO3_RC == "ZWE" & YEAR == 1993, DAH := 0]
for (col in names(data)[names(data) %like% '_DAH']) {
  data[get(col) < 0 & ISO3_RC == "ZWE" & YEAR == 1993, eval(col) := 0]
}

# YEM fix
data[DAH < 0 & ISO3_RC == "YEM" & YEAR == 1990, DAH := 0]
for (col in names(data)[names(data) %like% '_DAH']) {
  data[get(col) < 0 & ISO3_RC == "YEM" & YEAR == 1990, eval(col) := 0]
}

rm(dt, neg_fix, col)
#-----------------------------------------------# ####

cat('  Generate variables & clean columns\n')
#----# Generate variables & clean columns #----# ####
data[, `:=`(FUNDING_COUNTRY = "NA", ISO3_FC = "NA", FUNDING_AGENCY_SECTOR = "IGO",
            FUNDING_AGENCY_TYPE = "IFI", DATA_SOURCE = "World Bank: Projects Database",
            DATA_LEVEL = "Project", FUNDING_TYPE = "LOAN")]
setnames(data, 'agreementtype', 'FUNDING_AGENCY')
# Remove character-stored numbers
data[, ibrdcommamt := gsub(',', '', ibrdcommamt)]
data[, ibrdcommamt := as.numeric(ibrdcommamt)]
data[, idacommamt := gsub(',', '', idacommamt)]
data[, idacommamt := as.numeric(idacommamt)]
# Sort & generate ordering
data <- data[order(projid, YEAR)]
data[, dummy := 1]
data[, n := 1:sum(dummy), by='projid']
data[, dummy := NULL]
# Fill commitment data
data[ibrdcommamt!=0, COMMITMENT := ibrdcommamt]
data[idacommamt!=0, COMMITMENT := idacommamt]
# Rename
setnames(data, c('projid', 'project_name', 'countryname', 'borrower', 'impagency'),
         c('PROJECT_ID', 'PROJECT_NAME', 'RECIPIENT_COUNTRY', 'RECIPIENT_AGENCY', 'IMPLEMENTING_AGENCY'))
for (i in 1:5) {
  setnames(data, paste0('theme', i, '_name'), paste0('PURPOSE', i))
  rm(i)
}
setnames(data, 'PURPOSE1', 'PURPOSE')
# Cleaning country names
data[RECIPIENT_AGENCY == "GOVERNMENT OF BENIN" & RECIPIENT_COUNTRY == "Africa", `:=`(ISO3_RC = "BEN", RECIPIENT_COUNTRY = "Benin")]
data[RECIPIENT_AGENCY == "UNITED REPUBLIC OF TANZANIA" & RECIPIENT_COUNTRY == "Africa", `:=`(ISO3_RC = "TZA", RECIPIENT_COUNTRY = "United Republic of Tanzania")]
data[RECIPIENT_AGENCY == "REPUBLIC OF COTE D'IVOIRE", `:=`(ISO3_RC = "CIV", RECIPIENT_COUNTRY = "Cote d'Ivoire")]
# Adding recipient agency sectors & types
data[, RECIPIENT_AGENCY_SECTOR := "GOV"]
data[RECIPIENT_AGENCY %in% c("UN INTERIM ADMIN MISSION IN KOSOVO", "UN INTERIM ADMINISTRATION (UNMIK)", "UNITED NATION DEVELOPMENT PROGRAMME",
                             "UNMIK", "WORLD FISH CTR, ACTS, ICARDA, AND IICA"),
     RECIPIENT_AGENCY_SECTOR := "IGO"]
data[RECIPIENT_AGENCY %in% c("UN INTERIM ADMIN MISSION IN KOSOVO", "UN INTERIM ADMINISTRATION (UNMIK)",
                             "UNITED NATION DEVELOPMENT PROGRAMME", "UNMIK"),
     RECIPIENT_AGENCY_TYPE := "UN"]
data[RECIPIENT_AGENCY_SECTOR == "GOV" & RECIPIENT_AGENCY_TYPE == "", RECIPIENT_AGENCY_TYPE := "UNSP"]
data[RECIPIENT_AGENCY ==  "WORLD FISH CTR, ACTS, ICARDA, AND IICA", RECIPIENT_AGENCY_TYPE := "OTHER"]
setnames(data, 'dateapproval', 'DATE_APPROVAL')
# Keep just certain columns
project_data <- data[, c('YEAR', 'DATA_SOURCE', 'DATA_LEVEL', 'PROJECT_ID', 'PROJECT_NAME', 'FUNDING_COUNTRY', 'ISO3_FC', 'FUNDING_AGENCY', 'FUNDING_AGENCY_SECTOR',
                         'FUNDING_AGENCY_TYPE', 'RECIPIENT_COUNTRY', 'ISO3_RC', 'RECIPIENT_AGENCY', 'RECIPIENT_AGENCY_SECTOR', 'RECIPIENT_AGENCY_TYPE', 'PURPOSE', 'PURPOSE2',
                         'PURPOSE3', 'PURPOSE4', 'PURPOSE5', 'COMMITMENT', 'DAH', 'DATE_APPROVAL', names(data)[names(data) %like% '_DAH'], 'IMPLEMENTING_AGENCY', 'FUNDING_TYPE'), with=F]
rm(data)
#----------------------------------------------# ####

cat('  Read in IDA inkind\n')
#----# Read in IDA inkind #----# ####
ida_inkind <- setDT(fread(paste0(get_path('WB', 'raw'), 'IDA_INKIND_1990-', dah.roots$report_year, '.csv')))[, c('YEAR', 'CHANNEL', 'INKIND_RATIO')]
ida_inkind <- ida_inkind[!is.na(INKIND_RATIO)]
if (max(ida_inkind$YEAR) < dah.roots$report_year) {
    ida_inkind <- rbind(ida_inkind, data.table('YEAR' = dah.roots$report_year, 'CHANNEL' = 'WB_IDA', INKIND_RATIO = 0))
    ida_inkind[, INKIND_RATIO_PRED := ((1/2)*(shift(INKIND_RATIO, 1, 'lag'))) + ((1/3)*(shift(INKIND_RATIO, 2, 'lag'))) + ((1/6)*(shift(INKIND_RATIO, 3, 'lag')))]
    ida_inkind[YEAR == dah.roots$report_year, INKIND_RATIO := INKIND_RATIO_PRED]
    ida_inkind[, INKIND_RATIO_PRED := NULL]
}
# Save out dataset for use in later file
save_dataset(dataset = ida_inkind, filename = 'inkind_ratio_ida', channel = 'WB', stage = 'int')
#------------------------------# ####

cat('  Read in IBRD inkind\n')
#----# Read in IBRD inkind #----# ####
ibrd_inkind <- setDT(fread(paste0(get_path('WB', 'raw'), 'IBRD_INKIND_1990-', dah.roots$report_year, '.csv')))[, c('Year', 'Ratio')]
setnames(ibrd_inkind, c('Year', 'Ratio'), c('YEAR', 'INKIND_RATIO'))
ibrd_inkind[, CHANNEL := 'WB_IBRD']
ibrd_inkind <- ibrd_inkind[!is.na(INKIND_RATIO)]
if (max(ibrd_inkind$YEAR) < dah.roots$report_year) {
    ibrd_inkind <- rbind(ibrd_inkind, data.table('YEAR' = dah.roots$report_year, 'CHANNEL' = 'WB_IBRD', INKIND_RATIO = 0))
    ibrd_inkind[, INKIND_RATIO_PRED := ((1/2)*(shift(INKIND_RATIO, 1, 'lag'))) + ((1/3)*(shift(INKIND_RATIO, 2, 'lag'))) + ((1/6)*(shift(INKIND_RATIO, 3, 'lag')))]
    ibrd_inkind[YEAR == dah.roots$report_year, INKIND_RATIO := INKIND_RATIO_PRED]
    ibrd_inkind[, INKIND_RATIO_PRED := NULL]
}
inkind_ratios <- rbind(ibrd_inkind, ida_inkind)
save_dataset(inkind_ratios, 'inkind_ratios', 'WB', 'int')
rm(ida_inkind, ibrd_inkind)
#-------------------------------# ####

cat('  Merge inkind & calculate\n')
#----# Merge inkind & calculate #----# ####
wb_pdb <- copy(project_data)
wb_pdb <- wb_pdb[YEAR >= 1990]
wb_pdb[, CHANNEL := paste0('WB_', FUNDING_AGENCY)]
wb_pdb[, m_m := 1]
inkind_ratios[, u_m := 2]
wb_pdb <- merge(wb_pdb, inkind_ratios, by=c('YEAR', 'CHANNEL'), all=T)
wb_pdb[, merge := rowSums(wb_pdb[, c('u_m', 'm_m')], na.rm=T)]
wb_pdb <- wb_pdb[merge %in% c(1,3), !c('merge', 'u_m', 'm_m')]

stopifnot( wb_pdb[is.na(INKIND_RATIO), .N] == 0 )

inkind_dah <- copy(wb_pdb)
for (col in c('DAH', names(inkind_dah)[names(inkind_dah) %like% '_DAH'])) {
  inkind_dah[, eval(col) := get(col) * INKIND_RATIO]
}
inkind_dah[, INKIND := 1]

wb_pdb <- rbind(wb_pdb, inkind_dah, fill=T)
wb_pdb[INKIND != 1, INKIND := 0]
#------------------------------------# ####

cat('  Prep total disbursement data\n')
#----# Prep total disbursement data #----# ####
total_disb <- copy(project_data)
total_disb <- collapse(total_disb, 'sum', c('YEAR', 'FUNDING_AGENCY'), 'DAH')
total_disb <- dcast(total_disb, formula = 'YEAR ~ FUNDING_AGENCY', value.var = 'DAH')
setnames(total_disb, c('IBRD', 'IDA'), c('disb_ibrd', 'disb_ida'))
#----------------------------------------# ####

cat('  Save Dataset\n')
#----# Save dataset #----# ####
save_dataset(wb_pdb, 'wb_pdb', 'WB', 'int')
save_dataset(total_disb, 'total_disb', 'WB', 'int')
save_dataset(wb_pdb, paste0('WB_INTPDB_1990_', dah.roots$report_year), 'WB', 'fin')
#------------------------# ####

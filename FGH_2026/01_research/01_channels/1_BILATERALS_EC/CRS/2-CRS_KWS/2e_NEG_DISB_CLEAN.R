#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Clean CRS keyword search output & resolve negative disbursements
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)
# Variable prep
codes <- get_path("meta", "locs")
#----------------------------# ####


cat('\n\n')
cat(green(' #####################################\n'))
cat(green(' #### CRS KWS & NEG DISB CLEANING ####\n'))
cat(green(' #####################################\n\n'))


cat('  Read in CRS withoutprops data\n')
#----# Read in CRS withoutprops data #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_DAH_HFAS_withprops_[crs.update_mmyy].csv'),
            strip.white = FALSE)

#-----------------------------------------# ####

cat('  Replace & subset\n')
#----# Replace & subset #----# ####
dt[donor_name == "United States" & year > 2003 & hiv_level > 0, pepfar := 1]
dt <- dt[pepfar == 1 & donor_name == "United States", ]
dt[recipient_name=="Bilateral, unspecified", ISO3_RC := 'QZA']
#----------------------------# ####

cat('  Collapse sum & gen new cols\n')
#----# Collapse sum & gen new cols #----# ####
hiv_cols <- grep("hiv_.+_disbcurr", names(dt), value = TRUE)
dt <- dt[, lapply(.SD, sum, na.rm = TRUE),
         by = .(year, ISO3_RC), .SDcols = hiv_cols]
dt[, total_pepfar := rowSums(.SD, na.rm = TRUE), .SDcols = hiv_cols]

for (col in names(dt)[names(dt) %like% 'disbcurr']) {
    dt[, eval(paste0('crs_frct_', col)) := get(col) / total_pepfar]
}

crs_pepfar <- dt[, c('year', 'ISO3_RC', names(dt)[names(dt) %like% 'crs_frct_']), with=F]
rm(dt)
#---------------------------------------# ####

cat('  Read in PEPFAR budget\n')
#----# Read in PEPFAR budget #----# ####
dt <- fread(get_path('PEPFAR', 'raw', 'PEPFARBudgetaryCOP[report_year]_new.csv'))
setnames(dt, 'YEAR', 'year')
dt[, hfa_pa := paste0("DAH_", hfa_pa)]
pepfar_hfa_cols <- unique(dt$hfa_pa)

dt <- dcast(dt, year + operating_unit + country + iso3 ~ hfa_pa,
            value.var = "amount")
#---------------------------------# ####

cat('  Further process PEPFAR data\n')
#----# Further process PEPFAR data #----# ####
pepfar1 <- copy(dt)
setnames(pepfar1, c('country', 'iso3'), c('country_lc', 'iso3_orig'))
isos <- setDT(read.dta13(paste0(codes, 'countrycodes_official.dta'))
              )[ ,c('country_lc', 'iso3')]
isos[country_lc == "Swaziland", country_lc := "Eswatini"] #  to match PEPFAR data

pepfar1 <- merge(pepfar1, isos, by='country_lc', all.x=T)

pepfar1[country_lc %like% "Asia Regional Program" | country_lc == "Asia Region (pre-FY24)",
        iso3 := 'QRA']
pepfar1[country_lc %like% "Caribbean Region", iso3 := 'QNC']
pepfar1[country_lc %like% "Central America Region", iso3 := 'QNC']
pepfar1[country_lc %like% "Central Asia Region", iso3 := 'QRS']
pepfar1[country_lc %like% "Global", iso3 := 'WLD']
pepfar1[country_lc %like% "Eswatini", iso3 := 'SWZ']
pepfar1[country_lc %like% "West Africa Region", iso3 := "QME"]
pepfar1[country_lc %like% "Western Hemisphere Region", iso3 := "QNA"]
pepfar1 <- pepfar1[country_lc != "Invalid"]
pepfar1[, iso3_orig := NULL]

setnames(pepfar1, c('country_lc', 'iso3'), c('recipient_name', 'ISO3_RC'))


dah_cols <- grep("DAH_", names(pepfar1), value = TRUE)
pepfar1 <- pepfar1[, lapply(.SD, sum, na.rm = TRUE),
                   by = .(ISO3_RC, year),
                   .SDcols = c(dah_cols)]
pepfar1[, amount := rowSums(.SD, na.rm = TRUE),
        .SDcols = dah_cols]

dah_non_split_cols <- grep("split", dah_cols, invert = TRUE, value = TRUE)
pepfar1[, DAH_total := rowSums(.SD, na.rm = TRUE),
        .SDcols = dah_non_split_cols]

pepfar1[, diff := amount - DAH_total] # should equal DAH_hiv_split

for (col in gsub("DAH_", "", dah_non_split_cols)) {
    pepfar1[, eval(paste0('pepfar_', col, '_frct')) := get(paste0('DAH_', col)) / DAH_total]
    # if diff == amount & amount != 0, then we don't have data on the disaggregation
    # so we assume equal distribution over the 7 categories
    pepfar1[diff == amount & amount != 0, eval(paste0('pepfar_', col, '_frct')) := 1/length(dah_non_split_cols)]
}

pepfar1 <- pepfar1[, c('ISO3_RC', 'year', names(pepfar1)[names(pepfar1) %like% '_frct']), with=F]

# drop missing/NaN fractions
pepfar1 <- melt(pepfar1, id.vars = c('ISO3_RC', 'year'))
pepfar1 <- pepfar1[!is.na(value)]

# ensure fractions sum to 1
stopifnot(
    pepfar1[, sum(value), by = .(ISO3_RC, year)][abs(V1 - 1) > 1e-6, .N] == 0
)

pepfar1 <- dcast(pepfar1, ISO3_RC + year ~ variable, value.var = 'value')

rm(isos, col)
#---------------------------------------# ####

cat('  Split the remaining PEPFAR data\n')
#----# Split the remaining PEPFAR data #----# ####
pepfar_split <- copy(dt)
dah_cols <- grep("DAH_", names(pepfar_split), value = TRUE)
pepfar_split[, amount := rowSums(.SD, na.rm = TRUE), .SDcols = dah_cols]
dah_cols <- grep("DAH_", names(pepfar_split), value = TRUE)
pepfar_split <- pepfar_split[, lapply(.SD, sum, na.rm = TRUE),
                             by = .(year),
                             .SDcols = c(dah_cols, "amount")]

dah_non_split_cols <- grep("split", dah_cols, invert = TRUE, value = TRUE)
pepfar_split[, DAH_total := rowSums(.SD, na.rm = TRUE),
              .SDcols = dah_non_split_cols]

pepfar_split[, diff := amount - DAH_total] # should equal DAH_hiv_split
for (col in gsub("DAH_", "", dah_non_split_cols)) {
  pepfar_split[, eval(paste0('pepfar_', col, '_frct')) := get(paste0('DAH_', col)) / DAH_total]
  pepfar_split[diff == amount & amount != 0, eval(paste0('pepfar_', col, '_frct')) := 1/length(dah_non_split_cols)]
}

pepfar_split[, ISO3_RC := 'QZA']
pepfar_split <- pepfar_split[, c('year', 'ISO3_RC', names(pepfar_split)[names(pepfar_split) %like% '_frct']), with=F]

pepfar_split <- rbind(pepfar_split, pepfar1)

stopifnot(
    # ensure fractions sum to 1
    melt(pepfar1,
         id.vars = c("year", "ISO3_RC")
         )[,
           sum(value),
           by = .(year, ISO3_RC)
           ][abs(V1 - 1) > 1e-6, .N] == 0
)

rm(pepfar1, dt)
#-------------------------------------------# ####

cat('  Merge CRS and PEPFAR fractions\n')
#----# Merge CRS and PEPFAR fractions #----# ####
pepfar_split[, m_m := 1]
crs_pepfar[, u_m := 2]
pepfar_split <- merge(pepfar_split, crs_pepfar, by=c('year', 'ISO3_RC'), all=T)
pepfar_split[, merge := rowSums(pepfar_split[, c('u_m', 'm_m')], na.rm=T)]
pepfar_split[merge == 3, PEPFAR_TAG := 1]
pepfar_split <- pepfar_split[merge == 3, !c('u_m', 'm_m', 'merge')]

## integrate hiv_amr fraction into pepfar fractions
pepfar_split <- pepfar_split[, c('year', 'ISO3_RC',
                                 names(pepfar_split)[names(pepfar_split) %like% 'pepfar' & names(pepfar_split) %like% '_frct'],
                                 'crs_frct_hiv_amr_disbcurr', 'PEPFAR_TAG'), with=F]
setnames(pepfar_split,
         c('crs_frct_hiv_amr_disbcurr'),
         c('pepfar_hiv_amr_frct'))
pepfar_split[, new_total_frct := rowSums(.SD, na.rm=T),
             .SDcols = grep("pepfar_hiv", names(pepfar_split), value = TRUE)]

for (col in names(pepfar_split)[names(pepfar_split) %like% 'pepfar_hiv']) {
    pepfar_split[, eval(paste0('new_', col)) := get(col) * (1/new_total_frct)]
}

pepfar_split[, final_total_frct := rowSums(.SD, na.rm=T),
             .SDcols = grep("new_pepfar_hiv", names(pepfar_split), value = TRUE)]
pepfar_split <- pepfar_split[, c('year', 'ISO3_RC', names(pepfar_split)[names(pepfar_split) %like% 'new_pepfar_hiv'],
                                 'PEPFAR_TAG', 'final_total_frct'), with=F]
setnames(pepfar_split,
         names(pepfar_split)[names(pepfar_split) %like% 'new_pepfar_hiv'],
         gsub('new_', '', names(pepfar_split)[names(pepfar_split) %like% 'new_pepfar_hiv']))
rm(crs_pepfar, col)
#------------------------------------------# ####

cat('  Read in CRS PEPFAR projects\n')
#----# Read in CRS PEPFAR projects #----# ####
data2 <- fread(get_path('CRS', 'int', 'B_CRS_DAH_HFAS_withprops_[crs.update_mmyy].csv'),
               strip.white = FALSE)
data2[donor_name == "United States" & year > 2003 & hiv_level > 0, pepfar := 1]
data2[recipient_name=="Bilateral, unspecified", ISO3_RC := 'QZA']
#---------------------------------------# ####

cat('  Merge with PEPFAR fractions & process\n')
#----# Merge with PEPFAR fractions & process #----# ####
pepfar_dah <- copy(data2[pepfar == 1 & donor_name == "United States" & year > 2003, ])
pepfar_split[, u_m := 2]
pepfar_dah[, m_m := 1]
pepfar_dah <- merge(pepfar_dah, pepfar_split, by=c('ISO3_RC', 'year'), all.x=T)
pepfar_split[, u_m := NULL]
pepfar_dah[, merge := rowSums(pepfar_dah[, c('u_m', 'm_m')], na.rm=T)]
pepfar_dah[merge == 3, pepfar_replace := 1]
pepfar_dah <- pepfar_dah[, !c('u_m', 'm_m', 'merge')]

setnames(pepfar_dah,
         names(pepfar_dah)[names(pepfar_dah) %like% 'final_hiv_' & names(pepfar_dah) %like% '_frct'],
         gsub('final_', '', names(pepfar_dah)[names(pepfar_dah) %like% 'final_hiv_' & names(pepfar_dah) %like% '_frct']))
pepfar_dah[, `:=`(final_total_frct.x = NULL, final_total_frct.y = NULL)]

pepfar_dah <- rowtotal(pepfar_dah, 'nonHIV_total', names(pepfar_dah)[names(pepfar_dah) %like% 'final_' & names(pepfar_dah) %like% '_frct'])
pepfar_dah[, hiv_total_original := 1 - nonHIV_total]
pepfar_dah <- rowtotal(pepfar_dah, 'pepfar_hiv_total', names(pepfar_dah)[names(pepfar_dah) %like% 'pepfar_hiv_' & names(pepfar_dah) %like% '_frct'])
pepfar_dah[, crs_scale_props := hiv_total_original / pepfar_hiv_total]
for (col in names(pepfar_dah)[names(pepfar_dah) %like% 'pepfar_hiv_' & names(pepfar_dah) %like% '_frct']) {
  pepfar_dah[, eval(paste0('new_', col)) := get(col) * crs_scale_props]
}

pepfar_dah <- rowtotal(pepfar_dah, 'final_hiv_total_frct', names(pepfar_dah)[names(pepfar_dah) %like% 'new_pepfar_hiv_' & names(pepfar_dah) %like% '_frct'])
pepfar_dah <- pepfar_dah[, !c('crs_scale_props', 'final_hiv_total_frct', names(pepfar_dah)[names(pepfar_dah) %like% 'pepfar_hiv_' & names(pepfar_dah) %like% '_frct' &
                                                                                             !(names(pepfar_dah) %like% 'new_')]), with=F]
setnames(pepfar_dah,
         names(pepfar_dah)[names(pepfar_dah) %like% 'new_' & names(pepfar_dah) %like% '_frct'],
         gsub('new_', '', names(pepfar_dah)[names(pepfar_dah) %like% 'new_' & names(pepfar_dah) %like% '_frct']))
setnames(pepfar_dah,
         names(pepfar_dah)[names(pepfar_dah) %like% 'hiv_' & names(pepfar_dah) %like% '_frct' & !(names(pepfar_dah) %like% 'pepfar_')],
         paste0('final_', names(pepfar_dah)[names(pepfar_dah) %like% 'hiv_' & names(pepfar_dah) %like% '_frct' & !(names(pepfar_dah) %like% 'pepfar_')]))

pepfar_hiv_cols <- grep("pepfar_hiv_.+_frct$", names(pepfar_dah), value=T)
pepfar_hiv_cols <- gsub("pepfar_", "", gsub("_frct", "", pepfar_hiv_cols))
for (col in pepfar_hiv_cols) {
  pepfar_dah[pepfar_replace == 1, eval(paste0('final_', col, '_frct')) := get(paste0('pepfar_', col, '_frct'))]
}
pepfar_dah <- rowtotal(pepfar_dah, 'final_total_frct', names(pepfar_dah)[names(pepfar_dah) %like% 'final_' & names(pepfar_dah) %like% '_frct'])

pepfar_dah <- rowtotal(pepfar_dah, 'hiv_commitment_current', names(pepfar_dah)[names(pepfar_dah) %like% '_commcurr' & names(pepfar_dah) %like% 'hiv_'])
pepfar_dah <- rowtotal(pepfar_dah, 'hiv_commitment_constant', names(pepfar_dah)[names(pepfar_dah) %like% '_commcons' & names(pepfar_dah) %like% 'hiv_'])
pepfar_dah <- rowtotal(pepfar_dah, 'hiv_disbursement_current', names(pepfar_dah)[names(pepfar_dah) %like% '_disbcurr' & names(pepfar_dah) %like% 'hiv_'])
pepfar_dah <- rowtotal(pepfar_dah, 'hiv_disbursement_constant', names(pepfar_dah)[names(pepfar_dah) %like% '_disbcons' & names(pepfar_dah) %like% 'hiv_'])
for (col in pepfar_hiv_cols) {
  pepfar_dah[pepfar_replace == 1, eval(paste0(col, '_commcurr')) := get(paste0('final_', col, '_frct')) * hiv_commitment_current]
  pepfar_dah[pepfar_replace == 1, eval(paste0(col, '_commcons')) := get(paste0('final_', col, '_frct')) * hiv_commitment_constant]
  pepfar_dah[pepfar_replace == 1, eval(paste0(col, '_disbcurr')) := get(paste0('final_', col, '_frct')) * hiv_disbursement_current]
  pepfar_dah[pepfar_replace == 1, eval(paste0(col, '_disbcons')) := get(paste0('final_', col, '_frct')) * hiv_disbursement_constant]
}

pepfar_dah <- pepfar_dah[, !c('nonHIV_total', 'hiv_total_original', names(pepfar_dah)[names(pepfar_dah) %like% 'hiv_comm' | names(pepfar_dah) %like% 'hiv_disb']), with=F]
rm(pepfar_split, col)
#-------------------------------------------------# ####

cat('  Drop & append PEPFAR fractions\n')
#----# Drop & append PEPFAR fractions #----# ####
data2 <- data2[!(pepfar == 1 & donor_name == 'United States'), ]
data2 <- rbind(data2, pepfar_dah, fill=T)
rm(pepfar_dah)
#------------------------------------------# ####

cat('  Prep data for negative disb removal\n')
#----# Prep data for negative disb removal #----# ####
dt <- copy(data2)
# Generate flags + tags
dt <- dt[order(donor_agency, recipient_code, crs_id, year, -disbursement_current), ]
dt[, dummy := 1]
dt[, proj_N := sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, proj_n := 1:sum(dummy), by=c('donor_agency', 'recipient_code', 'crs_id')]
dt[, dummy := NULL]

dt[disbursement_current < 0, neg := 1]
dt[, neg_proj := sum(neg, na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
setnames(dt, c('commitment_current', 'commitment_constant', 'disbursement_current', 'disbursement_constant'),
         c('all_commcurr', 'all_commcons', 'all_disbcurr', 'all_disbcons'))
#-----------------------------------------------# ####

cat('  Subset to negative disb projects & re-calculate\n')
#----# Subset to negative disb projects & re-calculate #----# ####
neg <- copy(dt[neg_proj >= 1, ])

units <- c('disbcurr', 'disbcons')
hfas <- names(neg)[names(neg) %like% 'commcurr' & !(names(neg) %like% 'all')]
hfas <- gsub('_commcurr', '', hfas)
for (unit in units) {
  for (t in 1:length(hfas)) {
    neg[, eval(paste0(hfas[t], '_', unit, '_fix_0')) := get(paste0(hfas[t], '_', unit))]
    neg[is.na(get(paste0(hfas[t], '_', unit, '_fix_0'))), eval(paste0(hfas[t], '_', unit, '_fix_0')) := 0]
  }
}

cat('    Fixing ')
for (unit in units) {
  for (t in 1:length(hfas)) {
    cat(paste0(hfas[t], ' ', unit, ', '))
    for(i in 1:max(neg$proj_N)) {
      j <- i - 1
      # Generate new disbursement variable
      neg[, eval(paste0(hfas[t], '_', unit, '_fix_', i)) := get(paste0(hfas[t], '_', unit, '_fix_', j))]
      neg[is.na(get(paste0(hfas[t], '_', unit, '_fix_', i))), eval(paste0(hfas[t], '_', unit, '_fix_', i)) := 0]
      # Isolate negative values
      neg[get(paste0(hfas[t], '_', unit, '_fix_', i)) < 0 & proj_n == proj_N - j, eval(paste0(hfas[t], '_', unit, '_neg_', i)) := get(paste0(hfas[t], '_', unit, '_fix_', i))]
      neg[, eval(paste0(hfas[t], '_', unit, '_negp_', i)) := sum(get(paste0(hfas[t], '_', unit, '_neg_', i)), na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
      # Subtract the negative value from the previous disbursement
      neg[get(paste0(hfas[t], '_', unit, '_negp_', i)) != 0 & proj_n == proj_N - i,
          eval(paste0(hfas[t], '_', unit, '_fix_', i)) := get(paste0(hfas[t], '_', unit, '_fix_', i)) + get(paste0(hfas[t], '_', unit, '_negp_', i))]
      # Create dummy variable to indicate if the value was changed
      neg[get(paste0(hfas[t], '_', unit, '_negp_', i)) != 0 & (proj_n == proj_N - i) & proj_N >= (i + 1) & get(paste0(hfas[t], '_', unit, '_fix_', i)) != 0,
          eval(paste0(hfas[t], '_', unit, '_change', i)) := 1]
      neg[, eval(paste0(hfas[t], '_', unit, '_pchange', i)) := sum(get(paste0(hfas[t], '_', unit, '_change', i)), na.rm=T), by=c('donor_agency', 'recipient_code', 'crs_id')]
      # Update the negative value to 0
      neg[get(paste0(hfas[t], '_', unit, '_negp_', i)) != 0 & (proj_n == proj_N - j) & get(paste0(hfas[t], '_', unit, '_pchange', i)) == 1,
          eval(paste0(hfas[t], '_', unit, '_fix_', i)) := 0]
    }
    setnames(neg, paste0(hfas[t], '_', unit, '_fix_', max(neg$proj_N)), paste0(hfas[t], '_', unit, '_noneg'))
    neg <- neg[, !c(names(neg)[names(neg) %like% paste0(hfas[t], '_', unit, '_fix')],
                    names(neg)[names(neg) %like% paste0(hfas[t], '_', unit, '_neg_')],
                    names(neg)[names(neg) %like% paste0(hfas[t], '_', unit, '_negp_')],
                    names(neg)[names(neg) %like% paste0(hfas[t], '_', unit, '_change')],
                    names(neg)[names(neg) %like% paste0(hfas[t], '_', unit, '_pchange')]), with=F]
  }
}
cat('\n')

for (unit in units) {
  for (t in 1:length(hfas)) {
    neg[, eval(paste0(hfas[t], '_', unit)) := get(paste0(hfas[t], '_', unit, '_noneg'))]
  }
}

for (unit in units) {
  neg <- rowtotal(neg, paste0('new_tot_', unit), paste0(hfas, '_', unit))
}

neg[, `:=`(all_disbcurr = new_tot_disbcurr, all_disbcons = new_tot_disbcons)]
neg <- neg[, !c(names(neg)[names(neg) %like% 'new_tot'], names(neg)[names(neg) %like% '_noneg']), with=F]
#-----------------------------------------------------------# ####

cat("  Drop negative disb's & append recalculated data\n")
#----# Drop negative disb's & append recalculated data #----# ####
dt <- dt[!(neg_proj >= 1), ]
dt <- rbind(dt, neg, fill=T)

dt[, PEPFAR_FC := 0]
for (country in c(288, 769, 285, 282, 218, 266, 261, 275, 259, 248, 349, 446, 238, 247, 227)) {
  dt[(pepfar == 1 | (hiv_level >= 1)) & (recipient_code == country) & year > 2003 & isocode == "USA", PEPFAR_FC := 1]
}
#-----------------------------------------------------------# ####

cat('  Save dataset\n')		
#----# Save dataset #----# ####
save_dataset(dt, 'B_CRS_DAH_before_NGO_id_[crs.update_mmyy]', 'CRS', 'int')
#------------------------# ####
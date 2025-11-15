#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Prepare CRS PMI data before UAE
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, readstata13, stringr)
# keyword prep
mal_treat <- c('ABCDEFGHI', " ARTEMISININ ", " PRIMAQUINE ", " ACT ", " DRUG", " TREAT", " CASE MANAGEMENT ", " COMBINATION THERAPY ", " ANTI MALARIAL ", " ANTIMALARIAL ", 
               " CHLOROQUINE ", " MIP ", " MALARIA IN PREGNANCY ", " IPTP ", " ACTS ", " ARTESUNATE " )
mal_diag <- c('ABCDEFGHI', " DIAGNOS", " CASE DETECTION ", " MICROSCOPY ", " BLOOD SURVEY", " BIOLOGICAL TESTING ", " EDT ", " LAMP ", " RDT ", " RDTS ")
mal_con_nets <- c('ABCDEFGHI', " BEDNET", " BED NET", " SMITN ", " ITN ", " LLIN ", " INSECTICIDAL NET", " INSECTICIDE TREAT", " LLINS ", " ITNS ")
mal_con_irs <- c('ABCDEFGHI', " INDOORRESIDUALSPRAY", " IRS ", " REDUCE THE PARASITE RESERVOIR ", " FOGGING ", " COILS ", " LARVICID", " LARVACID", " VECTOR CONTROL", "RESIDUAL SPRAY", 
                 " RESIDUALSSPRAY ", "INDOOR SPRAY", " INDOORSPRAY " )
mal_con_oth <- c('ABCDEFGHI', " PREVENT", " IPT ", " SMC ", " SEASONAL MALARIA CHEMOPREVENTION " )
mal_comm_con <- c('ABCDEFGHI', " COMMUNITYOUTREACH ", " OUTREACH ", " COMMUNITY MOBILIZATION", " AWARE", " COMMUNICATION STRATEGY ", " SOCIAL COMMUNICATION ", " PARTNERSHIP", 
                  " ACTIVITIES NEAR COMMUNITIES ", " BCC ", " BEHAVIORAL CHANGE COMMUNICATION ", " BEHAVIOURAL CHANGE COMMUNICATION ", " BEHAVIOR CHANGE COMMUNICATION ", 
                  " BEHAVIOUR CHANGE COMMUNICATION ", " SOCIAL MOBILIZATION ", " SBCC ", " BEHAVIOR CHANGE AND COMMUNICATION ", " COMMUNITY BASED ")
mal_hss_other <- c('ABCDEFGHI', " SWAP", " DATA SYSTEM", " SECTOR WIDE APPROACH", " HEALTH SYSTEM", " SECTOR PROGRAM", " BUDGET SUPPORT", " SECTOR SUPPORT ", " HSS ", " TRACKING PROGRESS ", 
                   " FACILITIES ", " ESSENTIAL MEDICINES ", " POLICY DEVELOPMENT", " INSTITUTIONAL STRENGTHENING ", " HSPSP ", " M&E ", " M & E ", " MONITORING ", 
                   " SURVEILLANCE ", " GOVERNANCE ", " SCALING UP ", " REALLOCATE RESOURCES ", " STRATEGIES AND PROGRAM", " HIV STRATEG", " PROGRAM IN COUNTRY ACTIVITIES ", 
                   " STRATEGIC INFORMATION ", " PROCUREMENT ", " EVIDENCE BASED ", " CASE REPORTING ", " OPERATIONAL RESEARCH ", " SUPPORTIVE ENVIRONMENT ", 
                   " INFORMATION SYSTEM", " CASE NOTIFICATION ", " CASE FINDING ", " LABORATORY STRENGTHENING ", " LABORATORY QUALITY ", " LABORATORY NETWORK", 
                   " CONTROL SERVICES ", " INFECTION CONTROL ", " CONTROL PROGRAM", " SCALE UP", " STOP TB STRATEGY ", " SUPPLY ", " HEALTH POLICY ", " COLD CHAIN", 
                   " HEALTH PROMOTION ", " DSS ", " DISTRIBUTION SYSTEMS ", " SERVICE DELIVERY ", " SM&E ", " NMCP STRENGTHENING ", " OPERATIONS RESEARCH ", 
                   " PHARMACEUTICAL SYSTEMS REFORM ", " MANAGEMENT OF PHARMACEUTICAL SUPPLIES ", " ADMINISTRATIVE EXPENSES ", " ORGANIZATIONAL DEVELOPMENT ", 
                   " TECHNICAL SUPPORT ", " LMIS ", " REGIONAL COORDINATION ", " DHS ", " MIS ")
mal_hss_hrh <- c('ABCDEFGHI', " INFRASTRUCTUR", " MEDICAL EQUIPMENT", " SURGICAL EQUIPMENT", " HOSPITAL EQUIPMENT", " HOSPITAL EQMT ", " BUILDINGS ", " HEALTH FACILIT", 
                 " CONSTRUCT", " MEDICAL SCHOOL", "CENTERS OF EXCELLENCE", " TRAINING ", " CAPACIT", " SKILLED WORKER", " HEALTH WORKER", " HEALTH PROFESSIONAL", 
                 " HUMAN RESOURCE", " HUMAN CAPITAL ", " MEDICAL WORKER", " WORKFORCE ", " MEDICAL EDUCATION ", " HEALTH EDUCATION ", " CONTINUING EDUCATION ", 
                 " HEALTH MANAGEMENT", " MANAGEMENT AND COORDINATION ", " ADMINISTRATIVE MANAGEMENT ", " MANAGEMENT AND ADMINISTRATION ", "NURSE", "DOCTOR", "PHYSICIAN", 
                 "MIDWIFE", "MIDWIVES", "MEDICAL LABORATORY SCIENTIST", "SURGEON", "SPECIALIST", "PHARMACIST", "HEALTH LABOR", "LABOR MARKET", "PERSONNEL", "MEDICAL PRACTIONER", 
                 "DENTAL PRACTIONER", "TASK SHIFTING", " TECHNICAL ASSISTANCE ", " ADMIN ", " STAFF ", " MGMT ", " STAFFING ", " HEALTHCARE WORKER", " CHVS ")
mal_amr <- c('ABCDEFGHI', " ANTIMICROBIAL RESISTAN", " ANTI MICROBIAL RESISTAN", " ANTIBIOTIC RESISTAN", " AMR ", "DRUG RESISTAN", " MDR ", " XDR ", " RESISTANCE TESTING ", 
             " DRUG SUSCEPTIBILITY TESTING ", " DST ", " SECOND LINE " )

hfa <- list('mal_treat' = mal_treat, 'mal_diag' = mal_diag, 'mal_con_nets' = mal_con_nets, 'mal_con_irs' = mal_con_irs, 'mal_con_oth' = mal_con_oth, 
            'mal_comm_con' = mal_comm_con, 'mal_hss_other' = mal_hss_other, 'mal_hss_hrh' = mal_hss_hrh, 'mal_amr' = mal_amr)
rm(mal_treat, mal_diag, mal_con_nets, mal_con_irs, mal_con_oth, mal_hss_hrh, mal_hss_other, mal_amr, mal_comm_con)
#----------------------------# ####


cat('\n\n')
cat(green(' #################################\n'))
cat(green(' #### CRS PMI BEFORE UAE PREP ####\n'))
cat(green(' #################################\n\n'))


cat('  Read in post_CUV dataset\n')
#----# Read in post_CUV dataset #----# ####
dt <- fread(get_path('CRS', 'int', 'pmi_prepped.csv'))
#------------------------------------# ####

cat('  Perform manual keyword search on two cols\n')
#----# Perform manual keyword search on two cols #----# ####
for (healthfocus in names(hfa)) {
  i <- 1
  n <- length(hfa[[healthfocus]])
  cat(paste0('    Searching ', healthfocus, '\n'))
  while (i <= n) {
    srchstr <- hfa[[healthfocus]][[i]]
    for (var in c('purpose', 'proposedactivity')) {
      dt[, eval(paste0(healthfocus, '_', i, '_', var)) := str_count(dt[, get(paste0("upper_", var))], pattern = srchstr)]
    }
    i <- i + 1
  }
  dt <- rowtotal(dt, healthfocus, names(dt)[names(dt) %like% paste0(healthfocus, '_')])
  dt <- dt[, !c(names(dt)[names(dt) %like% paste0(healthfocus, '_')]), with=F]
}

dt[, mal_total := rowSums(.SD, na.rm = TRUE), .SDcols = names(hfa)]

for (pa in names(hfa)) {
  dt[, eval(paste0('final_', pa, '_frct')) := get(pa) / mal_total]
  dt[is.na(get(paste0('final_', pa, '_frct'))), eval(paste0('final_', pa, '_frct')) := 0]
  dt[, eval(paste0(pa, '_amt')) := get(paste0('final_', pa, '_frct')) * budget]
}

dt[mal_total == 0, final_mal_other_frct := 1]
dt[mal_total == 0, mal_other_amt := budget]

dt[, tmp := rowSums(.SD, na.rm = TRUE), .SDcols = grep("_amt", names(dt))]
if (dt[abs(tmp - budget) > 1, .N] != 0)
    stop("Error allocating PMI budgets to malaria HFAs")
#-----------------------------------------------------# ####

cat('  Save all-countries dataset\n')
#----# Save all-countries dataset #----# ####
save_dataset(dt, 'allcountries', 'PMI', 'raw')
#--------------------------------------# ####

cat('  Reshape + save out different formats\n')
#----# Reshape + save out different formats #----# ####
dt <- dt[, c('year', 'country', 'iso3', names(dt)[names(dt) %like% '_amt']), with=F]
dt <- collapse(dt, 'sum', c('year', 'country', 'iso3'), names(dt)[names(dt) %like% '_amt'])

dt <- melt(dt, measure.vars = names(dt)[names(dt) %like% '_amt'])
dt[, variable := gsub("_amt", "", variable)]
setnames(dt, c('country', 'variable', 'value'), c('country_lc', 'hfa', 'expenditure'))

dt[, annualtotal := sum(expenditure, na.rm=T), by=c('country_lc', 'year', 'iso3')]
dt[, frct := expenditure / annualtotal]
dt[, annualtotal := NULL]

save_dataset(dt, paste0('PMI_final_data_newway_', get_dah_param('CRS', 'update_MMYY')), 'PMI', 'fin')

#---

t <- copy(dt[, c('country_lc', 'hfa', 'year', 'expenditure')])
t <- dcast(t, formula = 'hfa + country_lc ~ year', value.var = 'expenditure')
setnames(t, names(t)[names(t) %ni% c('hfa', 'country_lc')], paste0('expenditure', names(t)[names(t) %ni% c('hfa', 'country_lc')]))
save_dataset(t, paste0('PMI_expenditure_HFA_', get_dah_param('CRS', 'update_MMYY')), 'PMI', 'fin')

#---

t <- copy(dt[, c('country_lc', 'hfa', 'year', 'frct')])
t <- dcast(t, formula = 'hfa + country_lc ~ year', value.var = 'frct')
setnames(t, names(t)[names(t) %ni% c('hfa', 'country_lc')], paste0('frct', names(t)[names(t) %ni% c('hfa', 'country_lc')]))
save_dataset(t, paste0('PMI_frct_HFA_', get_dah_param('CRS', 'update_MMYY')), 'PMI', 'fin')

#---

setnames(dt, 'iso3', 'ISO3_RC')
dt <- dt[, c('ISO3_RC', 'hfa', 'year', 'frct')]
dt <- collapse(dt, 'sum', c('year', 'ISO3_RC', 'hfa'), 'frct')
dt <- dcast(dt, formula = 'year + ISO3_RC ~ hfa', value.var = 'frct')
for (col in names(dt)[names(dt) %ni% c('year', 'ISO3_RC')]) {
  dt[is.na(get(col)), eval(col) := 0]
}
setnames(dt, 'year', 'YEAR')
setnames(dt, names(dt)[names(dt) %ni% c('YEAR', 'ISO3_RC')], paste0('frct', names(dt)[names(dt) %ni% c('YEAR', 'ISO3_RC')]))
save_dataset(dt, paste0('PMI_frct_merge_', get_dah_param('CRS', 'update_MMYY')), 'PMI', 'fin')

rm(dt, t, col, healthfocus, i, pa, srchstr, var, n, hfa)
#------------------------------------------------# ####

cat('  Bring in CRS data\n')
#----# Bring in CRS data #----# ####
dt <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_ADB_PDB_beforePMI.csv'))
#-----------------------------# ####

cat('  Merge PMI fractions\n')
#----# Merge PMI fractions #----# ####
t <- copy(dt[CHANNEL == 'BIL_USA' & YEAR %in% 2006:get_dah_param('CRS', 'data_year'), ])
frcts <- fread(get_path('PMI', 'fin', 'PMI_frct_merge_[crs.update_mmyy].csv'))

t <- merge(t, frcts, by=c('YEAR', 'ISO3_RC'), all.x=T)

t[, mal_DAH := rowSums(.SD, na.rm = TRUE),
  .SDcols = grep("mal_.+_DAH", names(t), value = TRUE)]

to_calc <- gsub("frct", "", names(t)[names(t) %like% 'frct'])
for (pa in to_calc) {
  t[, eval(paste0(pa, '_DAH_PMI')) := mal_DAH * get(paste0('frct', pa))]
}
pmiexp <- copy(t)

cat('    save out dataset for methods annex graph\n')
save_dataset(pmiexp, 'pmi_data_for_annex_graph', 'CRS', 'int')

rm(t, to_calc, pa, frcts)

#---

dt <- dt[!(CHANNEL == 'BIL_USA' & YEAR %in% 2006:get_dah_param('CRS', 'data_year')), ]
dt <- rbind(dt, pmiexp, fill=T)

to_calc <- gsub("frct", "", names(dt)[names(dt) %like% 'frct'])
for (pa in to_calc) {
  dt[!is.na(mal_other_DAH_PMI), eval(paste0(pa, '_DAH')) := get(paste0(pa, '_DAH_PMI'))]
}

dt <- dt[, !c(names(dt)[names(dt) %like% 'mal_' & names(dt) %like% '_PMI'],
              names(dt)[names(dt) %like% 'frctmal_'], 'mal_DAH'), with=F]
#-------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt, 'B_CRS_[crs.update_mmyy]_ADB_PDB_PMI_beforeUAE',
             'CRS', 'fin')
#------------------------# ####
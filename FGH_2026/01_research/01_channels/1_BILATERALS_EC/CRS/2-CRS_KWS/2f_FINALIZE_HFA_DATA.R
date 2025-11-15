#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize CRS HFA data
#---------------------# ####
#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)

SEARCH_COLS <- c("project_title", "short_description", "long_description",
                 "channel_name", "channel_reported_name")
#----------------------------# ####


cat('\n\n')
cat(green(' ###############################\n'))
cat(green(' #### CRS FINALIZE HFA DATA ####\n'))
cat(green(' ###############################\n\n'))


cat('  Read in DAH before NGOs\n')
#----# Read in DAH before NGOs #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_DAH_before_NGO_id_[crs.update_mmyy].csv'),
            strip.white = FALSE)

# restore the "original" versions of the search cols
# - before sending the data to stata for keyword search, we saved the original
#   string columns as "orig_<colname>" since we needed to apply some cleaning in
#   R to the columns that stata expects to receive (stata expects to receive
#   project_title, etc).
#   Now we have the "upper_<colname> cols, which are the final, cleaned columns
#   that the keyword search was run across. So we can restore the original columns
#   for the sake of tracking the raw string data.
#   However, note that some part of the stata process mangles strings, replacing
#   some characters with R-style hexadecimal representations
#   (e.g., e with an accent, or "é", becomes "<e9>"). So we want to
#   convert those back to the original unicode characters. Thus, we map on the
#   original strings, as saved just before the keyword search.
#
string_data <- arrow::read_parquet(get_path("crs", "raw", "crs_string_map.parquet"))
setDT(string_data)
repl_cols <- grep("fgh_id", names(string_data), invert = TRUE, value = TRUE)
string_data[, str_flag := 1]

dt[, (repl_cols) := NULL]
dt <- merge(dt, string_data, by = "fgh_id", all.x = TRUE)
if (dt[is.na(str_flag), .N] != 0) {
    stop("Some rows were not matched to the string data")
}

# also drop the copies that were kept on the dataframe and went through stata
dt[, paste0("orig_", repl_cols) := NULL]
dt[, str_flag := NULL]
rm(string_data)


#-----------------------------------# ####
tmp <- dt[, .(channel_code, channel_name)] |> unique()

cat('  Tag transfers for elimination\n')
#----# Tag transfers for elimination #----# ####
## note - these should be reviewed regularly as new channels are added
dt[, eliminations := 0]
# PPPs and mulilaterals
dt[channel_code_rev %in% c(
    # UN agenices (-WHO)
    41110, 41119, 41122, 47083,
    # WHO
    41143, 41307, 41321,
    # EC
    42001, 42003,
    # PPP
    30010,
    31006, #cepi
    47045, #gfatm
    47107, #gavi
    47122  #gavi
    ),
   eliminations := 1]

# regional development banks and World Bank
dt[channel_code_rev %in% c(
    # World Bank
    44000, 44001, 44002, 44007,
    # AfDB
    46002, 46003,
    # AsDB
    46004, 46005,
    # IDB
    46012, 46013
    ),
   eliminations := 1]

save_dataset(dt[channel_code_rev %in% c(46002, 46003, 46004, 46005, 46009, 46012), ],
             'CRS_bilateral_transfers_to_devbanks_1990_[crs.data_year]_[crs.update_mmyy]',
             'CRS', 'fin')


srch_strs <- c(
    # multi
    "UNAIDS",
    "UNITED NATIONS PROGRAMME ON HIV",
    "PAHO",
    "PAN AMERICAN HEALTH ORGANIZATION",
    "UNICEF",
    "UNITED NATIONS CHILDREN S FUND",
    "UNITED NATIONS CHILDREN FUND",
    "UNFPA",
    "UNITED NATIONS POPULATION FUND",
    "UNITAID",
    "WORLD HEALTH ORGANIZATION",
    "EUROPEAN COMMISSION",
    # ppp
    "CEPI",
    "COALITION FOR EPIDEMIC PREPAREDNESS",
    "GAVI",
    "GLOBAL FUND FOR VACCINES",
    "GFATM",
    "THE GLOBAL FUND",
    # bilat
    "EEA",
    "EUROPEAN ECONOMIC AREA",
    # gates
    "BMGF",
    "GATES FOUNDATION",
    # dev bank
    "WORLD BANK",
    "INTERNATIONAL BANK FOR RECONSTRUCTION AND DEVELOPMENT",
    "WB IBRD",
    "INTERNATIONAL DEVELOPMENT ASSOCIATION",
    "WB IDA",
    "AFDB",
    "AFRICAN DEVELOPMENT BANK",
    "AFRICAN DEVELOPMENT FUND",
    "ADB",
    "ASIAN DEVELOPMENT BANK",
    "ASIAN DEVELOPMENT FUND",
    "IDB",
    "INTER AMERICAN DEVELOPMENT BANK"
)

dt[, elim_str := ""] # to inspect results

for (str in srch_strs) {
    dt[upper_channel_reported_name %ilike% str & eliminations == 0,
       `:=`(
           elim_str = paste0(elim_str, "; ", str),
           eliminations = 1
       )]
}




# US inter-agency transfers to NIH research
dt[nih == 1 & isocode == "USA", eliminations := 1]
#-----------------------------------------# ####

cat('  Country-allocable projects\n')
#----# Country-allocable projects #----# ####
dt[, country := 1]
dt[recipient_code %in% c(89, 189, 237, 289, 298, 380, 389, 489, 498, 
                         589, 619, 679, 689, 789, 798, 889, 998, 9998), 
   country := 0]
#--------------------------------------# ####

cat("  Search for NGO names in CRS proj desc's\n")
#----# Search for NGO names in CRS proj desc's #----# ####

# load ngo list created by NGOs channel
ngo_years <- read.dta13(get_path("NGO", 'int',
                                 'NGO_agencyyears_[report_year]_0425.dta'))
setDT(ngo_years)
ngo_years[, RECIPIENT_AGENCY := string_to_std_ascii(RECIPIENT_AGENCY,
                                                    pad_char = NULL)]
ngolist <- unique(ngo_years$RECIPIENT_AGENCY)


# Combine all strings of interest into one, so only need to call %like% once
dt[, srchstr := paste(upper_channel_reported_name, upper_channel_name,
                      upper_project_title,
                      upper_short_description, upper_long_description,
                      sep = "; ")]
dt[, ngo_name := '']
nsrch <- length(ngolist)
for (i in seq_along(ngolist)) {
    if ((i - 1) %% 100 == 0)
        cat(paste0("[", i, "/", nsrch, " searches]\n"))
    l <- ngolist[i]
    dt[srchstr %flike% l, ngo_name := l]
}

dt[, ngo := 0]
dt[trimws(ngo_name) != '', ngo := 1]
dt[, srchstr := NULL]


# cache because ngo search can take a while
save_dataset(dt,
             "2f_post_ngo_search",
             channel = "CRS", stage = "int")

#---------------------------------------------------# ####

cat('  Fix NGOs that are misclassified\n')
#----# Fix NGOs that are misclassified #----# ####
dt[ngo_name %in% c('CHILDREN S FUND', 'CHILDRENS FUND', 'CHILDREN S FUND', 'CHILDRENS FUND'), 
   ngo := 0]
dt[ngo_name == 'EYE CARE', ngo := 0]
dt[ngo_name == 'INTERNATIONAL AID', ngo := 0]
dt[ngo_name == 'HEALTH LIMITED', ngo := 0]
dt[ngo_name == 'EL PORVENIR', ngo := 0]
dt[ngo_name == 'FUTURE GENERATIONS' & ! upper_channel_name %like% 'NGO', ngo := 0]
dt[ngo_name == 'PEOPLE IN NEED', ngo := 0]
dt[ngo_name == 'PLANNING ASSISTANCE', ngo := 0]

dt[ngo_name == "E CO", ngo := 0]
dt[ngo_name == "VIDA", ngo := 0]
dt[ngo_name == "GOAL", ngo := 0]
dt[upper_channel_reported_name %like% "GOAL", ngo := 1]
dt[ngo_name == "VITA", ngo := 0]
dt[upper_channel_reported_name == "VITA", ngo := 1]
dt[ngo_name == "PATH", ngo := 0]
dt[upper_channel_reported_name == "PATH", ngo := 1]
dt[ngo_name == "IPAS", ngo := 0]
dt[upper_channel_reported_name %like% "^IPAS", ngo := 1]
dt[ngo_name == "ZOE", ngo := 0]


# make sure we're not over tagging projects with simple phrases
dt[ngo == 1, .N, by = ngo_name][order(-N)][1:20]


dt[ngo_name == '', ngo := 0]
dt[ngo == 0, ngo_name := '']

dt[, NGO_NAME := channel_reported_name]
dt[NGO_NAME == "", NGO_NAME := channel_name]
dt[ngo_name != "", NGO_NAME := ngo_name]

# Below should be updated: looks similar to NGO_agency_years but names are diff
hst <- setDT(read.dta13(paste0(dah.roots$j,
                               'FILEPATH/FGH_2015_NGOfix.dta')))
hst[, u_m := 2]
dt[, m_m := 1]
dt <- merge(dt, hst, by='NGO_NAME', all=T)
dt[, merge := rowSums(dt[, c('u_m', 'm_m')], na.rm=T)]
dt <- dt[merge %in% c(1,3), !c('u_m', 'm_m')]
dt[merge == 3, ngo := 1]

dt[, keep := 0]
for (i in 1990:2014) {
   dt[year == get(paste0('obs', i)), keep := 1]
}
dt[, merge := NULL]
#-------------------------------------------# ####

cat('  Merge with NGO-years to tag elimination CRS NGO data\n')
#----# Merge with NGO-years to tag elimination CRS NGO data #----# ####
setnames(dt, 'NGO_NAME', 'RECIPIENT_AGENCY')
dt <- merge(dt, ngo_years, by='RECIPIENT_AGENCY', all.x=T)

# Fix colnames that contain .x and .y
to_fix <- names(dt)[names(dt) %like% '.x' & (names(dt) %like% 'obs' | names(dt) %like% 'intl')]
to_fix <- gsub('.x', '', to_fix)
for (col in to_fix) {
    dt[is.na(get(paste0(col, '.x'))) & !is.na(get(paste0(col, '.y'))), 
       eval(paste0(col, '.x')) := get(paste0(col, '.y'))] 
   dt[, eval(paste0(col, '.y')) := NULL]
   setnames(dt, paste0(col, '.x'), col)
}

yrcol <- paste0("obs", get_dah_param("crs", "data_year"))
if (! yrcol %in% names(dt)) {
   dt[, (yrcol) := get(paste0("obs", get_dah_param("crs", "data_year") - 1))] 
}

for (i in 1990:get_dah_param('CRS', 'data_year')) {
   dt[year == get(paste0('obs', i)), keep := 1]
}

dt[(ngo == 1 & keep == 1 & intl == 0) & donor_name != "EU Institutions", 
   ngo_eliminations := 1]
dt[(ngo == 1 & keep == 1 & intl == 1) & donor_name != "EU Institutions", 
   ingo_eliminations := 1]
dt[, ngo_name := NULL]
setnames(dt, 'RECIPIENT_AGENCY', 'ngo_name')

dt[ISO3_RC == "", ISO3_RC := 'QZA']
dt[, LEVEL := 'COUNTRY']
dt[country == 0 & ISO3_RC %in% c("QMA","QMC","QMD","QME","QNA","QNB","QNC","QNE",
                                 "QRA","QRB", "QRC","QRD", "QRE", "QRS", "QSA", "QTA"), 
   LEVEL := 'REGIONAL']
dt[country == 0 & ISO3_RC %in% c("QZA", "WLD"), 
   LEVEL := 'GLOBAL']
#----------------------------------------------------------------# ####

cat('  Generate indicator for DAH_G\n')
#----# Generate indicator for DAH_G #----# ####
# DAH-G = aid through government channels
# gov = 0 means DAH is unallocable (DAH - NA), gov = 1 means DAH-G, gov = 2 means DAH-NG.
# Generating indicator for DAH-G --  aid through government channels. 
# We only recognize DAHG/NG on country level projects and regional projects, NOT global /unallocable projects. We also set PEPFAR projects to DAHNA.

setnames(dt, 'gov', 'govkws')
dt[, gov := NA_real_]

# DAHG: 10000 (public sector), 11000 (donor government), 12000 (recipient government), 13000 (third country government)
dt[(channel_code_rev <20000 & channel_code_rev >0) & PEPFAR_FC != 1 & LEVEL != "GLOBAL", gov := 1]

# DAHNG: multilaterals, dev banks, NGOs
# 20000, 21016 21018 red cross, 21020 hiv alliance, 21023 federation...... 22000 23000 ngos, 30000 foundations, ......,31000 network/partnership, 
#     4000 organizations, 41000 41114 41119 41124 41126 41127 41128 41130 41135(un to volunteers and uni), 41304, 41305 41310 41312 41313 UN, 
# 41103 41110 41122 organizations and fund, 41140 world food programme, 41143 41307 WHO, 41301 food and agriculture, 41302 International Labour Organisation - 
#     Assessed Contributions, 42000 420001 EC, 44000 44001 44002 44003 44004 ... 46004, 46013 banks, 
# 47000 multi foundations universities, 47011 Caribbean Community Secretariat, 47015 47017 fund... , 50000 60000 enterprises, 70000 universities
dt[(channel_code_rev >=20000 ) & PEPFAR_FC != 1 & LEVEL != "GLOBAL", gov := 2]
dt[channel_name %in% c("National NGOs", "International NGOs", "Donor country-based NGO", "International NGO"), gov := 2]

# DAHNA: All the others. 
# Missings and global projects get filled in with gov=0. Gavi and GFATM are set to DAHNA. PEPFAR projects are set to NA.
dt[channel_code_rev %in% c(47122, 47045), gov := 0]
dt[is.na(gov), gov := 0]

dt[channel_name %like% 'International Food Policy Research Institute', gov := 2]
dt[channel_name == "Banks (deposit taking corporations)", gov := 2]
#----------------------------------------# ####

cat('  Final processing & saving datasets\n')
#----# Final processing & saving datasets #----# ####

dt <- dt[, !c('proj_N', 'proj_n', 'neg', 'neg_proj', 'keep', names(dt)[names(dt) %like% 'obs']), with=F]

save_dataset(dt, 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2', 'CRS', 'int')

# Create file of bilateral agencies' total DAH by channel-type (used for PUBLIC SECTOR DAH BY DONOR COUNTRY/ CHANNEL figure)
dt <- dt[eliminations != 1, ]
dt[channel_code_rev >= 10000 & channel_code_rev < 20000, channel_code_rev := 10000]
dt[channel_code_rev >= 20000 & channel_code_rev < 30000, channel_code_rev := 20000]
dt[channel_code_rev >= 30000 & channel_code_rev < 40000, channel_code_rev := 30000]
dt[channel_code_rev >= 40000 & channel_code_rev < 50000, channel_code_rev := 40000]
dt[channel_code_rev >= 50000 & channel_code_rev <= 70000, channel_code_rev := 50000]

dt <- collapse(dt[year == get_dah_param('CRS', 'data_year'), ], 'sum', c('isocode', 'channel_code_rev'), c('all_commcurr', 'all_disbcurr'))
for (type in c('comm', 'disb')) {
   dt[, eval(paste0('tot_', type)) := sum(get(paste0('all_', type, 'curr')), na.rm=T), by='isocode']
   dt[, eval(paste0('pct_chn_', type)) := get(paste0('all_', type, 'curr')) / get(paste0('tot_', type))]
}

dt[is.na(pct_chn_disb), pct_chn_disb := pct_chn_comm]
dt <- dt[, c('isocode', 'channel_code_rev', 'pct_chn_disb')]

dt <- dcast(dt, formula = 'isocode ~ channel_code_rev', value.var = 'pct_chn_disb')
setnames(dt, names(dt)[names(dt) %like% '0'], paste0('pct_chn_disb', names(dt)[names(dt) %like% '0']))
for (col in names(dt)[names(dt) %like% 'pct_chn_disb']) {
   dt[is.na(get(col)), eval(col) := 0]
}

setnames(dt, 'isocode', 'ISO_CODE')

save_dataset(dt, 'B_CRS_[crs.update_mmyy]_PCTDISB_BYCHANNEL_[crs.data_year]', 'CRS', 'int')
#----------------------------------------------# ####
## checking runtime for slurm optimazation purpose
end.time <- Sys.time()
time.taken <- end.time - start.time

cat('\n\n')
cat(green(' #####################################\n',paste0('####          END OF 2f          ####\n'),
          paste0('#### Time to Run: ', round(time.taken/60,3),' minutes  ####\n'),'#####################################\n\n'))

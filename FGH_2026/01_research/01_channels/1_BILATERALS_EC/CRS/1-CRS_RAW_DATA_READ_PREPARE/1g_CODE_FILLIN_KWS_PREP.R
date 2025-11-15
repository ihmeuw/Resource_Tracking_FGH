#### #----#                    Docstring                    #----# ####
# Project:  FGH 
# Purpose:  Fill in channel codes. This involves two steps. A - filling in missing codes
#           using reported channel name. B - performing keyword search on
#           descriptive variables to fill in channel codes. Finally, it also saves
#           out the data set used as input in the keyword search stage (stage 2).
#           Step 6 of previous 1_CRS_DB_PRPEARE.do
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(readstata13, crayon, stringr)

# Variable prep
channels <- list(
  mincd = c('MINISTRY', 'MINISTERIO', 'MINISTRIES', 'MINISTERE'),
  pscd = c(" LTD ", " INC ", " PRIVATE ", 'ENTERPRISES', " GMBH ", 'SOFITEL', 'HOTEL', 'COMMERCIAL', "PRICE WATERHOUSE COOPERS", "EXPORT COMPANY"),
  consult = c('CONSULTING', " CONSULTA"),
  univcd = c('COLLEGE', " UNIV ", 'UNIVERSI', 'SCHOOL', 'ECOLE', "UNIVERITY OF", "UNIVER OF"),
  gavi = c('GAVI', 'IFFIM', "GLOBAL ALLIANCE FOR VAC", "GLOBAL FUND FOR VAC", "INTERNATIONAL FINANCE FACILITY FOR IMMUN"),
  gfatm = c('GFATM', 'UNITAID', "GLOBAL FUND"),
  who = c(" WHO ", "WORLD HEALTH ORG", "WHODRUGS", "VOLUNTARY FUND FOR HEALTH PROMOTION", "VFHP", "WHO TRUST FUND"),
  unicef = c('UNICEF', "UNITED NATIONS CHILDREN"),
  unfpa = c('UNFPA', "UNITED NATIONS POPULATION FUND"),
  unaids = c('UNAIDS', "JOINT PROGRAMME ON HIV AIDS"),
  undp = c(" UNDP ", "UNITED NATIONS DEVELOPMENT PROGRAM"),
  paho = c('PAHO', "PAN AMERICAN HEALTH ORG"),
  ida = c(" IDA ", "INTERNATIONAL DEVELOPMENT ASSOC"),
  ibrd = c(" IBRD ", "INTERNATIONAL BANK FOR RECONSTRUCTION"),
  pmi = c('PRESIDENTSMALARIAINITIATIVE', " PMI ",  "PRESIDENTS MALARIA INITIATIVE", "PRESIDENT S MALARIA INITIATIVE"),
  adb = c(" ADB ", "ASIAN DEVELOPMENT BANK", "ASIAN DEVELOPMENT FUND"),
  afdb = c(" AFDB ", "AFRICAN DEVELOPMENT BANK", "AFRICAN DEVELOPMENT FUND"),
  pepfar = c(" PEPFAR ", "PRESIDENT S EMERGENCY PLAN FOR AIDS RELIEF", "GLOBAL HIV AIDS INITIATIVE"),
  rbm = c("ROLL BACK MALARIA"),
  stp = c("STOP TB"),
  tdr = c(" TDR ", "SPECIAL PROGRAMME FOR RESEARCH AND TRAINING IN TROPICAL DISEASES"),
  sak = c("SASAKAWA HEALTH TRUST FUND"),
  apoc = c(" APOC ", "ONCHOCERCIASIS CONTROL"),
  hdr = c("SPECIAL PROGRAMME OF RESEARCH DEVELOPMENT AND RESEARCH TRAINING IN HUMAN REPRODUCTION"),
  gain = c(" GAIN ", "GLOBAL ALLIANCE FOR IMPROVED NUTRITION"),
  iavi = c(" IAVI ", "INTERNATIONAL AIDS VACCINE INITIATIVE"),
  ipm = c("PARTNERSHIP ON MICROBICIDES", "PARTNERSHIP FOR MICROBICIDES"),
  nih = c(" NIH ", "NATIONAL INSTITUTE OF HEALTH", "NATIONAL INSTITUTES OF HEALTH"),
  res = c('RESEARCH', "R&D", "R & D", "RESEARCH AND DEVELOPMENT", "DRUG DISCOVERY", 'RECHERCHE', 'FORSCHUNG', 'INVESTIGACION', 'ONDERZOEK', 'STUDY', 'ETUDE',
          "RECHERCHE ET LE DEVELOPPEMENT", 'UNTERSUCHUNG', 'ESTUDIO', "ONDERZOEK EN ONTWIKKELING", "STUDIE ", 'RICERCA', 'STUDIO'),
  study = c('STUDY', 'STUDIES', 'ESTUDIO', " ETUDE "),
  gov = c('GOVERNMENT', 'GOBIERNO', 'GOUVERNEMENT', 'MINISTRY', 'MINISTERIO', 'MINISTRIES', 'MINISTERE', 'MINISTERO', 'MINISTERIE', 'MINISTERIUM', 'REGERING',
          'REGIERUNG', 'REGIERUNGSFORM', 'GOVERNO', 'OVERHEID', 'HALSOMINISTERIET', 'HALLITUS', "DEPARTMENT OF HEALTH", "DEPARTMENT OF PLANNING"),
  ngos = c(" NGO ", " NGOS ", " ONG ", "NON GOVERNMENT"),
  gain_corr = c(" TO GAIN ", " GAIN DE TEMPS ", " WEIGHT GAIN ", " GAIN ACCESS ", " WILL GAIN ", " GAIN A BETTER", " GAIN BETTER", " GAIN ACCURATE ",
                " CAN GAIN ", " GAIN OF ", "ECONOMIC GAIN", "BRAIN GAIN", "GREATEST GAIN"),
  who_corr = c("PEOPLE WHO", "WOMEN WHO", "MEN WHO", "MALES WHO", "GIRLS WHO", "RESEARCHERS WHO", "THOSE WHO", "CITIZENS WHO", "ADVISER WHO", "WHO HOLD",
               "WHO HAVE", " WHO HAD FACILITATED", "WHO ARE", "WHO CONSTITUTE", "WHO WORK", "WHO WOULD", "WHO IS BEING", "WORKERS WHO", "PRINCIPALS WHO",
               "SPECIALISTS WHO")
)
#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### CRS PRE KWS CLEANING ####\n'))
cat(green(' ##############################\n\n'))


cat('  Read input data\n')
#----# Read input data #----# ####
dt <- fread(get_path('CRS', 'int', '1f_proj_match.csv'))

channel_names <- fread(
    get_path('CRS', 'common', 'B_CRS_unspecified_channelnames.csv')
)
channel_names[, act := NULL]
if (channel_names[, .N, by = channel_reported_name][N > 1, .N] != 0) {
    stop('Duplicate channel mappings in the channel names database')
}
#---------------------------------------------------------------------#

cat('  Fill in channel codes\n')
#----# Fill in channel codes #----# ####
dt[, channel_reported_name := gsub(
    "\u0081", # a control character in utf8 which will prevent merging
    "<81>",   # replace with literal string representation
    channel_reported_name
)]
dt[, channel_reported_name := fcase(
    channel_reported_name == "Médecins sans Frontières", "MsF",
    rep_len(TRUE, .N), channel_reported_name
)]


dt[, channel_code_miss := fifelse(is.na(channel_code) | channel_code == 0, 1, 0)]
dt[, channel_name_miss := fifelse(channel_name == '', 1, 0)]
dt[, channel_reported_name_miss := fifelse(channel_reported_name == '', 1, 0)]

# if channel name/code is missing but the reported-name is available,
# fill in the channel name/code based on the 'unspecified channelnames' database
dt[, ch_reportedname_fill := fifelse(
    channel_reported_name_miss == 0 & channel_name_miss == 1 & channel_code_miss == 1,
    1,
    0
)]
dt_with_chreportedname <- dt[ch_reportedname_fill == 1]
dt_with_chreportedname <- merge(dt_with_chreportedname,
                                channel_names,
                                by = "channel_reported_name", all.x = TRUE)
#NOTE:
# Add new entries to the B_CRS_unspecified_channelnames database (by hand) and re-run above.
# Identify new entries with the below line, and find the correct channel code
# from the database.
# dt_with_chreportedname[is.na(channel_code_new), unique(channel_reported_name)]
# You must map the channel reported name to a channel-code - this requires some
# investigation to determine if the channel already exists in the CRS, or to
# determine what the best match is.
if (dt_with_chreportedname[is.na(channel_code_new), .N] != 0) {
    stop("New channel_reported_name entries found. Please add them to the database.")
}

dt <- dt[ch_reportedname_fill == 0]
dt[, channel_code_new := NA]
dt <- rbind(dt, dt_with_chreportedname)
dt[, ch_reportedname_fill := NULL]

dt[, channel_code_rev := 0]
dt[!is.na(channel_code), channel_code_rev := channel_code]
dt[!is.na(channel_code_new), channel_code_rev := channel_code_new]
dt[!is.na(channel_code) & channel_code > 50000, channel_code_rev := 0] # Other
rm(channel_names, dt_with_chreportedname)
#---------------------------------------------------------------------#

cat('  Clean characters\n')
#----# Clean characters #----# ####
string_cols <- c('donor_name',
                 'channel_name', 'channel_reported_name',
                 'recipient_name',
                 'project_title', 'short_description', 'long_description')
dt[, paste0("upper_", string_cols) := lapply(.SD, string_to_std_ascii),
    .SDcols = string_cols]

#---------------------------------------------------------------------#

cat('  Run local keyword search\n')
#----# Run local keyword search #----# ####
for (channel in names(channels)) {
  i <- 1
  
  cat(paste0('    Searching for ', channel, ' keywords\n'))
  
  while (i <= length(channels[[channel]])) {
    srchstr <- channels[[channel]][[i]]
    
    dt[, eval(paste0(channel, '_', i)) := 0]
    
    if (channel %in% c('mincd', 'pscd', 'univcd')) {
      dt[upper_channel_name %like% srchstr | upper_channel_reported_name %like% srchstr,
         eval(paste0(channel, '_', i)) := 1]
    } else {
      dt[upper_project_title %like% srchstr | upper_short_description %like% srchstr |
           upper_long_description %like% srchstr | upper_channel_name %like% srchstr |
           upper_channel_reported_name %like% srchstr, eval(paste0(channel, '_', i)) := 1]
    }
    
    i <- i + 1
  }
  
  for (col in names(dt)[names(dt) %like% paste0(channel, '_')]) {
    dt[get(col) == 1, eval(channel) := 1]
  }
  dt[is.na(get(channel)), eval(channel) := 0]
  
  dt <- dt[, !c(names(dt)[names(dt) %like% paste0(channel, '_')]), with=FALSE]
  rm(channel, i, col, srchstr)
}

dt[gain_corr == 1 & gain == 1, gain := 0]
dt[who_corr == 1 & who == 1, who := 0]

dt[, numagencies := rowSums(.SD, na.rm = TRUE),
   .SDcols = c('gavi', 'gfatm', 'unaids', 'unicef', 'undp', 'unfpa', 'who', 'rbm', 'stp', 'apoc', 'tdr',
               'sak', 'hdr', 'ibrd', 'pmi', 'pepfar', 'ida', 'gain', 'iavi', 'ipm', 'adb', 'afdb', 'paho',
               'pscd', 'consult')]
#---------------------------------------------------------------------#

cat('  Fill in channel codes using KWS results\n')
#----# Fill in channel codes using KWS results #----# ####
dt[univcd == 1 & channel_code_rev == 0, channel_code_rev := 70000]
dt[pscd == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 60000]
dt[gavi == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 47122]
dt[gfatm == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1 &
     !str_detect(upper_long_description, 'GLOBAL FUND LIAISON'), 
   channel_code_rev := 47045]
dt[unicef == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 41122]
dt[unfpa == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 41119]
dt[unaids == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 41110]
dt[undp == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1,
   channel_code_rev := 41114]
dt[paho == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 47083]
dt[ida == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 44002]
dt[ibrd == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 44001]
dt[pmi == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 21032]
dt[adb == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 46004]
dt[afdb == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 46002]
dt[pepfar == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 10000]
dt[(who == 1 | rbm == 1 | stp == 1 | apoc == 1 | tdr == 1 | sak == 1 | hdr == 1) & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1,
   channel_code_rev := 41307]
dt[gain == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 30001]
dt[iavi == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 30005]
dt[ipm == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & numagencies == 1, 
   channel_code_rev := 30006]
dt[consult == 1 & (channel_code_rev == 0 | channel_code_rev == 50000) & !str_detect(upper_short_description, "CONSULTING ROOM") & 
     !str_detect(upper_short_description, "CONSULTANCY ROOM"), 
   channel_code_rev := 60001]

dt[consult == 1 & channel_code_rev == 60001, gov := 0]
dt[consult == 1 & channel_code_rev == 60001, ngos := 0]

dt[gov == 1 & ngos == 0 & (channel_code_rev == 0 | channel_code_rev == 50000)	& numagencies == 0, channel_code_rev := 10000]
dt[ngos == 1 & gov == 0 & (channel_code_rev == 0 | channel_code_rev == 50000)	& numagencies == 0, channel_code_rev := 20000]
dt[ngos == 1 & gov == 1 & upper_short_description==" GOVERNMENT CONTRIBUTIONS VIA NGO ", channel_code_rev := 20000]

dt[ngos == 1 & gov == 1 & upper_short_description==" GOVERNMENT CONTRIBUTIONS VIA NGO ", gov := 0]
#---------------------------------------------------------------------#

cat("  Correcting channelcodes for projects we're aware of\n")
#----# Correcting channelcodes for projects we're aware of #----# ####
dt[project_title == "GLOBAL ALLIANCE FOR VACCIN & IMMUNIZ", channel_code_rev := 47122]
dt[channel_reported_name == "Global Alliance TB Drug Develo", channel_code_rev := 30000]
dt[channel_reported_name == "GLOBAL ALLIANCE FOR TB DRUG DEVELOPMENT", channel_code_rev := 30000]
dt[channel_reported_name == "Global Alliance for Improved Nutrition", channel_code_rev := 30001]
dt[channel_reported_name == "GAIN (GLOBAL ALLIANCE FOR IMPROVED NUTRITION)", channel_code_rev := 30001]
dt[channel_reported_name == "American Austrian Foundation" | channel_reported_name == "AAF", channel_code_rev := 20000]

dt[upper_channel_reported_name == upper_recipient_name, channel_code_rev := 12000]
dt[, upper_recipient_name := NULL]

dt[upper_channel_reported_name == upper_donor_name, channel_code_rev := 11000]
dt[, upper_donor_name := NULL]

dt[str_detect(upper_channel_reported_name, "DFID") | str_detect(upper_channel_reported_name, "PUBLIC CONTRACTORS"), 
   channel_code_rev := 10000]
dt[str_detect(upper_channel_reported_name, "ICDDR"), 
   channel_code_rev := 47053]
dt[channel_reported_name == "Dejene, Dr. Michael" | str_detect(upper_channel_reported_name, "VIVERRA") |
     str_detect(upper_channel_reported_name, "ERNST & YOUNG") | str_detect(upper_channel_reported_name, "AUSTIN POCK + PARTNERS") |
     str_detect(upper_channel_reported_name, "PRIVATE INDIVIDU") |
     channel_reported_name == "APIDC Biotechnology Venture Fund" | channel_reported_name == "CHATEAU LAURIER" |
     channel_reported_name == "Craig Edmund Courtney" | channel_reported_name == "Creative Storm Networks" |
     (str_detect(upper_channel_reported_name, "DELOITTE") & channel_code != 10000), 
   channel_code_rev := 60000]


dt[channel_reported_name %in% c("AURION", "AL-OMEGA ACCOUNTANTS", "BLAKE DAWSON WALDRON", "BRYANT, CERI L", 
                              "AMS NON-CONTRACT PARTNER", "CHAMBERLIN, CHRISTOPHER", "O’NEILL, JANETTE",
                              "SAMPSON, LOUISE", "SHAW, LEA", "CODE=50000; NAME: TIRSOVA KINDERKLINIK BELGRAD",
                              "CODE=50000; NAME: ADC DEVELOPMENT CORPORATION PROJEKTMANAGEMENT GESMBH",
                              "VIVERRA FILMS", "UNIVERSAL FINANCIAL MANAGEMENT SOLUTION", "TREATMENT ACTION CAMPAIGN",
                              "Solli, Nomatemba", "NICOLAES TULP INSTITUUT", "NEPALI TECHNICAL ASSISTANCE GROUP",
                              "ERNST & YOUNG BUSINESS ADVISORY"), 
   channel_code_rev := 60001]
dt[channel_reported_name %in% c("PLAN INTERNATIONAL AUSTRALIA", "FRED HOLLOWS FOUNDATION, THE", 
                              "THE WALTER AND ELIZA HALL INSTITUTE OF MEDICAL RESEARCH", 
                              "Makassed Islamic Charitable Society", "ST JOHN OF GOD HEALTH CARE INC T/A ST JOHN OF GOD HOSPITAL BALLARAT") |
     str_detect(upper_channel_reported_name,"CARTER CENTER"), channel_code_rev := 20000]
dt[channel_reported_name == "ASIA PACIFIC ECONOMIC CO-OPERATION", channel_code_rev := 47000]

dt[isocode == "ARE" & channel_code_rev == 0 & (str_detect(upper_channel_reported_name," HUMANITARIAN FOUNDATION ") | 
                                                upper_channel_reported_name == " RED CRESCENT " | upper_channel_reported_name == " NOOR DUBAI FOUNDATION " |
                                                upper_channel_reported_name == " LIFE FOR RELIEF AND DEVELOPMENT "), channel_code_rev := 20000]
dt[isocode == "ARE" & upper_channel_reported_name == " SHARJAH AWQAF GENERAL TRUST AWQAF ", channel_code_rev := 11000]
#---------------------------------------------------------------------#

cat('  Final cleaning\n')
#----# Final cleaning #----# ####
dt <- dt[order(year, project_number),]
dt[, num_id := 1:.N]


if (dt[, .N, by = fgh_id][N > 1, .N] != 0) {
    stop("At least one 'fgh_id' appears multiple times. This is a unique identifier",
         "so this means rows have been duplicated during data processing.\n",
         "       Please investigate and fix.")
}

dt[, paste0("upper_", string_cols) := NULL]
#---------------------------------------------------------------------#

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'CRS_[crs.update_mmyy]_before_keywordsearch',
             'CRS', 'int')
#---------------------------------------------------------------------#

cat('\n\n\t ###############################\n\t #### file  CRS 1g complete ####\n\t ###############################\n\n')


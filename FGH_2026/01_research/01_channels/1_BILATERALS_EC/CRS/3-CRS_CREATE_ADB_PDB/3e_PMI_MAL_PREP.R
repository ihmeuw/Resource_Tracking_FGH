#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Prepare PMI malaria data by combining files and cleaning strings
#---------------------# ####

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
start.time <- Sys.time()
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon)

#----------------------------# ####


cat('\n\n')
cat(green(' ##############################################\n'))
cat(green(' #### PMI MALARIA PREP - CLEAN STRING COLS ####\n'))
cat(green(' ##############################################\n\n'))


cat('  Read in + split Mekong Subregion data\n')
#----# Read in + split Mekong Subregion data #----# ####
dt <- fread(get_path("pmi", "raw", "[pmi.update_mmyy]/greater-mekong-subregion.csv"),
            encoding = "Latin-1")
for (c in c('Laos', 'Vietnam', 'China')) {
  dt[, COUNTRY := c]
  dt[COUNTRY == 'Laos', iso3 := 'LAO']
  dt[COUNTRY == 'Vietnam', iso3 := 'VNM']
  dt[COUNTRY == 'China', iso3 := 'CHN']
  fwrite(dt, get_path('PMI', 'raw', paste0(c, '.csv')))
}
rm(dt, c)
#-------------------------------------------------# ####

cat('  Prepare data for create_upper_vars\n')
#----# Prepare data for create_upper_vars #----# ####
pmi_extract_raw_dir <- get_path("pmi", "raw", "[pmi.update_mmyy]")
files <- list.files(pmi_extract_raw_dir, pattern = "*.csv")

dt <- data.table()
for (file in files) {
  t <- fread(file.path(pmi_extract_raw_dir, file), encoding = "Latin-1")
  colnames(t) <- gsub(" ", "", tolower(colnames(t)))
  t[, budget := as.character(budget)]
  t[, budget := gsub("$", "", budget, fixed=TRUE)]
  t[, budget := gsub(",", "", budget, fixed=TRUE)]
  dt <- rbind(dt, t, fill=T)
  rm(t)
}
dt <- dt[, c("year", "country", "iso3", "budget", "hfa",
             "purpose", "proposedactivity")]


# String cleaning - should be unnecessary now that encoding is correctly specified
dt[proposedactivity == 'Procurement of artemether<ad> lumefantrine for the public and private sectors', 
   proposedactivity := 'Procurement of artemether- lumefantrine for the public and private sectors']
dt[proposedactivity == 'Support the establishment of the Partners<92> Forum secretariat', 
   proposedactivity := "Support the establishment of the Partners' Forum secretariat"]
dt[proposedactivity == 'Support to the Partners<92> Forum secretariat', 
   proposedactivity := "Support to the Partners' Forum secretariat"]
dt[proposedactivity == 'Malaria Partners<92> Forum', 
   proposedactivity := "Malaria Partners' Forum"]
dt[proposedactivity == 'Support to Malaria Partners<92> Forum secretariat', 
   proposedactivity := "Support to Malaria Partners' Forum secretariat"]
dt[proposedactivity == 'Support to Malaria Partners<92> Forum Secretariat', 
   proposedactivity := "Support to Malaria Partners' Forum Secretariat"]
dt[proposedactivity == '1. Support development and implementation of new integrated communication strategy. Support household visits and group education to promote net use and malaria prevention, recognizing signs of malaria and increasing care seeking behavior and encouraging ANC attendance and IPTp through women<92>s groups, CHWs, and mass media', 
   proposedactivity := "1. Support development and implementation of new integrated communication strategy. Support household visits and group education to promote net use and malaria prevention, recognizing signs of malaria and increasing care seeking behavior and encouraging ANC attendance and IPTp through women's groups, CHWs, and mass media"]
dt[proposedactivity == 'Strengthen Benin<92>s HMIS system and NMCP<92>s M&E capacity', 
   proposedactivity := "Strengthen Benin's HMIS system and NMCP's M&E capacity"]
dt[proposedactivity == "Strengthen Benin's HMIS system and NMCP<92>s M&E capacity", 
   proposedactivity := "Strengthen Benin's HMIS system and NMCP's M&E capacity"]
dt[proposedactivity == '1. Support household visits and group education to promote net use and malaria prevention through women<92>s groups and community health workers and mass media', 
   proposedactivity := "1. Support household visits and group education to promote net use and malaria prevention through women's groups and community health workers and mass media"]
dt[proposedactivity == 'Subtotal <96> Pharmaceutical Management', 
   proposedactivity := 'Subtotal : Pharmaceutical Management']
dt[proposedactivity == 'Procurement<a0>and<a0>distribution<a0>of<a0>SP', 
   proposedactivity := 'Procurement and distribution of SP']
dt[proposedactivity == 'Procurement and<a0>distribution<a0>of<a0>RDTs', 
   proposedactivity := 'Procurement and distribution of RDTs']
dt[proposedactivity == 'Procurement and<a0>distribution<a0>of<a0>ACTs<a0>(AL)', 
   proposedactivity := 'Procurement and distribution of ACTs (AL)']
dt[proposedactivity == 'Support to Donor<92>s coordination', 
   proposedactivity := "Support to Donor's coordination"]
dt[proposedactivity == 'Support for Journ<e9>es scientifiques', 
   proposedactivity := 'Support for Journees scientifiques']
dt[proposedactivity == 'Contribute to the organization of Journ<e9>es Scientifique in conjunction with World Malaria Day', 
   proposedactivity := 'Contribute to the organization of Journees Scientifique in conjunction with World Malaria Day']
dt[proposedactivity == 'Contribute to mid<ad> term review of 2016-2020 National Malaria Control Strategic Plan', 
   proposedactivity := 'Contribute to mid- term review of 2016-2020 National Malaria Control Strategic Plan']
dt[proposedactivity == 'SUBTOTAL IN<ad> COUNTRY STAFFING', 
   proposedactivity := 'SUBTOTAL IN- COUNTRY STAFFING']
dt[proposedactivity == 'IEC/BCC for LLINs/ACTs manage<ad> ment (CBO support)', 
   proposedactivity := 'IEC/BCC for LLINs/ACTs manage- ment (CBO support)']
dt[proposedactivity == 'Procurement of insecti<ad> cide', 
   proposedactivity := 'Procurement of insecti- cide']
dt[proposedactivity == 'Environmental com<ad> pliance', 
   proposedactivity := 'Environmental com- pliance']
dt[proposedactivity == 'Support for quality assur<ad> ance system for micro<ad> scopy and RDTs', 
   proposedactivity := 'Support for quality assur- ance system for micro- scopy and RDTs']
dt[proposedactivity == "Support for quality assur- ance system for micro<ad> scopy and RDTs", 
   proposedactivity := 'Support for quality assur- ance system for micro- scopy and RDTs']
dt[proposedactivity == 'Procurement of chloro<ad> quine, pre-referral treat<ad> ment and drugs for severe malaria', 
   proposedactivity := 'Procurement of chloro- quine, pre-referral treat- ment and drugs for severe malaria']
dt[proposedactivity == 'Strengthening of drug management system ca<ad> pacity', 
   proposedactivity := 'Strengthening of drug management system ca- pacity']
dt[proposedactivity == 'Training Zonal Health Officers in Data Man<ad> agement', 
   proposedactivity := 'Training Zonal Health Officers in Data Man- agement']
dt[proposedactivity == 'SUBTOTAL <97> ITNs', 
   proposedactivity := 'SUBTOTAL - ITNs']
dt[proposedactivity == 'SUBTOTAL <97> IRS', 
   proposedactivity := 'SUBTOTAL - IRS']
dt[proposedactivity == 'SUBTOTAL <97> IPTp', 
   proposedactivity := 'SUBTOTAL - IPTp']
dt[proposedactivity == 'SUBTOTAL <97> Case Management and Diagnosis', 
   proposedactivity := 'SUBTOTAL - Case Management and Diagnosis']
dt[proposedactivity == 'SUBTOTAL <97> Case Management and Treatment', 
   proposedactivity := 'SUBTOTAL - Case Management and Treatment']
dt[proposedactivity == 'SUBTOTAL <97> Pharmaceutical Management', 
   proposedactivity := 'SUBTOTAL - Pharmaceutical Management']
dt[proposedactivity == 'SUBTOTAL <97>HIV and Malaria', 
   proposedactivity := 'SUBTOTAL -HIV and Malaria']
dt[proposedactivity == 'SUBTOTAL <97> Community Mobilization & Capacity Building', 
   proposedactivity := 'SUBTOTAL - Community Mobilization & Capacity Building']
dt[proposedactivity == 'SUBTOTAL <97> M&E', 
   proposedactivity := 'SUBTOTAL - M&E']
dt[proposedactivity == 'SUBTOTAL <97> In-Country Staffing', 
   proposedactivity := 'SUBTOTAL - In-Country Staffing']
dt[proposedactivity == 'Long term Training <96> Field Epidemiology and Laboratory Training Program', 
   proposedactivity := 'Long term Training : Field Epidemiology and Laboratory Training Program']
dt[proposedactivity == '1. Procure treatments of sulphadoxine<ad> pyrimethamine (SP)', 
   proposedactivity := '1. Procure treatments of sulphadoxine- pyrimethamine (SP)']
dt[proposedactivity == 'SUBTOTAL IN<ad> COUNTRY STAFFING', 
   proposedactivity := 'SUBTOTAL IN- COUNTRY STAFFING']

dt[purpose == 'Case Management <96> Diagnosis', purpose := 'Case Management : Diagnosis']
dt[purpose == 'Case Management <96> Treatment', purpose := 'Case Management : Treatment']
dt[purpose == 'Entomological monitoring in Zamb<e9>zia and Nampula', purpose := 'Entomological monitoring in Zambezia and Nampula']

dt[proposedactivity == 'Zamb<e9>zia, Provide support to FBO consortium to mobilize Nampula, & Support to NGOs to conduct community communities around prevention and treatment additional mobilization activities IRCMM of malaria province  $0 MONITORING AND EVALUATION', 
   proposedactivity := 'Zambezia, Provide support to FBO consortium to mobilize Nampula, & Support to NGOs to conduct community communities around prevention and treatment additional mobilization activities IRCMM of malaria province  $0 MONITORING AND EVALUATION']
dt[proposedactivity == 'Prevention <96> insecticide-treated nets', 
   proposedactivity := 'Prevention : insecticide-treated nets']
dt[proposedactivity == 'Prevention <96> indoor residual spraying', 
   proposedactivity := 'Prevention : indoor residual spraying']
dt[proposedactivity == 'Treatment <96> malarial illnesses', 
   proposedactivity := 'Treatment : malarial illnesses']
dt[proposedactivity == 'Treatment <96> IPT for pregnant women', 
   proposedactivity := 'Treatment : IPT for pregnant women']
dt[proposedactivity == 'Support MISAU<92>s malaria BCC activities', 
   proposedactivity := "Support MISAU's malaria BCC activities"]
dt[proposedactivity == 'National <91>malaria-free<92> campaign', 
   proposedactivity := "National 'malaria-free' campaign"]
dt[proposedactivity == 'Integrated <91>malaria-free<92> campaign', 
   proposedactivity := "Integrated 'malaria-free' campaign"]
dt[proposedactivity == 'Procure artemether<ad> lumefantrine', 
   proposedactivity := 'Procure artemether- lumefantrine']
dt[proposedactivity == '4. Artemisinin<ad> based combination therapy for ADDOs', 
   proposedactivity := '4. Artemisinin- based combination therapy for ADDOs']
dt[proposedactivity == 'Procurement of malaria drugs <96> ACTs and severe malaria', 
   proposedactivity := 'Procurement of malaria drugs : ACTs and severe malaria']
dt[proposedactivity == 'Procure DHaP for pre<ad> elimination use', 
   proposedactivity := 'Procure DHaP for pre- elimination use']
dt[proposedactivity == 'Procurement of rectal artesunate for pre<ad> referral treatment', 
   proposedactivity := 'Procurement of rectal artesunate for pre- referral treatment']
dt[proposedactivity == 'An evaluation of the performance of health workers<92> use of RDTs at the CSB level.', 
   proposedactivity := "An evaluation of the performance of health workers' use of RDTs at the CSB level."]
dt[proposedactivity == '1. IPTp <96> Mainland', 
   proposedactivity := '1. IPTp : Mainland']
dt[proposedactivity == '1. PMI Country Staff <96> Zanzibar', 
   proposedactivity := '1. PMI Country Staff : Zanzibar']
dt[proposedactivity == '6. M&E Advisor <96> Mainland', 
   proposedactivity := '6. M&E Advisor : Mainland']
dt[proposedactivity == '7. Demand creation and BCC <96> Mainland', 
   proposedactivity := '7. Demand creation and BCC : Mainland']
dt[proposedactivity == '7. M&E Advisor <96> Zanzibar', 
   proposedactivity := '7. M&E Advisor : Zanzibar']
dt[proposedactivity == '8. Demand creation and BCC <96> Zanzibar', 
   proposedactivity := '8. Demand creation and BCC : Zanzibar']
dt[proposedactivity == '9. Larviciding <ad> Urban malaria control in Dar es Salaam', 
   proposedactivity := '9. Larviciding - Urban malaria control in Dar es Salaam']
dt[proposedactivity == 'A&P survey in Zamb<e9>zia in areas that are receiving different types of vector control to guide malaria control', 
   proposedactivity := 'A&P survey in Zambezia in areas that are receiving different types of vector control to guide malaria control']
dt[proposedactivity == 'ANC training and supervision in Zamb<e9>zia and Nampula', 
   proposedactivity := 'ANC training and supervision in Zambezia and Nampula']
dt[proposedactivity == 'Contribute to 2016<ad> 2017 Malaria Indicator Survey', 
   proposedactivity := 'Contribute to 2016- 2017 Malaria Indicator Survey']
dt[proposedactivity == 'Entomological monitoring in Zamb<e9>zia', 
   proposedactivity := 'Entomological monitoring in Zambezia']
dt[proposedactivity == 'Evaluation of ongoing IRS activities in Zamb<e9>zia Province', 
   proposedactivity := 'Evaluation of ongoing IRS activities in Zambezia Province']
dt[proposedactivity == 'H.5 Urban Malaria Control <96> Larviciding <96> Mainland', 
   proposedactivity := 'H.5 Urban Malaria Control : Larviciding : Mainland']
dt[proposedactivity == 'In-country technical assistance for pre<ad> elimination activities', 
   proposedactivity := 'In-country technical assistance for pre- elimination activities']
dt[proposedactivity == 'Integrated Supportive Supervision and Coordination including bi<ad> annual meeting with CHMT/DHMT/RHMT', 
   proposedactivity := 'Integrated Supportive Supervision and Coordination including bi- annual meeting with CHMT/DHMT/RHMT']
dt[proposedactivity == 'J. Epidemic Surveillance & Response <96> Mainland', 
   proposedactivity := 'J. Epidemic Surveillance & Response : Mainland']
dt[proposedactivity == 'J. Epidemic Surveillance & Response <96> Zanzibar', 
   proposedactivity := 'J. Epidemic Surveillance & Response : Zanzibar']
dt[proposedactivity == 'O.7. M&E Advisor <96> Mainland/Zanzibar', 
   proposedactivity := 'O.7. M&E Advisor : Mainland/Zanzibar']
dt[proposedactivity == 'Provide support to strengthen NAFDAC<92>s capacity', 
   proposedactivity := "Provide support to strengthen NAFDAC's capacity"]
dt[proposedactivity == 'Provide support to strengthen the national drug regulatory agency<92>s (NAFDAC) capacity', 
   proposedactivity := "Provide support to strengthen the national drug regulatory agency's (NAFDAC) capacity"]
dt[proposedactivity == 'Provide targeted support to the DOMC<92>s acquisition of routine data through the MIAS and data consolidation and analysis', 
   proposedactivity := "Provide targeted support to the DOMC's acquisition of routine data through the MIAS and data consolidation and analysis"]
dt[proposedactivity == 'Provide technical assistance to develop innovative, effective <93>keep- up<94> strategies for deliver of LLINs to vulnerable populations.', 
   proposedactivity := "Provide technical assistance to develop innovative, effective 'keep- up' strategies for deliver of LLINs to vulnerable populations."]
dt[proposedactivity == 'Provide technical assistance to develop innovative, effective <93>keep-up<94> strategies for deliver of LLINs to vulnerable populations.', 
   proposedactivity := "Provide technical assistance to develop innovative, effective 'keep-up' strategies for deliver of LLINs to vulnerable populations."]
dt[proposedactivity == 'Sensitize and train healthcare workers on MIP simplified guidelines and IPTp <93>memo<94>', 
   proposedactivity := "Sensitize and train healthcare workers on MIP simplified guidelines and IPTp 'memo'"]
dt[proposedactivity == 'SUBTOTAL <ad> Pharmaceutical Management', 
   proposedactivity := 'SUBTOTAL - Pharmaceutical Management']
dt[proposedactivity == 'SUBTOTAL <ad> Treatment', 
   proposedactivity := 'SUBTOTAL - Treatment']
dt[proposedactivity == 'Support for Thailand<92>s TES activities', 
   proposedactivity := "Support for Thailand's TES activities"]
dt[proposedactivity == 'Support IRS in additional districts of Zamb<e9>zia Province', 
   proposedactivity := 'Support IRS in additional districts of Zambezia Province']
dt[proposedactivity == 'Support IRS in eight districts of Zamb<e9>zia province', 
   proposedactivity := 'Support IRS in eight districts of Zambezia province']
dt[proposedactivity == 'Support IRS in five districts of Zamb<e9>zia province', 
   proposedactivity := 'Support IRS in five districts of Zambezia province']
dt[proposedactivity == 'Support IRS in four districts of Zamb<e9>zia province', 
   proposedactivity := 'Support IRS in four districts of Zambezia province']
dt[proposedactivity == 'Support IRS in six districts of Zamb<e9>zia province', 
   proposedactivity := 'Support IRS in six districts of Zambezia province']
dt[proposedactivity == 'Technical assistance for DHIS<ad> 2', 
   proposedactivity := 'Technical assistance for DHIS- 2']
dt[proposedactivity == 'Support IRS in six districts of Zamb<e9>zia Province', 
   proposedactivity := 'Support IRS in six districts of Zambezia Province']

dt <- dt[country != 'Greater Mekong Subregion', ]
dt[budget == "" | budget == "-", budget := NA]
dt <- dt[budget != '' & !is.na(budget) & !(budget %like% 'Costs under'), ]
dt <- dt[!tolower(proposedactivity) %like% 'total', ]
dt <- dt[budget != "0" & budget != '$0' & budget != '' & !is.na(budget),]
dt[, budget := as.numeric(budget)]
#----------------------------------------------# ####

cat('  create_upper_vars\n')
#----# create_upper_vars in R #----# ####
str_cols <- c('purpose', 'proposedactivity')
dt[, paste0("upper_", str_cols) := lapply(.SD, string_to_std_ascii),
   .SDcols = str_cols]

# Save pre_kws dataset
save_dataset(dt, 'pmi_prepped',
             'CRS', 'int')

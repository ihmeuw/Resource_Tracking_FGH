#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Save out CRS ADB_PDB subsets for UN channel and NGOs
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
pacman::p_load(crayon)

#----------------------------# ####


cat('\n\n')
cat(green(' ##############################\n'))
cat(green(' #### CRS FINALIZE ADB_PDB ####\n'))
cat(green(' ##############################\n\n'))


cat('  READ IN ADB_PDB data\n')
#----# Pull in CRS ADB PDB data #----# ####
dt <- fread(get_path('CRS', 'fin', 'B_CRS_[crs.update_mmyy]_ADB_PDB.csv'))
#---------------------------------------####

## UN CHANNELS

cat('  UNAIDS data\n')
#----# Pull in UNFPA data #----# ####
unaids <- copy(dt)
unaids <- unaids[CHANNEL == 'UNAIDS',]
save_dataset(unaids, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNAIDS'), 'CRS', 'fin')


cat('  UNFPA data\n')
#----# Pull in UNFPA data #----# ####
unfpa <- copy(dt)
unfpa <- unfpa[CHANNEL == 'UNFPA',]
save_dataset(unfpa, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNFPA'), 'CRS', 'fin')

cat('  UNICEF data\n')
#----# Pull in UNICEF data #----# ####
unicef <- copy(dt)
unicef <- unicef[CHANNEL == 'UNICEF',]
save_dataset(unicef, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNICEF'), 'CRS', 'fin')

cat('  UNITAID data\n')
#----# Pull in UNITAID data #----# ####
unitaid <- copy(dt)
unitaid <- unitaid[CHANNEL == 'UNITAID',]
save_dataset(unitaid, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_UNITAID'), 'CRS', 'fin')

cat('  PAHO data\n')
#----# Pull in PAHO data #----# ####
paho <- copy(dt)
paho <- paho[CHANNEL == 'PAHO',]
save_dataset(paho, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_PAHO'), 'CRS', 'fin')

cat('  WHO data\n')
#----# Pull in WHO data #----# ####
who <- copy(dt)
who <- who[CHANNEL == 'WHO',]
save_dataset(who, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_WHO'), 'CRS', 'fin')

cat('  NGO data\n')
#----# Pull in NGO data #----# ####
ngo <- copy(dt)
ngo <- ngo[CHANNEL %in% c('DONORCOUNTRYNGOS', 'INTERNATIONALNGOS', 'INTL_NGO'),]
save_dataset(ngo, paste0('B_CRS_', get_dah_param('CRS', 'update_MMYY'), '_ADB_PDB_NGO'), 'CRS', 'fin')

#-------------------------------------------------#
#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize COVID data for combining with main channel data
#---------------------# ####
rm(list=ls(all.names = TRUE))

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


cat('\n\n')
cat(crayon::green(' #############################\n'))
cat(crayon::green(' #### Finalize COVID Data ####\n'))
cat(crayon::green(' #############################\n\n'))



cat('  Read in COVID WHO data\n')
dt <- setDT(fread(paste0(get_path('WHO', 'int'), 'WHO_COVID_CLEAN_oct.csv')))


dt <- dt[, .(disbursement = sum(disbursement, na.rm = TRUE)),
         by = .(
            year,
            iso_code,
            donor_name = donor,
            INCOME_SECTOR,
            INCOME_TYPE,
            iso3_rc,
            recipient_country
         )]

save_dataset(dt, "COVID_prepped",
             channel = "who",
             stage = "fin")


cat('  Save aggregate data for 2_WHO_INC_EXP.do\n')
agg <- dt[, .(oid_covid_DAH = sum(disbursement, na.rm = TRUE)),
          by = .(
            iso_code,
            year,
            donor_name,
            INCOME_SECTOR,
            INCOME_TYPE
          )]
agg[, channel := "WHO"]
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
agg <- merge(agg,
             locs[, .(iso_code = ihme_loc_id, donor_country = location_name)],
             by = "iso_code",
             all.x = TRUE)
names(agg) <- toupper(names(agg))
setnames(agg, "OID_COVID_DAH", "oid_covid_DAH")

## needed for script 2_WHO_INC_EXP.do
save_dataset(agg,
             "COVID_DONOR_AGGREGATE_EBOLA_MERGE",
             channel = "who",
             stage = "fin")

#----# Docstring #----# ####
# Project:  FGH 
# Purpose: combine covid data from the 2 sources
#---------------------# 
# ==== Env ====
rm(list = ls())
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


# load OECD prepped
oecd <- fread(get_path("bmgf", "fin", "OECD_COVID_prepped.csv"),
              encoding = "Latin-1")
names(oecd) <- tolower(names(oecd))

# load GF grants database data
grant <- fread(get_path("bmgf", "fin", "GRANTS_COVID_prepped.csv"))
names(grant) <- tolower(names(grant))


cols_to_drop <- names(oecd)[grepl("_amt", names(oecd)) & names(oecd) != "total_amt"]
oecd <- oecd[year < report_year]
oecd[, (cols_to_drop) := NULL]
oecd[, vax_total := NULL]
oecd[, oid_covid := 1]
grant <- grant[year >= report_year]
cols_to_drop <- names(grant)[grepl("_amt", names(grant)) & names(grant) != "total_amt"]
grant[, (cols_to_drop) := NULL]
# drop:
drop_grant_recips <- grant[elim_ch == 1, unique(recipient_agency)]

save_dataset(grant, "grants_for_coding.csv", "BMGF", "int")
grant <- fread(get_path("BMGF", "raw","grants_coded.csv"))
grant <- grant[is.na(drop)]
grant <- grant[! recipient_agency %in% drop_grant_recips]
grant[, elim_ch := 0]

all <- rbind(oecd, grant, fill = TRUE)
setnames(all, old = c("year", "iso3_rc", "channel", "source", "total_amt", "donor_name", "recipient_agency", "elim_ch"),
         new = c("YEAR", "ISO3_RC", "CHANNEL", "SOURCE", "DAH", "DONOR_NAME", "RECIPIENT_AGENCY", "ELIM_CH"))

hfas <- c("oid_covid", "oid_other", "tb_other", "swap_hss_pp", "oid_hss_other", "hiv_other",
          "swap_hss_other", "nch_cnn", "mal_other", "rmh_fp")
for (col in hfas) {
  all[[paste0(col, "_DAH")]] <- all[[col]] * all[["DAH"]]
  all[, (col) := NULL]  # Remove the original columns
}

all <- copy(all)
all[, INCOME_TYPE := "FOUND"]
all[, INCOME_SECTOR := "PRIVATE"]
all[, INKIND := 0]
all[ISO3_RC == "G", gov := 0]
all[is.na(gov), gov := 2]

all[,RECIPIENT_AGENCY_UPPER := toupper(RECIPIENT_AGENCY)]
all[RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANIZATION' | RECIPIENT_AGENCY_UPPER %like% 'WORLD HEALTH ORGANISATION' | RECIPIENT_AGENCY_UPPER %like% 'WHO', 
   ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'UNICEF' | RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS CHILDREN', 
   ELIM_CH := 1]
all[RECIPIENT_AGENCY %like% 'UNITED NATIONS POPULATION FUND' | RECIPIENT_AGENCY_UPPER %like% 'UNFPA', 
   ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL FUND TO FIGHT AIDS' | RECIPIENT_AGENCY_UPPER %like% 'UNITED NATIONS PROGRAMME ON HIV' | RECIPIENT_AGENCY_UPPER %like% 'FUND FOR THE GLOBAL FUND', 
   ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'GLOBAL ALLIANCE FOR VACCINES' | RECIPIENT_AGENCY_UPPER %like% 'GAVI', 
   ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'PAN AMERICAN HEALTH', ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'INTER AMERICAN DEVELOPMENT BANK', ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'ASIAN DEVELOPMENT BANK', ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'AFRICAN DEVELOPMENT BANK', ELIM_CH := 1]
all[RECIPIENT_AGENCY_UPPER %like% 'BANK FOR RECONSTRUCTION AND DEVELOPMENT', ELIM_CH := 1]
all[is.na(ELIM_CH), ELIM_CH := 0]
all[, RECIPIENT_AGENCY_UPPER := NULL]


adbpdb <- fread(get_path("bmgf", "fin", paste0("BMGF_ADB_PDB_FGH_", report_year, ".csv")))
allnames <- c(names(adbpdb), "oid_covid_DAH")
allnames <- allnames[allnames %in% names(all)]
all <- all[, ..allnames]
all[, SOURCE := "IRS 990s"]
save_dataset(all, "COVID_prepped",
             channel = "bmgf", stage = "fin")

adbpdb_cov <- rbind(all, adbpdb, fill = TRUE)
adbpdb_cov <- adbpdb_cov[YEAR < dah_cfg$report_year]


#

#
save_dataset(adbpdb_cov, paste0("GATES_ADB_PDB_FGH", report_year, ".csv"),
             channel = "bmgf", stage = "fin")

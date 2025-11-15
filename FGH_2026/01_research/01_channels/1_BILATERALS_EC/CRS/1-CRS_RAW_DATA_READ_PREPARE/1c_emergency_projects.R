#### #----#                    Docstring                    #----# ####
# Project:  FGH 
# Purpose:  Search emergency sectorcode 720 for ebola, zika, and covid projects
# Description: In the next script, we will filter down to the health sector_codes,
#    so before we do, we want to determine if there are any health-related projects
#    that we should be including from the emergency response sectorcode 720.
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, readstata13)

#---------------------------------------------------------------------#


cat("\n\n")
cat(green(" ##############################\n"))
cat(green(" #### CRS LAUNCH EBOLA KWS ####\n"))
cat(green(" ##############################\n\n"))


cat("  Read input data\n")
#----# Read input data #----# ####
crs_full <- fread(get_path("CRS", "int", "B_CRS_ALL_SECTORS_[crs.update_mmyy].csv"))
#---------------------------------------------------------------------#

# In FGH 2018, we added ebola funding classified under emergency aid sectorcode 720
# We search these projects and keep only projects containg the words "EBOLA" or "ZIKA"
# Note there are 3 purpose names.
# "emergency food aid" purposename (~50 projects, 78 for FGH2019)
#  - seems to be mostly food aid and non-medical aid, so we will exclude these
# "relief coordination" purposename (~150 projects, 137 for FGH2019)
#  - seems to be mostly medical aid, so we will include
# "material relief assistance and services" purposename (~520 projects, 572 for FGH2019)
# - is a little harder as many seemed to not have enough detail to tell if it was medical aid.
# to note, most projects seemed to be through UN agencies/WHO or NGOs, rather than bilaterals,
# so these projects will be removed in double counting elimination.

# filter to emergency response, but not "emergency food assistance"
emergency <- crs_full[sector_code == 720 & purpose_code != 72040]
rm(crs_full)

# create cleaned string columns
## converts to ascii, removes punctuation, converts to upper-case, and pads with
## spaces
search_cols <- c("short_description", "long_description", "project_title",
                 "channel_name", "channel_reported_name")
clean_scols <- paste0("upper_", search_cols)
emergency[, (clean_scols) := lapply(.SD, string_to_std_ascii),
          .SDcols = search_cols]

cat("  Isolate Ebola, Zika, Cholera, and COVID emergency projects\n")
#----# Tag zika/ebola projects #----# ####

## initializes an empty character vector to append to, for each row
emergency[, emergency_response := lapply(.N, \(x) list(character(0)))]

# helper function to tag projects
set_tag_projects <- function(dt, keywords, val) {
    for (kw in keywords) {
        dt[
            upper_short_description %flike% kw |
            upper_long_description %flike% kw |
            upper_project_title %flike% kw |
            upper_channel_name %flike% kw |
            upper_channel_reported_name %flike% kw,
            emergency_response := lapply(emergency_response, append, val)
        ]
    }
    return(dt)
}



# isolate ebola projects
oid_ebz_keywords <- c(" EBOLA ", " EBOV ", " EVD ") # from HEALTH_FOCUS_AREAS_all.do
set_tag_projects(emergency, oid_ebz_keywords, "ebola")

# isolate zika projects
oid_zika_keywords <- c(" ZIKA ", " ZIKV ")
set_tag_projects(emergency, oid_zika_keywords, "zika")


# isolate cholera projects
oid_cholera_keywords <- c(" CHOLERA ", " CHOLERAE ", " COLERA ")
set_tag_projects(emergency, oid_cholera_keywords, "cholera")

# isolate covid projects
oid_covid_keywords <- c(
    " CORONAVIRUS", " COVID ", " SARS ", " SARS COV 2 ",
    " SEVERE ACUTE RESPIRATORY SYNDROME ", " NCOV ", " COVID19 "
)
set_tag_projects(emergency, oid_covid_keywords, "covid")

emergency[year < 2020,
          # drop covid if tagged before 2020
          emergency_response := lapply(emergency_response,
                                       \(x) x[x != "covid"])]




cat ("  Finalize\n")
#----# Finalize #----# ####
# collapse emergency_response vectors into a single string for each row
emergency[, emergency_response := vapply(
    emergency_response, \(x) paste(unique(x), collapse = ";"), character(1)
)]

# drop untagged projects
emergency <- emergency[emergency_response != ""]

# remove non-dah projects
#
# searching for keywords to tag projects that are non-health related
wordlist <- c("WATER", "SANITATION", "FOOD", "HUNGER", "EARTHQUAKE", "HURRICANE",
              "TERREMOTO", "HURACAN", "FLOOD", "FAMILY CARE STRUCTURES",
              "VIOLENCE", "SOCIO ECONOMIC")

emergency[, drop_proj := FALSE]
for (word in wordlist) {
    for (col in clean_scols) {
        emergency[get(col) %flike% word, drop_proj := TRUE]
    }
}

emergency <- emergency[drop_proj == FALSE, -"drop_proj"]


# drop cleaned string columns and flags
emergency[, (clean_scols) := NULL]

# save
save_dataset(emergency,
             filename = "crs_emergency",
             channel = "crs", stage = "int")

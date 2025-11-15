#
# Project: FGH
# Channel: AsDB
#
# prep data for project-level database and run keyword search
#


code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


# load data
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))

disb <- fread(get_path(
    "asdb", "fin", "AsDB_Disbursements_from_correspondence_FGH[report_year].csv"
))
disb <- disb[year <= report_year]



#
# step 1: standardize country codes
#

# NOTE:
#     The dataset from correspondence does not use iso3 codes throughout.
#     Manually generate the countries based on AsDB's countries from
#     https://www.adb.org/countries/main

disb[, ihme_loc_id := fcase(
    DMCs == "BAN", "BGD",
    DMCs == "BHU", "BTN",
    DMCs == "PRC", "CHN",
    DMCs == "INO", "IDN",
    DMCs == "CAM", "KHM",
    DMCs == "SRI", "LKA",
    DMCs == "MAR" | DMCs == "RMI", "MHL",
    DMCs == "MYA", "MMR",
    DMCs == "MON", "MNG",
    DMCs == "MAL", "MYS",
    DMCs == "NEP", "NPL",
    DMCs == "PHI", "PHL",
    DMCs == "REG", "S6",
    DMCs == "SOLOMON ISLANDS", "SLB",
    DMCs == "TAJ", "TJK",
    DMCs == "VIE", "VNM",
    DMCs == "VAN", "VUT",
    DMCs == "SAM", "WSM",
    rep_len(TRUE, .N), DMCs
)]


disb <- merge(disb, locs[, .(ihme_loc_id, location_name)],
              by = "ihme_loc_id",
              all.x = TRUE)
stopifnot( disb[is.na(location_name), .N] == 0 )

setnames(disb,
         c("year", "ihme_loc_id", "location_name",
           "projid", "ProjectName", "ProjectDescription"),
         c("YEAR", "ISO3_RC", "RECIPIENT_COUNTRY",
           "PROJECT_ID", "PROJECT_NAME", "PROJECT_DESCRIPTION"))
disb[, c("DMCs", "AmountforHealth", "DateofApproval", "CurrentClosingDate",
         "ActualClosingDate") := NULL]

disb[, `:=`(
    FUNDING_COUNTRY = "",
    ISO3_FC = "",
    FUNDING_AGENCY = "ADB",
    FUNDING_AGENCY_SECTOR = "IGO",
    FUNDING_AGENCY_TYPE = "IFI",
    DATA_LEVEL = "Project",
    DATA_SOURCE = "ADB Online Projects Database",
    RECIPIENT_AGENCY_SECTOR = "GOV",
    RECIPIENT_AGENCY_TYPE = "UNSP",
    SECTOR = "HEALTH",
    PURPOSE = "UNSP"
)]

disb[, FUNDING_TYPE := fcase(
    Modality %in% c("Grant", "Policy Based-Grant", "Program Grant",
                    "Project Grant", "TF Grant", "TA"),
    "GRANT",
    Modality %in% c("Loan", "Policy Based-Loan", "Policy-Based Loan",
                    "Program Loan", "Project Loan", "TF Loan",
                    "Multi trache" # not true, some grants
                    ),
    "LOAN",
    default = NA
)]


#
# Launch keyword search
#
save_dataset(disb, "asdb_pre_kws",
             channel = "asdb",
             stage = "int",
             format = "dta")


create_Health_config(
    data_path = get_path("asdb", "int", "asdb_"),
    channel_name = "asdb",
    varlist = c("PROJECT_NAME", "PROJECT_DESCRIPTION"),
    language = "english",
    function_to_run = 1
)

launch_Health_ADO(channel_name = "asdb", queue = "all.q",
                  job_mem = 5, job_threads = 1, runtime = "00:10:00",
                  output_path = "./asdb_kws.out",
                  errors_path = "./asdb_kws.err")





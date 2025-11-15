#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Clean Unitaid correspondence expenditure data, generate the
#'                  intpdb data frame.
#------------------------------------------------------------------------------#
####-------------------------# environment #------------------------------####
code_repo <- 'FILEPATH'


REPORT_YEAR <- 2024

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))

RUN_TESTS <- TRUE


# CONSTS
REPORT_YEAR <- get_dah_param("report_year")
PREV_REPORT_YEAR <- get_dah_param("prev_report_year")
UPDATE_DATE <- get_dah_param("UNITAID", "update_MMYY")

# FILE PATHS
UNITAID_EXP_FP <- get_path(
    "UNITAID", "raw",
    paste0("project_expenditure_", UPDATE_DATE, "_hfa_update.csv")
)

UNITAID_INKIND_FP <- get_path("UNITAID", "raw", "P_UNITAID_INKIND.csv")


####-------------------------# helpers #------------------------------####
.g <- \(...) cat(crayon::green(...), "\n")


####-------------------------# data loaders #------------------------------####
get_unitaid_expenditure <- function() {
    # load expenditure data from correspondence w unitaid
    u <- data.table::fread(UNITAID_EXP_FP)
    # rename vars
    setnames(u,
             c("disease_area", "program_area", "receiver_orgs",
               "transaction_amount"),
             c("healthfocusarea", "programarea", "grantee",
               "disbursement"))
    return(u)    
}

get_unitaid_inkind <- function() {
    i <- data.table::fread(
            UNITAID_INKIND_FP,
            select = list(character = c("CHANNEL"),
                          numeric = c("YEAR", "INKIND_RATIO"))
        )
    names(i) <- tolower(names(i))
    return(i)
}


####-------------------------# mutate #------------------------------####

# Tag HFAs according to FGH framework in UNITAID expenditure
tag_hfas <- function(unitaid) {

    # remove some whitepace from string col
    unitaid[, healthfocusarea := gsub("\r", "",
                                      gsub("\n", " ", healthfocusarea, fixed = TRUE),
                                fixed = TRUE)]
    unitaid[,
        healthfocusarea :=
            data.table::fcase(
                healthfocusarea %in% c(
                    "Cross-cutting",
                    "Cross-cutting/Health system strengthening"), "SWAP",
                healthfocusarea %in% c(
                    "HIV and co-infections",
                    "HIV and co-infections/Health system strengthening",
                    "HIV/Health system strengthening"), "HIV",
                healthfocusarea == "TB", "Tuberculosis",
                healthfocusarea == "Other infectious diseases", "OID",
                healthfocusarea == "Noncommunicable diseases", "NCD",
                healthfocusarea %in% c(
                    "Newborn and \nchild health",
                    "Newborn and child health"), "NCH",
                # default - keep current hfa
                rep(TRUE, .N), healthfocusarea
    )]

    # deal with projects tagged as rmnch
    ## they need to be split into RMH and NCH, so disbursement is split between.
    ## then add back onto unitaid
    rmnch <- unitaid[healthfocusarea %in% c("RMNCH", "MNCH"), ]
    rmnch[, `:=` (disbursement = disbursement / 2, healthfocusarea = "RMH")]
    nch <- copy(rmnch)
    nch[, healthfocusarea := "NCH"]
    rmnch <- rbind(rmnch, nch)

    unitaid <- rbind(
        unitaid[! healthfocusarea %in% c("RMNCH", "MNCH"), ],
        rmnch
    )
    return(unitaid)
}


# Manually Tag PAs according to FGH framework
tag_pas <- function(unitaid) {
    unitaid[,
        programarea :=
            data.table::fcase(
                # swap
                healthfocusarea == "SWAP" & programarea %in% 
                  c("Sector Wide Approach/ Health system strengthening",
                    "Sector Wide Approach/ Health system strengthening/AMR",
                    "Sector Wide Approach",
                    "Other control", "Treatment/AMR", 
                    "Cross-cutting/Health system strengthening",
                    "Treatment",
                    "Treatment/AMR"), "swap_hss_other",

                # HIV
                healthfocusarea == "HIV" & programarea %in% c(
                    "Counseling and testing",
                    "Counseling and testing/AMR",
                    "Diagnosis",
                    "Diagnosis/AMR",
                    "Diagnostics/AMR",
                    "Diagnostics"), "hiv_ct",
                healthfocusarea == "HIV" & programarea %in% c(
                    "Prevention/AMR",
                    "Prevention"), "hiv_prev",
                healthfocusarea == "HIV" & programarea %in% c(
                    "Sector Wide Approach/ Health system strengthening",
                    "Sector Wide Approach/ Health system strengthening/AMR",
                    "HIV/Health system strengthening"), "hiv_hss_other",
                healthfocusarea == "HIV" & programarea %in% c(
                    "Treatment",
                     "Treat",
                     "Treatment/AMR"), "hiv_treat",
                healthfocusarea == "HIV" & programarea == "PMTCT", "hiv_pmtct",
                healthfocusarea == "HIV" & programarea == "Other", "hiv_other",

                # malaria
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Bednets",
                    "Bednets/AMR"), "mal_con_nets",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Other control",
                    "Other control/AMR"), "mal_con_oth",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Other control/Community outreach"), "mal_comm_con",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Sector Wide Approach/ Health system strengthening/AMR",
                    "Sector Wide Approach/ Health system strengthening"
                    ), "mal_hss_other",
                healthfocusarea == "Malaria" &
                    programarea == "Indoor Residual Spraying/AMR", "mal_con_irs",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Treatment",
                    "Treatment/AMR"), "mal_treat",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Diagnostics",
                    "Diagnosis/AMR",
                    "Diagnostics/AMR"), "mal_diag",
                healthfocusarea == "Malaria" & programarea %in% c(
                    "Other",
                    "Prevention"), "mal_other",

                # tuberculosis
                healthfocusarea == "Tuberculosis" & programarea %in% c(
                    "Diagnosis",
                    "Diagnosis/AMR",
                    "Diagnostics/AMR",
                    "Diagnostics"), "tb_diag",
                healthfocusarea == "Tuberculosis" & programarea %in% c(
                    "Treatment",
                    "Treatment/AMR"), "tb_treat",
                healthfocusarea == "Tuberculosis" & programarea %in% c(
                    "Diagnosis/Health system strengthening",
                    "Sector Wide Approach/ Health system strengthening/AMR",
                    "Sector Wide Approach/ Health system strengthening"),
                        "tb_hss_other",
                healthfocusarea == "Tuberculosis" & programarea %in% c(
                    "Prevention/AMR",
                    "Counseling and testing",
                    "Prevention"), "tb_other",
                
                # other infectious diseases
                healthfocusarea == "OID" & programarea %in% c(
                    "envelope/other"
                ), "oid_hss_other",

                # neonatal and child health
                healthfocusarea == "NCH" & programarea %in% c(
                    "envelope/other",
                    "Treatment"), "nch_other",
                healthfocusarea == "NCH"
                 & programarea == "Sector Wide Approach/ Health system strengthening",
                    "nch_other",
                # reproductive and maternal health
                healthfocusarea == "RMH" & programarea == "Treatment",
                    "rmh_other",
                healthfocusarea == "RMH"
                 & programarea == "Sector Wide Approach/ Health system strengthening",
                    "rmh_other",
                # non-communicable diseases
                healthfocusarea == "NCD", "ncd_hss_other",

                # default - keep current programarea
                rep(TRUE, .N), programarea
            ) # end fcase
    ]

    return(unitaid)
}

# Separate process to manually tag OID HFAs and PAs, since we want to 
# single out covid
tag_oid_covid <- function(unitaid) {
    # COVID
    unitaid[transaction_internal_reference == "COVID-19"
            | healthfocusarea %like% "COVID",
            `:=`(
                programarea = "oid_covid",
                healthfocusarea = "OID"
            )]

    return(unitaid)
}

# Deal with a few special cases
tag_special <- function(unitaid) {
    # a few cases where specific activity IDs are always associated with the
    # same HFA and PA, so we can impute any missing HFA/PA
    unitaid[activity_identifier == "XM-DAC-30010-D2EFT",
            `:=`(healthfocusarea = "HIV", programarea = "hiv_treat")]
    unitaid[activity_identifier == "XM-DAC-30010-VayuExplore",
            `:=`(healthfocusarea = "NCH", programarea = "nch_other")]
    # a search for this activity id revealed this doc:
    # https://terrance.who.int/mediacentre/unitaid/cepheid/cepheid.xml
    # title: "Scaling up access to contemporary TB diagnostics"
    # conlude that PA is tb_diag (HFA already Tuberculosis)
    unitaid[activity_identifier == "XM-DAC-30010-utd-cepheid",
            programarea := "tb_diag"]
    
    # https://unitaid.org/uploads/R21_2024-e_PAHO-AHD-enabler-grant.pdf
    unitaid[activity_identifier == "AHD PAHO",
            programarea := "hiv_treat"]
    
    # unitaid tags this as being COVID, but we believe it should be reallocated
    # to oid_other based on further investigation. See more details here:
    # https://unitaid.org/news-blog/unitaid-and-partners-launch-first-of-its-kind-regional-manufacturing-initiative-to-improve-access-to-medical-oxygen-in-sub-saharan-africa/
    unitaid[activity_identifier == "EAPOA",
            programarea := "oid_hss_other"]
    
    return(unitaid)
}

# clean names of implementing partners and add variable for channel
clean_vars <- function(unitaid) {
    # apply string clean (utils.R) to the grantee var
    unitaid[, upper_grantee := string_to_std_ascii(grantee)]

    # manual fixes
    unitaid[,
            upper_grantee := data.table::fcase(
                upper_grantee %like% "CHAI",
                    "CLINTON HEALTH ACCESS INITIATIVE",
                upper_grantee %like% "ELIZABETH",
                    "ELIZABETH GLASER PEDIATRIC AIDS FOUNDATION",
                upper_grantee %like% "EGPAF",
                    "ELIZABETH GLASER PEDIATRIC AIDS FOUNDATION",
                upper_grantee %like% "GLOBAL FUND", "GFATM",
                upper_grantee %like% "MMV", "MEDICINES FOR MALARIA VENTURE",
                upper_grantee %like% "PSI", "POPULATION SERVICES INTERNATIONAL",
                upper_grantee %like% "PIH", "PARTNERS IN HEALTH",
                upper_grantee %like% "FOUNDATION FOR INNOVATIVE", "FIND",
                upper_grantee %like% "FIND", "FIND",
                upper_grantee %like% "WORLD HEALTH", "WHO",
                upper_grantee %like% "WHO", "WHO",
                upper_grantee %like% "CLINTON HEALTH INITIATIVE FOUNDATION/",
                    "CLINTON HEALTH INITIATIVE FOUNDATION",
                upper_grantee %like% "IBB", "INSTITUT BOUISSON BERTRAND",
                # default - keep current string
                rep(TRUE, .N), upper_grantee
            )
    ]
    # trim white space and use as grantee
    unitaid[, upper_grantee := trimws(upper_grantee, which = "both")]
    unitaid[, `:=`(
        grantee = upper_grantee,
        upper_grantee = NULL
    )]
    # add channel
    unitaid[, channel := "UNITAID"]
    # add year var
    unitaid[, year := data.table::year(transaction_date)]
    return(unitaid)
}

# flag specific grantee's for double counting
# https://hub.ihme.washington.edu/display/RT/DAH+by+Channels
# gist - we flag a grantee if they are also considered to be a channel within
# the FGH project.
# To visually inspect grantees and determine if any are FGH channels, try:
#    data.frame(x = unique(unitaid$grantee)) |> View()
flag_double_counting <- function(unitaid) {
    channel_list <- c('CLINTON HEALTH ACCESS INITIATIVE',
                      'GFATM', 'PARTNERS IN HEALTH', 
                      'POPULATION SERVICES INTERNATIONAL',
                      'WHO', 'UNICEF', 'PAHO',
                      'ELIZABETH GLASER PEDIATRIC AIDS FOUNDATION',
                      'MEDICINES SANS FRONTIERES', 'GLI',
                      'UNITED NATIONS POPULATION FUND')
    # flag specific channels as being double counted
    unitaid[, elim_ch := 0]
    for (ch in channel_list) {
        unitaid[grantee %like% ch, elim_ch := 1]
    }
    
    return(unitaid)
}


####----------------------# agg / transform #---------------------------####
apply_inkind_ratio <- function(unitaid, inkind) {
    unitaid <- merge(
        unitaid, inkind[, -"channel"],
        by = c("year"), all.x = TRUE
    )
    ink <- copy(unitaid)
    ink[, dah := dah * inkind_ratio]
    ink[, inkind := 1]
    unitaid[, inkind := 0]
    unitaid <- rbind(unitaid, ink)
    return(unitaid)
}


####
####-------------------------# main #------------------------------####
####
main <- function() {
    # load inputs
    .g("Loading UNITAID expenditures and inkind ratios")
    unitaid <- get_unitaid_expenditure()
    inkind <- get_unitaid_inkind()

    # tag HFAs, then PAs
    .g("Categorizing Health Focus Areas and Program Areas")
    unitaid <- tag_hfas(unitaid)
    unitaid <- tag_pas(unitaid)
    unitaid <- tag_oid_covid(unitaid)
    unitaid <- tag_special(unitaid)
    unitaid[programarea == "mal_con_other",
            programarea := "mal_con_oth"]
    
    # test to make sure we have tagged all the PAs
    test_program_areas(unitaid)
    
    # clean remaining vars
    .g("Cleaning UNITAID variables")
    unitaid <- clean_vars(unitaid)
    # flag double counting
    unitaid <- flag_double_counting(unitaid)
    # aggregate to donor-program_area-year
    .g("Aggregating to donor-program_area-year and applying inkind ratio.")
    unitaid <- unitaid[, .(dah = sum(disbursement, na.rm = TRUE)),
                       by = .(year, grantee, programarea, elim_ch, channel)]
    # apply inkind ratio
    int_pdb <- apply_inkind_ratio(unitaid, inkind)
    names(int_pdb) <- toupper(names(int_pdb))
    
    # save data
    if (RUN_TESTS) {
        .g("Running tests...")
        test_output(int_pdb)
    }
    ## all years
    save_dataset(int_pdb, paste0("UNITAID_INTPDB_FGH", REPORT_YEAR),
                 channel = "UNITAID",
                 stage = "int")
    ## for report year only
    save_dataset(int_pdb[YEAR == REPORT_YEAR, ],
                 paste0("UNITAID_INTPDB", REPORT_YEAR),
                 channel = "UNITAID",
                 stage = "int")
}

main()

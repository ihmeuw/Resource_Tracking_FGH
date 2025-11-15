#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Break expenditure data down by income shares for HFA and PA;
#'                  creates ADB_PDB
#------------------------------------------------------------------------------#
####-------------------------# environment #-----------------------------####
code_repo <- 'FILEPATH'


REPORT_YEAR <- 2024

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))

RUN_TESTS <- TRUE

# CONSTS
REPORT_YEAR <- get_dah_param("report_year")
PREV_REPORT_YEAR <- get_dah_param("prev_report_year")
UPDATE_DATE <- get_dah_param("UNITAID", "update_MMYY")

# FILE PATHS
## income shares updated in earlier step
INCOME_SHARES_FP <- file.path(
    get_path(channel = "UNITAID", stage = "int"),
    "UNITAID_INC_SHARES.csv"
)

## intpdb for FGH - ie all years up to report year 
INTPDB_FGH_FP <- file.path(
    get_path(channel = "UNITAID", stage = "int"),
    paste0("UNITAID_INTPDB_FGH", REPORT_YEAR, ".csv")
)

## inkind
INKIND_FP <- file.path(
    get_path(channel = "UNITAID", stage = "raw"),
    "P_UNITAID_INKIND.csv"
)

####-------------------------# helpers #------------------------------####
.g <- \(...) cat(crayon::green(...), "\n")


####-------------------------# data loaders #------------------------------####
get_intpdb_fgh <- function() {
    int <- data.table::fread(INTPDB_FGH_FP)
    names(int) <- tolower(names(int))
    return(int)
}


get_inc_shares <- function() {
    inc <- data.table::fread(INCOME_SHARES_FP)
    names(inc) <- tolower(names(inc))
    return(inc)
}

get_inkind <- function() {
    inkind <- data.table::fread(INKIND_FP,
                                select = list(numeric = c("INKIND_RATIO",
                                                          "YEAR"))
                                )
    names(inkind) <- tolower(names(inkind))
    return(inkind)
}


####-------------------------# agg / merge #------------------------------####



# merge HFA with income data
make_hfa <- function(dt, inc) { #intpdb, income_shares
    hfas <- unique(dt$healthfocusarea)

    # aggregate for merging
    dt2 <- dt[, lapply(.SD, sum),
              by = c("year", "healthfocusarea", "elim_ch", "inkind"),
               .SDcols = "dah"]
    dt2 <- dcast.data.table(dt2,
                            year + elim_ch + inkind ~ healthfocusarea,
                            value.var = "dah")
    # expands to include all donors
    dt_hfa <- merge(dt2, inc, by = "year", all = TRUE, allow.cartesian = TRUE)

    # multiply each hfa by the income share
    dt_hfa[, (hfas) := lapply(.SD, \(x) x * inc_share), 
           .SDcols = hfas]
    setnafill(dt_hfa, fill = 0, cols = hfas)

    # sum dah
    dt_hfa[, dah := rowSums(.SD), .SDcols = hfas]

    setnames(dt_hfa, hfas, paste0(hfas, "_dah"))
    return(dt_hfa)
}


# merge PA with income data
gen_pa_cols <- function(dt, inc) { #intpdb, income_shares
    pas <- unique(dt$programarea)
    # aggregate for merging
    dt2 <- copy(dt[, lapply(.SD, sum),
                   by = c("year", "programarea", "elim_ch", "inkind"), 
                   .SDcols = "dah"])
    dt2 <- dcast.data.table(dt2,
                            year + elim_ch + inkind ~ programarea,
                            value.var = "dah")
    dt_pa <- merge(dt2, inc, by = "year", all = TRUE, allow.cartesian = TRUE)

    # multiply each pa by the income share
    dt_pa[, (pas) := lapply(.SD, \(x) x * inc_share), 
          .SDcols = pas]
    
    dah_cols <- paste0(pas, "_dah")
    setnames(dt_pa, pas, dah_cols)
    
    setnafill(dt_pa, fill = 0, cols = dah_cols)
    dt_pa[, dah := rowSums(.SD), .SDcols = dah_cols]
    
    return(dt_pa)
}

# combine hfa and pa data into one
finalize_adbpdb <- function(adb) {
    dt <- copy(adb)
    # keep all inkind obs.
    dt[inkind == 1, elim_ch := 0]

    # standardize donors for dt
    .g("  Standardizing donor names")
    dt_donors <- standardize_donors(dt, "donor_name")
    # check for the donor names that don"t match and update as needed

    # proceed to use the matched dataset
    dt <- dt_donors[[1]]
    dt[, `:=`(iso_code = ISO_CODE,
              income_sector = INCOME_SECTOR,
              income_type = INCOME_TYPE,
              donor_name = IHME_NAME,
              # rm extra cols added by standardize_donors
              ISO_CODE = NULL, INCOME_SECTOR = NULL, INCOME_TYPE = NULL,
              IHME_NAME = NULL, checked_upper_donor_name = NULL)]

    # set recipient ISO_CODE to unspecified per UNITAID guidance
    dt[, iso3_rc := "UNSP"]
    setcolorder(dt,
                c("year", "elim_ch", "donor_name", "donor_country",
                  "income_sector", "income_type", "iso_code", "iso3_rc"))
    return(dt)
}

# make INC file
make_inc <- function(dt_all, inkind) {
    all_inc <- dt_all[, .(dah = sum(dah, na.rm = TRUE)),
                      by = c("year", "channel", "income_sector", "income_type",
                             "donor_name", "donor_country", "elim_ch", "inkind")
                      ]

    ## add inkind ratio per year for the merge in file 4
    all_inc <- merge(all_inc, inkind, by = "year")
    setnames(all_inc, "dah", "outflow")
    all_inc <- all_inc[!is.na(donor_name), ]
    return(all_inc)
}


####-------------------------# main #------------------------------####
main <- function() {
    .g("Loading inputs")
    intpdb <- get_intpdb_fgh()
    incshares <- get_inc_shares()
    inkind <- get_inkind()

    .g("Breaking INTPDB into PAs and adding income shares")
    adb <- gen_pa_cols(dt = intpdb, inc = incshares)

    .g("Finalizing ADBPDB")
    adbpdb <- finalize_adbpdb(adb)

    .g("Making INC dataset")
    inc_all <- make_inc(dt_all = adbpdb, inkind = inkind)

    # tests
    if (RUN_TESTS) {
        .g("Running tests...")
        tests(adbpdb = adbpdb, inc_all = inc_all)
    }
    
    names(adbpdb) <- toupper(names(adbpdb))

    # save outputs
    .g("Saving outputs...")
    save_dataset(adbpdb,
                 paste0("UNITAID_ADBPDB_FGH", REPORT_YEAR),
                 channel = "UNITAID", stage = "fin")
    save_dataset(inc_all,
                 paste0("UNITAID_INC_FGH", REPORT_YEAR),
                 channel = "UNITAID", stage = "fin")
}

main()

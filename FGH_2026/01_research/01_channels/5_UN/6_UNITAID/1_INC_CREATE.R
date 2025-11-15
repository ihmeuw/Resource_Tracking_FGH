### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Generate Global Fund proportions from raw Global Fund data
#'                  to be used in Unitaid
#------------------------------------------------------------------------------#
####-------------------------# environment #------------------------------####
code_repo <- 'FILEPATH'


REPORT_YEAR <- 2024
RUN_TESTS <- TRUE

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))


# CONSTS
REPORT_YEAR <- get_dah_param("report_year")
PREV_REPORT_YEAR <- get_dah_param("prev_report_year")
UPDATE_DATE <- get_dah_param("UNITAID", "update_MMYY")

# file paths
UNITAID_INCOME_FP <- get_path(
    "UNITAID", "raw",
    paste0("UNITAID_income_2006_", PREV_REPORT_YEAR, ".csv")
)

GFATM_INCOME_FP <- get_path("UNITAID", "int", "GFATM_INCOME_ALL.csv")

COUNTRY_CODE_FP <- get_path("meta", "locs", "countrycodes_official.csv")

DONOR_FP <- "FILEPATH/UNITAID_donors_ex.csv"

####-------------------------# helpers #------------------------------####
clean_names <- function(col_names) {
    .gsub <- \(pat, repl, x) gsub(pat, repl, x, fixed = TRUE)
    .clean_name <- function(nm) {
        nm <- tolower(nm)
        nm <- .gsub(" ", "_", .gsub("(", "", .gsub(")", "", nm)))
        return(nm)
    }
    return(unname(sapply(col_names, .clean_name)))
}

fun_check_na <- function(x, fun) {
    # Returns NA if all elements in `x` are NA.
    # Otherwise, returns the `fun` of `x`, with na.rm = TRUE
    if (all(is.na(x))) {
        return(NA_real_)
    } else {
        return(fun(x, na.rm = TRUE))
    }
}

.g <- \(...) cat(crayon::green(...), "\n")
.err <- \(...) cat(crayon::red(...), "\n")


####-------------------------# data loaders #------------------------------####
get_donor_metadata <- function() {
    donor <- data.table::fread(
        DONOR_FP,
        header = TRUE,
        select = list(character = c("DONOR_NAME", "INCOME_SECTOR",
                                    "INCOME_TYPE", "SOURCE"))
    )
    names(donor) <- clean_names(names(donor))
    donor[, donor_name := string_to_std_ascii(donor_name, pad_char = "")]
    return(donor)
}

get_country_codes <- function() {
    cc <- data.table::fread(COUNTRY_CODE_FP,
                            select = c("country_lc", "iso3", "countryname_ihme"))
    return(unique(cc))
}

get_gfatm_income <- function(minyr, maxyr) {
    # load
    gfatm <- data.table::fread(GFATM_INCOME_FP)
    names(gfatm) <- clean_names(names(gfatm))
    # transform
    gfatm <- gfatm[year %in% minyr:maxyr, ]
    gfatm[iso3 == "", iso3 := NA_character_]
    
    # fix global fund donor info (should really be corrected by GFATM channel)
    warning("Manual fixes to Global Fund donor data. Double-check each year.")
    
    d2h <- unique(gfatm[donor_country %ilike% "DEBT2HEALTH", .(donor_country)])
    d2h[, new := gsub("DEBT2HEALTH - ", "", toupper(donor_country))]
    d2h[, c("source", "dest") := tstrsplit(new, "-", fixed = TRUE)]
    gfatm <- merge(
        gfatm,
        unique(d2h[, .(donor_country, d2h = source)]),
        by = "donor_country", all.x = TRUE
    )
    gfatm[!is.na(d2h), donor_country := d2h]
    gfatm[, d2h := NULL]
    rm(d2h)
    cc <- get_country_codes()
    gfatm <- merge(gfatm, cc[, .(country_lc, iso3_new = iso3)],
                 by.x = c("donor_country"),
                 by.y = c("country_lc"),
                 all.x = TRUE)
    gfatm[is.na(iso3), iso3 := iso3_new]
    gfatm[, iso3_new := NULL]
    gfatm[is.na(iso3), `:=`(
        donor_country = NA_character_
    )]
    
    
    # add suffix to names to prep for merge
    cols <-  c("iso3", "donor_name", "donor_country")
    new_cols <- vapply(cols, \(x) paste0(x, "_new"), character(1))
    setnames(gfatm, old = cols, new = new_cols)
    # re-specify donor name
    gfatm[, donor_name := "The Global Fund"]
    # aggregate income to donor year
    gfatm_yr <- gfatm[,
                      .(income_all_share = fun_check_na(income_all_share,
                                                        fun = sum)
                        ),
                      by = .(year, donor_name, donor_name_new, iso3_new,
                             donor_country_new, income_sector, income_type)
          ]
    return(gfatm_yr)
}

get_unitaid_income <- function() {
    unitaid <- data.table::fread(
        UNITAID_INCOME_FP,
        select = list(character = c("donor"),
                      numeric = c("year", "contribution")),
    )
    setnames(unitaid, c("donor"), c("country_lc"))

    # load country codes and left-join onto unitaid
    unitaid <- merge(unitaid, get_country_codes(),
                     by = "country_lc", all.x = TRUE)

    # mutate vars
    unitaid[, `:=`(
        donor_country_name = country_lc,
        channel = "UNITAID",
        source = "UNITAID Annual Financial Statements",
        donor_name = country_lc,
        outflow = contribution
    )]
    setnames(unitaid, c("countryname_ihme"), c("donor_country"))

    return(unitaid)
}

####---------------------# mutate / aggregate #--------------------------####
.update_donation_values <- function(unitaid_yr) {
    # there are negative values of "outflow", which we need to fix.
    # group by donor and, if next year's outflow < 0, then we subtract the neg
    # outflow from the current year's outflow, and set the negative outflow to 0
    # (in practice, we are saying that if a donor's outflowing donations are
    #  negative for a year, then it hasn't actually donated anything so
    #  it's correct outflow is 0. Further, we want to adjust the previous year,
    #  since evidently some of the outflow from that year has been pulled back.)

    # sort by donor-year, ascending 
    data.table::setorder(unitaid_yr,
                         donor_name, year)
    # create lags and leads by donor
    ## fill must be 0 instead of NA so fifelse (below) does not return NA
    unitaid_yr[, `:=`(
        outflow_lag = data.table::shift(outflow, n = 1,
                                        fill = 0, type = "lag"),
        outflow_lead = data.table::shift(outflow, n = 1,
                                         fill = 0, type = "lead")
        ),
        by = donor_name]

    # group-by donor, adjust negative values
    unitaid_yr[, `:=`(
        outflow = data.table::fifelse(
            outflow_lead < 0, outflow + outflow_lead, outflow
        )
    ), by = donor_name]
    # replace negatives w 0
    unitaid_yr[, outflow := data.table::fifelse(outflow < 0, 0, outflow)]
    
    unitaid_yr[, c("outflow_lag", "outflow_lead") := NULL]
    
    return(unitaid_yr)
}

.oldfn <- function(dataset) {
  #' Function to clean - from original script
  #' @param dataset [data.frame/data.table] Dataset intended to be cleaned for negative donation values
  DT <- setDT(copy(dataset))
  DT<- DT[order(donor_name, -year)]
  DT[, outflow := ifelse(shift(outflow,1,0,"lag") < 0 & shift(donor_name,1,0,"lag") == donor_name & (year-shift(year,1,0,"lead"))==1,
                         shift(outflow,1,0,"lag")+outflow,outflow)]
  DT[, outflow := ifelse(shift(donor_name,1,0,"lead")==donor_name & outflow < 0 & (year-shift(year,1,0,"lead"))==1,
                         0,outflow)]
  
  return(DT)
}


# converts unitaid to donor-year aggregation, corrects donation values,
# calculates annual outflows
agg_unitaid <- function(unitaid) {
    # aggregate outflow by donor-year
    unitaid_yr <- unitaid[, .(outflow = sum(outflow)),
                          by = .(year, donor_name, donor_country,
                                 iso3, channel, source)]
    # adjust outflow values if they are < 0
    unitaid_yr <- .update_donation_values(unitaid_yr)    
    if (RUN_TESTS) {
        jo <- .oldfn(unitaid[, .(outflow = sum(outflow)),
                          by = .(year, donor_name, donor_country,
                                 iso3, channel, source)])
        test <- merge(unitaid_yr, jo[, .(donor_name, year, outflow_j = outflow)],
                      by = c("donor_name", "year"))
        test[, ne := outflow != outflow_j]
        if (nrow(test[ne == TRUE, ]) != 0)
            .err("Test Failed - update_donation_values doesn't match old behavior.")
    }

    # get annual outflow total per year and income share of total
    unitaid_yr[, annual_outflow := sum(outflow), by = year]
    unitaid_yr[, inc_share := outflow / annual_outflow]
    return(unitaid_yr)
}



# we only have income shares for years up to report_year - 1, so we need to
# extrapolate them somehow.
# historically, we just reused the previous year's income shares, but this is
# not always ideal.
# For example, although Gates Foundation didn't contribute in 2022 or 2023, we knew they will
# in 2024 so when working on FGH 2024 we needed to adjust for this.
extrapolate_inc_shares <- function(unitaid) {
    # for report-year, reuse the last year of data
    inc_ry <- unitaid[year == PREV_REPORT_YEAR, ]
    inc_ry[, year := REPORT_YEAR]

    # finalize
    out <- rbind(unitaid, inc_ry)
    return(out)
}



####-------------------------# merge #------------------------------####
merge_inputs <- function(unitaid, gfatm, donor) {
    # outer-join unitaid and gfatm
    ## there will be missing values for all values not originating from the
    ug <- merge(unitaid, gfatm,
                by = c("year", "donor_name"), all = TRUE)
    # filter out earlier years included by the gfatm data
    ug <- ug[!(donor_name == "The Global Fund"
               & is.na(outflow)
               & year < PREV_REPORT_YEAR)]

    # break global fund out by new source info
    ## income share from unitaid data * gfatm income_all_share
    ug[donor_name == "The Global Fund",
       `:=`(
        inc_share = inc_share * income_all_share,
        donor_country = donor_country_new,
        iso3 = iso3_new,
        donor_name = donor_name_new
       )]
    ug[iso3 == "", iso3 := NA_character_]
    
    
    # need unique donor-year rows
    cntry2iso <- unique(ug[!is.na(iso3), .(donor_country, iso3)])
    donor2cntry <- unique(ug[!is.na(donor_name), .(donor_name, donor_country)])
    donor2cntry[, donor_country := fcase(
        tolower(donor_name) %like% "gates foundation", "United States",
        tolower(donor_name) %like% "millenium foundation", "Switzerland",
        tolower(donor_name) %like% "wellcome trust", "United Kingdom",
        rep_len(TRUE, .N), donor_country
    )]
    donor2cntry[is.na(donor_country), donor_country := "unallocable"]
    donor2cntry <- unique(donor2cntry)

    if ( length(unique(donor2cntry$donor_name)) != nrow(donor2cntry) )
        stop("Merging of inputs failed - ",
             "donor-names cannot repeat, but they do. Fix needed.")

    # sum income shares
    ug <- ug[, .(inc_share = sum(inc_share, na.rm = TRUE)),
             by = .(year, donor_name, channel, source)]
    
    # merge in country and iso3
    ug <- merge(ug, donor2cntry, by = "donor_name", all.x = TRUE)
    ug <- merge(ug, cntry2iso, by = "donor_country", all.x = TRUE)
    ug[donor_country == "unallocable", iso3 := "QZA"]

    if ( nrow(unique(ug[, .(donor_name, year)])) != nrow(ug) )
        stop("Merging of inputs failed - donor-year rows are not unique.")

    # clean names so they can merge with donor data
    ug[, donor_name := string_to_std_ascii(donor_name, pad_char = "")]
    # left-join on income sector and income type vars from donor metadata
    ug <- merge(ug, donor[, .(donor_name, income_sector, income_type)],
                by = "donor_name", all.x = TRUE)
    
    # try merging with gfatm income sector too
    gfi <- unique(gfatm[, .(donor_name = donor_name_new,
                            income_sector,
                            income_type)])
    gfi[, donor_name := string_to_std_ascii(donor_name, pad_char = "")]
    ## need to remove duplicates (where donor_name is mapped to multiple different
    ## income categories) before merging
    gfi <- gfi[, .(income_sector = unique(income_sector[income_sector != ""])[1],
                   income_type   = unique(income_type[income_type != ""])[1]),
               by = .(donor_name)]
    
    ug <- merge(ug, unique(gfi),
                by = c("donor_name"),
                all.x = TRUE,
                suffixes = c("", "_new"))
    ug[is.na(income_sector), income_sector := income_sector_new]
    ug[is.na(income_type), income_type := income_type_new]
    ug[, grep("_new$", names(ug)) := NULL]
    
    # catch any missed donors
    ug[donor_name == "WELLCOME TRUST",
       `:=`(
           income_sector = "PRIVATE",
           income_type = "FOUND"
       )]
    
    ## check all income sector values are filled
    stopifnot( ug[is.na(income_sector), .N] == 0 )
    
    setnames(ug, "iso3", "iso_code")
    return(ug)
}

####-------------------------# main #------------------------------####
# function to run some comparisons between an old data output and its new vars
# summary_var - any variable that is in both versions, used to compare summary()
compare_old_new <- function(old, new_, summary_var) {
    .msg <- \(...) cat(crayon::yellow(...), "\n")
    
    new <- data.table::copy(new_)
    names(new) <- tolower(names(new))
    names(old) <- tolower(names(old))
    
    for (col in names(old)) {
        if (!(col %in% names(new))) {
            .err(col, " - variable is in old data but not in new data.")
        }
    }
    
    for (col in names(new)) {
        if (!(col %in% names(old))) {
            .err(col, " - variable is in new data but not in old data.")
        }
    }

    .msg("Old summary of ", summary_var)
    print(summary(old[[summary_var]]))
    .msg("New summary of ", summary_var)
    print(summary(new[[summary_var]]))
    cat("\n\n")
    .msg("Total NAs in old: ", sum(is.na(old)))
    .msg("Total NAs in new: ", sum(is.na(new)))
}



####-------------------------# main #------------------------------####
main <- function() {
    .g("Loading and aggregating UNITAID.")
    unitaid <- get_unitaid_income()
    unitaid <- agg_unitaid(unitaid)
    unitaid <- extrapolate_inc_shares(unitaid)

    .g("Loading and aggregating GFATM.")
    gfatm <- get_gfatm_income(minyr = min(unitaid$year),
                              maxyr = REPORT_YEAR)

    .g("Loading donor metadata.")
    donor <- get_donor_metadata()

    .g("Merging UNITAID & GFATM to calculate income shares")
    income_shares <- merge_inputs(unitaid, gfatm, donor)
    

    # save unitaid income share
    if (RUN_TESTS) {
        .g("Running tests...")
        tests(income_shares = income_shares)
    }
    .g("Saving...")
    setnames(income_shares,
             names(income_shares),
             vapply(names(income_shares), \(x) toupper(x), character(1)) 
             )

    ## overall income shares
    save_dataset(income_shares, "UNITAID_INC_SHARES",
                 channel = "UNITAID",
                 stage = "int")


    if (interactive()) {
        # we have to manually enter income shares into a spreadsheet so do a
        # visual check for anomalous values - may be misentered
        ggplot(income_shares[!is.na(INC_SHARE) & INC_SHARE != 0],
               aes(x = YEAR, y= INC_SHARE)) +
            geom_line() +
            geom_point() +
            facet_wrap(~DONOR_NAME, scales = "free_y")
    }
}

main()

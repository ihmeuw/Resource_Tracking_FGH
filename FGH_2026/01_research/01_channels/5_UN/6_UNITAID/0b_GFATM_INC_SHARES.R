#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Finalize INC shares and save out to be used in UNITAID
####-------------------------# environment #------------------------------####
code_repo <- 'FILEPATH'


REPORT_YEAR <- 2024

source(paste0(code_repo, "/FGH_", REPORT_YEAR, "/utils.R"))
pacman::p_load(stringr, openxlsx, data.table)

# CONSTS
REPORT_YEAR <- get_dah_param('report_year')
PREV_REPORT_YEAR <- get_dah_param('prev_report_year')
UPDATE_DATE <- get_dah_param('UNITAID', 'update_MMYY')

# FILE PATHS
INCOME_FP <- get_path("gfatm", "raw", "P_GFATM_INCOME_ALL.csv")

XRATE_FP <- get_path("meta", "rates",
                     paste0("OECD_XRATES_NattoUSD_1950_", REPORT_YEAR, ".csv"))

OUTPUT_FILE <- "GFATM_INCOME_ALL.csv"

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

.g <- \(...) cat(crayon::green(...), "\n")

####-------------------------# data loaders #------------------------------####
get_gfatm_income <- function() {
    income_all <- data.table::fread(
        INCOME_FP,
        select = list(
            character = c("DONOR_NAME", "ISO3", "DONOR_COUNTRY", "INCOME_SECTOR",
                          "INCOME_TYPE", "Country", "CURRENCY", "CHANNEL"),
            numeric = c("YEAR", "EXCHRATE", "INCOME_REG_PAID", "INCOME_REG_PLEDGED")
        )
    )
    names(income_all) <- clean_names(names(income_all))
    return(income_all)
}

get_exchange_rates <- function() {
    # load
    xrate <- data.table::fread(
        XRATE_FP,
        select = list(
            character = c("LOCATION"),
            numeric = c("Value", "TIME")
        )
    )
    setnames(xrate,
             c("LOCATION", "TIME", "Value"),
             c("iso3", "year", "exchrate"))
    # fix specific issues
    xrate[iso3 == "EA19", iso3 := "FRA"]
    # generate currency col
    xrate[, currency := data.table::fcase(
        iso3 == "AUS", "AUD",
        iso3 == "CAN", "CAD",
        iso3 == "CHE", "CHF",
        iso3 == 'DNK', 'DKK',
        iso3 == 'FRA', 'EUR',
        iso3 == 'GBR', 'GBP',
        iso3 == 'JPN', 'JPY',
        iso3 == 'NOR', 'NOK',
        iso3 == 'SWE', 'SEK',
        iso3 == 'NZL', 'NZD',
        iso3 == 'USA', 'USD',
        iso3 == 'IDN', 'IDR',
        iso3 == 'KOR', 'KRW'
    )]
    return(xrate[!is.na(currency), ])
}


####-------------------------# main #------------------------------####
# merge exchange rate onto gfatm and convert currency
make_gfatm_income <- function() {
    # load input data
    .g("  Loading gfatm_income_data: ", INCOME_FP)
    income <- get_gfatm_income()

    .g("  Loading exchange rates: ", XRATE_FP)
    xrates <- get_exchange_rates()

    # merge exchange rate onto income, and fill in missing exchange rates
    .g("  Imputing exchange rates")
    income <- merge(income,
                    xrates[, .(currency, year, exchrate)],
                    by = c("currency", "year"),
                    suffixes = c("", ".tmp"),
                    all = TRUE)
    income[is.na(exchrate), exchrate := exchrate.tmp]
    income[, exchrate.tmp := NULL]
    income[currency == "USD", exchrate := 1]

    # convert income data to USD
    .g("  Converting currencies")
    inccols <- c("income_reg_paid", "income_reg_pledged")
    income[year > 2013 & currency != "" & !is.na(exchrate),
           (inccols) := lapply(.SD, \(x) x / exchrate),
           .SDcols = inccols
           ]
    setnafill(income, fill = 0, cols = inccols)

    # remove rows w missing income
    income <- income[!(income_reg_paid == 0 & income_reg_pledged == 0), ]

    # calculate income_reg_paid's share of total income
    .g("  Calculating GFATM income shares")
    income[, income_all := income_reg_paid]
    income[, income_total_yr := sum(income_all, na.rm = TRUE),
           by = .(year)]
    income[, income_all_share := income_all / income_total_yr]
 
    # finalize dataset and then save
    .g("  Finalizing and saving to", OUTPUT_FILE)
    income <- income[!is.na(income_all_share),
                     .(channel, year, iso3, donor_name, donor_country,
                       income_sector, income_type, income_all_share)]
 
    save_dataset(income, OUTPUT_FILE,
                 channel = "unitaid", stage = "int")

    .g("income_all_share summary:")
    print(summary(income$income_all_share))
}

make_gfatm_income()

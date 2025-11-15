#
# Goal: to have a data set at the country-year level which contains fractions
# that can be used to estimate the share of spending that goes to each
# country in a given year.
#
# General steps:
# 1) load and combine extractions
# 2) handle any multi-year data (if relevant for agency)
# 3) handle any regional-level data (if relevant for agency)
# 4) handle any 'multi-country' data (if relevant for agency)
# 5) standardize recipient countries by mapping all rows to an ISO3 code
#
# Final result should be at the iso3-year level
#
library(data.table)
library(openxlsx)

IN_DIR <- "/home/j/Project/IRH/DAH/RESEARCH/un_agency_recipient_extractions/"

# internal DAH function, copied here for convenience
string_to_std_ascii <- function(x,
                                repl_char = " ",
                                post_fn = toupper,
                                pad_char = " ") {
    # convert to ascii
    x <- iconv(x, to = "ASCII//TRANSLIT")
    # remove non-alphanumeric characters
    if (!is.null(repl_char)) {
        x <- gsub("[^[:alnum:] ]", repl_char, x)
    }
    # remove multiple spaces and any whitespace at beginning or end
    x <- trimws(gsub("\\s+", " ", x))
    # apply case function (e.g., upper case all chars)
    if (!is.null(post_fn))
        x <- post_fn(x)
    # pad phrase with leading and trailing characters
    if (!is.null(pad_char) && pad_char != "")
        x <- paste0(pad_char, x, pad_char)
    return(x)
}


# helper to clean extracted totals
clean_numeric <- function(x) {
    neg <- grepl("^\\(", x) # negative values reported like '(1000)'
    x[neg] <- gsub("\\)", "", gsub("\\(", "-", x[neg]))
    x <- gsub("[^0-9.-]", "", x)
    x <- as.numeric(x)
    return(x)
}


#
# Load in the data from your excel sheets =====================================
#
unfpa_full <- data.table()
for (yr in c(
    2000, 2002:2007, "2008_2009", "2010_2011", 2012:2023
)) {
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNFPA", paste0("unfpa_", yr, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    dt[, year := yr]
    unfpa_full <- rbind(unfpa_full, dt, fill = TRUE)
}

## clean 'total' column - make sure nothing is converted to NA that shouldn't be!
unfpa_full[, total := clean_numeric(total)]

## (convert to thousands of dollars to match other years)
unfpa_full[year %in% 2002:2003, total := total/1e3]


## misc. cleaning to prep for below steps
unfpa_full[, region := toupper(region)]
unfpa_full[region == "LATIN AMERICA AND CARIBBEAN",
          region := "LATIN AMERICA AND THE CARIBBEAN"]
unfpa_full[region == "GLOBAL", country := "GLOBAL"]


#
# Handle biennium years =======================================================
#

## For biennium years (or any reports that span multiple years),
## we can simply duplicate the rows for the number of years,
## and re-label the year for each row,
## since we are only interested in fractions and we will assume the biennium
## fractions hold across both (all) years.

## duplicate biennium totals
unfpa_bi1 <- unfpa_full[year %in% c("2008_2009", "2010_2011")]
unfpa_bi1[, c("year1", "year2") := tstrsplit(year, "_", fixed = TRUE)]
unfpa_bi2 <- copy(unfpa_bi1)
unfpa_bi1[, `:=`(year = unlist(year1), 
                 year1 = NULL, 
                 year2 = NULL)]
unfpa_bi2[, `:=`(year = unlist(year2),
                 year1 = NULL, 
                 year2 = NULL)]

## re-combine with the rest of the data
unfpa_full <- rbind(
    unfpa_full[!year %in% c("2008_2009", "2010_2011")],
    unfpa_bi1,
    unfpa_bi2
)
rm(unfpa_bi1, unfpa_bi2)


unfpa_tst <- copy(unfpa_full)


#
# Disaggregate regional activities ============================================
#
## UNFPA, and some other UN channels, have 'regional' programs that support several
## countries in the given region. We use the observed distribution of spending
## in the countries in the region to disaggrgeate the regional total.

## identify which rows are regional-level projects
unfpa_full[, is_regional := country %ilike% "^regional"]

unfpa_region <- unfpa_full[is_regional == TRUE, -"country"]

## compute country fractions for each region-year
unfpa_reg_fracs <- unfpa_full[is_regional == FALSE,
                             .(year, country, region, total)]
unfpa_reg_fracs[, region_total := sum(total, na.rm = TRUE), by = .(year, region)]
unfpa_reg_fracs[, frac := total / region_total]

## merge country fractions onto regional totals
unfpa_region <- merge(
    unfpa_region,
    unfpa_reg_fracs[, .(year, region, country, frac)],
    by = c("year", "region"),
    all.x = TRUE
)
stopifnot(unfpa_region[is.na(frac), .N] == 0) ## all regions should have fractions

## re-distirbute the regional total to the countries
unfpa_region[, total := total * frac]

## re-combine with full data
unfpa_full <- rbind(
    unfpa_full[is_regional == FALSE, .(year, country, total, earmarked, unearmarked)],
    unfpa_region[, .(year, country, total, earmarked, unearmarked)]
)


#
# Expand multi-country activities =============================================
#
## Some UNFPA activities are 'multi-country'. This differs from regional, because
## we don't have observed country fractions to use for disaggregation.
## For example, instead of reporting the smaller Caribbean islands individually,
## UNFPA reports a single total for the entire Caribbean 'multi-country' group.
## We simply disaggregate this total amount to the country level by
## evenly splitting up the total among all members of the multi-country group.
## The group members were determined a) by looking at the footnotes in the
## financial documents, which listed the members, or b) googling around and
## finding official UNFPA sites/documents.

## First standardize the multi-country group names
unfpa_full[, country := fcase(
    country %ilike% "pacific multi|pacific islands \\(multi-country\\)",
    "Pacific Islands (multi-country)",
    country %ilike% "caribbean multi-country programme, english-and dutch-",
    "Caribbean (multi-country)",
    country %ilike% "caribbean multi-country programme, english- and dutch-",
    "Caribbean (multi-country)",
    country %ilike% "caribbean, english- and dutch-speaking",
    "Caribbean (multi-country)",
    country %ilike% "english- and dutch-speaking caribbean countries",
    "Caribbean (multi-country)",
    country %ilike% "caribbean \\(multi-country\\)",
    "Caribbean (multi-country)",
    country == "Caribbean",
    "Caribbean (multi-country)",
    country %in% c("English-speaking Caribbean", "Caribbean, English-speaking"),
    "English-speaking Caribbean",
    rep_len(TRUE, .N), country
)]

## re-aggregate standardized data
unfpa_full <- unfpa_full[, .(total = sum(total, na.rm = TRUE)),
                         by = .(year, country)]


## expand the multi-country activities by exploding the multi-country rows
## into individual country rows.
unfpa_expand <- unfpa_full[country %in% c("Caribbean (multi-country)",
                                         "English-speaking Caribbean",
                                         "Pacific Islands (multi-country)",
                                         "Commonwealth of Independent States")]
unfpa_expand[, region := country]
unfpa_expand[, country := fcase(
    ## (countries based on listing in footnotes)
    region == "Pacific Islands (multi-country)",
    paste(c("Cook Islands", "Fiji",
            "Kiribati", "Marshall Islands",
            "Micronesia (Federated States of)",
            "Nauru", "Niue",
            "Palau", "Samoa",
            "Solomon Islands", "Tokelau",
            "Tonga", "Tuvalu",
            "Vanuatu"), collapse = ";"),
    region == "Caribbean (multi-country)",
    paste(c("Anguilla", "Antigua and Barbuda",
            "Aruba", "Bahamas", "Barbados",
            "Belize", "Bermuda", "British Virgin Islands",
            "Cayman Islands", "Curaçao", "Dominica",
            "Grenada", "Guyana", "Jamaica",
            "Montserrat", "Saint Lucia",
            "Saint Kitts and Nevis",
            "Saint Vincent and the Grenadines",
            "Sint Maarten", "Suriname",
            "Trinidad and Tobago", "Turks and Caicos Islands"), collapse = ";"),
    region == "English-speaking Caribbean",
    paste(c("Anguilla", "Antigua and Barbuda", "Bahamas",
            "Barbados", "Belize", "British Virgin Islands",
            "Cayman Islands", "Dominica",
            "Grenada", "Guyana", "Jamaica", "Monsterrat",
            "Saint Kitts and Nevis", "Saint Lucia",
            "Saint Vincent and the Grenadines",
            "Trinidad and Tobago", "Turks and Caicos Islands"), collapse = ";"),
    # https://eeca.unfpa.org/sites/default/files/pub-pdf/youth-eng_0.pdf
    region == "Commonwealth of Independent States",
    paste(c("Armenia", "Azerbaijan", "Belarus",
            "Kazakhstan", "Kyrgyzstan",
            "Moldova", "Tajikistan",
            "Ukraine", "Uzbekistan"), collapse = ";")
)]

## explode
unfpa_expand <- unfpa_expand[, .(country = unlist(strsplit(country, ";"))),
                             by = .(year, region, total)]
## distribute
unfpa_expand[, num_countries := .N, by = .(year, region)]
unfpa_expand[, total := total / num_countries]


## re-combine
unfpa_full <- rbind(
    unfpa_full[! country %in% unique(unfpa_expand$region)],
    unfpa_expand[, .(year, country, total)]
)

## test that total spending has been maintained, ie, region and multi-country
## disaggregation didn't result in loss of funds
stopifnot(unfpa_full[, sum(total)] == unfpa_tst[, sum(total)])


#
# Standardize location names =================================================
#
## Now all data is at the country-year level, so we are ready to standardize
## country names.
unfpa <- copy(unfpa_full)


## load and prep location mapping
locs <- fread("/home/j/Project/IRH/DAH/RESEARCH/INTEGRATED DATABASES/COUNTRY FEATURES/FGH_2024/countrycodes_official.csv")
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(country_clean, iso3)])

## merge country iso codes onto unfpa data
unfpa[, country_clean := string_to_std_ascii(country, pad_char = "")]
unfpa[, country_clean := gsub("[0-9]", "", country_clean)] ## remove any numeric characters (footnotes)
unfpa <- merge(
    unfpa,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)

## manually fix a few cases
unfpa[, iso3 := fcase(
    country_clean == "GLOBAL", "WLD",
    country_clean %like% "D IVOIRE", "CIV",
    country_clean %like% "CONGO DEMOCRATIC REPUBLIC OF THE", "COD",
    country_clean == "CU BA", "CUB",
    country_clean == "ESWATINI", "SWZ",
    country_clean %like% "GEORGIA", "GEO",
    country_clean %like% "KOSOVO", "XKX",
    country_clean %like% "MACEDONIA", "MKD",
    country_clean %like% "MONSTERRAT", "MSR",
    country_clean %like% "PALESTINE", "PSE",
    country_clean %like% "SERBIA", "SRB",
    country_clean %like% "SINT MAARTEN", "SXM",
    country_clean %like% "TURKIYE", "TUR",
    country_clean %like% "TANZANZIA", "TZA",
    rep_len(TRUE, .N), iso3
)]

## ensure all countires have an iso code
stopifnot(unfpa[is.na(iso3), .N] == 0)

## re-aggregate final dataset
unfpa <- unfpa[, .(total = sum(total)),
               by = .(year = as.integer(year), iso3)]

# done!
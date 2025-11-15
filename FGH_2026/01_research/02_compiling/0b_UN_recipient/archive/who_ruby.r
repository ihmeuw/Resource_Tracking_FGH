library(data.table)
library(openxlsx)
library(readxl)
library(dplyr)
library(stringr)

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


# Load in the data from your excel sheets =====================================
#
who_22_23 <- data.table()

## 2022 and 2023 are hand extracted
for (yr in c(
    2022:2023
)) {
    dt <- as.data.table(readxl::read_excel(
        file.path(IN_DIR, "WHO", paste0("who_", yr, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    dt[, year := yr]
    who_22_23 <- rbind(who_22_23, dt, fill = TRUE)
}
who_22_23 <- who_22_23[!is.na(total)]

who_17_21 <- data.table()
## 2017-2021 are extracted from IATI
for (yr in c(
    2017:2021
)) {
    dt <- as.data.table(readxl::read_excel(
        file.path(IN_DIR, "WHO", paste0("who_", yr, "_recipient.xlsx")),
        sheet = "Data"
    ))
    names(dt) <- tolower(names(dt))
    dt[, year := yr]
    who_17_21 <- rbind(who_17_21, dt, fill = TRUE)
}

## rename and select columns
who_17_21 <- who_17_21 %>%
    rename(country_iso2 = `recipient country or region`,
           total = `value (usd) (disbursement, expenditure)`) %>%
    select(-`value (usd) (budget)`)

## standardize location for 2017-2021 dataset
iso2<- str_split_fixed(who_17_21$country_iso2, " - ", 2)
who_17_21 <- cbind(who_17_21, iso2)
who_17_21 <-who_17_21 %>%
    rename(country = V2, iso2 = V1) %>%
    select(-country_iso2)
locs <- fread("/home/j/Project/IRH/DAH/RESEARCH/INTEGRATED DATABASES/COUNTRY FEATURES/FGH_2024/countrycodes_official.csv")
#locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(iso3, iso2)])

who_17_21 <- merge(
    who_17_21,
    locs[, .(iso2, iso3)],
    by = "iso2",
    all.x = TRUE
)

who_17_21[, iso3 := fcase(
    country == "Namibia", "NAM",
    country == "South Sudan", "SSD",
    rep_len(TRUE, .N), iso3
)]

setorder(who_17_21, year)

## standarize location for 2022-2023
locs <- fread("/home/j/Project/IRH/DAH/RESEARCH/INTEGRATED DATABASES/COUNTRY FEATURES/FGH_2024/countrycodes_official.csv")
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(country_clean, iso3)])

who_22_23[, country_clean := string_to_std_ascii(country, pad_char = "")]
who_22_23[, country_clean := gsub("[0-9]", "", country_clean)] ## remove any numeric characters (footnotes)
who_22_23 <- merge(
    who_22_23,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)

## manually clean some 
who_22_23[, iso3 := fcase(
    country_clean == "ESWATINI", "SWZ",
    country_clean %like% "NORTH MACEDONIA", "MKD",
    country_clean %like% "PALESTINE", "PSE",
    country_clean %like% "TURKIYE", "TUR",
    rep_len(TRUE, .N), iso3
)]

## WHO's IATI data appears incomeplete prior to 2020
who_17_21 <- who_17_21[year >= 2020]
who_full <- rbind(who_17_21[, .(year, country, iso3, total)], who_22_23[, .(year, country, iso3, total)])
who_full[, total := clean_numeric(total)]

## ensure all countires have an iso code
stopifnot(who_full[is.na(iso3), .N] == 0)

## re-aggregate final dataset
who <- who_full[, .(total = sum(total)),
                by = .(year = as.integer(year), iso3)]

who[, ann_tot := sum(total), by = year]
who[, frac := total/ann_tot]






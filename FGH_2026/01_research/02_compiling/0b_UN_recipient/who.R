library(data.table)
library(openxlsx)
library(duckdb)
library(dplyr)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

IN_DIR <- "FILEPATH"

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


# Load in the data from WHO financial reports =================================
#
who_finrp <- data.table()

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
    who_finrp <- rbind(who_finrp, dt, fill = TRUE)
}
who_finrp <- who_finrp[!is.na(total)]

## standardize location for 2022-2023
locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(country_clean, iso3)])

who_finrp[, country_clean := string_to_std_ascii(country, pad_char = "")]
who_finrp[, country_clean := gsub("[0-9]", "", country_clean)] ## remove any numeric characters (footnotes)
who_finrp <- merge(
    who_finrp,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)
who_finrp[, iso3 := fcase(
    country_clean == "ESWATINI", "SWZ",
    country_clean %like% "NORTH MACEDONIA", "MKD",
    country_clean %like% "PALESTINE", "PSE",
    country_clean %like% "TURKIYE", "TUR",
    rep_len(TRUE, .N), iso3
)]
who_finrp[, total := clean_numeric(total) * 1e3]

## drop high-income recipients
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
who_finrp <- merge(who_finrp,
                   ig[, .(year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
                   by = c("year", "iso3"),
                   all.x = TRUE)
who_finrp[is.na(INC_GROUP), INC_GROUP := ""]
who_finrp <- who_finrp[INC_GROUP != "H"]

# calculate fracs
who_finrp <- who_finrp[total > 0]
who_finrp[, ann_tot := sum(total), by = year]
who_finrp[, frac := total / ann_tot]



#
# Load in WHO data from CRS ==================================================
#
crs_parq_path <- "FILEPATH"

con <- duckdb::dbConnect(duckdb::duckdb()) ## create a DuckDB connection
crs_tbl <- dplyr::tbl(con, paste0("read_parquet('", crs_parq_path, "')")) ## special syntax to connect to parquet file as a DB

all_crs_recip <- crs_tbl |>
    group_by(year, recipient_name) |>
    summarise(usd_disbursement = sum(usd_disbursement, na.rm = TRUE)) |>
    collect() |>
    as.data.table()

all_crs_recip <- all_crs_recip[usd_disbursement > 0]
all_crs_recip[, recipient_clean := string_to_std_ascii(recipient_name, pad_char = "")]
all_crs_recip <- merge(
    all_crs_recip,
    locs[, .(country_clean, iso3)],
    by.x = "recipient_clean",
    by.y = "country_clean",
    all.x = TRUE
)
all_crs_recip[, iso3 := fcase(
    recipient_clean %like% "CHINA|CHINESE", "CHN",
    recipient_clean == "NORTH MACEDONIA", "MKD",
    recipient_clean == "ESWATINI", "SWZ",
    recipient_clean == "TURKIYE", "TUR",
    rep_len(TRUE, .N), iso3
)]



who_crs_chan <- crs_tbl |>
    filter(
        channel_name %in% c(
            "World Health Organisation - assessed contributions",
            "World Health Organisation - core voluntary contributions account",
            "World Health Organisation - Strategic Preparedness and Response Plan"
        ) | 
            donor_name == "World Health Organisation"
    ) |>
    collect() |>
    as.data.table()

who_crs_chan <- who_crs_chan[year >= 2010] # pre-2011, disbursements are far too low and not many projects
who_crs <- who_crs_chan[, .(disb = sum(usd_disbursement * 1e6, na.rm = TRUE)),
                        by = .(year, recipient_name)]
who_crs[, recipient_clean := string_to_std_ascii(recipient_name, pad_char = "")]

who_crs <- merge(
    who_crs, unique(all_crs_recip[, .(recipient_clean, iso3)]),
    by = "recipient_clean", all.x = TRUE
)
who_crs <- who_crs[recipient_clean != "BILATERAL UNSPECIFIED"]
who_crs <- who_crs[!recipient_clean %like% "REGIONAL|UNSPECIFIED"]
who_crs <- who_crs[, .(disb = sum(disb)), by = .(year, iso3)]

grid <- data.table::CJ(
    year = 2000:2021,
    iso3 = unique(who_crs$iso3)
)
grid <- merge(
    grid,
    who_crs[, .(year, iso3, value = disb)],
    by = c("year", "iso3"),
    all.x = TRUE
)

# determine minimum year of any disbursement in CRS to avoid disbursing to
# countries pre-existence
grid <- merge(
    grid,
    all_crs_recip[, .(year, iso3, any_spend = usd_disbursement)],
    by = c("year", "iso3"),
    all.x = TRUE
)
setnafill(grid, fill = 0, cols = "any_spend")
grid[, min_yr := min(year[any_spend > 0]), by = iso3]
grid[, max_yr := max(year[any_spend > 0]), by = iso3]
grid[, orig_value := value]

# for missing years which are in-between 2 non-missing years, use average
setorder(grid, year)
grid[, last_val := zoo::na.locf(value, na.rm = FALSE), by = iso3]
grid[, next_val := zoo::na.locf(value, fromLast = TRUE, na.rm = FALSE), by = iso3]

grid[!is.na(last_val), value_pr := (last_val + next_val)/2]
grid[is.na(value), value := value_pr]

# for missing years which precede any disbursement, use average of first 3 years
grid[, min_disb_yr := min(year[!is.na(value)]), by = iso3]
grid[order(year),
     mean_last_3 := mean(value[year >= min_disb_yr & !is.na(value)][1:3]),
     by = iso3]
grid[is.na(value) & year < min_disb_yr, value := mean_last_3]

# for missing years which come after any disbursement, use average of last 3 years
grid[, max_disb_yr := max(year[!is.na(value)]), by = iso3]
grid[order(year, decreasing = TRUE),
     mean_next_3 := mean(value[year <= max_disb_yr & !is.na(value)][1:3]),
     by = iso3]
grid[is.na(value) & year > min_disb_yr, value := mean_next_3]

# now value is only missing if the recipient has only received disbursements in
# one year - in that case, let's assume 0 in other years.
grid[is.na(value), value := 0]

# if outside of min_yr or max_yr of CRS disbursements, set to 0
grid[year < min_yr | year > max_yr, value := 0]


# compute recipient fractions
crs_fracs <- grid[, .(value = sum(value, na.rm = TRUE)), by = .(year, iso3)]

## drop high-income recipients
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
crs_fracs <- merge(crs_fracs,
                   ig[, .(year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
                   by = c("year", "iso3"),
                   all.x = TRUE)
crs_fracs[is.na(INC_GROUP), INC_GROUP := ""]
crs_fracs <- crs_fracs[INC_GROUP != "H"]
crs_fracs <- crs_fracs[value > 0]
crs_fracs[, total := sum(value, na.rm = TRUE), by = year]
crs_fracs[, frac := value / total]

# ensemble CRS-based fractions with WHO financial reporting fractions
fr <- dcast(who_finrp, iso3 ~ paste0("y", year), value.var = "frac")
fr[, mean_frac := rowMeans(.SD, na.rm = TRUE), .SDcols = patterns("^y")]
crs_fracs <- merge(
    crs_fracs,
    fr[, .(iso3, mean_frac)],
    by = "iso3",
    all.x = TRUE
)
crs_fracs[is.na(mean_frac), mean_frac := 0]
crs_fracs[, new_frac := (frac + mean_frac) / 2]
## re-normalize new_frac
crs_fracs[, new_frac := new_frac/sum(new_frac), by = year]


#
# combine CRS with 2022+ financial statement fractions =======================
#
fin <- rbind(
    crs_fracs[, .(year, iso3, frac = new_frac)],
    who_finrp[, .(year, iso3, frac)]
)
fin[, channel := "WHO"]


# save the result
fwrite(
    fin,
    get_path("compiling", "int", "who_recip_fracs.csv")
)

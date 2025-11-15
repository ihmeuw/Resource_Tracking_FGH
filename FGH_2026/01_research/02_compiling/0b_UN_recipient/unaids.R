code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


IN_DIR <- "FILEPATH"


clean_numeric <- function(x) {
    neg <- grepl("^\\(", x)
    x[neg] <- gsub("\\)", "", gsub("\\(", "-", x[neg]))
    x <- gsub("[^0-9.-]", "", x)
    x <- as.numeric(x)
    return(x)
}


#
# Post-process UNAIDS extracted recipient data ================================ 
#

unaids_raw <- data.table()
for (yr in c(
    "2000_2001",
    "2002_2003",
    "2004_2005",
    "2006_2007",
    "2008_2009",
    "2010_2011",
    2012:2023
)) {
    print(yr)
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNAIDS", paste0("unaids_", yr, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    if ("countries" %in% names(dt))
        setnames(dt, "countries", "country")
    # split bienniums
    if (grepl("_", yr, fixed = TRUE)) {
        dt[, year := strsplit(yr, "_")[[1]][1]]
        dt2 <- copy(dt)
        dt2[, year := strsplit(yr, "_")[[1]][2]]
        dt <- rbind(dt, dt2)
        dt[, `:=`(
            total = total/2,
            unearmarked = unearmarked/2,
            earmarked = earmarked/2
        )]
    } else {
        dt[, year := yr]
    }
    unaids_raw <- rbind(unaids_raw, dt, fill = TRUE)
}
unaids_raw[, year := as.integer(year)]
unaids_raw[year < 2012, `:=`( # values were reported in 1000s for just these years
    total = total * 1e3,
    unearmarked = unearmarked * 1e3,
    earmarked = earmarked * 1e3
)]



#
# Process country-level data first
#
unaids_country <- unaids_raw[year >= 2008]
unaids_country <- unaids_country[country != "Global"]


#
# Split regional projects
#
unaids_country[, country := trimws(country)]
unaids_country[, is_region := country %ilike% "^region"]

ureg <- unaids_country[is_region == TRUE, -"country"]
reg_fracs <- unaids_country[is_region == FALSE,
                            .(total = sum(total),
                              unearmarked = sum(unearmarked, na.rm = TRUE),
                              earmarked = sum(earmarked, na.rm = TRUE)),
                            by = .(country, region, year)]
reg_fracs[, `:=`(
    reg_tot = sum(total, na.rm = TRUE),
    reg_unear = sum(unearmarked, na.rm = TRUE),
    reg_ear = sum(earmarked, na.rm = TRUE)
), by = .(year, region)]
reg_fracs[, `:=`(
    tot_frac = total / reg_tot,
    unear_frac = unearmarked / reg_unear,
    ear_frac = earmarked / reg_ear
)]

ureg <- merge(
    ureg,
    reg_fracs[, .(region, year, country, tot_frac, unear_frac, ear_frac)],
    by = c("region", "year"),
    all.x = TRUE
)

stopifnot(
    ## ensure all fractions sum to 1 for all region years
    ureg[, sum(tot_frac), by = .(region, year)][abs(V1 - 1) > 1e-6, .N] == 0
)
ureg[, `:=`(
    total = total * tot_frac,
    unearmarked = unearmarked * unear_frac,
    earmarked = earmarked * ear_frac
)]

unaids_country <- rbind(
    unaids_country[is_region == FALSE, -"is_region"],
    ureg[, .(year, region, country, total, earmarked, unearmarked)]
)

unaids_country <- unaids_country[, .(
    total = sum(total, na.rm = TRUE),
    earmarked = sum(earmarked, na.rm = TRUE),
    unearmarked = sum(unearmarked, na.rm = TRUE)
), by = .(year, country)]


#
# Map on ISO codes
#
locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(country_clean, iso3)])

unaids_country[, country_clean := string_to_std_ascii(country, pad_char = "")]
## remove any numeric characters (footnotes)
unaids_country[, country_clean := gsub("[0-9]", "", country_clean)]
unaids_country <- merge(
    unaids_country,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)

unaids_country[, iso3 := fcase(
    country_clean == "BUTAN", "BTN",
    country_clean == "COMORES", "COM",
    country_clean == "D R CONGO", "COD",
    country_clean == "ESWATINI", "SWZ",
    country_clean %like% "GUINEA CON", "GIN",
    country_clean == "SAWZILAND", "SWZ",
    country_clean == "TURKMINISTAN", "TKM",
    country_clean %like% "MADAGASCAR", "MDG",
    country_clean %like% "INDIAN OCEAN INITIATIVE", "MDG",
    rep_len(TRUE, .N), iso3
)]

stopifnot( unaids_country[is.na(iso3), .N] == 0 )


unaids_country <- unaids_country[,
                                 .(total = sum(total),
                                   earmarked = sum(earmarked),
                                   unearmarked = sum(unearmarked)),
                 by = .(year, iso3)]


#
# Drop high-income countries ================================================
#

## drop High-income
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
unaids_country <- merge(unaids_country,
                        ig[, .(year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
                        by = c("year", "iso3"),
                        all.x = TRUE)
unaids_country[is.na(INC_GROUP), INC_GROUP := ""]
unaids_lmic <- unaids_country[INC_GROUP != "H"]

#
# Impute years pre-2008 ======================================================
#
unaids <- unaids_lmic[total > 0, .(year, iso3, total)]

## calculate average disbursement over 2008-2013 (3 bienniums) as an initial
## imputation for years before 2008
impt_unaids <- unaids_country[between(year, 2008, 2013),
                              .(avg_total = sum(total) / 6),
                              by = iso3]

## use ADB PDB to identify fluctuations in HIV spending by country
adb <- fread(get_path("compiling", "fin", "DAH_ADB_PDB_1990_2024.csv"))
adb <- adb[ELIM_CH == 0 & ELIM_DONOR == 0]
adb <- adb[ISO3_RC %in% unaids_lmic$iso3,
           .(hiv_dah = sum(hiv_DAH_24, na.rm = TRUE)),
           by = .(year = YEAR, iso3 = ISO3_RC)]
adb <- adb[hiv_dah > 0]
adb_hiv <- data.table::CJ(
    year = unique(adb$year),
    iso3 = unique(adb$iso3)
)
adb_hiv <- merge(
    adb_hiv,
    adb[, .(year, iso3, hiv_dah)],
    by = c("year", "iso3"),
    all.x = TRUE
)
setnafill(adb_hiv, fill = 0, cols = "hiv_dah")
adb_hiv[, min_yr := min(year[hiv_dah > 0]), by = iso3]

## smooth hiv_dah to avoid fluctuations between spend and no spend
## (ie, NA pct-chg and infinite pct-chg problems)
## and to enforce some averaging for more robust prediction
adb_hiv[, hiv_dah_sm := hiv_dah]
adb_hiv[hiv_dah_sm == 0, hiv_dah_sm := 0.01]
adb_hiv[order(year), hiv_dah_sm := exp(fitted(loess(
    log(hiv_dah_sm) ~ year, span = 0.75
))), by = iso3]

adb_hiv[order(year), l1 := shift(hiv_dah_sm, type = "lead"), by = iso3]
adb_hiv[, pct_chg := (hiv_dah_sm - l1) / l1] # YoY change going backward in time


impt_unaids <- impt_unaids[, .(year = 2000:2007), by = names(impt_unaids)]
impt_unaids <- merge(
    impt_unaids,
    adb_hiv[, .(year, iso3, min_yr, pct_chg)],
    by = c("year", "iso3"),
    all.x = TRUE
)
impt_unaids <- impt_unaids[!is.na(min_yr)] # drop countries if no ADB data

## apply YoY change from 2008 onwards to impute pre-2008
impt_unaids[, l1 := avg_total]
for (yr in 2007:2000) {
    impt_unaids[year == yr, total := l1 * (1 + pct_chg)]
    impt_unaids[year == yr & is.na(pct_chg), total := 0]
    impt_unaids[order(year), l1 := shift(total, type = "lead"), by = iso3]
}


unaids <- rbind(
    impt_unaids[, .(year, iso3, total)],
    unaids[, .(year, iso3, total)]
)

unaids[, frac := total / sum(total), by = year]
unaids <- unaids[, .(
    year,
    iso3,
    channel = "UNAIDS",
    frac
)]



# Save the cleaned data =======================================================
fwrite(
    unaids,
    get_path("compiling", "int", "unaids_recip_fracs.csv")
)

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
# Post-process UNFPA extracted recipient data ================================ 
#
unfpa_raw <- data.table()
for (yr in c(
    "1990", "1991_1992", "1993_1994", "1995_1996", "1997_1998", "1999"
)) {
    print(yr)
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNFPA", paste0("unfpa_", yr, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    tot_cols <- grep("total", names(dt), value = TRUE)
    dt[, (tot_cols) := lapply(.SD, clean_numeric), .SDcols = tot_cols]
    dt <- melt(dt, id.vars = c("region", "country"),
               value.name = "total",
               variable.name = "year",
               variable.factor = FALSE)
    dt[, year := as.integer(gsub("total_", "", year))]
    unfpa_raw <- rbind(unfpa_raw, dt)
}
unfpa_raw[year <= 1992, total := total / 1e3] # convert to thousands to match other years

for (yr in c(
    2000:2023
)) {
    print(yr)
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNFPA", paste0("unfpa_", yr, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    dt[, year := yr]
    unfpa_raw <- rbind(unfpa_raw, dt, fill = TRUE)
}
unfpa_raw[, `:=`(
    total = clean_numeric(total),
    earmarked = clean_numeric(earmarked),
    unearmarked = clean_numeric(unearmarked)
)]
unfpa_raw[year %in% 2002:2003, total := total/1e3] ## convert to thousands to match other years
## for years 2018+, the 'total' includes institutional budget, but we want
##   earmarked program spending + unearmarked program spending for consistency
##   with earlier years
unfpa_raw[year >= 2018, `:=`(
    earmarked = fifelse(is.na(earmarked), 0, earmarked),
    unearmarked = fifelse(is.na(unearmarked), 0, unearmarked),
    total = NA
)]
unfpa_raw[year >= 2018,
          total := earmarked + unearmarked]

unfpa_raw[, region := toupper(region)]
unfpa_raw[region == "LATIN AMERICA AND CARIBBEAN",
          region := "LATIN AMERICA AND THE CARIBBEAN"]
unfpa_raw[region == "GLOBAL", country := "GLOBAL"]

unfpa_raw <- unfpa_raw[!is.na(total) & total > 0]


unfpa_tst <- copy(unfpa_raw)



## disaggregate regional activities
unfpa_raw[, is_regional := country %ilike% "^regional"]

unfpa_region <- unfpa_raw[is_regional == TRUE, -"country"]

unfpa_reg_fracs <- unfpa_raw[is_regional == FALSE,
                             .(year, country, region, total, earmarked, unearmarked)]
unfpa_reg_fracs[, `:=`(
    region_total = sum(total, na.rm = TRUE),
    region_earmarked = sum(earmarked, na.rm = TRUE),
    region_unearmarked = sum(unearmarked, na.rm = TRUE)
), by = .(year, region)]
unfpa_reg_fracs[, `:=`(
    total_frac = total / region_total,
    earmarked_frac = earmarked / region_earmarked,
    unearmarked_frac = unearmarked / region_unearmarked
)]
unfpa_region <- merge(
    unfpa_region,
    unfpa_reg_fracs[, .(year, region, country,
                        total_frac, earmarked_frac, unearmarked_frac)],
    by = c("year", "region"),
    all.x = TRUE,
    allow.cartesian = TRUE # merging many recips onto each region
)
stopifnot(unfpa_region[is.na(total_frac), .N] == 0)
unfpa_region[, `:=`(
    total = total * total_frac,
    earmarked = earmarked * earmarked_frac,
    unearmarked = unearmarked * unearmarked_frac
)]

unfpa_raw <- rbind(
    unfpa_raw[is_regional == FALSE, .(year, country, total, earmarked, unearmarked)],
    unfpa_region[, .(year, country, total, earmarked, unearmarked)]
)


## expand multi-country activities
unfpa_raw[, country := fcase(
    country %ilike% "pacific multi|pacific islands \\(multi-country\\)|pacific island countries",
    "Pacific Islands (multi-country)",
    country %ilike% "pacific sub",
    "Pacific Islands (multi-country)",
    country %ilike% "caribbean multi-country programme, english-and dutch-",
    "Caribbean (multi-country)",
    country %ilike% "caribbean multi-country programme, english- and dutch-",
    "Caribbean (multi-country)",
    country %ilike% "caribbean, english- and dutch-speaking",
    "Caribbean (multi-country)",
    country %ilike% "caribbean, english- and dutch- speaking",
    "Caribbean (multi-country)",
    country %ilike% "english- and dutch-speaking caribbean countries",
    "Caribbean (multi-country)",
    country %ilike% "caribbean \\(multi-country\\)",
    "Caribbean (multi-country)",
    country %ilike% "caribbean, countries and territories|caribbean countries and territories",
    "Caribbean (multi-country)",
    country %ilike% "caribbean sub",
    "Caribbean (multi-country)",
    country == "Caribbean",
    "Caribbean (multi-country)",
    country %in% c("English-speaking Caribbean", "Caribbean, English-speaking"),
    "English-speaking Caribbean",
    country %in% c("Commonwealth of Independent States",
                   "Commonwealth lndepdent States  / CARK",
                   "Central Asian countries"),
    "Commonwealth of Independent States",
    rep_len(TRUE, .N), country
)]

unfpa_raw <- unfpa_raw[, .(
    total = sum(total, na.rm = TRUE),
    earmarked = sum(earmarked, na.rm = TRUE),
    unearmarked = sum(unearmarked, na.rm = TRUE)
    ),
    by = .(year, country)]


unfpa_expand <- unfpa_raw[country %in% c("Caribbean (multi-country)",
                                         "English-speaking Caribbean",
                                         "Pacific Islands (multi-country)",
                                         "Commonwealth of Independent States",
                                         "Trust Territory of the Pacific Islands")]
## (based on listing in foot-notes)
unfpa_expand[, region := country]
unfpa_expand[, country := fcase(
    region == "Pacific Islands (multi-country)",
    paste0(c("COK", "FJI", "KIR", "MHL", "FSM", "NRU", "NIU", "PLW", "WSM",
             "SLB", "TKL", "TON", "TUV", "VUT"),
           collapse = ";"),
    region == "Caribbean (multi-country)",
    paste0(c("AIA", "ATG", "ABW", "BHS", "BRB", "BLZ", "BMU",
             "VGB", "CYM", "CUW", "DMA", "GRD", "GUY", "JAM",
             "MSR", "LCA", "KNA", "VCT", "SXM", "SUR", "TTO",
             "TCA"), collapse = ";"),
    region == "English-speaking Caribbean",
    paste0(c("AIA", "ATG", "BHS", "BRB", "BLZ", "VGB",
             "CYM", "DMA", "GRD", "GUY", "JAM", "MSR",
             "KNA", "LCA", "VCT", "TTO", "TCA"), collapse = ";"),
    region == "Trust Territory of the Pacific Islands",
    paste0(c("MHL", "PLW", "FSM"), collapse = ";"),
    # https://eeca.unfpa.org/sites/default/files/pub-pdf/youth-eng_0.pdf
    region == "Commonwealth of Independent States",
    paste(c("ARM", "AZE", "BLR", "KAZ", "KGZ", "MDA", "TJK", "UKR", "UZB"),
           collapse = ";")
)]
unfpa_expand <- unfpa_expand[, .(country = unlist(strsplit(country, ";"))),
                               by = .(year, region, total, earmarked, unearmarked)]

## calculate recipient fractions from observed DAH
### need multiple data sets here since some regions share member countries
adb <- fread(get_path("compiling", "fin", "DAH_ADB_PDB_1990_2024.csv"))
adb <- adb[ELIM_CH == 0 & ELIM_DONOR == 0 & CHANNEL != "UNFPA"]
adb <- adb[ISO3_RC != "QZA" & INKIND == 0,
           .(dah = sum(DAH_24, na.rm = TRUE)),
           by = .(year = YEAR, iso3 = ISO3_RC)]

adb_reg1 <- copy(adb)
adb_reg1[, region := fcase(
    iso3 %in% c("AIA", "ATG", "BHS", "BRB", "BLZ", "VGB",
                "CYM", "DMA", "GRD", "GUY", "JAM", "MSR",
                "KNA", "LCA", "VCT", "TTO", "TCA"),
    "Caribbean (multi-country)",
    iso3 %in% c("COK", "FJI", "KIR", "MHL", "FSM", "NRU",
                "NIU", "PLW", "WSM", "SLB", "TKL",
                "TON", "TUV", "VUT"),
    "Pacific Islands (multi-country)",
    iso3 %in% c("ARM", "AZE", "BLR", "KAZ", "KGZ",
                "MDA", "TJK", "UKR", "UZB"),
    "Commonwealth of Independent States",
    default = "Other"
)]
adb_reg1[, frac := dah / sum(dah), by = .(year, region)]

adb_reg2 <- copy(adb)
adb_reg2[, region := fcase(
    iso3 %in% c(
        "AIA", "ATG", "BHS", "BRB", "BLZ", "VGB",
        "CYM", "DMA", "GRD", "GUY", "JAM", "MSR",
        "KNA", "LCA", "VCT", "TTO", "TCA"
    ),
    "English-speaking Caribbean",
    iso3 %in% c("MHL", "PLW", "FSM"),
    "Trust Territory of the Pacific Islands",
    default = "Other"
)]
adb_reg2[, frac := dah / sum(dah), by = .(year, region)]

adb_reg <- rbind(adb_reg1, adb_reg2)
unfpa_expand <- merge(
    unfpa_expand, adb_reg[, .(year, region, country = iso3, frac)],
    by = c("year", "region", "country"),
    all.x = TRUE
)
setnafill(unfpa_expand, fill = 0, cols = "frac") # if frac is missing, recip-year doesn't receive any DAH in our estimates
stopifnot(
    ## ensure all regions have country fractions that sum to 1
    unfpa_expand[, sum(frac), by = .(year, region)][abs(V1 - 1) > 1e-1, .N] == 0
)
unfpa_expand[, frac := frac / sum(frac), by = .(year, region)] # make fracs exact

unfpa_expand[, `:=`(
    total = total * frac,
    earmarked = earmarked * frac,
    unearmarked = unearmarked * frac
)]

unfpa_raw <- rbind(
    unfpa_raw[! country %in% unique(unfpa_expand$region)],
    unfpa_expand[, .(year, country, total, earmarked, unearmarked)]
)

stopifnot( abs(unfpa_raw[, sum(total)] - unfpa_tst[, sum(total)]) < 1e-6 )


#
# Standardize location names =================================================
#
unfpa <- copy(unfpa_raw)

locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[, .(country_clean, iso3)])

unfpa[, country_clean := gsub("(P)", "", country, fixed = TRUE)] # old reports mark "priority" countries
unfpa[, country_clean := gsub("{P)", "", country_clean, fixed = TRUE)]
unfpa[, country_clean := string_to_std_ascii(country_clean, pad_char = "")]
## remove any numeric characters (footnotes)
unfpa[, country_clean := gsub("[0-9]", "", country_clean)]
unfpa <- merge(
    unfpa,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)

unfpa[country %in% unfpa_expand$country, iso3 := country]
unfpa[, iso3 := fcase(
    country_clean == "GLOBAL", "WLD",
    country_clean %like% "D IVOIRE", "CIV",
    country_clean %like% "DEMOCRATIC PEOPLE S REPUBLIC OF KOREA", "PRK",
    country_clean %like% "CONGO DEMOCRATIC REPUBLIC OF THE|DEMOCRATIC REPUBLIC OF THE CONGO",
    "COD",
    country_clean == "CU BA", "CUB",
    country_clean == "EASTTIMOR", "TLS",
    country_clean == "ESWATINI", "SWZ",
    country_clean %like% "GEORGIA", "GEO",
    country_clean == "HA ITI", "HTI",
    country_clean %like% "KOSOVO", "XKX",
    country_clean %like% "LAO PEOPLE", "LAO",
    country_clean == "LYBIA", "LBY",
    country_clean %like% "MACEDONIA", "MKD",
    country_clean %like% "MONSTERRAT", "MSR",
    country_clean %like% "PALESTINE", "PSE",
    country_clean %like% "REPUBLIC OF MOLDOVA", "MDA",
    country_clean == "SAO TOMIE AND PRINCIPE", "STP",
    country_clean %like% "SERBIA", "SRB",
    country_clean %like% "SINT MAARTEN", "SXM",
    country_clean %like% "TURKIYE", "TUR",
    country_clean %like% "TANZANZIA", "TZA",
    country_clean == "ZAIRE", "COD", # Zaire is the former name of the Democratic Republic of the Congo
    country_clean %like% "HONG KONG", "CHN",
    country_clean %like% "PALAU", "PLW",
    country_clean %like% "YUGOSLAVIA", "XYG", # former Yugoslavia
    rep_len(TRUE, .N), iso3
)]

stopifnot(unfpa[is.na(iso3) | iso3 == "", .N] == 0)

unfpa <- unfpa[, .(total = sum(total),
                   earmarked = sum(earmarked),
                   unearmarked = sum(unearmarked)),
               by = .(year = as.integer(year), iso3)]


# drop high-income recipients
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
unfpa <- merge(unfpa,
               ig[, .(year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
               by = c("year", "iso3"),
               all.x = TRUE)
unfpa[is.na(INC_GROUP), INC_GROUP := ""]
unfpa_lmic <- unfpa[INC_GROUP != "H"]

unfpa_fracs <- unfpa_lmic[total > 0]
unfpa_fracs[, annual_toal := sum(total), by = year]
unfpa_fracs[, frac := total / annual_toal]
unfpa_fracs <- unfpa_fracs[, .(year, channel = "UNFPA", iso3, frac)]

# save ========================================================================
fwrite(
    unfpa_fracs,
    get_path("compiling", "int", "unfpa_recip_fracs.csv")
)

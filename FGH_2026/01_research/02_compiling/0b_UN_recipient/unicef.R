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
# Post-process UNICEF extracted recipient data ================================ 
#

unicef_raw <- data.table()
for (yr in c(
    2012:2023
)) {
    print(yr)
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNICEF", paste0("unicef_", yr, "_recipient.xlsx"))
    ))
    names(dt) <- tolower(names(dt))
    dt[, year := yr]
    unicef_raw <- rbind(unicef_raw, dt, fill = TRUE)
}
unicef_raw[, expenditure := clean_numeric(expenditure)]
setnames(unicef_raw, "expenditure", "total")

## load interregional, i.e. 'global' expenses
inter <- as.data.table(openxlsx::read.xlsx(
    file.path(IN_DIR, "UNICEF", "unicef_interregional_expenses.xlsx"),
    sheet = "interregional"
))
inter <- inter[, .(year,
                   region = "Global",
                   country = "Global",
                   total = clean_numeric(total) * 1e6)]
if (unicef_raw[! year %in% unique(inter$year), .N] > 0)
    stop("Interregional expenses are not extracted for all years")

unicef_raw <- rbind(unicef_raw, inter)



# Load biennium reports for pre-2012 years
for (yr in seq(1990, 2010, by = 2)) {
    print(yr)
    dt <- as.data.table(openxlsx::read.xlsx(
        file.path(IN_DIR, "UNICEF", paste0("unicef_", yr, "_", yr+1, "_recipient.xlsx")),
        sheet = "extraction"
    ))
    names(dt) <- tolower(names(dt))
    
    ## duplicate for sake of biennium
    dt[, total := clean_numeric(total) / 2] # divide by 2 since biennium total
    dt[, year := yr]
    dt2 <- copy(dt)
    dt2[, year := yr + 1]
    unicef_raw <- rbind(unicef_raw, dt, dt2)
}
setorder(unicef_raw, year)

## extracted values for these yrs are in 1000s, scale to match other years
unicef_raw[between(year, 2002, 2011), total := total * 1e3]

unicef_raw <- unicef_raw[!is.na(total)]
unicef_raw[region %ilike% "global", `:=`(
    region = "GLOBAL",
    country = "GLOBAL"
)]

unicef <- copy(unicef_raw)

#
# Split regional projects =====================================================
#
unicef[, is_region := country %ilike% "^region"]

ureg <- unicef[is_region == TRUE, -"country"]
reg_fracs <- unicef[is_region == FALSE, .(total = sum(total)),
                    by = .(country, region, year)]
reg_fracs[, reg_tot := sum(total), by = .(region, year)]
reg_fracs[, frac := total / reg_tot]

ureg <- merge(
    ureg,
    reg_fracs[, .(region, year, country, frac)],
    by = c("region", "year"),
    all.x = TRUE
)

stopifnot(
    ## ensure all fractions sum to 1 for all region years
    ureg[, sum(frac), by = .(region, year)][abs(V1 - 1) > 1e-6, .N] == 0
)
ureg[, total := total * frac]

unicef <- rbind(
    unicef[is_region == FALSE, -"is_region"],
    ureg[, .(year, region, country, total)]
)

unicef <- unicef[, .(total = sum(total, na.rm = TRUE)),
                 by = .(year, country)]


#
# Expand multi-country recipients =============================================
#
unicef[country %ilike% "yugoslav" & !country %ilike% "macedon",
       country := "Yugoslavia former"]
unicef[country %ilike% "pacific island countries",
       country := "Pacific Island Countries"]

umult <- unicef[country %in% c(
    "Pacific Island Countries",
    "Yugoslavia former"
)]
setnames(umult, "country", "region")
umult <- umult[, .(total = sum(total)), by = .(year, region)]

umult[, country := fcase(
    # https://www.unicef.org/pacificislands/about-us#:~:text=In%20the%20Pacific%2C%20we%20work,Tonga%2C%20Tuvalu%2C%20and%20Vanuatu.
    region == "Pacific Island Countries",
    "COK, FJI, KIR, MHL, FSM, NRU, NIU, PLW, WSM, SLB, TKL, TON, TUV, VUT",
    region == "Yugoslavia former",
    "BIH, HRV, MNE, SRB, SVN",
    default = NA
)]
umult <- umult[, .(country = tstrsplit(country, ", ", fixed = TRUE)), 
               by = .(region, year, total)]
umult[, country := trimws(country, which = "both")]

## calculate recipient fractions from observed DAH
adb <- fread(get_path("compiling", "fin", "DAH_ADB_PDB_1990_2024.csv"))
adb <- adb[ELIM_CH == 0 & ELIM_DONOR == 0]
adb <- adb[ISO3_RC != "QZA" & INKIND == 0 & CHANNEL != "UNICEF",
           .(dah = sum(DAH_24, na.rm = TRUE)),
           by = .(year = YEAR, iso3 = ISO3_RC)]
adb[, region := fcase(
    iso3 %in% c("COK", "FJI", "KIR", "MHL", "FSM", "NRU",
                "NIU", "PLW", "WSM", "SLB", "TKL", "TON", "TUV", "VUT"),
    "Pacific Island Countries",
    iso3 %in% c("BIH", "HRV", "MNE", "SRB", "SVN"),
    "Yugoslavia former",
    default = "Other"
)]
adb[, reg_tot := sum(dah), by = .(year, region)]
adb[, frac := dah / reg_tot]
adb[reg_tot == 0, frac := 1/.N, by = .(year, region)]

umult <- merge(
    umult, adb[, .(year, country = iso3, frac)],
    by = c("year", "country"),
    all.x = TRUE
)
setnafill(umult, fill = 0, cols = "frac")
stopifnot(
    umult[, sum(frac), by = .(year, region)][abs(V1 - 1) > 1e-1, .N] == 0
)
umult[, frac := frac / sum(frac), by = .(year, region)]
umult[, total := total * frac]


unicef <- rbind(
    unicef[!country %in% unique(umult$region)],
    umult[, .(year, country, total)]
)



#
# Standardize country names ===================================================
#

locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs[, country_clean := string_to_std_ascii(country_lc, pad_char = "")]
locs <- unique(locs[!is.na(iso3) & iso3 != "", .(country_clean, iso3)])


unicef[, country := gsub("a/", "", country, fixed = TRUE)]
unicef[, country := gsub("aa$", "a", country)]


unicef[, country_clean := string_to_std_ascii(country, pad_char = "")]
## remove any numeric characters (footnotes)
unicef[, country_clean := gsub("[0-9]", "", country_clean)]
unicef <- merge(
    unicef,
    locs[, .(country_clean, iso3)],
    by = "country_clean",
    all.x = TRUE
)
unicef[country %in% umult$country, iso3 := country]

unicef[, iso3 := fcase(
    country_clean %ilike% "COTE", "CIV",
    country_clean %ilike% "Dem.+Republic.+Congo", "COD",
    country_clean %ilike% "D.+ Republic of Korea", "KOR",
    country_clean %ilike% "eswatini", "SWZ",
    country_clean %ilike% "fiji", "FJI",
    country_clean %ilike% "Iran", "IRN",
    country_clean %ilike% "Kosovo", "XKX",
    country_clean %ilike% "Lao", "LAO",
    country_clean %ilike% "LEBANON", "LBN",
    country == "Madagucar", "MDG",
    country_clean %ilike% "Macedonia", "MKD",
    country_clean %ilike% "palestin", "PSE",
    country == "Renin", "BEN",
    country_clean %ilike% "Uzbekistan", "UZB",
    country_clean == "UNION OF SOVIET SOCIALIST REPUBLICS FORMER", "USSR_FRMR",
    country_clean %ilike% "Kyrgyz", "KGZ",
    country_clean %ilike% "montenegro", "MNE",
    country == "Syriau Arab Republic", "SYR",
    country_clean == "TURKIYE", "TUR",
    country_clean == "THE FORMER YUGOSLAV REPUBLIC OF MACEDONIE", "MKD",
    country_clean == "ZAIRE", "COD", # Zaire is now the DRC
    country == "GLOBAL", "WLD",
    rep_len(TRUE, .N), iso3
)]

stopifnot( unicef[is.na(iso3), .N] == 0 )

unicef <- unicef[, .(total = sum(total)),
                 by = .(year, iso3)]


stopifnot( unicef[, sum(total)] == unicef_raw[, sum(total)] )


## drop High-income
ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
unicef <- merge(unicef,
                ig[, .(year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
                by = c("year", "iso3"),
                all.x = TRUE)
unicef[is.na(INC_GROUP), INC_GROUP := ""]
unicef_lmic <- unicef[INC_GROUP != "H"]

unicef_fracs <- unicef_lmic[total > 0]
unicef_fracs[, annual_toal := sum(total), by = year]
unicef_fracs[, frac := total / annual_toal]
unicef_fracs <- unicef_fracs[, .(year, channel = "UNICEF", iso3, frac)]


# Save the cleaned data =======================================================
fwrite(
    unicef_fracs,
    get_path("compiling", "int", "unicef_recip_fracs.csv")
)

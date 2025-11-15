library(data.table)
library(openxlsx)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "/FUNCTIONS/currency_conversion.R"))


#
# Exchange rates
#
xrates <- fread(get_path("meta", "rates", "OECD_XRATES_NattoUSD_1950_[report_year].csv"),
                select = c("LOCATION", "TIME", "Value"))
setnames(xrates, c('LOCATION', 'TIME'), c('ISO3', 'YEAR'))
xrates <- dcast.data.table(xrates, formula = 'ISO3 ~ YEAR', value.var = 'Value')
# - update if new DAC members using the euro are added
eurozone <- c('AUT', 'BEL', 'DEU', 'EST', 'ESP', 'FIN', 'FRA', 'GRC', 'IRL',
              'ITA', 'LTU', 'LUX', 'NLD', 'PRT', 'SVK', 'SVN') 
xrates[ISO3 == 'EA19', ISO3 := 'EUR']
xrates[, maxn := nrow(xrates)]
# Duplicate the EUR obs `length(eurozone)` more times
xrates <- rbind(xrates,
                do.call("rbind",
                        replicate(length(eurozone), xrates[ISO3=='EUR', ], simplify = FALSE)))
xrates[, n2 := .I]

# break EU into the OECD countries from Europe
t <- 1
for (c in eurozone) {
    xrates[ISO3=="EUR" & n2 == maxn + t, ISO3 := c]
    t <- t + 1
}
xrates[, `:=`(maxn = NULL, n2 = NULL)]
xrates <- melt(
    xrates,
    id.vars = "ISO3",
    variable.name = "YEAR",
    variable.factor = FALSE,
    value.name = "exchange_rate"
)
xrates[, YEAR := as.integer(YEAR)]



#
# Deflator
#
defl <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
defl <- defl[, .(YEAR, defl = GDP_deflator_2023)]



bil_donors <- names(dah_cfg$crs$donors)
bil_donors <- bil_donors[bil_donors != "EC"]


out <- data.table()
for (loc in bil_donors) {
    # Read in country data
    message(loc)
    fp <- get_path('BILAT_PREDICTIONS', 'raw', paste0(loc, "_BUDGET_2025.xlsx"),
                   report_year = 2025)
    if (!file.exists(fp)) {
        print(paste("File not found for", loc))
        next
    }
    
    sheet <- "use"
    if ("channels" %in% getSheetNames(fp)) {
        sheet <- "channels"
    } else if ("prelim" %in% getSheetNames(fp)) {
        sheet <- "prelim"
    }
    t <- openxlsx::read.xlsx(fp, sheet = sheet)
    setDT(t)
    print(sheet)
    
    t <- t[!is.na(CURRENCY)]
    
    ## if only total DAH or total ODA is extracted, we assume it applies to bilateral
    if ("DAH" %in% names(t)) {
        setnames(t, "DAH", "BIL_DAH")
    }
    if ("ODA" %in% names(t)) {
        setnames(t, "ODA", "BIL_ODA")
    }
    
    val_cols <- grep("DAH|ODA", names(t), value = TRUE)
    t[, paste0(val_cols, "_orig") := lapply(.SD, identity), .SDcols = val_cols]
    
    # Convert currency
    curr <- toupper(unique(t$CURRENCY))
    if (length(curr) != 1 || is.na(curr)) {
        stop("Currency column is missing or invalid for ", loc, "")
    }
    t <- merge(
        t, xrates[ISO3 == loc, .(YEAR, exchange_rate)],
        by = "YEAR", all.x = TRUE
    )
    if (curr == "USA")
        t[, exchange_rate := 1]
    ## if no exchange rate for 2025+, use last available
    setorder(t, YEAR)
    t[, exchange_rate := zoo::na.locf(exchange_rate)]
    stopifnot(t[is.na(exchange_rate), .N] == 0)
    
    t[, (val_cols) := lapply(.SD, function(x) x / exchange_rate), .SDcols = val_cols]
    
    
    # Deflate wrt 2023
    t <- merge(t, defl, by = "YEAR", all.x = TRUE)
    t[, defl := zoo::na.locf(defl)]
    stopifnot(t[is.na(defl), .N] == 0)
    t[, (val_cols) := lapply(.SD, function(x) x / defl), .SDcols = val_cols]
    
    curr <- toupper(unique(t$CURRENCY))
    if (length(curr) != 1 || is.na(curr)) {
        stop("Currency column is missing or invalid for ", loc, "")
    }
    
    t <- t[, c("YEAR", val_cols), with = FALSE]
    setnames(t, "YEAR", "year")
    t[, source := loc]
    
    out <- rbind(out, t, fill = TRUE)
}


setorder(out, year, source)
out <- melt(
    out,
    id.vars = c("year", "source"),
    variable.name = "channel",
    value.name = "value"
)
out[, c("channel", "type") := tstrsplit(channel, "_", fixed = TRUE)]
stopifnot(
    out[! type %in% c("DAH", "ODA"), .N] == 0
)
out <- dcast(
    out,
    year + source + channel ~ type,
    value.var = "value"
)
names(out) <- tolower(names(out))

## ensure that the data is square before calculating pct changes
fin <- unique(out[, .(source, channel)])[
    ,
    .(
        year = seq(min(out$year), max(out$year))
    ),
    by = .(source, channel)
]
fin <- merge(
    fin,
    out,
    by = c("year", "source", "channel"),
    all.x = TRUE
)

fin[order(year), pct_chg_dah := dah / shift(dah) - 1, by = .(source, channel)]
fin[order(year), pct_chg_oda := oda / shift(oda) - 1, by = .(source, channel)]
## if DAH info was available, we prefer to use the DAH pct change, however we
##    default to overall ODA pct change if DAH is missing
fin[, pct_chg := fifelse(
    is.na(pct_chg_dah),
    pct_chg_oda,
    pct_chg_dah
)]

save_dataset(
    fin,
    "budgets_estimated_changes.csv",
    channel = "compiling",
    stage = "int"
)


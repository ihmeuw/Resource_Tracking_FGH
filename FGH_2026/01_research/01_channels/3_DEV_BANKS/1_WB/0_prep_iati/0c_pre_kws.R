#
# Filter and process the WB IATI data so that it approximates the cut of data
# we typically receive through correspondence.
#


code_repo <- 'FILEPATH'



report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

wb_health_sectors <- c(
    "Health",
    "Public Administration - Health",
    "Health Facilities and Construction"
)



#
# correspondent data, last received version (load for comparison with IATI)
#
## this contains disbusements by sector, for the health sector, through 2021
corr_raw <- openxlsx::read.xlsx(
    get_path("wb", "raw", "IHME_Data_Request_2022_01_19.xlsx",
             report_year = 2023),
    sep.names = " "
)
setDT(corr_raw)
corr_raw[, `2022` := NULL]

## for comparisons with IATI, clean corr data
corr <- copy(corr_raw)
names(corr) <- gsub(" |\\,", "_", tolower(names(corr)))
names(corr) <- gsub("__", "_", names(corr), fixed = TRUE)

corr[, id := .I]
corr <- melt(corr,
     id.vars = c("id", "region", "country", "proj_id", "proj_name",
                 "date_approval", "date_rev_closng", "proj_stat",
                 "lending_instr", "agreement_type", "overall_result"),
     variable.name = "year",
     variable.factor = FALSE,
     value.name = "disbursement")
corr[, year := as.integer(year)]
corr <- corr[!is.na(disbursement) & year < 2022] # 2021 is last complete yr

corr[, date_approval := as.Date(date_approval, format = "%m/%d/%Y")]
corr[, start_year := year(date_approval)]


#
# load iati data
#
iati <- rbind(
    fread(get_path("wb", "int", "worldbank_iati_clean.csv")),
    fread(get_path("wb", "int", "wbtf_iati_clean.csv"))
)

stopifnot(all(iati[!is.na(trans_value), unique(trans_currency)] == "USD"))
iati <- iati[trans_type == "Disbursement"]
setorder(iati, trans_year)


iati[, proj_id := gsub("44000-", "", iati_identifier)]
iati[, proj_id := gsub("XI-IATI-WBTF-", "", proj_id)]


iati[, agreement_type := fcase(
    provider %in% c("IDA Credit",
                    "International Development Association",
                    "International Development Association - Heavily Indebted Poor Countries Debt Initiative Trust Fund",
                    "International Development Association - Multilateral Debt Relief Initiative"),
    "IDA",
    provider %in% c("International Bank for Reconstruction and Development"),
    "IBRD",
    provider %in% c("Trust Funds", "World Bank Trust Funds"),
    "TF",
    default = NA_character_
)]

## Drop World Bank Trust Funds because the only disbursements reported are for
## the most recent year.
iati <- iati[pub_code != "wbtf"]


#
# extract health sector disbursements
#

# - first, split sector
sector <- unique(iati[, .(iati_identifier,
                          sector, sector_code, sector_narr, sector_vocab, sector_pct)])
sector <- sector[, .(sector = unlist(strsplit(sector, "<SEP>")),
                     sector_code = unlist(strsplit(sector_code, "&")),
                     sector_narr = unlist(strsplit(sector_narr, "<SEP>")),
                     sector_vocab = unlist(strsplit(sector_vocab, "<SEP>")),
                     sector_pct = unlist(strsplit(sector_pct, "&"))
                     ),
                 by = iati_identifier]

# use WB's own sectors to identify health sector
sector[, n_sector := sum(sector_vocab == "Reporting Organisation"),
       by = iati_identifier]
## ensure every project has a WB sector reported
stopifnot(sector[n_sector == 0, .N] == 0)
sector <- sector[sector_vocab == "Reporting Organisation"]

sector[, sector_pct := as.numeric(sector_pct)/100]
sector[, tot_pct := sum(sector_pct), by = iati_identifier]
# the sum of the percentages should be 100, but it isn't always. this is a
# confirmed problem in the reported data, not a problem in the scraping or
# post-processing.
sector[, sector_pct := sector_pct / tot_pct]


health <- sector[sector_narr %in% wb_health_sectors]
## sometimes the total percentage is 0, which is a reporting error. So assume
## even split across all reported sectors in this case.
health[tot_pct == 0, sector_pct := 1/n_sector]
health[tot_pct == 0, tot_pct := 1]

## aggregate health sector percentages
health <- health[, .(health_frac = sum(sector_pct)),
                 by = .(iati_identifier)]


# Filter IATI to health sector projects, and merge on health fraction
iati_health <- iati[iati_identifier %in% unique(health[, iati_identifier])]
iati_health <- merge(
    iati_health, health[, .(iati_identifier, health_frac)],
    by = "iati_identifier", all.x = TRUE
)

stopifnot( iati_health[is.na(health_frac), .N] == 0 )
iati_health[, trans_value := trans_value * health_frac]



if (interactive()) {

    cmp <- merge(
        corr[, .(corr = sum(disbursement, na.rm = TRUE)/1e9), by = .(year)],
        iati_health[,
                    .(iati = sum(trans_value, na.rm = TRUE)/1e9),
                    by = .(year = trans_year)],
        by = "year",
        all = TRUE
    )
    
    setNames(cmp[year >= 2010,], c("Year", "Correspondent", "IATI")) |>
        kableExtra::kbl(caption = "Billions of USD") |>
        kableExtra::kable_styling("striped", full_width = FALSE)
    
    
    ggplot(cmp[year >= 2010], aes(x = year)) +
        geom_line(aes(y = corr, color = "Correspondent")) +
        geom_line(aes(y = iati, color = "IATI")) +
        scale_x_continuous(breaks = seq(2010, 2024, 2)) +
        labs(title = "World Bank health sector disbursements",
             y = "Billions of USD",
             x = "Year",
             color = "") +
        coord_cartesian(ylim = c(0, NA)) +
        theme_minimal(16) +
        theme(legend.position = "top") +
        scale_color_manual(values = c("Correspondent" = "blue", "IATI" = "red"))
}



#
# Add recip iso codes and drop high-income
#
iati_health[recip_iso2 == "498",
            recip := "America, regional"]

iso2to3 <- fread(get_path("meta", "locs", "country_iso2to3.csv"))

iati_health <- merge(iati_health, iso2to3,
                     by.x = "recip_iso2", by.y = "iso2",
                     all.x = TRUE)

ig <- fread(get_path("meta", "locs", "wb_historical_incgrps.csv"))
iati_health <- merge(
    iati_health,
    ig[, .(trans_year = YEAR, iso3 = ISO3_RC, INC_GROUP)],
    by = c("trans_year", "iso3"),
    all.x = TRUE
)

iati_health <- iati_health[INC_GROUP != "H" | is.na(INC_GROUP)]



#
# Aggregate disbursements by project-year
#
iati_health[, status := fcase(
    activity_status == "Finalisation", "Closed",
    activity_status == "Implementation", "Active",
    default = NA_character_
)]

agg <- iati_health[, .(
    DAH = sum(trans_value, na.rm = TRUE)
), by = .(
    YEAR = trans_year,
    iati_identifier,
    proj_id,
    iso3,
    recip,
    agreement_type,
    finance_type,
    status,
    title_narr,
    description_long_narr
)]

agg[, `:=`(
    proj_name = string_to_std_ascii(title_narr, pad_char = ""),
    proj_desc = string_to_std_ascii(description_long_narr, pad_char = "")
)]



# save for keyword searching
save_dataset(agg, "iati_pre_kws", "WB", "int", format = "dta")


# launch keyword search
create_Health_config(data_path = get_path('WB', 'int', "iati_"), ## stata will look for "`path'_pre_kws.dta"
                     channel_name = "WB_iati",
                     varlist = c("proj_name", "proj_desc"),
                     language = "english",
                     function_to_run = 1)

launch_Health_ADO(channel_name = 'WB_iati')


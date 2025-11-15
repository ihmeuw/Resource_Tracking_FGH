
code_repo <-'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "/FUNCTIONS/currency_conversion.R"))




# New method for GAVI from various sources: want to use 2026-2030 replenishment budget data
# All of the pledges below are for 2026-2030, so take each amount, divide by 5. 
# Note that the currency is often not US dollars. You can assume all are in current currency.
# https://www.gavi.org/investing-gavi/resource-mobilisation-process/protecting-our-future

#x Gates Foundation $1.6b
#x Portugal Euro 2.5m
#x Canada CAD 675m
#x Australia AUD 386m
#x Norway $800m
#x South Korea $50m
#x Italy Euro 250m
#x Spain Euro 130m
#x UK Pounds 1.25b
#x Greece Euro 5m
#x Germany Euro 600m
#x France Euro 500m
#x Denmark Euro 17m
#x Luxembourg Euro 5m
#x US $0
#x Ireland Euro 21.6m (https://www.gov.ie/en/department-of-the-taoiseach/press-releases/taoiseach-miche%c3%a1l-martin-pledges-216-million-in-irish-funding-at-global-vaccine-summit/)
# China $20m
# Netherlands $139m
# Iceland Croner 300m. We think the speaker meant 300m, not 300b.

# EC $360m - OTHER
# Indonesia $30m - OTHERPUB
# Croatia Euro 1m - OTHERPUB
# Singapore $1m - OTHERPUB
# Uganda $1m - OTHERPUB
# Rwanda $1m - OTHERPUB
# Monaco Euro 1.2m - OTHERPUB
# India $20m - OTHERPUB
# WB $2B - OTHER
# Total raised amount is $9b… so the total amount of Gavi each year should be $1.8b per year.

# Other main donors we estimate without values: AUT, BEL, CHE, CHN, FIN, JPN, NLD, NZL, SWE. 
dah_ret <- fread(get_path("compiling", "int", "retro_fgh_data.csv")) 
dah_sc <- dah_ret[year >= 2020 & channel == "GAVI",
                  .(dah_channel = sum(dah)),
                  by = .(year, source, channel)]
gr <- unique(dah_sc[, .(source, channel)])
dah_sc <- rbind(
    dah_sc,
    gr[,
        .(year = 2026:2030, dah_channel = NA_real_),
        by = .(source, channel)
    ]
)

dah_sc[channel == "GAVI" & year %in% c(2026:2030), GAVI_channels := dah_channel]

# values in USD
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GATES", GAVI_channels := (1.6e9/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "NOR", GAVI_channels := (800e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "KOR", GAVI_channels := (50e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "JPN", GAVI_channels := (550e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "SWE", GAVI_channels := (223e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "CHN", GAVI_channels := (20e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "NLD", GAVI_channels := (139e6/5)]
## EC - assign to OTHER since we consider this a channel
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "OTHER", GAVI_channels := (360e6/5)]

# values in other currencies that need to be converted
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "PRT", GAVI_channels := (2.5e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "PRT", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "PRT"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "CAN", GAVI_channels := (675e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "CAN", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "CAN"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "AUS", GAVI_channels := (386e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "AUS", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "AUS"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ITA", GAVI_channels := (250e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ITA", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ITA"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ESP", GAVI_channels := (130e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ESP", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "ESP"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GBR", GAVI_channels := (1.25e9/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GBR", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GBR"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GRC", GAVI_channels := (5e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GRC", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "GRC"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DEU", GAVI_channels := (600e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DEU", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DEU"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "FRA", GAVI_channels := (500e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "FRA", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "FRA"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

# note that DNK value is reported in Euros but has a different local currency unit;
# need to make a fake iso3 column that is one of the Eurozone countries isocode to convert Euro amount
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DNK", GAVI_channels := (17e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DNK", euro_source := "DEU"]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DNK", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "DNK"],
               col.loc = 'euro_source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]
dah_sc[, euro_source := NULL]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "LUX", GAVI_channels := (5e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "LUX", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "LUX"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "IRL", GAVI_channels := (21.6e6/5)]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "IRL", GAVI_channels := 
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "IRL"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% 2026:2030 & source == "ISL", GAVI_channels := (300e6/5)]
dah_sc[channel == "GAVI" & year %in% 2026:2030 & source == "ISL", GAVI_channels :=
           currency_conversion(
               data = dah_sc[channel == "GAVI" & year %in% 2026:2030 & source == "ISL"],
               col.loc = 'source',
               col.value = 'GAVI_channels',
               currency = 'lcu',
               currency.year = 2025,
               base.year = 2025,
               base.unit = 'usd',
           )$GAVI_channels
]




# OTHERPUB group of donors: HRV + MCO + IDN + SGP + UGA + RWA + IND
## (these are not disaggregated as sources in our data)
HRV_dah_sc_annual <- data.table(source = "HRV", GAVI_channels=1e6/5)
HRV_dah_sc_annual[, GAVI_channels := 
                      currency_conversion(
                          data = HRV_dah_sc_annual,
                          col.loc = 'source',
                          col.value = 'GAVI_channels',
                          currency = 'lcu',
                          currency.year = 2025,
                          base.year = 2025,
                          base.unit = 'usd',
                      )$GAVI_channels
]

MCO_dah_sc_annual <- data.table(source = "MCO", GAVI_channels=1.2e6/5)
MCO_dah_sc_annual[, GAVI_channels := 
                      currency_conversion(
                          data = MCO_dah_sc_annual,
                          col.loc = 'source',
                          col.value = 'GAVI_channels',
                          currency = 'lcu',
                          currency.year = 2025,
                          base.year = 2025,
                          base.unit = 'usd',
                      )$GAVI_channels
]

dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "OTHERPUB",
       GAVI_channels := ((30+1+1+1+20)*1e6/5) +
           HRV_dah_sc_annual$GAVI_channels +
           MCO_dah_sc_annual$GAVI_channels]


# OTHER - WB
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & source == "OTHER",
       GAVI_channels := (2e9/5)]


# All other sources with no contributions should be zeroed out
dah_sc[channel == "GAVI" & year %in% c(2026:2030) &
           source %in% c('AUT', 'BEL', 'CHE', 'CZE', 'EST', 'FIN', 'HUN', 'LTU',
                         'NZL', 'POL', 'SVK', 'SVN', 'USA', 'DEBT'),
       GAVI_channels := 0]
# But PRIVATE should use the average fraction method (ie, leave existing estimates as-is)
unknown_sources_to_gavi <- c("DEBT", "PRIVATE")

# CC all values in 2025 USD to 2023 USD
dah_sc[, new_source := source]
dah_sc[source %in% c("GATES", "OTHER", "OTHERDAC", "OTHERPUB", "PRIVATE"), new_source := "USA"]
dah_sc <-  currency_conversion(
    data = dah_sc,
    col.loc = 'new_source',
    col.value = 'GAVI_channels',
    currency = 'usd',
    currency.year = 2025,
    base.year = 2023,
    base.unit = 'usd',
    simplify = F
)
stopifnot(dah_sc[!is.na(GAVI_channels) & is.na(GAVI_channels_new)] == 0)
dah_sc[, c("new_source", "currency", "currency_year", "currency_year_new", "currency_new", "deflator", "lcu_usd") := NULL]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & !(source %in% unknown_sources_to_gavi), 
       GAVI_channels_final := GAVI_channels_new]
dah_sc[channel == "GAVI" & year %in% c(2026:2030) & (source %in% unknown_sources_to_gavi), 
       GAVI_channels_final := GAVI_channels]
dah_sc[, c("GAVI_channels_new", "GAVI_channels") := NULL]
setnames(dah_sc, "GAVI_channels_final", "GAVI_channels")



fin <- dah_sc[
    year >= 2026,
    .(year, source, channel, flow_to_gavi = GAVI_channels, currency = "2023 USD")
]
fwrite(
    fin,
    get_path("compiling", "int", "gavi_2026_2030_replenishment.csv")
)


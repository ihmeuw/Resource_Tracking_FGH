#
# Pipeline to use extracted data to estimate Gavi's COVAX contributions.
#
# Produced for FGH 
#
library(data.table)
library(openxlsx)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

data_dir <- file.path(dirname(get_path("gavi", "raw")), "COVAX")

extract_file <- file.path(data_dir, "extractions.xlsx")


#
# Auxiliary Data
#

## load name to country mapping, used below
locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs <- unique(locs[, .(country_lc, iso3)])

## donor name database
dons <- fread(get_path("meta", "donor", "income_sector_and_type_assignments.csv"))
dons <- unique(dons[, .(DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE)])
dons[, DONOR_NAME := string_to_std_ascii(DONOR_NAME, pad_char = "")]
dons[ISO_CODE == "", ISO_CODE := NA_character_]
## remove duplicate entries
dons[, n := 1:.N, by = DONOR_NAME]
dons <- dons[n == 1, -"n"]



#
# cash_disbursed: Prep Cash Disbursed for COVAX AMC ===========================
#
## This gives us the total cash disbursement envelope
##
cash_disbursed <- as.data.table(read.xlsx(extract_file, sheet = "cash_disbursed"))
cash_disbursed[, value := value * scalar]


#
# doses_donated: Prep Doses Donated for COVAX AMC =============================
#
## This gives us the total doses disbursed envelope (we assume due to data limits
## that all doses make it to recipients), and also donor shares for the dose
## envelope.
doses_donated <- as.data.table(read.xlsx(extract_file, sheet = "doses_donated"))
doses_donated[, value := value * scalar]

# clean donor country name
doses_donated[, country := gsub(",", "", donor)]

sub_str <- c(
    "Kingdom of", "Federal Republic of", "Republic of", "Republicof", "Republic Of",
    "Commonwealth of", "Grand Duchy of", "Principality of", "State of the"
)
for (str in sub_str)
    doses_donated[, country := trimws(gsub(str, "", country))]

doses_donated <- merge(
    doses_donated, locs,
    by.x = "country",
    by.y = "country_lc",
    all.x = TRUE
)
doses_donated[, iso3 := fcase(
    country == "Hellenic Republic (Greece)", "GRC",
    country %like% "Hong Kong", "HKG",
    rep_len(TRUE, .N), iso3
)]
stopifnot( doses_donated[is.na(iso3), .N] == 0 )

doses_donated <- doses_donated[, .(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "CENTRAL",
    DONOR_NAME = toupper(country),
    ISO_CODE = iso3,
    year,
    value
)]
doses_donated[DONOR_NAME == "UNITED STATES OF AMERICA",
              DONOR_NAME := "UNITED STATES"]


#
# cash_received: Prep Cash Contributions from Donors ==========================
#
cash_received <- as.data.table(read.xlsx(extract_file, sheet = "cash_received"))
yr_cols <- grep("yr", names(cash_received), value = TRUE)
cash_received[, (yr_cols) := lapply(.SD, \(x) x * scalar), .SDcols = yr_cols]


# standardize donors
#
cash_received[, donor_orig := donor]
## remove any numbers since they come from footnotes
cash_received[, donor := trimws(gsub("[0-9]+", "", donor))]
## remove any quotes
cash_received[, donor := trimws(gsub("\"", "", donor))]

## merge on iso codes to identify country donors
cash_received <- merge(
    cash_received, locs,
    by.x = "donor",
    by.y = "country_lc",
    all.x = TRUE
)
cash_received[, iso3 := fcase(
    donor == "Scotland", "GBR",
    rep_len(TRUE, .N), iso3
)]

## classify other donors
cash_received[, donor_clean := string_to_std_ascii(donor, pad_char = "")]

cash_received <- merge(
    cash_received, dons,
    by.x = "donor_clean",
    by.y = "DONOR_NAME",
    all.x = TRUE
)
cash_received[!is.na(iso3), `:=`(
    INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "CENTRAL", ISO_CODE = iso3
)]
cash_received[, iso3 := NULL]

cash_received[donor == "Stadt Zug",
 `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "LOCAL",
    ISO_CODE = "CHE"
)]

# corporate donors
cash_received[donor %in% c(
    "Al Ansari Exchange",
    "Arm Limited",
    "Collins Aerospace (Goodrich Corporation)",
    "Dolby Laboratories Charitable Fund",
    "Shell International B.V."
), `:=`(
    INCOME_SECTOR = "INK",
    INCOME_TYPE = "CORP"
)]

# charitable foundations
cash_received[donor %in% c(
    "Croda Foundation",
    "ELMA Vaccines and Immunization Foundation",
    "McHugh O'Donovan Foundation",
    "Symasia Foundation"
), `:=`(
    INCOME_SECTOR = "PRIVATE",
    INCOME_TYPE = "FOUND"
)]

# individuals
cash_received[donor %in% c(
    "His Highness Sheikh Mohamed bin Zayed Al Nahyan"
), `:=`(
    INCOME_SECTOR = "PRIVATE",
    INCOME_TYPE = "INDIV"
)]

# ngos
cash_received[donor %in% c(
    "KS Relief", "Power of Nutrition"
), `:=`(
    INCOME_SECTOR = "PRIVATE",
    INCOME_TYPE = "NGO"
)]

# other - religious, international orgs
cash_received[donor %in% c(
    "IFFIm Proceeds",
    "International Federation of Pharmaceutical Wholesalers (IFPW)",
    "Kerk in Actie",
    "Sovereign Order of Malta",
    "The Church of Jesus Christ of Latter-day Saints"
), `:=`(
    INCOME_SECTOR = "OTHER",
    INCOME_TYPE = "OTHER"
)]

stopifnot( cash_received[is.na(INCOME_SECTOR), .N] == 0 )

cash_received <- melt(cash_received,
                      id.vars = c("donor_clean",
                                  "INCOME_SECTOR", "INCOME_TYPE", "ISO_CODE",
                                  "scalar"),
                      measure.vars = yr_cols,
                      variable.name = "year")
cash_received[, year := as.integer(gsub("yr", "", year))]
cash_received <- cash_received[, .(
    value = sum(value, na.rm = TRUE)
), by = .(
    DONOR_NAME = donor_clean,
    INCOME_SECTOR, INCOME_TYPE, ISO_CODE,
    year
)]


#
# recip_expense: Prep recipient country data #=================================
#
recip_expense <- data.table()
for (yr in 2020:2023) {
    tmp <- read.xlsx(extract_file, sheet = paste0(yr, "_country_expenses"))
    tmp$year <- yr
    recip_expense <- rbind(
        recip_expense, tmp, fill = TRUE
    )
}

# clean recipient country
recip_expense[, country_orig := country]
recip_expense[, country := tolower(gsub(",", "", country))]
recip_expense[, country := gsub("republicof", "republic of", country)]
recip_expense[, country := gsub("people 's", "peopl's", country)]
sub_str <- c(
    "islamic republic of", "people's democratic republic of", "people's republic of",
    "democratic people's republic of", "federated states of",
    "federal democratic republic of", "co-operative republic of",
    "united republic of", "socialist republic of", "bolivarian republic of",
    "democratic republic of", "plurinational state of", "arab republic of",
    "independent state of",
    "kingdom of", "federal republic of", "republic of",
    "commonwealth of", "grand duchy of", "principality of", "state of the",
    "state of", "union of", "democratic"
)
for (str in sub_str)
    recip_expense[, country := trimws(gsub(str, "", country))]
recip_expense[, country := trimws(gsub("the$", "", country))]

# merge on iso codes
recip_expense <- merge(
    recip_expense, unique(locs[, .(country_lc = tolower(country_lc), iso3)]),
    by.x = "country",
    by.y = "country_lc",
    all.x = TRUE
)
recip_expense[, iso3 := fcase(
    country == "eswatini", "SWZ",
    country == "kyrgyz", "KGZ",
    country %like% "lao peopl", "LAO",
    country %like% "libya", "LBY",
    country == "palestine", "PSE",
    country == "solomon island", "SLB",
    rep_len(TRUE, .N), iso3
)]


stopifnot( recip_expense[is.na(iso3), .N] == 0 )

# calculate recipient country quantities
recip_expense[, vaccine_expense := facility_vaccine_support]
recip_expense[, cash_expense := facility_cash_grants]
## cash for COVAX was received in 2020, but no data from annual reports on how
## much was disbursed to country programmes, so we assume the cash disbursed in
## 2020 was for "global public goods", like R&D, etc.
recip_expense <- recip_expense[year > 2020]

recip_expense <- recip_expense[, .(
    vaccine_expense = sum(vaccine_expense * scalar, na.rm = TRUE),
    cash_expense = sum(cash_expense * scalar, na.rm = TRUE)
), by = .(iso3_rc = iso3, year)]


#
# Main: Estimate COVAX Flows =================================================
#

#
# Cash disbursements:
#
## Disaggregate total cash disbursements by donor - use fractions based on
## cumulative cash received over COVAX period
cash_received[order(year),
              cum_value := cumsum(value),
              by = .(DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE)]

cash_received_totals <- cash_received[, .(value = sum(value)), by = year]
cash_received_totals[order(year), yr_cum_value := cumsum(value)]


cash <- merge(
    cash_received,
    cash_received_totals[, .(year, yr_cum_value)],
    by = "year", all.x = TRUE
)
cash[, donor_share := cum_value / yr_cum_value]

## Calculate cash disbursement fractions by recipient country
cash_recips <- recip_expense[, .(
    cash_expense = sum(cash_expense, na.rm = TRUE)
), by = .(iso3_rc, year)]
cash_recips[, annual_total := sum(cash_expense), by = year]
cash_recips[, recip_share := cash_expense / annual_total]

## Combine donor shares with total cash disbsursed
cash <- merge(cash,
              cash_disbursed[, .(year, cash_disbursed = value)],
              by = "year", all.x = TRUE)
cash[, cash_disbursed := cash_disbursed * donor_share]

## Merge on recipient shares by year and distribute cash across recipients
cash <- merge(cash,
              cash_recips[, .(iso3_rc, year, recip_share)],
              by = "year",
              all.x = TRUE,
              # allow.cartesian because there are many recipients per donor-year
              allow.cartesian = TRUE)
cash[year == 2020, `:=`(
    iso3_rc = "WLD",
    recip_share = 1
)]
cash[, cash_disbursed := cash_disbursed * recip_share]


#
# Vaccine Disbursements:
#
## Calculate total vaccine disbursements by donor - assume disbursements are
## equivalent to doses donated to Gavi
doses <- doses_donated[, .(
    vaccine_disbursed = sum(value, na.rm = TRUE)
), by = .(DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, year)]

## calculate vaccine disbursement fractions by recipient country
vaccine_recips <- recip_expense[, .(
    vaccine_expense = sum(vaccine_expense, na.rm = TRUE)
), by = .(iso3_rc, year)]
vaccine_recips[, annual_total := sum(vaccine_expense), by = year]
vaccine_recips[, recip_share := vaccine_expense / annual_total]

## Combine disbursements with recip shares
doses <- merge(
    doses,
    vaccine_recips,
    by = "year",
    all.x = TRUE,
    # allow.cartesian because there are many recipients per donor-year
    allow.cartesian = TRUE
)
doses[, vaccine_disbursed := vaccine_disbursed * recip_share]





#
# Combine all disbursements
#
covax <- rbind(
    cash[, .(year,
             DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE,
             iso3_rc,
             value = cash_disbursed,
             type = "cash")],
    doses[, .(year,
              DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE,
              iso3_rc,
              value = vaccine_disbursed,
              type = "vax")]
)
setnafill(covax, fill = 0, cols = "value")
covax <- covax[value != 0]



#
# Save
#
save_dataset(
    covax,
    "gavi_covax_disbursements",
    channel = "gavi",
    stage = "fin"
)


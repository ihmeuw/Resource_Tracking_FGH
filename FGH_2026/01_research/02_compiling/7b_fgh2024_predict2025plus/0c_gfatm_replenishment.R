#
# Since current replenishment round ends in 2025, we can estimate how much will
# be paid in 2025 based on pledges and contributions to date.
#
library(data.table)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))



gfr <- fread(get_path(
    "gfatm", "raw", "pledges_contributions_reference_rate_dataset_2025418.csv"
))
gfr <- gfr[DonorType1 == "Public Sector" | DonorName == "Gates Foundation"]
## convert to 2023 USD
defl <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
gfr <- merge(
    gfr,
    defl[, .(Year = YEAR, defl = GDP_deflator_2023)],
    by = "Year",
    all.x = TRUE
)
stopifnot(gfr[is.na(defl), .N] == 0)
gfr[, Amount_ReferenceRate := Amount_ReferenceRate / defl]

gfr_pledges <- gfr[ReplenishmentPeriod == "2023-2025" & IndicatorName %like% "Pledge",
                   .(pledge = sum(Amount_ReferenceRate)),
                   by = .(DonorName)]
gfr_contrib <- gfr[ReplenishmentPeriod == "2023-2025" & IndicatorName %like% "Contribution" &
                       Year < 2025,
                     .(contrib = sum(Amount_ReferenceRate)),
                     by = .(DonorName)]
gfr_pc <- merge(
    gfr_pledges,
    gfr_contrib,
    by = "DonorName",
    all.x = TRUE
)
gfr_pc[is.na(contrib), contrib := 0]
gfr_pc[, remainder := pledge - contrib]
gfr_pc[abs(remainder) < 1, remainder := 0]



# Standardize donor names to iso3
locs <- fread(get_path("meta", "locs", "countrycodes_official.csv"))
locs <- unique(locs[, .(country = string_to_std_ascii(country_lc, pad_char = ""), iso3)])
gfr_pc[, country := string_to_std_ascii(DonorName, pad_char = "")]
gfr_pc <- merge(gfr_pc, locs, by = "country", all.x = TRUE)
gfr_pc[, iso3 := fcase(
    DonorName == "Gates Foundation", "GATES",
    country == "ESWATINI", "SWZ",
    country == "TANZANIA UNITED REPUBLIC", "TZA",
    rep_len(TRUE, .N), iso3
)]
## Disaggregate EC and determine how to allocate "OTHER PUBLIC"
ret_dah <- fread(get_path("compiling", "int", "retro_fgh_data.csv"))
ret_dah[, atm_dah := hfa_hiv_dah + hfa_tb_dah + hfa_mal_dah]
atm_dah <- ret_dah[channel == "EC",
                   .(atm_dah = sum(atm_dah)),
                   by = .(source)]
atm_dah[, frac := atm_dah / sum(atm_dah)]
gfr_pc <- merge(
    gfr_pc,
    atm_dah[, .(DonorName = "European Commission", source, frac)],
    by = "DonorName",
    all.x = TRUE
)
gfr_pc[
    DonorName == "European Commission",
    `:=`(
        iso3 = source,
        remainder = remainder * frac
    )
]
gfr_pc[, c("source", "frac") := NULL]

## can't find info on which countries this includes, so drop
gfr_pc <- gfr_pc[DonorName != "Other Public"]

fin <- gfr_pc[
    ,
    .(remainder_2025 = sum(remainder)),
    by = .(source = iso3)
]

fwrite(
    fin,
    get_path("compiling", "int", "gfatm_replenishment_2025_estimate.csv")
)

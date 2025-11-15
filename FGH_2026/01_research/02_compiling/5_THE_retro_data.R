########################################################################################
## 5_THE_retro_data
## Project: FGH 
## Description: Create DAH datasets for forecasting, rewritten from Stata code
########################################################################################
## Clear environment
rm(list = ls(all.names = TRUE))

## Set filepaths
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "/FUNCTIONS/currency_conversion.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

## Set variables
report_yr   <- dah.roots$report_year
asn_date    <- format(Sys.time(), "%Y%m%d")
# asn_date <- pass in argument
print(asn_date)
in.dir <- paste0(dah.roots$j, "FILEPATH")
FIN <- get_path('compiling', 'fin')

ei_base <- get_dah_param("ei_base_year")
dah.yr <- paste0("DAH_", substr(ei_base, 3, 4))

print('Read in ADB-PDB')
dt <- arrow::read_parquet(paste0(FIN, 'DAH_ADB_PDB_1990_', dah.roots$report_year, '.parquet'))

## ensure gdp deflator was merged on properly in script 4_finalization, or else
## we will mangle the estimates
stopifnot( dt[is.na(get(paste0("GDP_deflator_", report_yr))), .N] == 0 )

## ----------------------------------------------------------------------------
## Change deflation base year according to the Economic Indicators team's needs
if (ei_base != dah.roots$report_year) {
    cat("ADB PDB: Deflating to", ei_base, "base year from", dah.roots$report_year, "\n")
    # merge on defl
    dfl <- fread(get_path("meta", "defl",
                          paste0("imf_usgdp_deflators_", get_dah_param("defl_mmyy"), ".csv")))
    dt <- merge(dt,
                dfl[, c("YEAR", paste0("GDP_deflator_", ei_base)), with = FALSE],
                by = "YEAR", all.x = TRUE)
    
    dah_cols <- grep(paste0("DAH_", dah.roots$abrv_year), names(dt), value = TRUE)
    new_cols <- gsub(paste0("DAH_", dah.roots$abrv_year), dah.yr, dah_cols,
                     fixed = TRUE)
    # reflate to nominal, deflate wrt to EI base year
    dt[, (new_cols) := lapply(.SD, \(x)
                              get(paste0("GDP_deflator_", dah.roots$report_year)) *
                                  x / get(paste0("GDP_deflator_", ei_base))
    ),
    .SDcols = dah_cols]
    dt[, (dah_cols) := NULL]
}

#
# save
fwrite(dt,
       paste0(in.dir, "DATA REQUESTS/FGH_", report_yr,
              "/EI_DAH_ADB_PDB_1990_", report_yr, ".csv"))
arrow::write_parquet(dt,
                     paste0(in.dir,
                            "DATA REQUESTS/FGH_", report_yr,
                            "/EI_DAH_ADB_PDB_1990_", report_yr, ".parquet"))


dt[, gdp_deflator := get(paste0("GDP_deflator_", ei_base))]
dt <- dt[(ELIM_CH == 0 & ELIM_DONOR == 0), ]


## -------------------------------------------------------------------------------------
## Sum DAH by recipient and year
recipient_dt <- dt[, lapply(.SD, sum), by = .(YEAR, ISO3_RC, INKIND, gdp_deflator),
                   .SDcols = dah.yr]
recipient_dt <- recipient_dt[order(YEAR, ISO3_RC, INKIND)] 

print(paste0('Save dah_by_recipient_year to', in.dir, 'DATA REQUESTS/'))
fwrite(recipient_dt, paste0(in.dir, "DATA REQUESTS/FGH_", report_yr, "/dah_by_recipient_year_", report_yr, ".csv"))

## -------------------------------------------------------------------------------------
## Sum DAH by donor and year
donor_dt <- copy(dt)

## Reassign income sectors based on donor names
donor_dt[(DONOR_NAME == "BMGF" | CHANNEL == "BMGF"), INCOME_SECTOR := 'BMGF']
donor_dt[INCOME_SECTOR == "INK", INCOME_SECTOR := "PRIVINK"]
donor_dt[INCOME_SECTOR == "Corporation" & INKIND == 0,
         INCOME_SECTOR := "PRIVATE"]
donor_dt[INCOME_SECTOR == "Corporation" & INKIND == 1,
         INCOME_SECTOR := "PRIVINK"]
donor_dt[INCOME_SECTOR == "PRIVATE_INK",
         INCOME_SECTOR := "PRIVINK"]

all_dac <- names(dah_cfg$crs$donors)
all_dac <- all_dac[all_dac != "ARE"] ## drop ARE since they are not actually a DAC member

## call out these donor countries
locs <- c("USA", "GBR", "DEU", "FRA", "CAN", "AUS", "JPN", "NOR", "ESP", "NLD", "AUT", 
          "BEL", "DNK", "FIN", "GRC", "IRL", "ITA", "KOR", "LUX", "NZL", "PRT", "SWE", 
          "CHE", "CHN")
for (loc in locs) { 
  donor_dt[INCOME_SECTOR == "PUBLIC" & ISO_CODE == loc, INCOME_SECTOR := loc]
}

other_dac <- setdiff(all_dac, locs)
donor_dt[INCOME_SECTOR == "PUBLIC" & ISO_CODE %in% other_dac,
         INCOME_SECTOR := "OTHERDAC"]

## Non-OECD DAC countries including ARE, which haven't been reassigned above
donor_dt[INCOME_SECTOR == "PUBLIC",  INCOME_SECTOR := "OTHERPUB"]

## Sum by income sector AKA donor
donor_dt <- donor_dt[, lapply(.SD, sum),
                     by = .(YEAR, INCOME_SECTOR, INKIND, gdp_deflator),
                     .SDcols = (dah.yr)]
donor_dt <- donor_dt[order(YEAR, INCOME_SECTOR, INKIND)] 

print(paste0('Save dah_by_donor_year to', in.dir, 'DATA REQUESTS/'))
fwrite(donor_dt,  paste0(in.dir, "DATA REQUESTS/FGH_", report_yr, "/dah_by_donor_year_", report_yr, ".csv"))


#
# create EI base year version of FGH EZ data set
#
ez <- fread(get_path("compiling", "fin", "FGH_EZ_[report_year].csv"))
if (ei_base != dah.roots$report_year) {
    cat("FGH EZ: Deflating to", ei_base, "base year from", dah.roots$report_year, "\n")
    # merge on defl
    dfl <- fread(get_path("meta", "defl",
                          paste0("imf_usgdp_deflators_", get_dah_param("defl_mmyy"), ".csv")))
    names(dfl) <- tolower(names(dfl))
    ez <- merge(ez,
                dfl[, c("year", paste0("gdp_deflator_", ei_base)), with = FALSE],
                by = "year", all.x = TRUE)
    
    dah_cols <- grep(paste0("dah_", dah.roots$abrv_year), names(ez), value = TRUE)
    new_cols <- gsub(paste0("dah_", dah.roots$abrv_year), tolower(dah.yr), dah_cols,
                     fixed = TRUE)
    # reflate to nominal, deflate wrt to EI base year
    ez[, (new_cols) := lapply(.SD, \(x)
                              get(paste0("gdp_deflator_", dah.roots$report_year)) *
                                  x / get(paste0("gdp_deflator_", ei_base))
    ),
    .SDcols = dah_cols]
    ez[, (dah_cols) := NULL]
}

#
# save
fwrite(ez,
       paste0(in.dir, "DATA REQUESTS/FGH_", report_yr, "/EI_FGH_EZ_", report_yr, ".csv"))


print('End of Script')

#
# Final ADB PDB creation and testing.
# Saving of ADB PDB and FGH EZ data.
#
rm(list = ls(all.names = TRUE))
code_repo <- 'FILEPATh'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


#
# final aggregation
#
adb <- arrow::read_feather(
    get_path("compiling", "int", "adb_post_recip_impute.arrow")
)

final <- adb[, lapply(.SD, sum, na.rm = TRUE),
             .SDcols = grep("DAH_", names(adb), value = TRUE),
             by = c(
                 "YEAR",
                 "ELIM_CH", "ELIM_DONOR",
                 "DONOR_NAME", "ISO_CODE", "DONOR_COUNTRY",
                 "INCOME_SECTOR", "INCOME_TYPE",
                 "CHANNEL", "REPORTING_AGENCY", "SOURCE_CH", "GOV",
                 "LEVEL", "ISO3_RC",
                 "INKIND",
                 "prelim_est")]

## merge on recipient country name
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
final <- merge(
    final, locs[, .(ISO3_RC = ihme_loc_id, RECIPIENT_COUNTRY = location_name)],
    by = "ISO3_RC", all.x = TRUE
)
final[ISO3_RC == "WLD", RECIPIENT_COUNTRY := "World"]
final[ISO3_RC == "XKX", RECIPIENT_COUNTRY := "Kosovo"]

defl <- fread(get_path(
    "meta", "defl",
    paste0("imf_usgdp_deflators_", dah.roots$defl_MMYY, ".csv")
), select = c("YEAR", paste0("GDP_deflator_", dah.roots$report_year)))

final <- merge(final, defl, by = 'YEAR', all.x = T)



#
## FGH 2024 TEMPORARY:
## Rescale Gates foundation estimates for years 2020+:
##
## We extracted health spending estimates from Gates Annual Report and believe
## they are more accurate than the data-driven estimates, so we are rescaling
## our Gates estimates accordingly.
dah_yr <- paste0("DAH_", substr(report_year, 3, 4))

gates_est <- fread(get_path("bmgf", "raw", "BMGF_HEALTH_SPEND.csv"))[, -"Source"]
## (deflate the annual report estimates)
defl <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"))
gates_est <- merge(gates_est,
                   defl[, .(YEAR, defl = get(paste0("GDP_deflator_", report_year)))],
                   by = "YEAR", all.x = TRUE)
gates_est[, HEALTH_SPENDING := HEALTH_SPENDING / defl]


## calculate total DAH without double-counting, but also without in-kind because
## the annual report estimates are based on non-in-kind spending
dah_est <- final[INCOME_SECTOR == "BMGF" &
                     ELIM_CH == 0 & ELIM_DONOR == 0 & INKIND == 0,
                 .(dah = sum(get(dah_yr))),
                 by = YEAR]
gates_est <- merge(
    gates_est, dah_est,
    by = "YEAR", all.x = TRUE
)
gates_est[, gates_dah_ratio := dah / HEALTH_SPENDING]

final <- merge(
    final,
    gates_est[, .(YEAR, gates_dah_ratio)],
    by = "YEAR", all.x = TRUE
)

## apply the gates_dah_ratio to the DAH estimates for 2020-2023 so that the
## aggregate total matches the (deflated) annual report total.
## note - also apply to in-kind because in-kind estimates are based on the
## unscaled DAH estimates
dah_cols <- grep("DAH_", names(final), value = TRUE)
final[INCOME_SECTOR == "BMGF" &
          YEAR >= 2020 & YEAR < dah_cfg$report_year &
          ELIM_CH == 0 & ELIM_DONOR == 0,
      (dah_cols) := lapply(.SD, \(x) x/gates_dah_ratio),
      .SDcols = dah_cols]
final[, gates_dah_ratio := NULL]
##
# end fgh2024





#
# testing:
#
## ensure final HFA and PA estimates are consistent (i.e., parts sum to wholes)
ay <- dah.roots$abrv_year
final[, hfa_tot := rowSums(.SD), .SDcols = paste0(dah.roots$regular_hfa_vars, "_DAH_", ay)]
final[, pa_tot := rowSums(.SD), .SDcols = paste0(dah.roots$regular_pa_vars, "_DAH_", ay)]
stopifnot(final[abs(DAH_24 - hfa_tot) > 0.1, .N] == 0,
          final[abs(DAH_24 - pa_tot) > 0.1, .N] == 0)

## for each HFA, check that PAs sum to parent HFA
error <- FALSE
hfa_list <- dah.roots$regular_hfa_vars
hfa_list[hfa_list == "swap_hss_total"] <- "swap_hss"
hfa_list <- hfa_list[! hfa_list %in% c("unalloc", "other")] ## these HFAs have no PAs
for (col in hfa_list) {
    pa_cols <- grep(paste0("^", col, "_"), names(final), value = TRUE)
    if (col == "swap_hss") {
        hfa_col <- paste0("swap_hss_total", "_DAH_", ay)
    } else {
        hfa_col <- paste0(col, "_DAH_", ay)
    }
    pa_cols <- pa_cols[pa_cols != hfa_col]
    final[, tmp := rowSums(.SD, na.rm = TRUE),
        .SDcols = pa_cols]
    if (final[abs(tmp - get(hfa_col)) > 0.1, .N] != 0) {
        warning(paste0("PA sums do not match HFA for ", col))
        error <- TRUE
    }
}
if (error)
    stop("One or more of the HFAs has sub-components that don't sum to the total. (See warnings).")

final[, c("hfa_tot", "pa_tot", "tmp") := NULL]


## -------------------------------------------------------------------------------------
print('4. Save the ADB_PDB dataset------------------')
## -------------------------------------------------------------------------------------

save_dataset(
    final,
    "DAH_ADB_PDB_1990_[report_year].csv",
    channel = "compiling",
    stage = "fin"
)

arrow::write_parquet(
    final,
    get_path("compiling", "fin", "DAH_ADB_PDB_1990_[report_year].parquet")
)
cat("Parquet version saved.\n")

## -------------------------------------------------------------------------------------
print('5. Prepare and save FGH_EZ dataset------------------')
## -------------------------------------------------------------------------------------
# regional_fixed <- fread(paste0(FIN, 'DAH_ADB_PDB_1990_2020_AFG_fix.csv')) # read in fixed ADB_PDB (see AFG_ad_hoc_fix.R)
regional_fixed <- fread(paste0(get_path('compiling', 'fin'), 'DAH_ADB_PDB_1990_',dah.roots$report_year,'.csv')) 
## Drop transfers that are double counted 
regional_fixed <- regional_fixed[ELIM_CH == 0 & ELIM_DONOR == 0, ]

## INKIND to recipient countries fix
## To match prospective estimates and improve consistency we are renaming all recipient isocode
## to INKIND where INKIND is not equal to 0 because from the recipient's perspective they do not
## receive this money and therefore we do not want to include it in our estimates by recipient
## Just chaning the EZ for now but may want to add to ADB_PDB in the future
regional_fixed[INKIND != 0, `:=` (ISO3_RC = 'INKIND', INC_GROUP = 'INKIND',
                                  gbd_region_name = 'INKIND',
                                  gbd_superregion_name = 'INKIND',
                                  WB_REGION = 'INKIND', WB_REGIONCODE = 'INKIND',
                                  RECIPIENT_COUNTRY = 'INKIND')]


## Group sources
regional_fixed[DONOR_NAME == 'BMGF' | CHANNEL == 'BMGF', INCOME_SECTOR := "BMGF"]
regional_fixed[INCOME_SECTOR == "INK", INCOME_SECTOR := "PRIVINK"]

# list of DAC countries that we keep track of
for (loc in names(dah.roots$CRS$donors)) {
  regional_fixed[INCOME_SECTOR == "PUBLIC" & ISO_CODE == loc, INCOME_SECTOR := loc]
}

## Non-OECD DAC countries (including ARE as DAC)
regional_fixed[INCOME_SECTOR == "PUBLIC", INCOME_SECTOR := "OTHERPUB"] 

## Name regions
regional_fixed[is.na(gbd_superregion_name) | gbd_superregion_name == "", 
               gbd_superregion_name := "Unallocable"]
stopifnot(nrow(regional_fixed[is.na(gbd_superregion_name), ]) == 0)
stopifnot(nrow(regional_fixed[gbd_superregion_name == "", ]) == 0)

regional_fixed[gbd_superregion_name %like% "Latin America and Caribbean", 
               gbd_sregion_short := "LAC"]
regional_fixed[gbd_superregion_name %like% "Sub-Saharan", 
               gbd_sregion_short := "SSA"]
regional_fixed[gbd_superregion_name %like% "North Africa and Middle East", 
               gbd_sregion_short := "NAME"]
regional_fixed[gbd_superregion_name %like% "Central Europe, Eastern Europe, and Central Asia", 
               gbd_sregion_short := "ECA"]
regional_fixed[gbd_superregion_name %like% "South Asia", 
               gbd_sregion_short := "SA"]
regional_fixed[gbd_superregion_name %like% "Southeast Asia, East Asia, and Oceania",
               gbd_sregion_short := "EAP"]
regional_fixed[gbd_superregion_name %like% "Unallocable", 
               gbd_sregion_short := "UNIDREG"]
regional_fixed[gbd_superregion_name %like% "Global", 
               gbd_sregion_short := "WLD"]
regional_fixed[gbd_superregion_name %like% "High-income", 
               gbd_sregion_short := "HIC"]
regional_fixed[gbd_superregion_name == "INKIND",
               gbd_sregion_short := "INKIND"]

## Set column names to lower case
colnames(regional_fixed) <- tolower(colnames(regional_fixed))
setnames(regional_fixed, 'income_sector', 'source')


keep.cols <- c(
    "year","source","channel","donor_name","iso3_rc",
    "source_ch","gov","prelim_est","level",
    "gbd_region_name","gbd_superregion_name","recipient_country",
    "gbd_sregion_short",
    paste0("gdp_deflator_", dah.roots$report_year)
)
dah_cols <- grep("dah_", names(regional_fixed), value = TRUE)
ez_final <- regional_fixed[, lapply(.SD, sum, na.rm = TRUE),
                           .SDcols = dah_cols,
                           by = keep.cols]

print(paste0('Write FGH_EZ to ', get_path("compiling", "fin")))
save_dataset(ez_final, "FGH_EZ_[report_year]",
             channel = "compiling",
             stage = "fin")


print('End of Script')
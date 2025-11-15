#
# FGH: Ad hoc adjustments to recipient-level DAH to reduce the amount of
#   unallocable DAH
# - for bilaterals and dev banks, anywhere we have recip QZA we use observed
#   distribution over recipients for the channel to disaggregate
# - for UN agencies, we extracted additional data to compute recipient fractions
#   for the latest year available
# - for NGOs, we created recipient fractions from additional NGO data
#
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

DAH_YR <- paste0("DAH_", dah_cfg$abrv_year)



dah_full <- fread(get_path("compiling", "int", "adb_post_disagg.csv"))

DAH_COLS <- grep(DAH_YR, names(dah_full), value = TRUE)

dah_full[, is_unalloc := ISO3_RC == "QZA"]
last_obs_years <- dah_full[is_unalloc == FALSE, .(last_yr = max(YEAR)), by = CHANNEL]
dah_full <- merge(
    dah_full,
    last_obs_years,
    by = "CHANNEL",
    all.x = TRUE
)




#
# Impute unallocable DAH for bilaterals and development banks =================
#
## This will be based on observed recipient distributions from years where we
## do have data.


dah <- copy(dah_full)
dah[, c("RECIPIENT_COUNTRY", "INC_GROUP", "WB_REGION", "WB_REGIONCODE",
        "gbd_region_name", "gbd_superregion_name") := NULL]

## we will append back on the data that is not being adjusted, so tag it for convenience
dah[, append := FALSE]
## if not a bilateral, EC, or dev bank, not adjusting right now
dah[(!CHANNEL %like% "BIL_") &
        (!CHANNEL %in% c("EC", "WB_IDA", "WB_IBRD", "AsDB", "AfDB", "IDB")),
    append := TRUE]
dah[CHANNEL == "BIL_CHN", append := TRUE] ## not imputing China though
## in-kind DAH stays as it is
dah[INKIND != 0, append := TRUE]
## years pre-2000 won't be imputed
dah[YEAR < 2000, append := TRUE]
## for AfDB, recip data starts in 2002, so don't impute before then
dah[YEAR <= 2002 & CHANNEL == "AfDB", append := TRUE]
## future years (after last observed year of recip data) won't be imputed
dah[YEAR > last_yr, append := TRUE]
## don't bother imputing ELIM_CH
dah[ELIM_CH != 0 | ELIM_DONOR != 0, append := TRUE]

#
# for each channel, impute unallocable DAH where present in observed years.
#
dah_cr_fracs <- dah[append == FALSE & is_unalloc == FALSE,
                    .(value = sum(get(DAH_YR))),
                    by = .(YEAR, CHANNEL, ISO3_RC)]
dah_cr_fracs[, cr_frac := value / sum(value), by = .(YEAR, CHANNEL)]

dah[is_unalloc == FALSE, append := TRUE]

dah_unalloc <- dah[append == FALSE] # append == FALSE means we are adjusting this data
dah_unalloc <- merge(
    dah_unalloc[, -"ISO3_RC"],
    dah_cr_fracs[, .(YEAR, CHANNEL, ISO3_RC, cr_frac)],
    by = c("YEAR", "CHANNEL"),
    all.x = TRUE,
    # allow.cartesian because we merge many recips onto each CHANNEL-year
    allow.cartesian = TRUE
)
dah_unalloc[, (DAH_COLS) := lapply(.SD, \(x) x * cr_frac), .SDcols = DAH_COLS]
dah_unalloc[, is_unalloc := FALSE]


# Re-combine dah with non-adjusted data
dah <- rbind(
    dah[append == TRUE, ],
    dah_unalloc[, -"cr_frac"]
)
rm(dah_unalloc, dah_cr_fracs)



#
# Apply channel-specific fractions ===========================================
#

# UN AGENCIES ====
un_agency_fracs <- fread(get_path("compiling", "int", "un_recip.csv"))
un_agency_fracs <- un_agency_fracs[, .(YEAR = year,
                                       CHANNEL = channel,
                                       ISO3_RC = iso3,
                                       cr_frac = frac)]

dah[, append := TRUE]
dah[CHANNEL %in% unique(un_agency_fracs$CHANNEL) &
        YEAR %in% unique(un_agency_fracs$YEAR) &
        ELIM_CH == 0 & ELIM_DONOR == 0 &
        INKIND == 0,
    append := FALSE]

dah_un <- dah[append == FALSE]
dah_un <- merge(
    dah_un[, -"ISO3_RC"],
    un_agency_fracs,
    by = c("YEAR", "CHANNEL"),
    all.x = TRUE,
    allow.cartesian = TRUE
)
dah_un[, (DAH_COLS) := lapply(.SD, \(x) x * cr_frac), .SDcols = DAH_COLS]
dah_un[, is_unalloc := FALSE]

dah_un <- dah_un[,
                 lapply(.SD, sum, na.rm = TRUE),
                 .SDcols = DAH_COLS,
                 by = .(
                     YEAR, CHANNEL, ELIM_CH, ELIM_DONOR, DONOR_NAME,
                     ISO_CODE, DONOR_COUNTRY, INCOME_SECTOR, INCOME_TYPE, REPORTING_AGENCY,
                     SOURCE_CH, GOV, LEVEL, ISO3_RC, INKIND, prelim_est,
                     last_yr, is_unalloc, append
                )]



dah <- rbind(
    dah[append == TRUE, ],
    dah_un
)


# NGOS ====
ngos_fracs <- fread(get_path("ngo", "fin", "iso_fractions.csv"))

dah[, append := TRUE]
dah[CHANNEL == "NGO" &
        REPORTING_AGENCY == "NGO" &
        YEAR %in% unique(ngos_fracs$YEAR) &
        ELIM_CH == 0 & ELIM_DONOR == 0 &
        is_unalloc == TRUE,
    append := FALSE]

dah_ngos <- dah[append == FALSE]
dah_ngos <- merge(
    dah_ngos[, -"ISO3_RC"],
    ngos_fracs[, .(YEAR, ISO3_RC, fraction)],
    by = c("YEAR"),
    all.x = TRUE,
    allow.cartesian = TRUE
)
dah_ngos[, (DAH_COLS) := lapply(.SD, \(x) x * fraction), .SDcols = DAH_COLS]
dah_ngos[, is_unalloc := FALSE]

dah <- rbind(
    dah[append == TRUE, ],
    dah_ngos[, -"fraction"]
)




#
# quick tests to ensure totals across recipients were maintained
#
cmp <- merge(
    dah_full[, .(orig = sum(get(DAH_YR))), by = .(YEAR, INCOME_SECTOR, ISO_CODE, CHANNEL)],
    dah[, .(new = sum(get(DAH_YR))), by = .(YEAR, INCOME_SECTOR, ISO_CODE, CHANNEL)],
    by = c("YEAR", "INCOME_SECTOR", "ISO_CODE", "CHANNEL"),
    all = TRUE
)
cmp[, diff := abs(orig - new)]
stopifnot(
    cmp[is.na(diff), .N] == 0,
    cmp[diff > 1, .N] == 0
)
rm(cmp)



#
# finalize & save
#
dah_fin <- dah[,
           lapply(.SD, sum, na.rm = TRUE),
           .SDcols = DAH_COLS,
           by = .(
               YEAR, CHANNEL, ELIM_CH, ELIM_DONOR, DONOR_NAME,
               ISO_CODE, DONOR_COUNTRY, INCOME_SECTOR, INCOME_TYPE, REPORTING_AGENCY,
               SOURCE_CH, GOV, LEVEL, ISO3_RC, INKIND, prelim_est
               )]

## test HFAs still sum to total DAH
dah_fin[, tmp := rowSums(.SD), .SDcols = paste0(dah_cfg$regular_hfa_vars, "_", DAH_YR)]
stopifnot( dah_fin[abs(tmp - get(DAH_YR)) > 1, .N] == 0 )
dah_fin[, tmp := NULL]


save_dataset(
    dah_fin,
    "adb_post_recip_impute.arrow",
    channel = "compiling",
    stage = "int",
    format = "arrow"
)

if (interactive()) {
ex <- rbind(
    dah_fin[, .(val = sum(get(DAH_YR)), version = "new"),
            by = .(year = YEAR, grp = fcase(ISO3_RC == "WLD", "Global",
                                            INKIND != 0, "Inkind",
                                            ISO3_RC == "QZA", "Unallocable",
                                            default = "Country")
                   )],
    dah_full[, .(val = sum(get(DAH_YR)), version = "old"),
             by = .(year = YEAR, grp = fcase(ISO3_RC == "WLD", "Global",
                                             INKIND != 0, "Inkind",
                                             ISO3_RC == "QZA", "Unallocable",
                                             default = "Country"))]
)
ex[, tot := sum(val), by = .(year, version)]
ggplot(ex[between(year, 2000, 2023)], aes(x = year, y = val/tot, fill = grp)) +
    geom_col() +
    facet_wrap(~version, labeller = as_labeller(c(
        old = "Old Methods",
        new = "New Methods"
    ))) +
    scale_x_continuous(breaks = seq(2000, 2023, by = 5)) +
    scale_y_continuous(labels = scales::percent) +
    scale_fill_brewer(palette = "Set1") +
    labs(
        title = "2000-2023 DAH by Recipient Type",
        x = "",
        y = "% of Total",
        fill = "") +
    theme_bw(18) +
    theme(
        legend.position = "top",
        plot.title = element_text(hjust = 0.5, size = 20)
    )


source("~/repos/fgh/FUNCTIONS/helper_functions.R")
dahr <- dah_fin[year <= 2023,
                .(dah = sum(dah)), by = .(year, iso3 = iso3_rc)]
dahr <- get_pops(dahr)
dahr[, dah_pc := dah / pop_totes]
dahr[order(-dah_pc)][1:20]

}
#
# Back-cast DAH for a select group of donors
#
# In 2024, for the FGH 2024 report, we added 8 new donors so that we track every
# DAC member, instead of just the 24 previously tracked. Of these 8 new members,
# most do not have project-level data in the CRS dating back to 1990, like the
# original 24 donors do. Instead, their project-level data starts in a later
# year. Therefore, we tried to determine if we need to back-cast DAH for these
# donors, so that their funding doesn't artificially begin at some year after 1990
# just due to lack of data.
# Their data is already back-cast for some years if there DAC health commitment
# data pre-dates their actual project-level data, so we are looking to fill in
# the gap between 1990 and the beginning of their DAC health commitment data.
# We first tried to confirm if there is evidence of DAH for these countries in
# the years before their DAC-DAH data begins. We found evidence for 4 of 8
# countries: Estonia, Iceland, Lithuania, and Slovakia.
# For the other 4, we didn't find evidence, mostly just due to a lack of data or
# public information
# 
# In order to back-cast, we take advantage of the fact that these countries have
# total ODA disbursement data from OECD starting in much earlier years, which
# we can use as a predictive variable.
#

code_repo <- 'FILEAPTH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

dir.create(get_path("crs", "output"), showWarnings = FALSE)


backcast_donors <- c(
    "EST", "ISL", "LTU", "SVK"
)

#
# Prep data ====
#

# DAH
biloda4 <- fread(get_path("crs", "int", "B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_4.csv"))

# calculate total envelope
dah <- biloda4[isocode %in% backcast_donors,
               .(dah = sum(final_all_disb_curr, na.rm = TRUE)),
               by = .(year, iso = isocode)]


# ODA disbursements
dac2 <- fread(get_path("dac", "raw", "OECD.DCD.FSD,DSD_DAC2@DF_DAC2A.csv"))
setnames(dac2,
         c("DONOR", "TIME_PERIOD", "OBS_VALUE"),
         c("iso", "year", "oda"))
# filter to current ODA disbursements
dac2 <- dac2[iso %in% backcast_donors &
                 
                 Measure == "Gross ODA" &
                 `Price base` == "Current prices" &
                 year >= 1990 &
                 Recipient == "Developing countries",
             .(iso, year, oda = oda * 1e6)]


#
# Combine and estimate ====
#
grid <- data.table::CJ(iso = backcast_donors,
                       year = seq(1990, dah_cfg$crs$data_year))


# add oda disbursement data
grid <- merge(grid, dac2, by = c("iso", "year"), all.x = TRUE)

## assume that if there is no ODA disbursement data, then there is 0 ODA
setnafill(grid, fill = 0, cols = "oda")


# merge in DAH data
grid <- merge(grid, dah, by = c("iso", "year"), all.x = TRUE)
grid[, pred := ifelse(is.na(dah), 1L, 0L)]


# backcast
setcolorder(grid, c("iso", "year", "pred", "oda", "dah"))
setorder(grid, iso, year)


## calculate dah_frac and estimate for period before observed DAH data
##  - use 3-year weighted average of first 3 years of observed data, with most
##    weight on the earliest year
grid[, dah_frac := dah / oda]

grid[, yr_min := min(year[!is.na(dah)]), by = iso]
grid[, w_mean_frac := frollapply(dah_frac, 3, align = "left", FUN = \(x) {
    weighted.mean(x, w = c(1/2, 1/3, 1/6)) # t, t+1, t+2
}), by = iso]
grid[, frac_est := w_mean_frac[year == yr_min], by = iso]

## apply estimated fraction to total oda 
## - first smooth the total oda envelope via rolling average since sharp
##   fluctuations in ODA don't necessarily reflect abrupt changes in DAH
grid[, oda_smooth := frollmean(oda, 3, align = "left"), by = iso]
grid[oda == 0, oda_smooth := 0]

## then apply estimated fraction to (smoothed) total oda
grid[, dah_est := oda_smooth * frac_est]
grid[pred == 0, dah_est := dah]


#
# Add to database ====
#
backcast <- grid[year < yr_min, .(
    isocode = iso,
    year = year,
    final_all_disb_curr = dah_est,
    crs_id = "DUMMY",  # will cause funding to be converted to unalloc_DAH in script 3d
    ISO3_RC = "QZA"
)]

donor_names <- unlist(dah_cfg$crs$donors)
backcast[, donor_agency := donor_names[isocode]]

biloda4 <- rbind(biloda4, backcast, fill = TRUE)

save_dataset(biloda4,
             "B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_4_backcast",
             "CRS", "int")


cat("\n\n#####################################\n",
    "####         END OF 3c2          ####\n",
    "#####################################\n\n",
    sep = "")

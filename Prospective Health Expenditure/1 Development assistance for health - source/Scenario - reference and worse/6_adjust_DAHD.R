# Purpose: Update 2024 estimates for these donors from retrospective.

require(data.table)

## Read in forecasts of DAH by donor
dah_s <- fread('FILEPATH')[iso3 != "AGGREGATE"]

## Read in retrospective DAH estimates
ret_dah_s <- fread('FILEPATH')
ret_dah_s[source == "PRIVINK", source := "PRIVATE"]

## Aggregate by donor and year
ret_dah_s <- ret_dah_s[, .(dah_ret = sum(dah)), by = .(year, source)]

cmp <- merge(
  dah_s,
  ret_dah_s,
  by.x = c("year", "iso3"),
  by.y = c("year", "source"),
  all = TRUE
)
cmp[, diff := dah_ret - dah_donor_ref]

# Update 2024 estimates
cmp[year == 2024 & !is.na(dah_ret) & abs(diff) > 1, dah_donor_ref := dah_ret]
cmp[, c("dah_ret", "diff") := NULL]

# Make new aggregate group
cmp_agg <- cmp[, .(dah_donor_ref = sum(dah_donor_ref)), by = c("year")]
cmp_agg[, iso3 := "AGGREGATE"]
cmp <- rbind(cmp, cmp_agg)

## Write out updated forecasts 
fwrite(cmp, 'FILEPATH')

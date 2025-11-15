#
# visually compare new data with previous round data - important to spend some
#   time doing this interactively to look for bugs or data shifts
#
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))



new <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID.csv'))
old <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID.csv',
                      crs.update_mmyy = get_dah_param("crs", "prev_update_mmyy")
                      ))


#
# compare annual disbursements
#
cmp <- merge(
    new[, .(new = sum(all_disbcurr, na.rm = TRUE)), by = .(year)],
    old[, .(old = sum(all_disbcurr, na.rm = TRUE)), by = .(year)],
    by = "year",
    all = TRUE
)

ggplot(cmp, aes(x = year)) +
    geom_line(aes(y = new, color = "new")) +
    geom_line(aes(y = old, color = "old")) +
    labs(
        title = "Disbursement comparison",
        x = "", y = "Millions USD"
    )


cmp <- merge(
    new[, .(new = sum(all_disbcurr, na.rm = TRUE)), by = .(year, isocode)],
    old[, .(old = sum(all_disbcurr, na.rm = TRUE)), by = .(year, isocode)],
    by = c("year", "isocode"),
    all = TRUE
)
ggplot(cmp, aes(x = year)) +
    geom_line(aes(y = new, color = "new")) +
    geom_line(aes(y = old, color = "old")) +
    facet_wrap(~isocode, scales = "free_y") +
    labs(
        title = "Disbursement comparison",
        x = "", y = "Millions USD")



#
# compare covid disbursements
#
covnew <- new[, .(new = sum(oid_covid_disbcurr, na.rm = TRUE)), by = year]
covold <- old[covid == TRUE, .(old = sum(all_disbcurr, na.rm = TRUE)), by = year]

cmp <- merge(covnew, covold, by = "year", all = TRUE)

ggplot(cmp[year >= 2020], aes(x = year)) +
    geom_line(aes(y = new, color = "new")) +
    geom_line(aes(y = old, color = "old")) +
    labs(
        title = "COVID disbursement comparison",
        x = "", y = "Millions USD"
    )




#
# compare HFAs
#
hfa_cols <- grep("_disbcurr", names(new), value = TRUE)
newhfa <- new[, lapply(.SD, sum, na.rm = TRUE),
              by = .(year), .SDcols = hfa_cols]
newhfa <- melt(newhfa, id.vars = c("year"), value.name = "new")
newhfa <- newhfa[variable != "all_disbcurr"]


hfa_cols <- grep("_disbcurr", names(old), value = TRUE)
oldhfa <- old[, lapply(.SD, sum, na.rm = TRUE),
              by = .(year, covid), .SDcols = hfa_cols]
oldhfa <- melt(oldhfa, id.vars = c("year", "covid"), value.name = "old")
oldhfa[covid == TRUE & variable != "all_disbcurr", old := 0]
oldhfa[covid == TRUE & variable == "all_disbcurr", variable := "oid_covid_disbcurr"]
oldhfa <- oldhfa[variable != "all_disbcurr", -"covid"]
oldhfa <- oldhfa[, .(old = sum(old)), by = c("year", "variable")]

cmp <- merge(newhfa, oldhfa, by = c("year", "variable"), all = TRUE)

ggplot(cmp, aes(x = year)) +
    geom_line(aes(y = new, color = "new"), alpha = 0.7) +
    geom_line(aes(y = old, color = "old"), alpha = 0.7) +
    facet_wrap(~variable, scales = "free_y") +
    labs(
        title = "HFA disbursement comparison",
        x = "", y = "Millions USD"
    )


# viewing a single HFA across donors
hfa_cols <- grep("_disbcurr", names(new), value = TRUE)
newhfa <- new[, lapply(.SD, sum, na.rm = TRUE),
              by = .(year, isocode), .SDcols = hfa_cols]
newhfa <- melt(newhfa, id.vars = c("year", "isocode"), value.name = "new")
newhfa <- newhfa[variable != "all_disbcurr"]

hfa_cols <- grep("_disbcurr", names(old), value = TRUE)
oldhfa <- old[, lapply(.SD, sum, na.rm = TRUE),
              by = .(year, isocode, covid), .SDcols = hfa_cols]
oldhfa <- melt(oldhfa, id.vars = c("year", "isocode", "covid"), value.name = "old")
oldhfa[covid == TRUE & variable != "all_disbcurr", old := 0]
oldhfa[covid == TRUE & variable == "all_disbcurr", variable := "oid_covid_disbcurr"]
oldhfa <- oldhfa[variable != "all_disbcurr", -"covid"]
oldhfa <- oldhfa[, .(old = sum(old)), by = c("year", "isocode", "variable")]

cmp <- merge(newhfa, oldhfa, by = c("year", "isocode", "variable"), all = TRUE)
cmp[, variable := gsub("_disbcurr", "", variable)]


HFA <- "swap_hss_hrh"
ggplot(cmp[variable == HFA], aes(x = year)) +
    geom_line(aes(y = new, color = "new"), alpha = 0.7) +
    geom_line(aes(y = old, color = "old"), alpha = 0.7) +
    facet_wrap(~isocode, scales = "free_y") +
    labs(
        title = "HFA disbursement comparison",
        x = "", y = "Millions USD"
    )


tmp <- merge(
    new[pepfar == 1,
        lapply(.SD, sum, na.rm = TRUE),
        by = .(year),
        .SDcols = grep("hiv_.+_disbcurr", names(new))] |>
        melt(id.vars = "year", variable.name = "hfa", value.name = "new"),
    old[pepfar == 1,
        lapply(.SD, sum, na.rm = TRUE),
        by = .(year),
        .SDcols = grep("hiv_.+_disbcurr", names(old))] |>
        melt(id.vars = "year", variable.name = "hfa", value.name = "old"),
    by = c("year", "hfa"),
    all = TRUE
)
ggplot(tmp, aes(x = year)) +
    geom_line(aes(y = new, color = "new")) +
    geom_line(aes(y = old, color = "old")) +
    facet_wrap(~hfa, scales = "free_y") +
    labs(
        title = "HIV disbursement comparison",
        x = "", y = "Millions USD"
    )

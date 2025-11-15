#
# FGH 2024 - we want to predict 2025 DAH based on news and budget documents, in
#  particular related to the expected cuts in ODA/foreign aid being made by the
#  US government and several other major country donors.
#
#
# Phase 1: Predict 2025 and 2026 (the years with the most information)
#
#
rm(list = ls(all.names = TRUE))
code_repo <- 'FILEPATH'



report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


pred_years <- 2025:2026




defl_dt <- fread(get_path("meta", "defl", "imf_usgdp_deflators_[defl_mmyy].csv"),
              select = c("YEAR",
                         paste0("GDP_deflator_", dah_cfg$ei_base_year),
                         paste0("GDP_deflator_", dah_cfg$report_year)))

setnames(defl_dt, c("year", "defl23", "defl24"))


loc_dt <- fread(get_path("meta", "locs", "fgh_location_set.csv"))



#
# Load ODA/DAH budget data for country donors
#
budgets <- fread(get_path("compiling", "int", "budgets_estimated_changes.csv"))
budgets <- budgets[, .(year, source, channel, pct_chg)]
budgets[channel == "WB", channel := "WB_IDA"]


#
# load FGH data and process for predictions 
#
dat <- fread(get_path("compiling", "int", "retro_fgh_data.csv")) 

ret_agg <- dat[, .(dah = sum(dah, na.rm = TRUE)),
            by = .(year,
                   source_group,
                   source,
                   channel)]

full <- copy(ret_agg)
full <- full[, .(scenario = c("ref")), ## use to have c("worse", "ref", "better" for USA specifically)
             by = names(ret_agg)]
## only USA has multiple scenarios
full <- full[! (source != "USA" & scenario != "ref")]


#
# Expand & merge on percent changes from budget data
#
gr <- unique(full[, .(source_group, source, channel, scenario)])
gr <- gr[
    ,
    .(year = 2000:2026),
    by = .(source_group, source, channel, scenario)
]
full <- merge(
    gr,
    full,
    by = c("year", "source_group", "source", "channel", "scenario"),
    all.x = TRUE
)
full[year <= 2024 & is.na(dah), dah := 0]

## merge on growth rates from budgets
### generate channel groupings for applying budget changes
full[,
     chann2 := fcase(
         source_group %in% c("OTHERPUB", "MAINPUB") &
             channel %like% "BIL|NGO|EC",
         "BIL",
         channel %in% c(
             "WB_IDA", "WB_IBRD", "AfDB", "AsDB", "IDB"
         ),
         "DEVBANK",
         channel %in% c(
             "WHO", "PAHO", "UNICEF", "UNFPA", "UNAIDS", "UNITAID"
         ),
         "UN",
         channel %in% c(
             "GFATM", "GAVI", "CEPI"
         ),
         "PPP",
         default = NA_character_
     )
]

## ensure we've captured all cases
stopifnot(
    budgets[! source %in% unique(full$source), .N] == 0,
    budgets[! channel %in% c(unique(full$channel), unique(full$chann2)), .N] == 0
)

### first merge growth rates by source-channel (to get specific source-channel changes)
full <- merge(
    full,
    budgets[year >= 2025, .(year, source, channel, pct_chg1 = pct_chg)],
    by = c("year", "source", "channel"),
    all.x = TRUE
)
### then merge growth rates by source-chann2 (to get more general source-channel group changes)
full <- merge(
    full,
    budgets[year >= 2025, .(year, source, chann2 = channel, pct_chg2 = pct_chg)],
    by = c("year", "source", "chann2"),
    all.x = TRUE
)

### use most specific growth rate available
full[, pct_chg := fifelse(is.na(pct_chg1), pct_chg2, pct_chg1)]
full[, has_budget_evidence := !is.na(pct_chg)]

### assume hold constant if no budget evidence
setnafill(full, fill = 0, cols = "pct_chg")

full[, c("pct_chg1", "pct_chg2") := NULL]


### special USA: input current bilateral cut estimate and scenarios
full[
    source == "USA" & chann2 == "BIL" & year == 2025,
    pct_chg := fcase(
        scenario == "ref", -0.62
    )
]
full[
    source == "USA" & chann2 == "BIL" & year == 2026,
    pct_chg := 0
]



#
# Make predictions ============================================================
#


# Make initial predictions by applying growth rates
#
full[, budget_pr_dah := dah]
for (yr in sort(pred_years)) {
    full[order(year),
         budget_pr_dah_lag := shift(budget_pr_dah),
         by = .(source, channel, scenario)]
    full[
        year == yr,
        budget_pr_dah := budget_pr_dah_lag * (1 + pct_chg)
    ]
}
full[, c("budget_pr_dah_lag") := NULL]



# Now make special predictions/handle special cases
#
# (otherwise budget-based prediction will be used)


## for use below:
full[, pr_dah := dah]
full[order(year),
     pr_dah_lag := shift(pr_dah),
     by = .(source, channel, scenario)]


##
## Bilaterals & partners ====
##

# no special cases for bilaterals



##
## UN Agencies ====
##

# WHO:
## USA expected to pull out from WHO completely in 2025 (all scenarios)
full[source == "USA" & channel == "WHO" & year %in% 2025:2026,
     pr_dah := 0]


# PAHO:
## USA expected to pull out from WHO completely in 2025 (all scenarios)
full[source == "USA" & channel == "PAHO" & year %in% 2025:2026,
     pr_dah := 0]

# UNICEF:
## USA pull out (all scenarios)
full[source == "USA" & channel == "UNICEF" & year %in% 2025:2026,
     pr_dah := 0]
## swiss cut unicef funding by 28%
full[source == "CHE" & channel == "UNICEF" & year == 2025,
     pr_dah := pr_dah_lag * (1 - 0.28)]
## swiss hold unicef funding flat in 2026
full[order(year), lag := shift(pr_dah), by = .(source, channel, scenario)]
full[source == "CHE" & channel == "UNICEF" & year == 2026,
     pr_dah := lag]
full[, lag := NULL]


# UNFPA:
## USA pull out (all  scenarios)
full[source == "USA" & channel == "UNFPA" & year %in% 2025:2026,
     pr_dah := 0]


# UNAIDS:
## USA expected to pull out from UNAIDS completely in 2025 (all scenarios)
full[source == "USA" & channel == "UNAIDS" & year %in% 2025:2026,
     pr_dah := 0]
## swiss pull out from UNAIDS completely in 2025
full[source == "CHE" & channel == "UNAIDS" & year %in% 2025:2026,
     pr_dah := 0]


# UNITAID
## USA pull out (all scenarios)
full[source == "USA" & channel == "UNITAID" & year %in% 2025:2026,
     pr_dah := 0]



#
# PPP ====
#

# GAVI:
#
## 2025: Use mean DAH to GAVI in 2023-2024 as the basis for 2025
full[channel == "GAVI",
     mean_dah := mean(dah[year %in% 2023:2024]),
     by = .(source, scenario)]
full[channel == "GAVI" & year == 2025,
     pr_dah := mean_dah]

## but US pull out completely
full[source == "USA" & channel == "GAVI" & year == 2025,
     pr_dah := 0]

## 2026: import gavi replenishment
gavi_repl <- fread(get_path("compiling", "int", "gavi_2026_2030_replenishment.csv"))
full <- merge(
    full,
    gavi_repl[, .(year, source, channel, flow_to_gavi)],
    by = c("year", "source", "channel"),
    all.x = TRUE
)
## replenishment based estimate is only missing for private donors - use mean
## DAH again for them
full[year == 2026 & channel == "GAVI" & is.na(flow_to_gavi),
     flow_to_gavi := mean_dah]
full[year == 2026 & channel == "GAVI",
     pr_dah := flow_to_gavi]

full[, c("mean_dah", "flow_to_gavi") := NULL]




# GFATM
#
gfr_pc <- fread(get_path(
    "compiling", "int", "gfatm_replenishment_2025_estimate.csv"
))
stopifnot( gfr_pc[!source %in% unique(full$source), .N] == 0 )

## don't believe US will pay more than 800m
gfr_pc[source == "USA", remainder_2025 := 800e6]


full <- merge(full, gfr_pc, by = "source", all.x = TRUE)
## pr_dah will be the remainder, if available, or the 2024 value if not.
full[channel == "GFATM" & year == 2025,
     pr_dah := remainder_2025]
full[channel == "GFATM" & year == 2025 & is.na(pr_dah),
     pr_dah := pr_dah_lag] ## assume last year's value if no remainder available
full[channel == "GFATM" & year == 2025 & source == "FRA",
     pr_dah := pr_dah_lag] ## assume last year's value since remainder too large

## now enforce the total Global Fund DAH for 2025 to be $700m cut exactly
gf24 <- full[year == 2024 & scenario == "ref" & channel == "GFATM", sum(dah)]
gf25 <- gf24 - 700e6
curr_gf25 <- full[year == 2025 & scenario == "ref" & channel == "GFATM", sum(pr_dah)]
unalloc_remainder <- gf25 - curr_gf25
full[year == 2025 & source == "UNALL" & channel == "GFATM",
     pr_dah := pr_dah + unalloc_remainder]

full[, remainder_2025 := NULL]


# USA-to-Global Fund:
## US won't give more than 33% of total contributions, so adjust accordingly
nonus <- full[channel == "GFATM" & year == 2025 & source != "USA" & scenario == "ref",
              sum(pr_dah)]
full[source == "USA" & channel == "GFATM" & year == 2025,
     pr_dah := nonus / 2] ## so US is 1/3 of total GF DAH.

## but the US won't give more than $800m, so use whichever is smaller
## (in all scenarios)
full[source == "USA" & channel == "GFATM" & year == 2025,
     pr_dah := min(pr_dah, 800e6)]



## 2026:
## Hold 2025 value constant until new replenishment round in Nov 2025
## (https://www.theglobalfund.org/en/updates/2025/2025-09-16-announcing-the-global-fund-s-eighth-replenishment-summit/)
full[order(year),
     lag := shift(pr_dah),
     by = .(source, channel, scenario)]
full[year == 2026 & channel == "GFATM", pr_dah := lag]
full[, lag := NULL]

### unless we have a budget-based-prediction for 2026 GFATM contributions (rare)
#full[year == 2026 & channel == "GFATM" & has_budget_evidence == TRUE,
#     pr_dah := budget_pr_dah]



# CEPI
## no special case


#
# Development Banks ====
#

# WB_IDA:
## 2025 hold USA constant, 2026 reduce by 22.75%
full[source == "USA" & channel == "WB_IDA" & year == 2025,
     pr_dah := pr_dah_lag]
full[order(year), lag := shift(pr_dah), by = .(source, channel, scenario)]
full[source == "USA" & channel == "WB_IDA" & year == 2026,
     pr_dah := lag * (1 - 0.2275)]

# WB_IBRD:
# no change



# AfDB:
# ## USA pulls out of ADF specifically...
# ## (but this currently has no impact since no income shares for AfDB)
# full[source == "USA" & channel == "AfDB" & year == 2025,
#      pr_dah := pr_dah_lag]
# full[order(year),
#      lag := shift(pr_dah),
#      by = .(source, channel, scenario)]
# full[source == "USA" & channel == "AfDB" & year == 2026,
#      adf_amt := lag * 0.215]
# full[source == "USA" & channel == "AfDB" & year == 2026,
#      pr_dah := lag - adf_amt]
# full[, c("lag", "adf_amt") := NULL]


# AsDB:
## no change

# IDB:
## no change


#
# additional adjustments ====
#


# GATES-all channels:
## generate 2025 prediction
gates_dah <- full[source == "GATES" & year <= 2024, .(dah = sum(dah)), by = year]

gatesb <- openxlsx::read.xlsx(get_path("bmgf", "raw", "BMGF_SPEND_COMMITMENT.xlsx"))
gatesb <- as.data.table(gatesb)[, .(year, budget)]
gatesb <- merge(gatesb, defl_dt, by = "year", all.x = TRUE)
gatesb[, budget := budget / defl23] ## convert from current to 2023 USD to match DAH

gatesb <- merge(gatesb, gates_dah, by = "year", all.x = TRUE)
frac <- gatesb[year == 2024, dah / budget] ## use last year's share
gatesb[year == 2025, dah := budget * frac]
gates_2025_total <- gatesb[year == 2025, dah]

## use 2025 channel shares to disaggregate 2025 total
full[source == "GATES" & year == 2025, total := sum(dah, na.rm = TRUE)]
full[source == "GATES" & year == 2025, pr_dah := (dah/total) * gates_2025_total]

full[, total := NULL]
rm(gates_dah, gatesb, frac, gates_2025_total)

## 2026 - remain flat from 2025
full[order(year),
     lag := shift(pr_dah),
     by = .(source, channel, scenario)]
full[year == 2026 & source == "GATES", pr_dah := lag]
full[, lag := NULL]






#
# finalize predictions =========================================================
#

pr <- full[year > 2024, ]
## we use the budget-based prediction, unless we implemented a special case
## (for flows with no budget info, the budget based prediction is to hold constant,
##  which we keep here)
pr[is.na(pr_dah), pr_dah := budget_pr_dah]
stopifnot(pr[is.na(pr_dah), .N] == 0)

fin <- unique(full[year > 2024, .(year, source_group, source, channel)])
fin <- fin[, .(scenario = c("ref")), 
           by = .(year, source_group, source, channel)]

fin <- merge(
    fin,
    ## merge on reference scenarios for predictions
    pr[scenario == "ref", .(year, source_group, source, channel, dah_ref = pr_dah)],
    by = c("year", "source_group", "source", "channel"),
    all.x = TRUE
)

fin <- merge(
    fin,
    ## merge on other scenarios
    pr[, .(year, source_group, source, channel, scenario, dah_scen = pr_dah)],
    by = c("year", "source_group", "source", "channel", "scenario"),
    all.x = TRUE
)

## if no scenarios for the source-channel, then use reference for all scenarios
fin[is.na(dah_scen), dah_scen := dah_ref]
fin[, dah := dah_scen]

fin[, c("dah_ref", "dah_scen") := NULL]


## append with retrospective data
ret_fin <- ret_agg[
    year < min(pred_years),
    .(scenario = unique(fin$scenario)),
    by = names(ret_agg)
]
fin <- rbind(
    ret_fin,
    fin
)


## determine end year of prediction for each source
budget_end_years <- budgets[
    !is.na(pct_chg),
    .(end_year = max(year)),
    by = .(source)
]
budget_end_years[
    end_year > max(pred_years),
    end_year := max(pred_years)
]

fin <- merge(
    fin,
    budget_end_years,
    by = "source",
    all.x = TRUE
)
fin[is.na(end_year), end_year := min(pred_years) - 1]
fin[source == "USA", end_year := 2026]


setorder(fin, year)



#
# TMP
old <- fread(get_path("compiling", "fin", "dah_2025plus_preds.csv"))[scenario == "ref"]
new <- fin[scenario == "ref"]

cmp <- merge(
    new[, .(new = sum(dah)), by = .(year, source, channel)],
    old[, .(old = sum(dah)), by = .(year, source, channel)],
    by = c("year", "source", "channel"),
    all = TRUE
)
cmp[, diff := new - old]
cmp[abs(diff)>1]
cmp[, sum(diff, na.rm = TRUE)] # should be about 0

tab <- new[year %in% 2023:2025, .(source, channel, year, dah = dah/1e6)]
tab <- rbind(
    tab,
    new[year %in% 2023:2025, .(dah = sum(dah)/1e6, channel = "z.Source Total"),
        by = .(year, source)]
)
tab <- dcast(tab,
             source + channel ~ year,
             value.var = "dah")
tab[, change_24_25 := `2025` - `2024`]
tab[, pct_chg_24_25 := (change_24_25 / `2024`) * 100]
setorder(tab, source, channel)
setnames(tab, paste(2023:2025), paste(2023:2025, "(m US$)"))
openxlsx::write.xlsx(tab, "~/DAH Source-Channel latest.xlsx", asTable = TRUE)
#/TMP
#



fin[, currency := "2023 USD"]
setcolorder(
    fin,
    c("year", "source_group", "source", "channel",
      "scenario", "currency", "dah", "end_year")
)
save_dataset(
    fin, "dah_2025plus_preds.csv",
    channel = "compiling",
    stage = "fin"
)



# DAH by Source
dah_s <- fin[
    year <= end_year &
        scenario == "ref",
    .(dah = sum(dah)),
    by = .(year, source_group, source)
]
## need all sources through at least the end of the retro year
grid <- unique(dah_s[, .(source_group, source)])[
    ,
    .(year = seq(min(ret_agg$year), max(pred_years))),
    by = .(source_group, source)
]
dah_s <- merge(
    grid,
    dah_s,
    by = c("year", "source_group", "source"),
    all.x = TRUE
)
dah_s[year <= max(ret_agg$year) & is.na(dah), dah := 0]
dah_s[, last_source_year := max(year[!is.na(dah)]), by = source]
dah_s[, currency := "2023 USD"]

save_dataset(
    dah_s,
    "dah_by_source_year.csv",
    channel = "compiling",
    stage = "fin",
    folder = "ei_dah_handoff"
)

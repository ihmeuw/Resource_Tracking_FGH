#######################################################################################
print('01_backcasting')
## Project: FGH
## Description: Backcast DAH for countries that split off from other countries because 
## they often have no observed DAH in years that they were part of a parent country
## We reallocate money to child countries in years they didn't exist because we want 
## a full time series of DAH for all recipients, which is in line with what GBD does.
## Rewritten from Stata code
#######################################################################################
## Clear environment
rm(list = ls(all.names = TRUE))

## Set filepaths
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

data_yr     <- get_dah_param('CRS', 'data_year')
args <- commandArgs(TRUE)
asn_date <- format(Sys.time(), "%Y%m%d")
INT <- get_path('compiling', 'int')
FIN <- get_path('compiling', 'fin')
dah_yr <- paste0("DAH_", dah.roots$abrv_year) 

cat('Input variables\n')
cat(paste0("dah.roots$report_year: ", dah.roots$report_year, '\n'))
cat(paste0("data_yr: ", data_yr, '\n'))
cat(paste0("dah.roots$abrv_year: ", dah.roots$abrv_year, '\n'))
cat(paste0("asn_date: ", asn_date, '\n'))
cat(paste0("intermediary output directory: ", INT, '\n'))

## -------------------------------------------------------------------------------------
print('Step 1. Split USSR_FRMR (former USSR) and XYG (former Yugoslavia) regional funding------------------')
## to constituent countries, to ensure USSR_FRMR and XYG do not appear in final data.

## We checked USSR_FRMR and XYG projects, which appear in CRS and US Foundations, 
## and it makes sense to split them because they do appear to mostly be regional 
## projects. 
dt <- fread(paste0(INT, "region_data/DAH_compiling_1990_", dah.roots$report_year, ".csv"))
splitformer_xyg <- dt[ISO3_RC %in% c("XYG"), ]
splitformer_ussr <- dt[ISO3_RC %in% c("USSR_FRMR"), ]

## Check on the years of XYG and USSR_FRMR data to determine overlap with high-income 
## constituent country years
print('Yugoslavia years')
table(splitformer_xyg$YEAR)
print('USSR years')
table(splitformer_ussr$YEAR)

## then check on all constituent countries' income groups to make sure we don't split 
## to any countries that are high income in certain years

## Assign recipient information for split Yugoslavia and USSR data
## Before 2007, there were 6 countries that we want to split to:
## (BIH, HRV, MKD, MNE, SRB, SVN). 
## However, SVN was high-income from 1997 to present, so we will need to remove it from 
## the split in those years 
dt_xyg_e <- splitformer_xyg[YEAR <= 1996, ]
6 * sum(dt_xyg_e[, get(dah_yr)])
dt_xyg_e <- setDT(rbind(dt_xyg_e, lapply(dt_xyg_e, rep, 5)))
dt_xyg_e[, ISO3_RC := rep(c('BIH', 'HRV', 'MKD', 'SVN', 'SRB', 'MNE'), length.out = .N), 
         by = c('YEAR', 'ISO3_RC', 'INCOME_SECTOR', 'ISO_CODE', 'CHANNEL', 'INKIND')]
print(paste0('Sum of Yugoslavia DAH before 1997:', sum(dt_xyg_e[, get(dah_yr)])))
table(dt_xyg_e$ISO3_RC)
table(dt_xyg_e$YEAR)
table(dt_xyg_e[, get(dah_yr)])

dt_xyg_m <- splitformer_xyg[YEAR %in% c(1997:2007), ]
5 * sum(dt_xyg_m[, get(dah_yr)])
dt_xyg_m <- setDT(rbind(dt_xyg_m, lapply(dt_xyg_m, rep, 4)))
dt_xyg_m[, ISO3_RC := rep(c('BIH', 'HRV', 'MKD',  'SRB', 'MNE'), length.out = .N), 
         by = c('YEAR', 'ISO3_RC', 'INCOME_SECTOR', 'ISO_CODE', 'CHANNEL', 'INKIND')]
print(paste0('Sum of Yugoslavia DAH 1997-2007:', sum(dt_xyg_m[, get(dah_yr)])))
table(dt_xyg_m$ISO3_RC)
table(dt_xyg_m[, get(dah_yr)])

## In 2008, Kosovo split off from Serbia, so there are 7 countries to split to. 
## However, SVN and HRV were high income during this period, so we will need to remove 
## them from the split
dt_xyg_l <- splitformer_xyg[YEAR >= 2008, ]
5 * sum(dt_xyg_l[, get(dah_yr)])
dt_xyg_l <- setDT(rbind(dt_xyg_l, lapply(dt_xyg_l, rep, 4)))
dt_xyg_l[, ISO3_RC := rep(c('BIH', 'MKD',  'SRB', 'MNE', 'KSV'), length.out = .N), 
         by = c('YEAR', 'ISO3_RC', 'INCOME_SECTOR', 'ISO_CODE', 'CHANNEL')]
print(paste0('Sum of Yugoslavia DAH after 2007:', sum(dt_xyg_l[, get(dah_yr)])))
table(dt_xyg_l$ISO3_RC)
table(dt_xyg_l[, get(dah_yr)])

## There are 15 constituent countries of the former USSR, some were high-income but not 
## in years overlapping with observed USSR_FRMR years so we split to all 15.
15 * sum(splitformer_ussr[, get(dah_yr)])
dt_ussr <- setDT(rbind(splitformer_ussr, lapply(splitformer_ussr, rep, 14)))
dt_ussr[, ISO3_RC := rep(c("ARM", "AZE", "BLR", "EST", "GEO", "KAZ", "KGZ", "LTU", 
                           "LVA", "MDA", "RUS", "TJK", "TKM", "UKR", "UZB"), 
                         length.out = .N), 
         by = c('YEAR', 'ISO3_RC', 'INCOME_SECTOR', 'ISO_CODE', 'CHANNEL')]
print(paste0('Sum of USSR DAH:', sum(dt_ussr[, get(dah_yr)])))
table(dt_ussr$ISO3_RC)
table(dt_ussr[, get(dah_yr)])

dah.cols <- grep("DAH", colnames(dt_ussr), value = TRUE)

for (col in dah.cols) {
	## We split the observed funding equally to all constituent countries
	dt_xyg_e[, eval(col) := get(col) / 6]
    dt_xyg_m[, eval(col) := get(col) / 5]
    dt_xyg_l[, eval(col) := get(col) / 5]
    dt_ussr[, eval(col) := get(col) / 15]
}
## Check that the DAH totals sum to the prior values
stopifnot((round(sum(splitformer_xyg[YEAR <= 1996, get(dah_yr)]), 0) == round(sum(dt_xyg_e[, get(dah_yr)]), 0)) & 
     (round(sum(splitformer_xyg[YEAR %in% c(1997:2007), get(dah_yr)]), 0) == round(sum(dt_xyg_m[, get(dah_yr)]), 0)) &
     (round(sum(splitformer_xyg[YEAR >= 2008, get(dah_yr)]), 0) == round(sum(dt_xyg_l[, get(dah_yr)]), 0)) &
     (round(sum(splitformer_ussr[, get(dah_yr)]), 0) == round(sum(dt_ussr[, get(dah_yr)], 0))))

splitformer <- rbind(dt_xyg_e, dt_xyg_m, dt_xyg_l, dt_ussr)
splitformer[, LEVEL := "COUNTRY_split"]

## Get rid of USSR_FRMR and XYG projects, and replace them with the new split projects
dt1 <- dt[!ISO3_RC %in% c("XYG", "USSR_FRMR"), ]
splitformer <- rbind(dt1, splitformer)

## Make sure DAH sum has not changed from original data.table
stopifnot(round(sum(dt$DAH, na.rm = T), 0) == round(sum(splitformer$DAH, na.rm = T), 0))
stopifnot(round(sum(dt[, get(dah_yr)], na.rm = T), 0) == round(sum(splitformer[, get(dah_yr)], na.rm = T), 0))

## -------------------------------------------------------------------------------------
print('2. Backcasting country-years------------------')
## -------------------------------------------------------------------------------------

## We create projects to append to the dataset where DAH received by the parent country 
## in years before the child country split off is redistributed between the parent and 
## child countries using a 3-year average proportion of parent and child country DAH in 
## the first 3 years the child country received DAH.

## Keep all parent and child countries we can backcast
## Many countries could not be backcast due to missing parent country DAH in years 
## before the child country existed
backcasted <- copy(splitformer[ISO3_RC %in% c("SSD", "SDN", "TLS", "IDN", 
                                              "ERI", "ETH", "KSV", "SRB"), ])
backcasted1 <- backcasted[ELIM_DONOR %in% c(NA, 0) & ELIM_CH %in% c(NA, 0), ]

backcasted1 <- backcasted1[, .(sum(get(dah_yr))), by = .(YEAR, ISO3_RC, INKIND)]
setnames(backcasted1, "V1", dah_yr)
backcasted1 <- backcasted1[YEAR <= data_yr, ]

## change_YEAR is the first year the child country started existing
backcasted1[ISO3_RC %in% c("SSD", "SDN"), change_YEAR := 2011]
backcasted1[ISO3_RC %in% c("TLS", "IDN"), change_YEAR := 1999]
backcasted1[ISO3_RC %in% c("ERI", "ETH"), change_YEAR := 1992]
backcasted1[ISO3_RC %in% c("KSV", "SRB"), change_YEAR := 2008]
stopifnot(!any(is.na(unique(backcasted1$change_YEAR))))
backcasted1 <- dcast(backcasted1,
                     YEAR + change_YEAR + INKIND ~ ISO3_RC, 
                     value.var = eval(dah_yr))

## Backcast envelopes for each pair of parent and child countries
## SSD and SDN
backcasted1[YEAR < change_YEAR, SSD := 0]
backcasted1[, SDNSSD := SDN + SSD]
backcasted1[, `:=`(frct_SDN = SDN / SDNSSD, frct_SSD = SSD / SDNSSD)]
backcasted1[(YEAR >= change_YEAR & YEAR <= (change_YEAR + 2)), new3years_SSD := 1]
backcasted1[is.na(new3years_SSD), `:=`(frct_SDN = NA, frct_SSD = NA)]

## Create 3-year average fractions 
backcasted1[, `:=`(avg_frct_SDN = mean(frct_SDN, na.rm = T), 
                   avg_frct_SSD = mean(frct_SSD, na.rm = T))]

## Create new DAH amounts based on fractions
backcasted1[YEAR < change_YEAR,
            `:=`(newSDN = avg_frct_SDN * SDNSSD, 
                 newSSD = avg_frct_SSD * SDNSSD)]

## Create the new projects that will be added, which are the difference between the 
## new and old DAH totals
backcasted1[, `:=`(diffSDN = newSDN - SDN, diffSSD = newSSD - SSD)]

## TLS and IDN
backcasted1[YEAR < change_YEAR, TLS := 0]
backcasted1[, IDNTLS := IDN + TLS]
backcasted1[, `:=`(frct_IDN = IDN / IDNTLS, frct_TLS = TLS / IDNTLS)]
backcasted1[(YEAR >= change_YEAR & YEAR <= (change_YEAR + 2)), new3years_TLS := 1]
backcasted1[is.na(new3years_TLS), `:=`(frct_IDN = NA, frct_TLS = NA)]
backcasted1[, `:=`(avg_frct_IDN = mean(frct_IDN, na.rm = T), 
                   avg_frct_TLS = mean(frct_TLS, na.rm = T))]
backcasted1[YEAR < change_YEAR, `:=`(newIDN = avg_frct_IDN * IDNTLS,
                                     newTLS = avg_frct_TLS * IDNTLS)]
backcasted1[, `:=`(diffIDN = newIDN - IDN, diffTLS = newTLS - TLS)]

## ERI and ETH
backcasted1[YEAR < change_YEAR, ERI := 0]
backcasted1[, ETHERI := ETH + ERI]
backcasted1[, `:=`(frct_ETH = ETH / ETHERI, frct_ERI = ERI / ETHERI)]
backcasted1[YEAR %in% c(1994:1996), new3years_ERI := 1] 
##handle zero DAH in first two years by manually setting avg of next three nonzero years
backcasted1[is.na(new3years_ERI), `:=`(frct_ETH = NA, frct_ERI = NA)]
backcasted1[, `:=`(avg_frct_ETH = mean(frct_ETH, na.rm = T), 
                   avg_frct_ERI = mean(frct_ERI, na.rm = T))]
backcasted1[YEAR < change_YEAR, `:=`(newETH = avg_frct_ETH * ETHERI,
                                     newERI = avg_frct_ERI * ETHERI)]
backcasted1[, `:=`(diffETH = newETH - ETH, diffERI = newERI - ERI)]

##KSV and SRB
backcasted1[, SRBKSV := SRB + KSV]
backcasted1[YEAR < change_YEAR & is.na(KSV), KSV := 0] 
backcasted1[, `:=`(frct_SRB = SRB / SRBKSV, frct_KSV = KSV / SRBKSV)]
backcasted1[(YEAR >= change_YEAR & YEAR <= (change_YEAR + 2)), new3years_KSV := 1]
backcasted1[is.na(new3years_KSV), `:=`(frct_SRB = NA, frct_KSV = NA)]
backcasted1[, `:=`(avg_frct_SRB = mean(frct_SRB, na.rm = T), 
                   avg_frct_KSV = mean(frct_KSV, na.rm = T))]
##do not reallocate observed DAH, just add the proportion from SRB on top (WHY?)
backcasted1[YEAR < change_YEAR, `:=`(newSRB = avg_frct_SRB * SRB, 
                                     newKSV = KSV + (avg_frct_KSV * SRB))]
backcasted1[, `:=`(diffSRB = newSRB - SRB, diffKSV = newKSV - KSV)]

## Formatting the new projects
backcasted1 <- backcasted1[YEAR < change_YEAR, ]

backcasted2 <- melt(backcasted1,
                    id.vars = c('YEAR', 'change_YEAR', 'INKIND'), 
                    measure.vars = c("ERI", "ETH", "IDN", "KSV", 
                                     "SDN", "SRB", "SSD", "TLS",
                                     "newERI", "newETH", "newIDN", "newKSV", 
                                     "newSDN", "newSRB", "newSSD", "newTLS",
                                     "diffERI", "diffETH", "diffIDN", "diffKSV", 
                                     "diffSDN", "diffSRB", "diffSSD", "diffTLS"),
                    variable.name = "ISO3_RC")
backcasted2[ISO3_RC %like% "diff", type := 'diff']
backcasted2[ISO3_RC %like% "new", type := 'new']
backcasted2[is.na(type), type := 'old']
backcasted2[, ISO3_RC := str_sub(ISO3_RC,-3,-1)]




# ------------------------------------------------------------------------------
#
# Calculate the total recipient-year amounts that need to be reallocated from
#   parent countries to child countries
#
realloc <- backcasted2[type == "diff" & ISO3_RC %in% c("SSD", "TLS", "ERI", "KSV")]
realloc[, parent := fcase(
  ISO3_RC == "SSD", "SDN",
  ISO3_RC == "TLS", "IDN",
  ISO3_RC == "ERI", "ETH",
  ISO3_RC == "KSV", "SRB"
)]
realloc <- realloc[!is.na(value) & value != 0,
                   .(YEAR, INKIND, parent, ISO3_RC, value)]
setnames(realloc, "value", "realloc_dah")



#
# Calculate flow fractions from parent countries to distribute the backcasted
#    DAH envelopes for child countries
#
fractions <- splitformer[ISO3_RC %in% unique(realloc$parent), ]
fractions <- fractions[(ELIM_CH %in% c(NA, 0) & ELIM_DONOR %in% c(NA, 0)), ]
fractions <- fractions[, lapply(.SD, sum, na.rm = TRUE),
                       .SDcols = grep("_DAH", names(fractions), value = TRUE),
                       by = .(YEAR,
                              INCOME_SECTOR, INCOME_TYPE, ISO_CODE,
                              CHANNEL,
                              ISO3_RC, INKIND)]
fractions <- melt(fractions,
                  id.vars = c("YEAR",
                              "INCOME_SECTOR", "INCOME_TYPE", "ISO_CODE",
                              "CHANNEL",
                              "ISO3_RC", "INKIND"),
                  variable.name = "hfa",
                  variable.factor = FALSE,
                  value.name = "dah")
fractions[, recip_yr_tot := sum(dah), by = .(ISO3_RC, YEAR, INKIND)]
fractions[, flow_frct := dah / recip_yr_tot]
fractions[, c("dah", "recip_yr_tot") := NULL]


#
# Calculate backcasted flows for child countries
#     which need to be *added* to the database
#
back_flows <- merge(
    realloc, fractions,
    by.x = c("parent",  "YEAR" , "INKIND"),
    by.y = c("ISO3_RC", "YEAR", "INKIND"),
    all.x = TRUE,
    allow.cartesian = TRUE
)
back_flows[, fin := realloc_dah * flow_frct]
back_flows[, tmp := sum(fin, na.rm = T), by = .(ISO3_RC, YEAR, INKIND)]

stopifnot(back_flows[abs(realloc_dah - tmp) > 0.01, .N] == 0)

back_flows[, realloc_dah := fin]
back_flows[, c("fin", "tmp", "flow_frct") := NULL]

back_flows <- dcast(back_flows, 
                    YEAR + INCOME_SECTOR + INCOME_TYPE + ISO_CODE +
                        CHANNEL + ISO3_RC + INKIND ~ hfa,
                    value.var = "realloc_dah")

back_flows[, (dah_yr) := rowSums(.SD, na.rm = TRUE),
           .SDcols = grep("_DAH", names(back_flows), value = TRUE)]

#
# *Subtract* amounts from parent countries
#     that have been reallocated to child countries
#

## convert all DAH to fractions of total RECIP-YEAR DAH
adjusted_adb <- copy(splitformer)
adjusted_adb[, recip_yr_tot := sum(get(dah_yr), na.rm = TRUE),
             by = .(YEAR, ISO3_RC, INKIND)]

dah_cols <- grep(dah_yr, names(adjusted_adb), value = TRUE)
adjusted_adb[, (dah_cols) := lapply(.SD, \(x) x / recip_yr_tot),
            .SDcols = dah_cols]
setnafill(adjusted_adb, fill = 0, cols = dah_cols)

## merge on backcasted envelopes to subtract from parent
realloc_remove <- realloc[, .(sub_dah = sum(realloc_dah, na.rm = TRUE)),
                          by = .(YEAR, ISO3_RC = parent, INKIND)]
adjusted_adb <- merge(
    adjusted_adb, realloc_remove,
    by = c("YEAR", "INKIND", "ISO3_RC"),
    all.x = TRUE
)
## (if sub_dah is NA, it's because the RECIP-YEAR doesn't need any reallocation)
setnafill(adjusted_adb, fill = 0, cols = "sub_dah")


## subtract reallocated amount from parent countries
adjusted_adb[, recip_yr_tot_realloc := recip_yr_tot - sub_dah]

## redistribute recip envelopes across flows
adjusted_adb[, (dah_cols) := lapply(.SD, \(x) x * recip_yr_tot_realloc),
            .SDcols = dah_cols]


# test redistribution went smoothly
adjusted_adb[, tst := sum(get(dah_yr), na.rm = TRUE), by = .(YEAR, INKIND, ISO3_RC)]
## test where we had DAH to subtract, the flows sum to the adjusted total 
stopifnot(
    adjusted_adb[sub_dah != 0 & abs(tst - recip_yr_tot_realloc) > 0.01, .N] == 0
)
## test where we had no DAH to subtract, the flows sum to the original total
stopifnot(
    adjusted_adb[sub_dah == 0 & abs(tst - recip_yr_tot) > 0.01, .N] == 0
)
adjusted_adb[, c("tst", "recip_yr_tot", "recip_yr_tot_realloc", "sub_dah") := NULL]


#
# Append backcasted child flows to the database and test results
#
back_flows[, `:=`(
    REPORTING_AGENCY = "BACKCASTING",
    LEVEL = "COUNTRY_backcasted"
)]

final_adb <- rbind(
    adjusted_adb, back_flows, fill = TRUE
)


## test
tst <- merge(
    final_adb[, .(new = sum(get(dah_yr), na.rm = TRUE)), by = .(YEAR, CHANNEL, ISO3_RC)],
    splitformer[, .(old = sum(get(dah_yr), na.rm = TRUE)), by = .(YEAR, CHANNEL, ISO3_RC)],
    by = c("YEAR", "CHANNEL", "ISO3_RC"),
    all = TRUE
)

change_locs <- tst[abs(new - old) > 1, unique(ISO3_RC)]
stopifnot(
    all(change_locs %in% c(unique(realloc$ISO3_RC), unique(realloc$parent)))
)
rm(tst, change_locs)


save_dataset(final_adb,
             paste0("DAH_compiling_1990_", dah.roots$report_year, "_backcasted"),
             channel = "compiling",
             stage = "int",
             folder = "region_data"
             )


print('* Done')


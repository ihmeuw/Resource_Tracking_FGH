#
# Finalize the extracted data to be used as health-focus-area fractions
#

code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))




#
# Load and prep WHO's IATI transaction data
#

iati <- fread(get_path("who", "int", c("iati_clean", "who.csv")))

# drop transactions with missing or zero value
iati <- iati[!is.na(trans_value) & trans_value != 0]
# keep only outgoing transactions (drop incoming)
iati <- iati[outgoing_flag == TRUE]

if (! all(unique(iati$trans_currency) == "USD")) {
    stop("There are transactions reported in a currency other than USD. ",
         "Standardize currencies before aggregating transaction value.")
}


kws <- readstata13::read.dta13(get_path("who", "int", "iati_post_kws.dta"))
setDT(kws)
kws <- kws[, c("iati_identifier",
               grep("^final_", names(kws), value = TRUE)),
           with = FALSE]


kws_long <- merge(
    iati[trans_type %in% c("Disbursement", "Expenditure"),
         .(val = sum(trans_value, na.rm = TRUE)),
         by = .(year = trans_year,
                iati_identifier)],
    kws,
    by = "iati_identifier",
    all.x = TRUE
)
kws_long <- melt(kws_long,
                 id.vars = c("iati_identifier", "year", "val"),
                 variable.name = "pa",
                 value.name = "frct")
kws_long <- kws_long[pa != "final_total_frct"]
kws_long[, pa := gsub("final_|_frct", "", pa)]
## normalize fractions to sume precisely to 1 (sum rounding errors)
kws_long[, tot := sum(frct), by = .(year, iati_identifier)]
kws_long[, frct := frct / tot]
kws_long[, tot := NULL]
kws_long[, val := val * frct]




#
# Explode transactions by sector
#
# aggregate each activity by transaction year and type
agg <- iati[trans_type %in% c("Disbursement", "Expenditure"),
            .(trans_value = sum(trans_value)),
     by = .(
         iati_identifier,
         trans_year,
         sector,
         sector_code,
         sector_vocab,
         sector_pct
     )]


agg[, flow_id := .I]

## note - this only works if all concatenated fields have the same number of
##   elements, which is ensured by the iati-aggregator library.
## data.table will error if this is not the case.
agg <- agg[, .(
    sector = unlist(strsplit(sector, "<SEP>", fixed = TRUE)),
    sector_code = unlist(strsplit(sector_code, "&", fixed = TRUE)),
    sector_vocab = unlist(strsplit(sector_vocab, "<SEP>", fixed = TRUE)),
    sector_pct = unlist(strsplit(sector_pct, "&", fixed = TRUE))
), by = .(flow_id, iati_identifier, trans_year, trans_value)]

agg[, sector_pct := as.numeric(sector_pct) / 100]

#
# Resolve sector percentages that add to greater than 100%
#
# Note: according to the IATI standard, if an org reports more than one sector
#  for a single sector code vocabulary, they must provide percentages which add
#  up to 100%.
#  See: https://iatistandard.org/en/iati-standard/201/activity-standard/iati-activities/iati-activity/sector/
#  Still, there is one WHO activity where the percentages for the
#  OECD DAC CRS 5 DIGIT sector vocabulary add up to greater than 100%.
#  It appears to be because they report some sectors twice, with different %s.
#  To resolve this, we will normalize the percentages to add up to 100%.
#
agg <- agg[, .(sector_pct = sum(sector_pct)),
           by = .(flow_id, iati_identifier, trans_year, trans_value,
                  sector, sector_code, sector_vocab)]

agg[, tot := sum(sector_pct), by = flow_id]
agg[, sector_pct := sector_pct / tot, by = flow_id]

agg[, tot := sum(sector_pct), by = flow_id]
if (agg[tot != 1, .N] > 0) {
    stop("There are transactions with sector percentages that do not add up to 100%.")
}
agg[, tot := NULL]




#
# Apply sector percentages to transaction value
#
agg[, sector_value := trans_value * sector_pct]

agg[, tot := sum(sector_value), by = flow_id]
if (agg[abs(trans_value - tot) > 1, .N]) {
    stop("There are transactions where the sum of sector values does not equal the transaction value.")
}
agg[, tot := NULL]



agg[, sec_pa := fcase(
    sector %in% c(
        "STD control including HIV/AIDS"
    ),
    "hiv_other",
    
    sector %in% c(
        "Tuberculosis control"
    ),
    "tb_other",
    
    sector %in% c(
        "Malaria control"
    ),
    "mal_con_oth",
    
    sector %in% c(
        "Infectious disease control"
    ),
    "oid_other",
    
    sector %in% c(
        "Other prevention and treatment of NCDs",
        "NCDs control, general",
        "Research for prevention and control of NCDs",
        "Control of harmful use of alcohol and drugs"
    ),
    "ncd_other",
    
    # sector %in% c(
    #     "Control of harmful use of alcohol and drugs"
    # ),
    # "ncd_other|swap_hss_other",
    
    sector %in% c(
        "Reproductive health care"
    ),
    "rmh_other",
    
    sector %in% c(
        "Medical research",
        "Relief co-ordination and support services"
    ),
    "swap_hss_other",
    
    sector %in% c(
        "Medical education/training",
        "Health personnel development"
    ),
    "swap_hss_hrh",
    
    sector %in% c(
        "Health policy and administrative management",
        "Population policy and administrative management"
    ),
    "swap_hss_hrh|swap_hss_other",
    
    sector %in% c(
        "Multi-hazard response preparedness",
        "Material relief assistance and services",
        "Basic Health Care Services in Emergencies"
    ),
    "swap_hss_pp",
    
    sector %in% c(
        "Promotion of mental health and well-being"
    ),
    "ncd_mental",
    
    sector %in% c(
        "Basic nutrition"
    ),
    "nch_cnn",
    
    sector %in% c(
        "Basic health care",
        "Food safety and quality"
    ),
    "other"
)]

if (agg[is.na(sec_pa), .N] > 0) {
    stop("There are OECD sectors that need to be assigned to an FGH program-area.")
}

cmb <- merge(
    agg,
    kws_long[, .(iati_identifier,
                 trans_year = year,
                 kws_pa = pa,
                 kws_frct = frct)],
    by = c("iati_identifier", "trans_year"),
    all.x = TRUE,
    # each activity merged with all program areas
    allow.cartesian = TRUE
)
## this data set is at the activity, year, sector, program-area level
## so for final fractions, if we use:
##   - kws fracs, need to be distributed across all sectors
##   - sector fracs, need to be distributed across all program areas

## Option3
cmb[, n_sector := .N, by = .(iati_identifier, trans_year, kws_pa)]
cmb[, n_hfa    := .N, by = .(iati_identifier, trans_year, sector)]
cmb[, final_value := sector_value * kws_frct]

bad_pas <- c("other", "swap_hss_other")


cmb[sector %in% c(
    "Infectious disease control",
    "Reproductive health care",
    "Basic nutrition",
    "Multi-hazard response preparedness",
    "Basic Health Care Services in Emergencies",
    "Relief co-ordination and support services"
    ) &
        kws_pa %in% bad_pas,
    `:=`(
        fin_pa = sec_pa
    )]


cmb[sector %in% c(
    "STD control including HIV/AIDS",
    "Tuberculosis control",
    "Malaria control",
    "Promotion of mental health and well-being"
    ),
    `:=`(
        fin_pa = sec_pa
    )]

cmb[kws_pa == "nch_cnv" & sec_pa == "oid_other",
    `:=`(
        fin_pa = "oid_other"
    )]


cmb[is.na(fin_pa), `:=`(
    fin_pa = kws_pa
)]


cmb[, sum(final_value)]
agg[, sum(sector_value)]



#
# Finalize data for use in WHO's main pipeline (0_WHO_HFA.do)
#

fin <- cmb[, .(value = sum(final_value)),
           by = .(year = trans_year, hfa_pa = fin_pa)]


## split flows that are to multiple HFA_PAs
## (as assigned above, e.g. "ncd_other|swap_hss_other")
fin[, id := .I]
fin <- fin[, .(
    hfa_pa = unlist(strsplit(hfa_pa, "|", fixed = TRUE))
    ), by = .(id, year, value)]
## split value evenly among HFA_PAs
fin[, n_hfas := .N, by = id]
fin[, value := value / n_hfas]
## re-aggregate
fin <- fin[, .(value = sum(value)), by = .(year, hfa_pa)]

# calculate fraction of total for each year
fin[, total := sum(value), by = year]
fin[, frct := value / total]
fin[, hfa_pa := paste0(hfa_pa, "_frct")]

fin[, tmp := sum(frct), by = year]

if (fin[abs(tmp - 1) > 1e-9, .N] > 0)
    stop("Fractions do not add up to 1.")

## ensure exact sum to 1
fin[, frct := frct / tmp]

fin <- dcast(fin, year ~ hfa_pa, value.var = "frct")

## for years pre-2020, we use previously collected data from budget documents,
## which are more complete than IATI for those years.
fin <- fin[year >= 2020]
setnames(fin, "year", "YEAR")

save_dataset(fin,
             "who_iati_hfa_frcts",
             channel = "who",
             stage = "int")
}


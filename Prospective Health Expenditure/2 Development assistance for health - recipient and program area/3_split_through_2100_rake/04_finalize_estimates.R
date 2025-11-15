#
# Create final databases
#
library(data.table)
library(arrow)
library(Cairo)
library(ggplot2)

scenario <- 'reference'
project <- "ID"
if(scenario == "reference") {
  WORK_DIR <- paste0("FILEPATH")
  suffix <- "ref"
} else {
  WORK_DIR <- paste0("FILEPATH", scenario, "FILEPATH")
  suffix <- scenario
}
end_yr <- 2100

## Retrieve location metadata
source("FILEPATH")
locs <- get_location_metadata(location_set_id = 35, release_id = 16)[level == 3]
lox <- unique(locs$ihme_loc_id)

.locs <- get_location_metadata(location_set_id = 35, release_id = 16)
add_source_info <- function(dt) {
  dt <- merge(
    dt,
    .locs[, .(source = ihme_loc_id, source_name = location_name)],
    by = "source",
    all.x = TRUE
  )
  dt[, source_name := fcase(
    source == "GATES", "Gates Foundation",
    source == "DEBT", "Debt Repayments",
    source == "OTHER", "Other",
    source == "OTHERDAC", "Other DAC Member Countries",
    source == "OTHERPUB", "Other Country Governments",
    source == "PRIVATE", "Private Philanthropy",
    source == "UNALL", "Unallocable",
    source == "OTHERPUB_NONGBD", "Other Non-GBD Country Governments",
    rep_len(TRUE, .N), source_name
  )]
  stopifnot(dt[is.na(source_name), .N] == 0)
  return(dt)
}
add_channel_info <- function(dt) {
  dt[, tmp := gsub("BIL_", "", channel)]
  dt <- merge(
    dt,
    .locs[, .(tmp = ihme_loc_id, channel_name = location_name)],
    by = "tmp",
    all.x = TRUE
  )
  dt[!is.na(channel_name),
     channel_name := paste0("Bilateral - ", channel_name)]
  dt[, channel_name := fcase(
    channel == "AfDB", "African Development Bank",
    channel == "AsDB", "Asian Development Bank",
    channel == "GATES", "Gates Foundation",
    channel == "CEPI", "Coalition for Epidemic Preparedness Innovations",
    channel == "EC", "European Commission",
    channel == "EEA", "European Economic Area",
    channel == "GAVI", "Gavi Alliance",
    channel == "GFATM", "The Global Fund",
    channel == "IDB", "Inter-American Development Bank",
    channel == "NGO", "Non-Governmental Organizations",
    channel == "INTL_NGO", "International Non-Governmental Organizations",
    channel == "PAHO", "Pan American Health Organization",
    channel == "UNAIDS", "UNAIDS",
    channel == "UNFPA", "United Nations Population Fund",
    channel == "UNICEF", "United Nations Children's Fund",
    channel == "UNITAID", "UNITAID",
    channel == "US_FOUND", "US Foundations",
    channel == "WB_IBRD", "World Bank IBRD",
    channel == "WB_IDA", "World Bank IDA",
    channel == "WHO", "World Health Organization",
    rep_len(TRUE, .N), channel_name
  )]
  stopifnot(dt[is.na(channel_name), .N] == 0)
  dt[, tmp := NULL]
  return(dt)
}
add_hfa_info <- function(dt) {
  dt[, hfa_name := fcase(
    hfa == "other", "Other",
    hfa == "swap_hss_total", "SWAps & HSS",
    hfa == "oid", "Other Infectious Diseases",
    hfa == "ncd", "Non-communicable Diseases",
    hfa == "nch", "Child Health",
    hfa == "hiv", "HIV/AIDS",
    hfa == "mal", "Malaria",
    hfa == "rmh", "Maternal health",
    hfa == "tb", "Tuberculosis",
    hfa == "unalloc", "Unallocable",
    default = NA_character_
  )]
  stopifnot(dt[is.na(hfa_name), .N] == 0)
  return(dt)
}
add_pa_info <- function(dt) {
  dt[, pa_name := fcase(
    pa == "other", "Other",
    pa == "swap_hss_hrh", "SWAps & HSS - Human Resources",
    pa == "swap_hss_me", "SWAps & HSS - Monitoring and Evaluation",
    pa == "swap_hss_pp", "SWAps & HSS - Pandemic Preparedness",
    pa == "swap_hss_other", "SWAps & HSS - Other",
    pa == "oid_amr", "Other Infectious Diseases - Antimicrobial Resistance",
    pa == "oid_covid", "Other Infectious Diseases - COVID-19",
    pa == "oid_ebz", "Other Infectious Diseases - Ebola",
    pa == "oid_hss_hrh", "Other Infectious Diseases - SWAps & HSS  - Human Resources",
    pa == "oid_hss_me", "Other Infectious Diseases - SWAps & HSS - Monitoring and Evaluation",
    pa == "oid_hss_other", "Other Infectious Diseases - SWAps & HSS - Other",
    pa == "oid_other", "Other Infectious Diseases - Other",
    pa == "oid_zika", "Other Infectious Diseases - Zika",
    pa == "ncd_hss_hrh", "Non-communicable Diseases - SWAps & HSS - Human Resources",
    pa == "ncd_hss_me", "Non-communicable Diseases - SWAps & HSS - Monitoring and Evaluation",
    pa == "ncd_hss_other", "Non-communicable Diseases - SWAps & HSS - Other",
    pa == "ncd_mental", "Non-communicable Diseases - Mental health",
    pa == "ncd_other", "Non-communicable Diseases - Other",
    pa == "ncd_tobac", "Non-communicable Diseases - Tobacco",
    pa == "nch_cnn", "Child Health - Nutrition",
    pa == "nch_cnv", "Child Health - Vaccines",
    pa == "nch_hss_hrh", "Child Health - SWAps & HSS - Human Resources",
    pa == "nch_hss_me", "Child Health - SWAps & HSS - Monitoring and Evaluation",
    pa == "nch_hss_other", "Child Health - SWAps & HSS - Other",
    pa == "nch_other", "Child Health - Other",
    pa == "hiv_amr", "HIV/AIDS - Antimicrobial Resistance",
    pa == "hiv_care", "HIV/AIDS - Care and strengthening",
    pa == "hiv_ct", "HIV/AIDS - Counseling and testing",
    pa == "hiv_hss_hrh", "HIV/AIDS - SWAps & HSS - Human Resources",
    pa == "hiv_hss_me", "HIV/AIDS - SWAps & HSS - Monitoring and Evaluation",
    pa == "hiv_hss_other", "HIV/AIDS - SWAps & HSS - Other",
    pa == "hiv_other", "HIV/AIDS - Other",
    pa == "hiv_ovc", "HIV/AIDS - Orphans and vulnerable children",
    pa == "hiv_pmtct", "HIV/AIDS - PMTCT",
    pa == "hiv_prev", "HIV/AIDS - Prevention (excluding PMTCT)",
    pa == "hiv_treat", "HIV/AIDS - Treatment",
    pa == "mal_amr", "Malaria - Antimicrobial Resistance",
    pa == "mal_comm_con", "Malaria - Community outreach",
    pa == "mal_con_irs", "Malaria - Indoor residual spraying",
    pa == "mal_con_nets", "Malaria - Nets",
    pa == "mal_con_oth", "Malaria - Other control",
    pa == "mal_diag", "Malaria - Diagnosis",
    pa == "mal_hss_hrh", "Malaria - SWAps & HSS - Human Resources",
    pa == "mal_hss_me", "Malaria - SWAps & HSS - Monitoring and Evaluation",
    pa == "mal_hss_other", "Malaria - SWAps & HSS - Other",
    pa == "mal_other", "Malaria - Other",
    pa == "mal_treat", "Malaria - Treatment",
    pa == "rmh_fp", "Maternal health - Family planning",
    pa == "rmh_hss_hrh", "Maternal health - SWAps & HSS - Human Resources",
    pa == "rmh_hss_me", "Maternal health - SWAps & HSS - Monitoring and Evaluation",
    pa == "rmh_hss_other", "Maternal health - SWAps & HSS - Other",
    pa == "rmh_mh", "Maternal health - Maternal health",
    pa == "rmh_other", "Maternal health - Other",
    pa == "tb_amr", "Tuberculosis - Antimicrobial resistance",
    pa == "tb_diag", "Tuberculosis - Diagnosis",
    pa == "tb_hss_hrh", "Tuberculosis - SWAps & HSS - Human Resources",
    pa == "tb_hss_me", "Tuberculosis - SWAps & HSS - Monitoring and Evaluation",
    pa == "tb_hss_other", "Tuberculosis - SWAps & HSS - Other",
    pa == "tb_other", "Tuberculosis - Other",
    pa == "tb_treat", "Tuberculosis - Treatment",
    pa == "unalloc", "Unallocable",
    default = NA_character_
  )]
  stopifnot(dt[is.na(pa_name), .N] == 0)
  return(dt)
}
add_recip_info <- function(dt) {
  dt <- merge(
    dt,
    .locs[, .(recip = ihme_loc_id, recip_name = location_name, location_id)],
    by = "recip",
    all.x = TRUE
  )
  dt[, recip_name := fcase(
    recip == "UNALLOCABLE_DAH_INK", "In-kind DAH",
    recip == "UNALLOCABLE_DAH_WLD", "Global DAH",
    recip == "UNALLOCABLE_DAH_QZA", "Unallocable DAH",
    rep_len(TRUE, .N), recip_name
  )]
  stopifnot(dt[is.na(recip_name), .N] == 0)
  return(dt)
}



# Bind retrospective and final projected estimates to create final data sets:
ret_dah_raw_hfa <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
ret_dah_raw_pa <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))

#
# a) Source-Channel
#
dah_sch <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
fin_sc <- dah_sch[year <= end_yr,
                  .(dah_ref = sum(dah_ref)),
                  by = .(year, source, channel)]
fin_sc[, currency := "2023 USD"]

setorder(fin_sc, year, source, channel)
setcolorder(fin_sc, c("year", "source", "channel", "currency", "dah_ref"))
fin_sc <- add_source_info(fin_sc)
fin_sc <- add_channel_info(fin_sc)
## save
fwrite(
  fin_sc,
  file.path(WORK_DIR, paste0("FILEPATH"))
)


#
# b) Source-Recipient
#
# retrospective
ret_dah <- ret_dah_raw_hfa[, .(dah = sum(dah)),
                           by = .(year, source, recip)]

# prospective

# DAH-S-R raked estimates for residual sources only
pro_dah <- arrow::read_feather(file.path(WORK_DIR, "data", paste0("FILEPATH")))
setDT(pro_dah)
# For top 10 sources, get unraked DAH-S-R
pro_dah_top10 <- arrow::read_feather(file.path(WORK_DIR, "FILEPATH"))
setDT(pro_dah_top10)
top10sources <- setdiff(pro_dah_top10$source, pro_dah$src)
pro_dah_top10 <- pro_dah_top10[source %in% top10sources & year %in% unique(pro_dah$year)]
pro_dah_top10 <- pro_dah_top10[, .(year, src, recip, ipf_value = dah_ref)]
# Append the two together
pro_dah <- rbind(pro_dah, pro_dah_top10, fill = T)

# final
fin_sr <- rbind(
  ret_dah[year < 2024,
          .(year, source, recip,
            dah_ref = dah, currency = "2023 USD")],
  pro_dah[year >= 2024,
          .(year, source = src, recip,
            dah_ref = ipf_value,
            currency = "2023 USD")]
)
## drop 0 flows
fin_sr <- fin_sr[!(dah_ref == 0)]
setorder(fin_sr, year, source, recip)
fin_sr <- add_source_info(fin_sr)
fin_sr <- add_recip_info(fin_sr)
fin_sr <- fin_sr[year <= end_yr]
## save
fwrite(
  fin_sr,
  file.path(WORK_DIR, paste0("FILEPATH"))
)


#
# c) Channel-HFA-Recipient
#
# retrospective
ret_dah_hfa <- ret_dah_raw_hfa[, .(dah = sum(dah)),
                               by = .(year, channel, hfa, recip)]

# prospective
pro_dah <- arrow::read_feather(file.path(WORK_DIR, "data", paste0("FILEPATH")))
setDT(pro_dah)
setnames(pro_dah, "src", "channel")

# final
ret_dah_hfa_recip <- ret_dah_hfa[, .(dah = sum(dah)), by = c("year", "channel", "recip")]
fin_chr <- rbind(
  ret_dah_hfa_recip[year < 2024,
                    .(year, channel, recip,
                      dah_ref = dah,
                      currency = "2023 USD")],
  pro_dah[year >= 2024,
          .(year, channel, recip,
            dah_ref = ipf_value,
            currency = "2023 USD")]
)
## drop 0 flows
fin_chr <- fin_chr[!(dah_ref == 0)]
setorder(fin_chr, year, channel, recip)

fin_chr <- add_channel_info(fin_chr)
fin_chr <- add_recip_info(fin_chr)
fin_chr <- fin_chr[year <= end_yr]

## save
fwrite(
  fin_chr,
  file.path(WORK_DIR, paste0("FILEPATH"))
)

# checks ==================================================================

# check total envelope is correct across all files
if(scenario == "reference") {
  dah_d_agg <- fread("FILEPATH")[iso3 == "AGGREGATE"]
} else if(scenario == "better") {
  dah_d_agg <- fread(paste0('FILEPATH'))[iso3 == 'AGGREGATE']
} else if(scenario == "worse") {
  dah_d_agg <- fread("FILEPATH")[iso3 == "AGGREGATE"]
}

final_dah_cr <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_cr <- final_dah_cr[, .(dah = sum(dah_ref)), by = "year"]
final_dah_sc <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_sc <- final_dah_sc[, .(dah = sum(dah_ref)), by = "year"]
final_dah_sr <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_sr <- final_dah_sr[, .(dah = sum(dah_ref)), by = "year"]

# check that recipient totals are correct
if(scenario == "reference") {
  dah_r <- fread("FILEPATH")
  unalloc <- fread("FILEPATH")
} else if(scenario == "better") {
  dah_r <- fread("FILEPATH")
  unalloc <- fread("FILEPATH")
} else if(scenario == "worse") {
  dah_r <- fread("FILEPATH")
  unalloc <- fread("FILEPATH")
}
dah_r <- dah_r[, .(year, recip = iso3, dah = mean)]
unalloc <- unalloc[year <= 2030 & iso3 %in% c("UNALLOCABLE_DAH_INK", "UNALLOCABLE_DAH_QZA", "UNALLOCABLE_DAH_WLD")]
unalloc <- unalloc[, .(year, recip = iso3, dah = dah_recip)]
dah_r <- rbind(dah_r, unalloc)
setorder(dah_r, recip, year)
dah_r[!(recip %in% c(lox, "OTH", "UNALLOCABLE_DAH_WLD", "UNALLOCABLE_DAH_INK", "QZA", "UNALLOCABLE_DAH_QZA")), recip := "OTH"]
dah_r[recip %in% c("OTH", "QZA", "UNALLOCABLE_DAH_INK", "UNALLOCABLE_DAH_WLD"), recip := "UNALLOCABLE_DAH_QZA"]
dah_r <- dah_r[, .(dah = sum(dah)),
               by = .(year, recip)]
dah_r[, tot := sum(dah), by = recip]
dah_r <- dah_r[tot > 0, -"tot"]


final_dah_cr <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_cr <- final_dah_cr[, .(dah = sum(dah_ref)), by = c("year", "recip")]

final_dah_sr <- fread(paste0(WORK_DIR, "FILEPATH"))
final_dah_sr <- final_dah_sr[, .(dah = sum(dah_ref)), by = c("year", "recip")]


# check that prospective file values through 2100 are consistent with files through 2030 up to 2030
final_dah_sr_2100 <- fread(paste0(WORK_DIR, "FILEPATH"))
View(final_dah_sr_2100[year <= 2030])
final_dah_sr_2030 <- fread(paste0(WORK_DIR, "FILEPATH"))

#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Intercept/tag COVID projects and create COVID output for compiling
#
# Prerequisites: 2f_FINALIZE_HFA_DATAS.R, COVID pipeline
# Outputs needed for: covid compiling
#---------------------# ####
# ==== ENV ====
rm(list = ls(all.names = TRUE))
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

# ==== MAIN ====
#
# TAG COVID PROJECTS IN CRS DATA
#
# load processed CRS data
dt <- fread(file.path(
    get_path("CRS", "int", "B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2.csv")
), encoding = "UTF-8", strip.white = FALSE)


dt[, covid := FALSE]
## all projects with explicit purpose of COVID-19 control are tagged
dt[purpose_name == "COVID-19 control", covid := TRUE]
## emergency response projects tagged as COVID-19 in earlier script 1c
dt[sector_code == 720 & emergency_response %like% "covid", covid := TRUE]
dt[purpose_name == "Infectious disease control" & keywords %like% "#COVID-19",
   covid := TRUE]

## reclassify some projects based on additional research:

# https://www.afd.fr/fr/carte-des-projets/financement-budgetaire-renforcement-du-systeme-de-sante-et-assurance-maladie-non-contributive
dt[donor_name == "France" &
       project_title == "RENFORCEMENT SYSTèME DE SANTé" &
       year %in% 2020:2022,
   covid := TRUE]

# https://www.afd.fr/en/actualites/communique-de-presse/france-and-rwanda-sign-financing-agreements-response-covid-19-support-vocational-training
dt[donor_name == "France" &
       project_title == "PLANS SANITAIRE ET SOCIALE COVID-19" &
       year %in% 2020:2022,
   covid := TRUE]

# see proj descr
dt[donor_name == "Germany" &
       project_title == "Emergency Response to COVID-19 Program (Covid-19 Vaccination Program Ind",
   covid := TRUE]

# see proj descr
dt[donor_name == "Germany" &
       project_title == "Supporting the vaccination logistics in Niger and Benin" &
       year == 2022,
   covid := TRUE]

# https://www2.jica.go.jp/en/evaluation/pdf/2021_0924_1_s.pdf
dt[donor_name == "Japan" &
       project_title == "Support to COVID-19 Responses in Africa" &
       year == 2022,
   covid := TRUE]


# we determined these large projects should not be allocated entirely as COVID-19,
#   and reviewed each loan to determine HFA fractions
# https://www.mofa.go.jp/ic/ap_m/page23e_000595.html#:~:text=Japan%20will%20proactively%20lead%20international,to%20help%20revitalize%20economic%20activities.
# India: https://www.jica.go.jp/Resource/india/english/office/topics/c8h0vm00009i08sm-att/press200831_01.pdf
# Cote d'Ivoire: https://www.jica.go.jp/english/information/press/2022/20220526_21_en.html
# Bangladesh: https://www.jica.go.jp/english/information/press/2021/20211124_30.html
# Philippines: https://www.jica.go.jp/english/information/press/2022/20220426_10e.html
# 
dt[, `:=`(is_cov_swp = FALSE, is_swp = FALSE)]
dt[donor_name == "Japan" &
       project_title == "COVID-19 Crisis Response Emergency Support Loan" &
       ISO3_RC == "IND",
   is_cov_swp := TRUE]

dt[donor_name == "Japan" &
       project_title == "COVID-19 Crisis Response Emergency Support Loan" &
       ISO3_RC == "CIV",
   is_swp := TRUE]

dt[donor_name == "Japan" &
       project_title == "COVID-19 Crisis Response Emergency Support Loan Phase 2" &
       ISO3_RC == "BGD" &
       year == 2021,
   is_cov_swp := TRUE]

dt[donor_name == "Japan" &
       project_title == "COVID-19 Crisis Response Emergency Support Loan Phase 2" &
       ISO3_RC == "BGD" &
       year == 2022,
   is_swp := TRUE]

dt[donor_name == "Japan" &
       project_title == "COVID-19 Crisis Response Emergency Support Loan Phase 2" &
       ISO3_RC == "PHL",
   is_cov_swp := TRUE]


#
# projects tagged as covid need other HFAs removed
#

# isolate all the columns that need to be adjusted 
# - need to zero out other fractions
frct_cols <- grep("final_.+_frct$", names(dt), value = TRUE)
frct_cols <- grep("final_total_frct", frct_cols, invert = TRUE, value = TRUE) # ignore total_frct
disb_cols <- grep("_disbcurr$|_disbcons$", names(dt), value = TRUE)
disb_cols <- grep("all_disb", disb_cols, invert = TRUE, value = TRUE) # ignore all_disb
comm_cols <- grep("_commcurr$|_commcons", names(dt), value = TRUE)
comm_cols <- grep("all_comm", comm_cols, invert = TRUE, value = TRUE) # ignore all_comm

# zero out
zero_cols <- c(frct_cols, disb_cols, comm_cols)

dt[covid == TRUE, (zero_cols) := 0]
dt[covid == TRUE, `:=` (
    final_oid_covid_frct = 1,
    oid_covid_disbcurr = all_disbcurr,
    oid_covid_disbcons = all_disbcons,
    oid_covid_commcurr = all_commcurr,
    oid_covid_commcons = all_commcons
)]

dt[is_cov_swp == TRUE, (zero_cols) := 0]
dt[is_cov_swp == TRUE, `:=` (
    final_swap_hss_pp_frct = 1/3,
    final_swap_hss_other_frct = 1/3,
    final_oid_covid_frct = 1/3,
    swap_hss_pp_disbcurr = all_disbcurr * 1/3,
    swap_hss_pp_disbcons = all_disbcons * 1/3,
    swap_hss_pp_commcurr = all_commcurr * 1/3,
    swap_hss_pp_commcons = all_commcons * 1/3,
    swap_hss_other_disbcurr = all_disbcurr * 1/3,
    swap_hss_other_disbcons = all_disbcons * 1/3,
    swap_hss_other_commcurr = all_commcurr * 1/3,
    swap_hss_other_commcons = all_commcons * 1/3,
    oid_covid_disbcurr = all_disbcurr * 1/3,
    oid_covid_disbcons = all_disbcons * 1/3,
    oid_covid_commcurr = all_commcurr * 1/3,
    oid_covid_commcons = all_commcons * 1/3
)]


dt[is_swp == TRUE, (zero_cols) := 0]
dt[is_swp == TRUE, `:=` (
    final_swap_hss_pp_frct = 1/2,
    final_swap_hss_other_frct = 1/2,
    swap_hss_pp_disbcurr = all_disbcurr * 1/2,
    swap_hss_pp_disbcons = all_disbcons * 1/2,
    swap_hss_pp_commcurr = all_commcurr * 1/2,
    swap_hss_pp_commcons = all_commcons * 1/2,
    swap_hss_other_disbcurr = all_disbcurr * 1/2,
    swap_hss_other_disbcons = all_disbcons * 1/2,
    swap_hss_other_commcurr = all_commcurr * 1/2,
    swap_hss_other_commcons = all_commcons * 1/2
)]


dt[, c("is_cov_swp", "is_swp") := NULL]

#
# TAG CLIMATE PROJECTS BASED ON RIO MARKERS
#
rio_markers <- c("climate_adaptation", "climate_mitigation",
                 "desertification", "biodiversity")

dt[, is_climate := FALSE]
for (marker in rio_markers) {
    dt[!is.na(get(marker)) & get(marker) > 0, is_climate := TRUE]
}


# resave health bil oda 2 with tagged covid & climate projects
save_dataset(dt,
             "B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_2_COVID",
             "CRS", "int")




## inspecting results:
if (FALSE) {
tmp <- dt[eliminations == 0,
          lapply(.SD, sum, na.rm = TRUE),
          .SDcols = grep("_disbcurr$", names(dt), value = TRUE),
          by = .(year, donor_name)]
tmp <- melt(tmp, id.vars = c("year", "donor_name"),
            variable.name = "hfa_pa")
tmp <- tmp[hfa_pa != "all_disbcurr"]
tmp[, hfa_pa := gsub("_disbcurr", "", hfa_pa)]

hfas <- dah_cfg$regular_hfa_vars
hfas[hfas == "swap_hss_total"] <- "swap_hss"
for (.h in hfas)
    tmp[grepl(paste0("^", .h), hfa_pa), hfa := .h]

tmp_hfa <- tmp[, .(value = sum(value, na.rm = TRUE)),
               by = .(year, donor_name, hfa)]

tmp_hfa[value > 0] |>
    ggplot(aes(x = year, y = value, fill = hfa)) +
    geom_col() +
    scale_fill_brewer(palette = "Set3") +
    facet_wrap(~donor_name, scales = "free")
}


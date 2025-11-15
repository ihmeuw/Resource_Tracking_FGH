#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         Combine all pre-created GFATM datasets
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ####################################\n"))
cat(green(" #### GFATM COMBINE ALL DATASETS ####\n"))
cat(green(" ####################################\n\n"))

#### #----#      Merge Disbursements & Commitments with Budgets      #----# ####
cat("  Merge Data before HFAs with HIV, MAL, TB budgets\n")
# Read in primary dataset (`_dc` for "Disbursements & Commitments")
dt_dc <- fread(paste0(get_path("GFATM", "int"), "data_before_hfas.csv"))

# HIV, Malaria, TB budgets
dt_budgets <- fread(paste0(get_path("GFATM", "int"), "budget.csv"))

## Merge Disbusements & Commitments DT with Budgets DT
dt_full <- merge(dt_dc, dt_budgets, by = c("YEAR", "PROJECT_ID", "ISO3_RC"),
                 all = T)

to_calc <- names(dt_full)[names(dt_full) %like% "(DAH|frct)"]
for (col in to_calc) {
  dt_full[, eval(col) := as.double(get(col))]
}
#------------------------------------------------------------------------------#

#### #----# Calculate level 1 HFAs #----# ####
cat("  Calculate level 1 HFAs\n")
hfas <- copy(dt_full)
# Sum cols with hfa-level DAH
hfas[, hfa := rowSums(.SD), .SDcols = c("tb", "mal", "swap_hss", "hiv")]
# Tag by level 1 var
hfas[, `:=` (level1_tb_frct = tb / hfa,
             level1_mal_frct = mal / hfa,
             level1_swap_hss_frct = swap_hss / hfa,
             level1_hiv_frct = hiv / hfa)]
#------------------------------------------------------------------------------#

#### #----# Add in region fractions #----# ####
cat("  Add in GBD region-level fractions\n")
# Read in gbd region-level data
dt_reg <- fread(paste0(get_path("GFATM", "int"), "regional_budget.csv"))

## Merge aggregated budgets data with regional data?
hfas <- merge(hfas, dt_reg, by = "gbd_region", all = T)

# Recalculate fractions
to_calc <- names(hfas)[names(hfas) %like% "_DAH$"]
for (col in to_calc) {
  hfas[, eval(paste0(col, "_frct")) := as.double(get(paste0(col, "_frct")))]
  hfas[is.na(get(paste0(col, "_frct"))),
       eval(paste0(col, "_frct")) := get(paste0(col, "_regionfrct"))]
}
hfas <- hfas[, !c(names(hfas)[names(hfas) %like% "_regionfrct"]), with = F]
#------------------------------------------------------------------------------#

#### #----# Validate HFA assignment #----# ####
cat("  Validate HFA assignment\n")
hdt <- copy(hfas)
# Sum all DAH cols
to_calc <- names(hdt)[names(hdt) %like% "_DAH" & !(names(hdt) %like% "frct")]
hdt <- rowtotal(hdt, "dah_check", to_calc)
hdt[is.na(hiv_treat_DAH_frct) & is.na(hiv_prev_DAH_frct) &
      is.na(hiv_hss_other_DAH_frct) & is.na(hiv_hss_hrh_DAH_frct) &
      is.na(hiv_hss_me_DAH_frct) &
      is.na(hiv_care_DAH_frct) &
      is.na(hiv_ct_DAH_frct) & is.na(hiv_pmtct_DAH_frct) &
      is.na(hiv_ovc_DAH_frct) & is.na(hiv_amr_DAH_frct) & hiv == 1 &
      dah_check==0,
    hiv_other_DAH_frct := 1]
hdt[is.na(tb_treat_DAH_frct) & is.na(tb_diag_DAH_frct) &
      is.na(tb_hss_other_DAH_frct) & is.na(tb_hss_hrh_DAH_frct) &
      is.na(tb_hss_me_DAH_frct) &
      is.na(tb_amr_DAH_frct) & tb == 1 & dah_check==0,
    tb_other_DAH_frct := 1]
hdt[is.na(mal_treat_DAH_frct) & is.na(mal_con_nets_DAH_frct) &
      is.na(mal_diag_DAH_frct) &
      is.na(mal_con_irs_DAH_frct) & is.na(mal_comm_con_DAH_frct) &
      is.na(mal_hss_other_DAH_frct) & is.na(mal_hss_hrh_DAH_frct) &
      is.na(mal_hss_me_DAH_frct) &
      is.na(mal_con_oth_DAH_frct) & is.na(mal_amr_DAH_frct) & mal == 1 &
      dah_check==0,
    mal_other_DAH_frct := 1]
hdt[is.na(swap_hss_pp_DAH_frct) & is.na(swap_hss_hrh_DAH_frct) &
      is.na(swap_hss_me_DAH_frct) & swap_hss == 1 &
      dah_check==0,
    swap_hss_other_DAH_frct := 1]
hdt[DISEASE_COMPONENT == "TB/HIV" & dah_check==0, hiv_other_DAH_frct := 0.5]
hdt[DISEASE_COMPONENT == "TB/HIV" & dah_check==0, tb_other_DAH_frct := 0.5]
hdt[DISEASE_COMPONENT == "Multicomponent" & dah_check == 0,
    `:=`(hiv_other_DAH_frct = (1/3),
         tb_other_DAH_frct = (1/3),
         mal_other_DAH_frct = (1/3))]
hdt[DISEASE_COMPONENT == "RSSH" & dah_check==0, swap_hss_other_DAH_frct := 1]
hdt <- hdt[, !c(names(hdt)[names(hdt) %like% "check"]), with = F]

# Recalculate fractions
to_calc <- names(hdt)[names(hdt) %like% "_DAH_frct"]
hdt <- rowtotal(hdt, "total_frct", 
                names(hdt)[names(hdt) %like% "_DAH_frct"])
for (col in to_calc) {
  hdt[, eval(col) := get(col) / total_frct]
}
hdt[, total_frct := NULL]
hdt <- rowtotal(hdt, "total_frct",
                to_calc)
# Rename
setnames(hdt, names(hdt)[names(hdt) %like% "_DAH"], 
         gsub("_DAH", "", names(hdt)[names(hdt) %like% "_DAH"]))
hdt <- hdt[, !c(names(hdt)[names(hdt) %like% "level1" | 
                             names(hdt) %like% "_DAH"]), with=F]
# Collapse sum
hdtm <- collapse(hdt, "mean", c("YEAR", "PROJECT_ID", "ISO3_RC", 
                                "RECIPIENT_COUNTRY", "DISEASE_COMPONENT", 
                                "PR_TYPE", "RECIPIENT_AGENCY", "ELIM_CH", 
                                 "shortdescription",
                                "projecttitle", "longdescription"),
                 c("COMMITMENT", names(hdt)[names(hdt) %like% "frct"]))
hdts <- collapse(hdt, "sum", c("YEAR", "PROJECT_ID", "ISO3_RC", 
                               "RECIPIENT_COUNTRY", "DISEASE_COMPONENT",
                               "PR_TYPE", "RECIPIENT_AGENCY", "ELIM_CH", 
                               "shortdescription",
                               "projecttitle", "longdescription"),
                 "DAH")
hdt <- merge(hdtm, hdts, by=c("YEAR", "PROJECT_ID", "ISO3_RC", "RECIPIENT_COUNTRY",
                              "DISEASE_COMPONENT", "PR_TYPE",
                              "RECIPIENT_AGENCY", "ELIM_CH",
                              "shortdescription",
                              "projecttitle", "longdescription"), all=T)
rm(hfas, hdtm, hdts)
#------------------------------------------------------------------------------#

#### #----# Fix negative values #----# ####
cat("  Fix negative values\n")
noneg <- copy(hdt)
noneg[DAH < 0, neg2 := DAH, by="PROJECT_ID"]
noneg[, neg := sum(neg2, na.rm=T), by="PROJECT_ID"]
noneg[neg > 0, neg := NA]
noneg[DAH < 0, DAH := 0]
noneg[, total_DAH := sum(DAH, na.rm=T), by="PROJECT_ID"]
noneg[, prop := DAH / total_DAH, by="PROJECT_ID"]
noneg[!is.na(neg), DAH := DAH + (prop * neg)]
noneg[, `:=`(total_DAH = NULL, neg = NULL, prop = NULL)]
#------------------------------------------------------------------------------#

#### #----# Distribute disbursement across HFAs #----# ####
cat("  Distribute disbursement across HFAs\n")
to_calc <- c("hiv_amr", "hiv_treat", "hiv_prev", "hiv_pmtct", "hiv_other",
             "hiv_care", "hiv_ovc", "hiv_ct", "hiv_hss_other",
             "hiv_hss_hrh", "hiv_hss_me",
             "tb_hss_other", "tb_hss_hrh", "tb_treat", "tb_diag", "tb_amr",
             "tb_other", "tb_hss_me",
             "mal_hss_other", "mal_hss_hrh", "mal_diag", "mal_con_nets",
             "mal_con_irs", "mal_con_oth", "mal_hss_me",
             "mal_treat", "mal_comm_con", "mal_amr", "mal_other",
             "swap_hss_other", "swap_hss_pp", "swap_hss_hrh", "swap_hss_me")

noneg <- rowtotal(noneg, "valid_pas_total_frct",
                  paste0(to_calc, "_frct"))
noneg[, eval(paste0(to_calc, "_frct")) :=
        {temp = rowSums(.SD) ; (.SD / valid_pas_total_frct) * (valid_pas_total_frct > 0)},
      .SDcols = paste0(to_calc, "_frct")]
for (col in to_calc) {
  noneg[, eval(paste0(col, "_DAH")) := DAH * get(paste0(col, "_frct"))]
  noneg[is.na(get(paste0(col, "_DAH"))), eval(paste0(col, "_DAH")) := 0]
}
#------------------------------------------------------------------------------#


#### #----# Save dataset #----# ####
cat("  Save dataset\n")
save_dataset(noneg, "noneg", "GFATM", "int")
#------------------------------------------------------------------------------#

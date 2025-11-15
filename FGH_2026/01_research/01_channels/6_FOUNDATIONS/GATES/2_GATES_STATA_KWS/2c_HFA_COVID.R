#----# Docstring #----# ####
# Project:  FGH 
# Purpose:  Run covid keyword search on covid projects received from GF
#---------------------# 
# ==== Env ====
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))



# ==== PATHS ====
covid_disb_path <- file.path(
  get_path("BMGF", "int"),
  paste0("BMGF_DISB_FINAL_1999_", report_year, "_covid.csv")
)

# ==== UTILS ====
# manual keyword search for R&D keywords
rd_keywords <- toupper(
  c("understand", "develop", "measure", "advance", "scientific tools",
    "evidence", "assess", "screen", "generate insights", "characteriz",
    "investigate", "determine", "dosing recommendations", "estimate",
    "data analyses", "production", "manufactur", "university"))

r_d_search <- function(dt, kws_cols) {
  srch_cols <- paste0(kws_cols, "_srch")
  # search for each keyword in each srch kws col, sum total keyword count
  for (i in seq_along(kws_cols)) {
    scol <- srch_cols[i]
    kcol <- kws_cols[i]
    dt[, eval(scol) := 0]
    for (kw in rd_keywords) {
      dt[, eval(scol) := get(scol) + grepl(kw, toupper(get(kcol)))]
    }
  }
  # compute total keyword tags for each project
  dt[, rd_tag := rowSums(.SD), .SDcols = srch_cols]
  return(dt[, -srch_cols, with = FALSE])
}


# Distribute outflow to each COVID HFA based on keyword proportion
distribute_disbursement <- function(dt) {
  dthfa <- data.table::copy(dt)
  dthfa[, `:=` (COVID_total = NULL, COVID_total_prop = NULL)]
  
  hfaprops <- grep("_prop", names(dthfa), value = TRUE)
  hfaamts <- paste0(gsub("_prop", "", hfaprops), "_amt")
  for (i in seq_along(hfaprops)) {
    # total disbursement for the project times the proportion in each hfa
    dthfa[, eval(hfaamts[i]) := disbursement * get(hfaprops[i])]
  }
  dthfa[, (hfaprops) := NULL]
  
  dthfa[, total_amt := rowSums(.SD), .SDcols = hfaamts]
  # test that props still add up
  check <- round(dthfa$disbursement, 2) != round(dthfa$total_amt, 2)
  if (sum(check, na.rm = T) != 0) {
    stop("Error distributing disbursements to COVID HFAs")
  }
  return(dthfa)
}

# ==== MAIN ====

# ==== prep data
dt <- data.table::fread(covid_disb_path, encoding = "Latin-1")
names(dt) <- tolower(names(dt))
dt[iso3_rc == "N/A", iso3_rc := "G"]

# isolate columns needed for compiling
dt <- dt[, .(
  year,
  channel,
  elim_ch,
  reporting_agency = "BMGF",
  iso3_rc,
  donor_country,
  donor_name,
  recipient_country,
  recipient_agency,
  money_type = "new money",
  grant_loan = "grant",
  commitment = 0,
  disbursement = outflow,
  # and for keyword search
  project_title, short_description, long_description
)]
setnafill(dt, fill = 0, cols = "elim_ch")


# run covid keyword search
cat("Running COVID keyword search\n")
dt <- covid_kws(dt,
                keyword_search_colnames = c("long_description",
                                            "short_description",
                                            "project_title"),
                keep_clean = TRUE,
                keep_counts = FALSE,
                languages = "english")

# replace 'other' projects with r&d, where appropriate
## run r&d kws
# run covid keyword search
cat("Running COVID keyword search\n")
dt <- covid_kws(dt,
                keyword_search_colnames = c("long_description",
                                            "short_description",
                                            "project_title"),
                keep_clean = TRUE,
                keep_counts = FALSE,
                languages = "english")

# replace 'other' projects with r&d, where appropriate
## run r&d kws
dt[, upper_recipient_agency := string_to_std_ascii(recipient_agency)]
dt[, upper_long_description := string_to_std_ascii(long_description)]
dt <- r_d_search(dt, kws_cols = c("upper_long_description", "upper_recipient_agency"))
dt[recipient_agency %like% "United Nations Development Program",
   rd_tag := 0]
dt[other_prop == 1 & rd_tag > 0, `:=` (`r&d_prop` = 1, other_prop = 0)]
dt[, rd_tag := NULL]


# cleanup
dt[, grep("^upper_", names(dt)) := NULL]
cat("Saving datasets")
# ==== save data
save_dataset(dt, "COVID_descriptions_with_hfas", "BMGF", "fin")


# ==== calculate outflow by covid health focus area
dthfa <- distribute_disbursement(dt)
hfacols <- grep("_amt", names(dthfa), value = TRUE)

# finalize & save
names(dthfa) <- toupper(names(dthfa))

save_dataset(dthfa, "OECD_COVID_prepped", "BMGF", "fin")
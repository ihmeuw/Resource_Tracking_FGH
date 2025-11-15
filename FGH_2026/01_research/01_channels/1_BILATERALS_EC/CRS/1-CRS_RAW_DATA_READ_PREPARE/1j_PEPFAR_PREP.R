#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  CRS PEPFAR data prep
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(crayon)
#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' #########################\n'))
cat(green(' #### CRS PEPFAR PREP ####\n'))
cat(green(' #########################\n\n'))


cat('  Read in PEPFAR OU Budgets\n')
#----# Read in PEPFAR OU Budgets #----# ####
dt <- fread(get_path('PEPFAR', 'raw', 'PEPFAR_OU_Budgets_by_Financial_Classifications.txt'),
            header = TRUE)
colnames(dt) <- gsub(' ', '_', tolower(colnames(dt)))
#---------------------------------------------------------------------#

cat('  Clean dataset\n')
#----# Clean dataset #----# ####
# reformat in csv so that all the desired headers are the top row (move all years up one row) and years are changed to YR_year
dt <- dt[country != '' & country != "Total"
         & operating_unit != 'Total' & program != 'Total', ]

dt[, program := fcase(
    program == "PREV", "Prevention",
    program == "C&T", "Care and Treatment",
    program == "SE", "Socio-Economic",
    program == "ASP", "Above-site programs",
    program == "HTS", "Testing",
    program == "PM", "Program Management",
    rep_len(TRUE, .N), program
)]

# pivot the data from wide to long
dt <- melt(dt,
           id.vars = c("operating_unit", "country", "iso3",
                       "program", "sub_program", "beneficiary", "sub_beneficiary"),
           measure.vars = patterns("^20"),
           variable.name = "YEAR",
           value.name = "amount",
           variable.factor = FALSE)
dt[, YEAR := as.integer(YEAR)]
dt[, amount := as.numeric(gsub(',|\\$', '', trimws(amount)))]

# aggregate, keeping only desired level of granularity
dt <- dt[, .(amount = sum(amount, na.rm = TRUE)),
         by = .(YEAR, operating_unit, country, iso3,
                program, sub_program, beneficiary, sub_beneficiary)]



#
# Map financial classification categories to HFA-PAs
#
cat("  Mapping financial classifications to HFA-PAs\n")

# load and prepare the program to HFA-PA mapping
fp <- file.path(dirname(get_path("pepfar", "raw")),
                "shared",
                "financial_classifications_to_hfas.xlsx")
pmap <- openxlsx::read.xlsx(fp)
setDT(pmap)
pmap <- pmap[, -"notes"]
hfa_cols <- grep("program", names(pmap), value = TRUE, invert = TRUE)
setnafill(pmap, fill = 0L, cols = hfa_cols)


## ensure that all entries in the map sum to 1
pmap[, tst := rowSums(.SD), .SDcols = hfa_cols]
stopifnot(pmap[tst != 1, .N] == 0)
pmap[, tst := NULL]

# merge the program to HFA-PA mapping with the CRS data
dt <- merge(dt, pmap,
            by = c("program", "sub_program"),
            all.x = TRUE)

# check beneficiary columns for OVC
dt[, hiv_ovc := 0]
dt[(beneficiary == "OVC" | sub_beneficiary == "Orphans & vulnerable children") &
       drop != 1,
   (hfa_cols) := lapply(.SD, \(x) x * 0.5),
   .SDcols = hfa_cols]
dt[(beneficiary == "OVC" | sub_beneficiary == "Orphans & vulnerable children") &
       drop != 1,
   hiv_ovc := 0.5]
hfa_cols <- c(hfa_cols, "hiv_ovc")

# check beneficiary column for PMTCT
dt[, hiv_pmtct := 0]
dt[beneficiary == "Pregnant & Breastfeeding Women" & drop != 1,
   (hfa_cols) := lapply(.SD, \(x) x * 0.5),
   .SDcols = hfa_cols]
dt[beneficiary == "Pregnant & Breastfeeding Women" & drop != 1,
   hiv_pmtct := 0.5]
hfa_cols <- c(hfa_cols, "hiv_pmtct")


# pivot the HFA_PAs from wide to long
dt <- melt(dt,
           id.vars = c("YEAR", "operating_unit", "country", "iso3",
                       "program", "sub_program", "beneficiary", "sub_beneficiary",
                       "amount"),
           variable.name = "hfa_pa",
           value.name = "prop")

# ensure the HFA_PA proportions sum to 1
stopifnot(
    dt[, sum(prop),
       by = .(YEAR, operating_unit, country,
              program, sub_program,
              beneficiary, sub_beneficiary)
       ][, unique(V1)] == 1
)

# apply the proportions to the funding amounts
dt[, amount := amount * prop]
dt[, prop := NULL]


# aggregate funding to country, year, hfa_pa level
dt <- dt[, .(amount = sum(amount, na.rm = TRUE)),
         by = .(operating_unit, country, iso3, hfa_pa, YEAR)]


if (interactive()) {
    
    # Prepare budget code data as it used to be used
    bcd <- fread(get_path('PEPFAR', 'raw', 'PEPFAR_OU_Budgets_by_Budget_Code.txt'),
                 header = TRUE)
    colnames(bcd) <- gsub(' ', '_', tolower(colnames(bcd)))
    bcd <- melt(bcd,
                id.vars = c("operating_unit", "country", "iso3", "budget_code", "record_type"),
                variable.name = "YEAR",
                value.name = "amount",
                variable.factor = FALSE)
    bcd[, YEAR := as.integer(YEAR)]
    bcd[, amount := as.numeric(gsub(',|\\$', '', trimws(amount)))]
    bcd[budget_code %in% c("HBHC", "PDCS"),
        hfa_pa := "hiv_care"]
    bcd[budget_code == "HVCT",
        hfa_pa := "hiv_ct"]
    bcd[budget_code %in% c("HLAB", "HVSI", "OHSS"),
        hfa_pa := "hiv_hss_other"]
    bcd[budget_code == 'HKID',
        hfa_pa := "hiv_ovc"]
    bcd[budget_code %in% c("CIRC", "HMBL", "HMIN", "HVAB", "HVOP", "IDUP"),
        hfa_pa := "hiv_prev"]
    bcd[budget_code %in% c("HTXD", "HTXS", "PDTX"),
        hfa_pa := "hiv_treat"]
    bcd[budget_code == "MTCT",
        hfa_pa := "hiv_pmtct"]
    bcd[budget_code == "HVMS",
        hfa_pa := "hiv_split"]
    bcd[budget_code == "NOT SPECIFIED",
        hfa_pa := "hiv_split"]
    bcd <- bcd[! budget_code %in% c("HVTB", "APPLIED PIPELINE", "Total", "")]
    
    # compare budget-code data with financial classification data
    old <- bcd[, .(old = sum(amount, na.rm = TRUE)),
               by = .(YEAR, hfa_pa)]
    new <- dt[, .(new = sum(amount, na.rm = TRUE)),
              by = .(YEAR, hfa_pa)]
    cmp <- merge(old, new, by = c("YEAR", "hfa_pa"), all = TRUE)
    
    # total envelope
    cmp[hfa_pa != "drop",
        .(old=sum(old, na.rm = T), new=sum(new, na.rm = T)), by = YEAR] |>
        ggplot(aes(x = YEAR)) +
        geom_line(aes(y = old, color = "old")) +
        geom_line(aes(y = new, color = "new"))
    
    # hfa fractions
    tmp <- melt(cmp[hfa_pa != "drop"], id.vars = c("YEAR", "hfa_pa"))
    tmp[, tot := sum(value, na.rm = T), by = .(YEAR, variable)]
    ggplot(tmp, aes(x = YEAR, y = value/tot, fill = hfa_pa)) +
        geom_bar(stat = "identity") +
        facet_wrap(~variable, scales = "free",
                   labeller = as_labeller(c(old = "Budget Code Data",
                                            new = "Financial Classification Data"))) +
        labs(
            title = "PEPFAR - HIV Program Area Fractions from Budget Data",
            x = "", y = "Fraction of Total Spending", fill = "HIV Program Area"
        ) +
        theme_bw(16) +
        coord_cartesian(expand = FALSE)
}


fin <- dt[hfa_pa != "drop"]

#---------------------------------------------------------------------#

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(fin, 'PEPFARBudgetaryCOP[report_year]_new', 'PEPFAR', 'raw')
#---------------------------------------------------------------------#

cat('\n\n\t ###############################\n\t #### file  CRS 1j complete ####\n\t ###############################\n\n')

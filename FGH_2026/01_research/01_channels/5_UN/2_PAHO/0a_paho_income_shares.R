
code_repo <-'FILEPATH'



report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


# Load income and expenditure extractions
income <- as.data.table(openxlsx::read.xlsx(
    get_path("paho", "raw", "M_PAHO_INC_EXP_1990_2023.xlsx"),
    sheet = "Income"
))


expend <- as.data.table(openxlsx::read.xlsx(
    get_path("paho", "raw", "M_PAHO_INC_EXP_1990_2023.xlsx"),
    sheet = "Stata"
))

# load donor assessed contributions
donor <- fread(get_path("paho", "raw", "M_PAHO_INC_DONOR_1990_2023.csv"))
donor_pub <- donor[INCOME_SECTOR == "PUBLIC" & DONOR_NAME != "CENTERS"]
donor_pub[, YEAR := gsub("-", "_", YEAR)]

donor_totals <- donor_pub[,
                         .(assessed_contrib = sum(INCOME, na.rm = TRUE)),
                         by = YEAR]


#
# Calculate "OTHER" income using income extractions
#
income <- merge(
    income,
    donor_totals,
    by = "YEAR",
    all.x = TRUE
)

## INCOME_PAHO_REG_BUDGET == total income - INCOME_WHO, so add back INCOME_WHO,
##  and then, add INCOME_ELIMINATIONS, which is a negative quantity that reduces
##  the total INCOME amount.
income[, total_income := INCOME_PAHO_REG_BUDGET + INCOME_WHO + INCOME_ELIMINATION]
## Total income is assessed contribution + WHO income + centers income + OTHER,
## where OTHER consists of voluntary contributions and miscellaneous (e.g., interest income)
income[, accounted_income := assessed_contrib + INCOME_WHO + INCOME_CENTERS]
## thus, the differences between total income and accounted income is "OTHER",
## however we also subtract off revolving fund expenditure because this is funding
## that comes into PAHO from countries and then is spent in those same countries,
## so it doesn't count as DAH and thus doesn't contributed to DAH income shares.
income[, unaccounted_income := total_income - accounted_income - RF_EXP]


# Cobmine WHO, CENTERS, and OTHER income with assessed contributions
don_inc <- income[, .(YEAR, INCOME_WHO, INCOME_CENTERS, unaccounted_income)]
don_inc <- melt(don_inc,
                id.vars = "YEAR",
                variable.name = "type", value.name = "INCOME")
setnafill(don_inc, fill = 0, col = "INCOME")
don_inc <- don_inc[INCOME != 0]


don_inc[type == "INCOME_WHO", `:=`(
    INCOME_SECTOR = "UN",
    INCOME_TYPE = "WHO",
    DONOR_NAME = "WHO",
    DONOR_COUNTRY = NA_character_,
    ISO_CODE = NA_character_
)]
don_inc[type == "INCOME_CENTERS", `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "CENTRAL",
    DONOR_NAME = "CENTERS",
    DONOR_COUNTRY = NA_character_,
    ISO_CODE = NA_character_
)]
don_inc[type == "unaccounted_income", `:=`(
    INCOME_SECTOR = "OTHER",
    INCOME_TYPE = NA_character_,
    DONOR_NAME = "Other PAHO Income",
    DONOR_COUNTRY = NA_character_,
    ISO_CODE = NA_character_
)]



## merge on "ADJ_EXP"
don_inc <- merge(
    don_inc,
    unique(expend[, .(YEAR, EXPENDITURE = ADJ_EXP)]),
    by = "YEAR", all.x = TRUE
)


## merge on "EXP_REG":
## for WHO - EXPENDITURE_WHO
## for CENTERS - EXPENDITURE_CENTERS
don_inc <- merge(
    don_inc,
    expend[, .(YEAR, EXPENDITURE_WHO, EXPENDITURE_CENTERS)],
    by = "YEAR", all.x = TRUE
)
don_inc[type == "INCOME_WHO", EXP_REG := EXPENDITURE_WHO]
don_inc[type == "INCOME_CENTERS", EXP_REG := EXPENDITURE_CENTERS]


## finalize
don_inc[, `:=`(
    CHANNEL = "PAHO",
    SOURCE_DOC = "Financial Report - Processed in R",
    type = NULL,
    EXPENDITURE_WHO = NULL,
    EXPENDITURE_CENTERS = NULL
)]

don_fin <- rbind(donor_pub, don_inc, fill = TRUE)
don_fin[, YEAR := gsub("_", "-", YEAR)]

don_fin <- don_fin[, c(
    "YEAR", "CHANNEL", "SOURCE_DOC", "INCOME_SECTOR", "INCOME_TYPE",
    "DONOR_NAME", "DONOR_COUNTRY", "ISO_CODE", "INCOME", "EXPENDITURE", "EXP_REG"
), with = FALSE]


fwrite(
    don_fin,
    get_path("paho", "raw", "paho_inc_donor_processed.csv")
)

cat("* Saved processed PAHO income and donor data to",
    get_path("paho", "raw", "paho_inc_donor_processed.csv"), "\n")


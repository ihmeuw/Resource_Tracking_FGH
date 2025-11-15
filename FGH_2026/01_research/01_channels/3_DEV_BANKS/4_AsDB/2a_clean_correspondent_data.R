#
# Project: FGH
# Channel: AsDB
#
# Intake data received from correspondent
#

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


# last year of available correspondent data
corr_data_year <- 2021
#
data_date <- "DATE"
prev_yr_data_date <- "DATE"
# correspondent data file
corr_file <- "DAH_20222202.xlsx"




#
# Step 1: import data from correspondence
#
raw_data <- openxlsx::read.xlsx(get_path("asdb", "raw", corr_file),
                                sep.names = ".")
setDT(raw_data)
names(raw_data) <- gsub(".", "", names(raw_data), fixed = TRUE)

# rename columns
setnames(raw_data,
         c("No", "Year"),
         c("projid", "YEAR_OF_START"))

a_cols <- grep("^a", names(raw_data), value = TRUE)
dah_cols <- gsub("^a", "DAH_", a_cols)
setnames(raw_data, a_cols, dah_cols)


#
# Step 2: clean data
#
raw_data <- raw_data[DMCs != ""]


# Destring DAH columns
raw_data[DAH_2004_OCR == "9.527280.78", DAH_2004_OCR := "9.52728078"]
raw_data[DAH_2018_COL == "7.44897.18", DAH_2018_COL := "7.4489718"]

dah_cols <- grep("^DAH_", names(raw_data), value = TRUE)
raw_data[, (dah_cols) := lapply(.SD, as.character), .SDcols = dah_cols]

# pivot longer
raw <- melt(raw_data,
            measure.vars = dah_cols,
            variable.factor = FALSE)

# adjust specific missing values
raw[value == "-", fix_value := TRUE]
raw[value == "-", value := NA]
raw[, value := as.numeric(value)]

raw[fix_value == TRUE & projid == "2785",
    value := AmountforHealth -
        (0.567 + 1.36 + 2.477 + 3.079 + 4.933 + 3.759 + 1.215654 + 0.289) 
    ]

raw[fix_value == TRUE & projid == "3506",
    value := AmountforHealth - 0]

raw[fix_value == TRUE & projid == "3600",
    value := AmountforHealth - 0.683]

raw[fix_value == TRUE & projid == "3666",
    value := Total_COL_mil - 0]

raw[, fix_value := NULL]



#
# Step 3: distribute disbursements
#

# aggregate dah
raw[, year := as.integer(sapply(strsplit(variable, split = "_"), `[`, 2))]
group_cols <- setdiff(names(raw), c("variable", "value"))
raw <- raw[, .(DAH = sum(value, na.rm = TRUE)), by = group_cols]


# reallocate DAH for projects with multiple DMCs (recipient)
## manual fixes
raw[, DMCs := fcase(
    projid == "9571", "S6",  # GBD: Southeast Asia, East Asia, and Oceania 
    projid == "3464", "CAM",
    projid == "3466", "MYA",
    projid == "3467", "VIE",
    projid == "787", "S6",
    projid == "3465" | projid == "G-516", "LAO",
    projid == "6556" | projid == "6557", "S5, S6",
    rep_len(TRUE, .N), DMCs
)]

## split multi-recipients evenly
raw[, group_id := .I]
group_cols <- setdiff(names(raw), "DMCs")

tot <- sum(raw$DAH, na.rm = TRUE)

raw <- raw[, .(DMCs = unlist(strsplit(DMCs, ", "))),
           by = group_cols]

raw[, nsplit := .N, by = group_id]
raw[, DAH := DAH / nsplit]

## ensure total is preserved
stopifnot( sum(raw$DAH, na.rm = TRUE) == tot )
rm(tot)

raw[, DMCs := trimws(DMCs)]


## re-distribute negative disbursements
setorder(raw, projid, year)
raw[, dah_lead := shift(DAH, type = "lead"), by = .(projid, DMCs)]

raw[projid == "G-26" & year == 2008,
    DAH := DAH + dah_lead] # subtract from 2008
raw[projid == "G-26" & year == 2009,
    DAH := 0]


raw[projid == "G-26" & year == 2010,
    DAH := DAH + dah_lead] # subtract from 2008
raw[projid == "G-26" & year == 2011,
    DAH := 0]

if (raw[DAH < 0, .N] != 0)
    stop("Negative disbursements remain. Resolve.")
raw[, dah_lead := NULL]


save_dataset(raw, "asdb_correspondent_disbs",
             channel = "asdb",
             stage = "int")




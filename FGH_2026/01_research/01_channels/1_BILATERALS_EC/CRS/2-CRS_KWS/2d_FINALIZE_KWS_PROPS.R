#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Finalize CRS initial keyword search
#---------------------# ####
#   Since we use long strings in the key word search process, Stata saves certain
#   variables in their "long string" format (see
#       https://www.stata.com/features/overview/long-strings/).
#   The 'haven' package is notoriously bad with long strings, and will error,
#   but 'readstata13' can read them, it just takes a (very) long time when the
#   data is large.
#   The package authors are working on an update to address this, so check github
#   and see if the updated package is available.
#   (See PR https://github.com/sjewo/readstata13/pull/88)
#   Until then, the work around is to convert the file to a CSV in Stata using a
#   batch job.
#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(readstata13, crayon)


SEARCH_COLS <- c("project_title", "short_description", "long_description",
                 "channel_name", "channel_reported_name")

#----------------------------# ####


cat('\n\n')
cat(green(' ####################################\n'))
cat(green(' #### CRS KWS PROPS FINALIZATION ####\n'))
cat(green(' ####################################\n\n'))


cat('  Read in post-create_kws_props data\n')
#
# HELPERS ====
#

# If the DTA file is too large, there can be trouble reading it into R.
# We can use STATA to convert from DTA to CSV.
# This launches a short cluster job which runs the dta_to_csv.ado converter on
# the provided filepath (fp).
# (Calling the stata script directly from R, for example with `system`, will fail
# because the environment your R session is running in does not have access to
# stata)
launch_dta_to_csv <- function(fp, mem = "10G", time = "0:05:00") {
    # check file path for errors before submitting job
    if (! file.exists(fp))
        stop(paste("Provided DTA file doesn't exist:", fp))
    fp_split <- strsplit(fp, ".", fixed = TRUE)[[1]]
    ext <- fp_split[[length(fp_split)]]
    if (tolower(ext) != "dta")
        stop(paste("Provided file must be a DTA file, not a", ext, "file."))
    # prep sbatch command and submit job to cluster
    cmd <- paste(
        file.path(code_repo, "shellstata15.sh"),
        file.path(code_repo,
                  paste0("FGH_", report_year),
                  "FILEPATHS/dta_to_csv.ado"),
        fp
    )
    sbatch <- paste(
        "sbatch -e ~/crs2d.err -o ~/crs2d.out -J crs_dta2csv",
        "-C archive -p all.q -A proj_fgh -c 1 --mem", mem, "-t", time,
        cmd)
    cat(green("*** Sending command to system:\n"))
    cat(sbatch, fill = TRUE)
    system(sbatch)
}


# query the names of running jobs for the user
query_jobs <- function(chars = 30) {
    # (format spec ensures we just get job names, with up to chars characters)
    jobs <- trimws(
        system(paste0("squeue --format='%.", chars,"j' --me"), intern = TRUE)
    )[-1] # drop header
    return(jobs)
}


# file paths
input_dta <- file.path(get_path("CRS", "int"), "crs_ckps_post_kws.dta")
input_csv <- gsub(".dta", ".csv", input_dta, fixed = TRUE)

# launch conversion job
if (file.exists(input_csv))
    file.remove(input_csv) # ensure it gets replaced
cat("Converting DTA to CSV:\n")
launch_dta_to_csv(input_dta,
                  mem = "10G", time = "0:10:00")

# wait for conversion to finish
while("crs_dta2csv" %in% query_jobs()) {
    cat("...Job still active, will check again in 2 minutes...\n")
    Sys.sleep(60 * 2)
}

# confirm success
if (!file.exists(input_csv))
    stop("Conversion of DTA to CSV failed. Check logs.")

cat("Conversion complete.\n")


#----# Read in post-create_kws_props data #----# ####
# # if the dta file can be loaded directly into R:
# dt <- read.dta13(file.path(get_path('CRS', 'int'), 'crs_ckps_post_kws.dta'))
dt <- data.table::fread(input_csv, strip.white = FALSE)

# load the pre-kws file to compare:
orig <- fread(get_path("crs", "int", "CRS_[crs.update_mmyy]_before_keywordsearch.csv"),
              select = 1)
nrow_orig <- nrow(orig)
rm(orig)

if (nrow(dt) != nrow_orig)
    stop("Number of rows in post-kws data does not match original data.")

#----------------------------------------------# ####
#
# convert stata dates
#
# the keyword search process utilizes stata and stata likes to convert dates
# into its own integer format. If this happens, it will mess things up in later
# scripts. So we need to convert them back to ISO standardized dates for R.
# Best to check manually if this is needed.
# load the original date columns plus the unique row id assigned to each row
# before
datemap <- arrow::read_parquet(get_path("crs", "raw", "crs_date_map.parquet"))
dt[, `:=` (
    completion_date = NULL,
    commitment_date = NULL,
    expected_start_date = NULL,
    repaydate1 = NULL,
    repaydate2 = NULL
)]
# merge the original dates back in
dt <- merge(dt, datemap, by = "fgh_id", all.x = TRUE)
rm(datemap)


cat('  Allocate commitments & disbursements\n')
#----# Allocate commitments & disbursements #----# ####
to_calc <- names(dt)[names(dt) %like% 'final_' & names(dt) %like% '_frct' &!(names(dt) %like% 'total')]
to_calc <- gsub('final_', '', gsub('_frct', '', to_calc))

for (col in to_calc) {
  dt[, eval(paste0(col, '_commcurr')) := get(paste0('final_', col, '_frct')) * commitment_current]
  dt[, eval(paste0(col, '_commcons')) := get(paste0('final_', col, '_frct')) * commitment_constant]
  dt[, eval(paste0(col, '_disbcurr')) := get(paste0('final_', col, '_frct')) * disbursement_current]
  dt[, eval(paste0(col, '_disbcons')) := get(paste0('final_', col, '_frct')) * disbursement_constant]
  dt[is.na(commitment_current), eval(paste0(col, '_commcurr')) := 0]
  dt[is.na(commitment_constant), eval(paste0(col, '_commcons')) := 0]
  dt[is.na(disbursement_current), eval(paste0(col, '_disbcurr')) := 0]
  dt[is.na(disbursement_constant), eval(paste0(col, '_disbcons')) := 0]
}
#------------------------------------------------# ####

cat('  Save dataset\n')
#----# Save dataset #----# ####
save_dataset(dt,
             'B_CRS_DAH_HFAS_withprops_[crs.update_mmyy]',
             'CRS', 'int')
#------------------------# ####

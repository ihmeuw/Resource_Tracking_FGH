#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Match and combine projects in the CRS.
#           Projects are reported to the CRS every year there is a disbursement.
#           However, some projects have more than 1 disbursement reported in a given year.
#           Also, some projects are often reported twice because the DAH committed is recorded separately from the DAH disbursed.
#           We want to combine and consolidate all these entries into a single identical project per year.
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(crayon)

#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' ############################\n'))
cat(green(' #### CRS MATCH PROJECTS ####\n'))
cat(green(' ############################\n\n'))

#---------------------------------------------------------------------#

cat('  Pull input data\n')
#----# Pull input data #----# ####
dt <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_1.csv'))
#---------------------------------------------------------------------#

# NOTE: think we can do 2 stages with separate group cols - finding duplicates
# should include more group cols (e.g., the date columns like in the above list).
# The imputing of missing descriptive cols can use a smaller set of group cols


#----# Clean data #----# ####
cat('  Identify and aggregate projects\n')

group_cols <- c('crs_id', 'donor_code', 'agency_code', 'recipient_code', 'year')
description_cols <- c('project_title', 'short_description', 'long_description',
                      'channel_code', 'channel_name', 'channel_reported_name')
value_cols <- c('commitment_constant', 'disbursement_constant',
                'commitment_current', 'disbursement_current')


# count the number of missing values for each descriptive column
dt[, channel_code := as.character(channel_code)]
for (col in description_cols) {
  dt[get(col) == ''| is.na(get(col)), eval(paste0(col, '_miss')) := 1]
  dt[, eval(paste0('proj_',col, '_miss')) := sum(get(paste0(col, '_miss')), na.rm = TRUE),
     by = group_cols]
}


#
# Identify and drop the subset of projects with duplicate rows that are identical
#
dt[, `:=`(
    dups = .N - 1,
    proj_N = .N,
    proj_n = 1:.N
), by = c(group_cols, description_cols)]

tot1 <- dt[, sum(disbursement_current, na.rm = TRUE)] ## throughout this script, ensute the total disbursement is the same


## compute total commitment and disbursement for each unique activity
## and assign the project totals to the duplicate projects
for (col in value_cols) {
    dt[, eval(paste0('total_',col)) := sum(get(paste0(col)), na.rm = TRUE),
       by = c(group_cols, description_cols)]
    dt[dups > 0,
       eval(paste0(col)) := get(paste0('total_', col))]
}
## then, only keep one copy
dt <- dt[proj_n == 1]

tot2 <- dt[, sum(disbursement_current, na.rm = TRUE)]
stopifnot( abs(tot1 - tot2) < 1 )


#
# Fill in and drop the subset of projects with duplicate rows that are not identical
#
dt[, `:=`(
    proj_N = .N,
    proj_n = 1:.N
), by = group_cols]
max_N <- max(dt$proj_N)
if (max_N != 2) {
    warning("Historically there have been a max of 2 projects per group.",
            " Now there is a max of ", max_N, ". Please investigate.")
}


## compute total commitment and disbursement for each unique activity
## and assign the project totals to the duplicate projects
for (col in value_cols) {
    dt[, eval(paste0('total_',col)) := sum(get(paste0(col)), na.rm = TRUE),
       by = group_cols]
    dt[proj_N > 1,
       eval(paste0(col)) := get(paste0('total_', col))]
}

## fill in descriptive information if it's not reported across all projects
## - there should be a max of 2 projects per year, but collapse with paste in
##   case that changes
for (col in description_cols) {
    dt[get(paste0("proj_", col, "_miss")) > 0,
       eval(col) := paste(
           unique( get(col)[!is.na(get(col)) & get(col) != ''] ),
           collapse = '; '),
       by = group_cols]
}

## only keep the first copy of each project
dt <- dt[proj_n == 1]

tot3 <- dt[, sum(disbursement_current, na.rm = TRUE)]
stopifnot( abs(tot1 - tot3) < 1 )

dt[, c("dups","proj_n", "proj_N",
       paste0("total_", value_cols)) := NULL]
#---------------------------------------------------------------------#

cat('  Match projects & fill descriptive variables\n')
#----# Match projects & fill descriptive variables #----# ####
# b. Second, match projects across all years and fill in descriptive variables
#    as much as possible for all observations
group_cols_2 <- c('donor_code', 'agency_code', 'recipient_code', 'crs_id') # no 'year'

for (col in c('project_title', 'short_description', 'long_description')) {
    dt[,
       eval(paste0('proj_',col, '_miss')) := sum(get(paste0(col, '_miss')), na.rm = TRUE),
       by = group_cols_2]
}

dt[, proj_N := .N,   by = group_cols_2]
dt[, proj_n := 1:.N, by = group_cols_2]

max_N <- max(dt$proj_N)

for (col in description_cols) {
    i <- 1
    m <- 2
    while(i < max_N) {
        dt[get(paste0('proj_', col, '_miss')) == i & proj_N == m,
           eval(col) := paste(
               unique( get(col)[!is.na(get(col)) & get(col) != ''] ),
               collapse = '; '
           ),
           by = group_cols_2]
        i <- i + 1
        m <- m + 1
    }
}

dt[, channel_code := as.integer(channel_code)]

dt[, c(paste0(description_cols, "_miss"),
       paste0("proj_", description_cols, "_miss"),
       "proj_N", "proj_n") := NULL]
#---------------------------------------------------------------------#

cat('  Save Stage 1f Data\n')
#----# Save Stage 1f data #----# ####
save_dataset(dt, paste0('1f_proj_match'), 'CRS', 'int')
#---------------------------------------------------------------------#

cat('\n\n\t ###############################\n\t #### file  CRS 1f complete ####\n\t ###############################\n\n')

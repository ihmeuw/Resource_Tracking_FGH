
code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


#
# Clean up keyword search results
#
post_kws <- readstata13::read.dta13(
    get_path("compiling", "int", "fa_post_kws.dta")
)
setDT(post_kws)

## clean up
post_kws[, final_total_frct := NULL]
final_frct_cols <- grep("final_.+_frct$", names(post_kws), value = TRUE)
id_cols <- c(
    "year",
    "international_sector_name",
    "international_purpose_name",
    "country_code",
    "country_name",
    "activity_project_number",
    "activity_name",
    "activity_description",
    "multilat",
    "aid_type_group_name",
    "disb"
)

## pivot long
post_kws <- melt(
    post_kws,
    id.vars = id_cols,
    measure.vars = final_frct_cols,
    variable.name = "pa",
    value.name = "frct"
)
post_kws[, pa := gsub("final_|_frct", "", pa)]

## ensure fractions sum to about 1
stopifnot( post_kws[, sum(frct), by = id_cols][abs(V1 - 1) > 1e-6, .N] == 0 )
## normalize fractions for more precision
post_kws[, frct := frct / sum(frct), by = id_cols]

## allocate disbursements
post_kws[, disb := disb * frct]
post_kws[, frct := NULL]
rm(final_frct_cols, id_cols)

post_kws[, hfa := tstrsplit(pa, "_", keep = 1)]
post_kws[hfa == "swap", hfa := "swap_hss_total"]


## Save
post_kws <- post_kws[disb > 0]
fwrite(
    post_kws,
    get_path("compiling", "int", "fa_projects_post_kws.csv")
)

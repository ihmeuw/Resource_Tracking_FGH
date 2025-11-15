
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))




inc <- fread(get_path("afdb", "raw", "AFDB_INCOME_SHARES_2017_2020.csv"))

## no share data past 2020, so use weighted average over 2017 to 2020
tots <- inc[, .(AMOUNT_PAID = sum(AMOUNT_PAID)),
            by = .(DONOR_NAME, INCOME_SECTOR)]
tots[, TOTAL_AMOUNT := sum(AMOUNT_PAID)]
tots[, INCOME_SHARE := 100 * AMOUNT_PAID / TOTAL_AMOUNT]


tots <- tots[, .(YEAR = seq(2021, report_year)),
             by = names(tots)]
tots[, `:=`(
    PCT_CALC = INCOME_SHARE,
    CURRENCY = "UA thousands"
)]


fin_inc <- rbind(inc, tots)
setorder(fin_inc, YEAR)

fwrite(
    fin_inc,
    get_path("afdb", "raw", "AFDB_INCOME_SHARES_2017_[report_year].csv")
)

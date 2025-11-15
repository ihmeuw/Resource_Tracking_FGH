# FGH
# UNFPA_preds
code_repo <- 'FILEPATH'


report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
library(ggplot2)
library(data.table)
library(readstata13)

# Data ========================================================================
# clean names
cn <- \(x) {names(x) <- tolower(names(x)); x}




# COVID
covidp <- fread(
    file.path(get_path("UNFPA", "fin"),
              "COVID_prepped_20_21.csv")
)



# MAIN ========================================================================
# Reported disbursements - https://www.unfpa.org/data/transparency-portal
# 2019 - 2022
report_disb <- c(
    # core resource + non-core resources
    222686964 + 711112659,
    237375397 + 790589140,
    249215666 + 837229948,
    308120957 + 910206133
)

# ESTIMATE 2022 OVERALL COVID DAH ===========
# first need a 2022 guesstimate, let's just consider reported total disbursements
covdah <- covidp[, .(dah = sum(total_amt)), by = .(year = YEAR)]
covdah <- rbind(covdah,
                data.table(year = 2022), fill = TRUE)
covdah[, disb := report_disb[-1]] #2020-2022

covdah[, disbfrac := dah / disb]
df20 <- covdah[year == 2020, disbfrac]
df21 <- covdah[year == 2021, disbfrac]
pctchg <- (df21 - df20) / df20
## assume fraction continues to change linearly
df22 <- df21 + (df21 * pctchg)

covdah[year == 2022, disbfrac := df22]
covdah[year == 2022, dah := disb * disbfrac]
COV22 <- covdah[year == 2022, dah]


covid_new <- data.table(
    YEAR = 2022,
    recipient_agency = "United Nations Population Fund",
    total_amt = COV22,
    other_amt = COV22
)
covid_22 <- rbind(covidp, covid_new, fill = TRUE)

save_dataset(covid_22, "COVID_prepped.csv", "UNFPA", "fin")

if (interactive()) {
    ggplot(covdah, aes(x = year)) +
        geom_line(aes(y = disb / 10, color = "Reported Disbursements (scaled down by 10)")) +
        geom_line(aes(y = dah, color = "Estimated DAH")) +
        geom_point(aes(x = 2022, y = covdah[year==2022, dah])) +
        labs(title = "UNFPA COVID DAH", x = "", y = "USD", color = "") +
        theme(legend.position = "top", legend.direction = "horizontal")
}



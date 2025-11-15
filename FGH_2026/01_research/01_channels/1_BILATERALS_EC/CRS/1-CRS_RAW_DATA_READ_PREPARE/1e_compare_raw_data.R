#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Compare changes in annual disbursements from most recent
#           data update to previous data update (step 4.5 of previous 1_CRS file)
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
# System prep
rm(list=ls(all.names = TRUE))
library(ggplot2)
library(patchwork)
library(data.table)

if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, 'FGH_', report_year, '/utils.R'))

# Variable prep
update_mmyy <- get_dah_param('CRS', 'update_MMYY')
prev_update_mmyy <- get_dah_param('CRS', 'prev_update_MMYY')
#---------------------------------------------------------------------#


cat('  Pull input data\n')
#----# Pull input data #----# ####
new_data <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_1.csv'))
old_data <- fread(get_path('CRS', 'int', 'B_CRS_[crs.update_mmyy]_HEALTH_BIL_ODA_1.csv',
                           crs.update_mmyy = prev_update_mmyy))

#---------------------------------------------------------------------#

cat('  Collapse & merge\n')
#----# Collapse & merge #----# ####
# sum by year
new <- new_data[, .(val = sum(disbursement_current, na.rm = T),
                    med = median(disbursement_current, na.rm = T),
                    lq = quantile(disbursement_current, 0.25, na.rm = T),
                    uq = quantile(disbursement_current, 0.75, na.rm = T),
                    count = .N,
                    src = "new"),
                by=year]

old <- old_data[, .(val = sum(disbursement_current, na.rm = T),
                    med = median(disbursement_current, na.rm = T),
                    lq = quantile(disbursement_current, 0.25, na.rm = T),
                    uq = quantile(disbursement_current, 0.75, na.rm = T),
                    count = .N,
                    src = "old"),
                by=year]

# merge
compare <- rbind(old, new)
setorder(compare, year, -src) # old, new
#---------------------------------------------------------------------#

cat('  Calculate differences\n')
#----# Calculate differences #----# ####
compare[, diff1 := diff(val), by=year]
compare[, pctdiff := (diff1 * 100) / val[which(src == "old")], by = year]
compare[diff1 == 0, pctdiff := 0 ]

# check that there are no raw data differences greater than 10% diff
compare[, pctdiff_check := ifelse(pctdiff > 10, 1, 0)]

if (sum(compare$pctdiff_check, na.rm = TRUE) > 0) {
  cat(red('ERROR: Greater than 10% difference in raw data - investigate \n'))
  stop()
}
#---------------------------------------------------------------------#

cat('  Plot...\n')
compare[, src := factor(src, levels = c("old", "new"))]

p1 <- ggplot(compare, aes(x = year, y = val, color = src)) +
    geom_line() +
    geom_point() +
    labs(title = "Annual Disbursements",
         x = "Year",
         y = "Disbursements (millions)",
         color = "") +
    theme_minimal()

p2 <- ggplot(compare[src == "new"], aes(x = year, y = pctdiff)) +
    geom_line() +
    geom_point() +
    geom_hline(yintercept = 0, linetype = "dashed") +
    labs(title = "Difference in Annual Disbursements",
         x = "Year",
         y = "Percent Difference (%)") +
    theme_minimal()


p3 <- ggplot(compare, aes(x = year, y = med, color = src)) +
    geom_point(shape = 3) +
    geom_errorbar(aes(ymin = lq, ymax = uq), alpha = 0.5) +
    facet_wrap(~src) +
    labs(title = "Median disbursement (millions USD) and IQR",
         x = "Year",
         y = "25%, 50%, and 75% quantiles") +
    theme_minimal() +
    theme(legend.position = "none")


p4 <- ggplot(compare, aes(x = year, y = count, color = src)) +
    geom_line() +
    geom_point() +
    labs(title = "Number of data points",
         x = "Year",
         y = "Count",
         color = "") +
    theme_minimal()

p <- (p1 + p2) / (p3 + p4) +
    plot_annotation(
        title = paste0("New (", update_mmyy, ") vs. Old (", prev_update_mmyy, ") data comparison"),
    ) &
    plot_layout(guides = "collect")

figure_dir <- get_path("crs", "int", "figures")
dir.create(figure_dir, showWarnings = FALSE)

fig_path <- file.path(figure_dir, "1e_compare_raw_data.pdf")
ggsave(filename = fig_path,
       plot = p,
       width = 16, height = 10)

cat('  Figure saved to: ', fig_path, '\n')
cat('  Done\n')



# additional investigations - good to do interactively each update
if (interactive()) {

# dibursements by donor
cmp <- merge(
    new_data[, .(new = sum(disbursement_current, na.rm = TRUE)), by = .(year, isocode)],
    old_data[, .(old = sum(disbursement_current, na.rm = TRUE)), by = .(year, isocode)],
    by = c("year", "isocode"),
    all = TRUE
)
ggplot(cmp, aes(x = year)) +
    geom_line(aes(y = new, color = "new")) +
    geom_line(aes(y = old, color = "old")) +
    facet_wrap(~isocode, scales = "free_y") +
    labs(title = "Annual Disbursements by Donor",
         x = "Year",
         y = "Disbursements (millions USD)",
         color = "") +
    theme_minimal() +
    theme(legend.position = "top")


# disbursements by channel
cmp <- merge(
    new_data[, .(new = sum(disbursement_current, na.rm = TRUE)), by = .(year, channel_code)],
    old_data[, .(old = sum(disbursement_current, na.rm = TRUE)), by = .(year, channel_code)],
    by = c("year", "channel_code"),
    all = TRUE
)
ggplot(cmp, aes(x = log(old), y = log(new))) +
    geom_abline(intercept = 0, slope = 1, color = "blue") +
    geom_point(size = 1, alpha = 0.5) +
    labs(title = "Annual Disbursements by Recipient - Logged",
         x = "Old (millions USD)",
         y = "New (millions USD)") +
    theme_minimal()


# disbursements by recipient
cmp <- merge(
    new_data[, .(new = sum(disbursement_current, na.rm = TRUE)), by = .(year, ISO3_RC)],
    old_data[, .(old = sum(disbursement_current, na.rm = TRUE)), by = .(year, ISO3_RC)],
    by = c("year", "ISO3_RC"),
    all = TRUE
)
ggplot(cmp, aes(x = log(old), y = log(new))) +
    geom_abline(intercept = 0, slope = 1, color = "blue") +
    geom_point(size = 1, alpha = 0.5) +
    labs(title = "Annual Disbursements by Recipient",
         x = "Old (millions USD)",
         y = "New (millions USD)") +
    theme_minimal()


# compare dah trend with total oda trend
all_oda <- fread(get_path("dac", "fin", "B_CRS_[crs.update_mmyy]_BILODA_BYDONOR_BYSECTOR.csv"))
all_oda <- all_oda[, .(oda = sum(disbursement_current, na.rm = TRUE)), by = year]
all_oda <- merge(
    all_oda,
    new_data[, .(dah = sum(disbursement_current, na.rm = TRUE)), by = year],
    by = "year", all = TRUE
)

ggplot(all_oda, aes(x = year)) +
    geom_line(aes(y = oda, color = "ODA")) +
    geom_line(aes(y = dah, color = "DAH")) +
    labs(title = "Annual Disbursements by Donor",
         x = "Year",
         y = "Disbursements (millions USD)",
         color = "") +
    theme_minimal() +
    theme(legend.position = "top")

}
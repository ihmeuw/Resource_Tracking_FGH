#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Graphs for presenatation
#---------------------# ####

#----# Environment Prep #----# ####
# System prep
rm(list=ls())
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)
source(paste0(dah.roots$k, "FILEPATH/get_location_metadata.R"))

output_path <- get_path("WHO", "output")

#-------------------------------------------------# ####
cat('  Read in final data\n')
#----# Read in data #----------------------------# ####

dt <- setDT(fread(paste0(get_path('WHO', 'fin'), 'COVID_prepped.csv')))

# Calculate total disbursement and commitment

disb_sum <- sum(dt[, 'disbursement'])
print(disb_sum)

# Graph by donor

donor_disb <- dt[, sum(disbursement), by = donor]
setnames(donor_disb, 'V1', 'disbursement')

p_donor <- ggplot(data = donor_disb) +
  geom_col(mapping=aes(x=donor, y=disbursement, fill=donor)) +
  theme_bw() +
  scale_y_continuous(labels = comma) +
  labs(x='Donor', y='Disbursement', fill='Donor',
       title='Funding Allocation by Donor')  +
  theme(axis.text.x = element_text(angle=45, vjust=1, hjust=1), legend.position = 'none')

ggsave(plot = p_donor, filename = paste0(output_path, 'COVID_by_donor.png'), device = 'png', width = 20, height = 10)

# Graph by recipient

recipient_disb <- dt[, sum(disbursement), by = recipient_country]
setnames(recipient_disb, 'V1', 'disbursement')

p_recipient <- ggplot(data = recipient_disb) +
  geom_col(mapping=aes(x=recipient_country, y=disbursement, fill=recipient_country)) +
  theme_bw() +
  scale_y_continuous(labels = comma) +
  labs(x='Recipient', y='Disbursement', fill='Recipient',
       title='Funding Allocation by Recipient')  +
  theme(axis.text.x = element_text(angle=45, vjust=1, hjust=1), legend.position = 'none')

ggsave(plot = p_recipient, filename = paste0(output_path, 'COVID_by_recipient.png'), device = 'png', width = 15, height = 10)











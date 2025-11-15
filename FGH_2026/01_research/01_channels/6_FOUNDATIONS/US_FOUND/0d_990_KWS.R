#----# Docstring #----# ####
# Project:  FGH
# Purpose:  Cleaning 990s
#---------------------#
#----# Environment Prep #----# ####
rm(list=ls())

if (!exists("code_repo"))  {
  code_repo <- "FILEPATH"
}
source(paste0(code_repo, 'FGH_2024/utils.R'))
report_year <- dah.roots$report_year
prev_year <- dah.roots$prev_report_year
#-----# Data Cleaning #--# ####
# Import dataset 
datalake0 <- fread("FILEPATH")
datalake1 <- fread("FILEPATH")
datalake2 <- fread("FILEPATH")
datalake3 <- fread("FILEPATH")
datalake4 <- fread("FILEPATH")
datalake5 <- fread("FILEPATH")
propublica <- fread("FILEPATH")
propublica <- propublica[!(grepl("Community", Foundation) & !grepl("Silicon", Foundation))]
compiled22 <- bind_rows(datalake0, datalake1, datalake2, datalake3, datalake4, datalake5, propublica)


datalake0 <- fread("FILEPATH")
datalake1 <- fread("FILEPATH")
datalake2 <- fread("FILEPATH")
datalake3 <- fread("FILEPATH")
datalake4 <- fread("FILEPATH")
datalake5 <- fread("FILEPATH")
datalake6 <- fread("FILEPATH")
datalake7 <- fread("FILEPATH")
datalake8 <- fread("FILEPATH")
propublica <- fread("FILEPATH")
propublica <- propublica[!(grepl("Community", Foundation) & !grepl("Silicon", Foundation))]
compiled21 <- bind_rows(datalake0, datalake1, datalake2, datalake3, datalake4, datalake5, propublica)


datalake0 <- fread("FILEPATH")
datalake1 <- fread("FILEPATH")
datalake2 <- fread("FILEPATH")
datalake3 <- fread("FILEPATH")
datalake4 <- fread("FILEPATH")
datalake5 <- fread("FILEPATH")
datalake6 <- fread("FILEPATH")
datalake7 <- fread("FILEPATH")
datalake8 <- fread("FILEPATH")
propublica <- fread("FILEPATH")
propublica <- propublica[!(grepl("Community", Foundation) & !grepl("Silicon", Foundation))]
compiled20 <- bind_rows(datalake0, datalake1, datalake2, datalake3, datalake4, 
                        datalake5, datalake6, datalake7, datalake8, propublica)
rm(datalake0, datalake1, datalake2, datalake3, datalake4, datalake5, datalake6, 
   datalake7, datalake8, propublica)

ngos <- fread("FILEPATH")
ngos[, agency := string_to_std_ascii(agency)]
ngos[, agency := sub(" INC$", "", agency)]


compiled22[, Year := 2022]
compiled22[, Recipient_clean := string_to_std_ascii(Recipient)]
compiled22[, Recipient_clean := sub(" INC$", "", Recipient_clean)]
agencyngos <- unique(ngos$agency)
compiled22 <- compiled22[Recipient_clean %ni% agencyngos]
compiled22[, Recipient_clean := NULL]


compiled21[, Year := 2021]
compiled21[, Recipient_clean := string_to_std_ascii(Recipient)]
compiled21[, Recipient_clean := sub(" INC$", "", Recipient_clean)]
compiled21 <- compiled21[Recipient_clean %ni% agencyngos]
compiled21[, Recipient_clean := NULL]


compiled20 <- compiled20[, Year := 2020]
agencyngos <- unique(ngos[year <= 2020]$agency)
compiled20[, Recipient_clean := string_to_std_ascii(Recipient)]
compiled20[, Recipient_clean := sub(" INC$", "", Recipient_clean)]
compiled20 <- compiled20[Recipient_clean %ni% agencyngos]
compiled20[, Recipient_clean := NULL]


compiled <- bind_rows(compiled20, compiled21, compiled22)

# List of keywords to search for
nonus <- compiled[Country != "US"]
#Remove Gates Foundation
nonus <- nonus[EIN %ni% c(562618866, 840474837)]



nonus[, Purpose := toupper(Purpose)]
nonus[, Recipient := toupper(Recipient)]
keywords <- c("HEALTH", "TREAT ", "TREATMENT", "DISEASE", "ILLNESS", "INFECT",
              "REPRODUCTIVE", "ABORTION", "FERTILITY", "FAMILY PLANNING", "CONTRACEP", "NATAL",
              "STD", "BIRTH", "MATERNAL", "FETAL", "NEWBORN",
              "VITAMIN", "VACCIN", "VIRUS", "IMMUNI", "PNEUMO", "FLU", "POLIO", "MENINGITIS", "DIPTHERIA",
              "AIDS", "HIV", "VIRAL", "MALARIA", "EBOLA", "ZIKA", "COVID", "TOBACCO", "CVD",
              "CANCER", "SMOK", "DISORDER", "ALZHEIMER", "DEMENTIA", "PARKINSON",
              "HOSPITAL", "DOCTOR", "NURSE ", "NURSES ", "PHYSICIAN", "PANDEMIC",
              "EPIDEMIC", "OUTBREAKS", "OPTOME", "WORLD VISION", "SURGERY", "SURGICAL",
              "SURGEON", "MIDWIFE", "GLASSES", "SEXUAL EDUCATION", "SAFE SEX", "HEAL",
              "HEART", "PHARMA", "MEDIC"
              )


match <- function(x, keywords) {
  any(sapply(keywords, function(keyword) grepl(keyword, x)))
}

# Filter rows where "Purpose" or "Recipient" contain any part of the keywords
# Last time ran took 7 minutes
a <- Sys.time()
filtered_dt <- nonus[apply(nonus[, .(Purpose, Recipient)], 1, function(row) match(row, keywords))]
b <- Sys.time()
print(paste0(round(as.numeric(difftime(time1 = b, time2 = a, units = "min")), 2), " Minutes"))
save_dataset(filtered_dt, "post_filtering", "US_FOUNDS", "raw")

nonus <- filtered_dt[Country %ni% c("AS", "AF", "FR", "CA", "", "IS", "GB", "UK", "NO")]

save_dataset(nonus, "foundations_DAH_labelled.csv", "US_FOUNDS", "int")


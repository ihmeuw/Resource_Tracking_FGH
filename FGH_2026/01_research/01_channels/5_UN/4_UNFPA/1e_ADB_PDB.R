## UNFPA_INC_EXP
## Description: Generate dataset with UNFPA expenditures imputed by sources of 
## income. This is R-translated version of the stata do file of the same name.
##
## NOTE: Due to permission issue, most files are getting saved out with
## alternate titles. Also, this script is a lot longer than the stata script
## because ot the character breaks and trying to match as exactly as they appear
## on stata version
## 
##
#------------------------------------------------------------------------#

####################################
# Set up  ##########################                                   
####################################
  rm(list = ls(all.names = TRUE))
  library(data.table)
  library(readxl)
  library(dplyr)
  library(tidyr)
  library(readstata13)
  library(haven)
  library(stringr)
  library(tidyverse)
  library(crayon)
  library(purrr)
  library(stringdist)

# Set file paths based on the operating system
  if (Sys.info()["sysname"] == "Linux") {
    j <- "FILEPATH"
    h <- paste0("/homes/", Sys.getenv("USER"), "/")
    k <- "FILEPATH"
    l <- "FILEPATH"
  } else {
    j <- "J:/"
    h <- "H:/"
    k <- "K:/"
    l <- "L:/"
  }

  if (!exists("code_repo"))  {
    code_repo <- 'FILEPATH'
  }
  report_year <- 2024		# Report year
  source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
  asn_date <- format(Sys.time(), "%Y%m%d")

# USER INPUTS
  report_yr <- report_year
  corr_data_year <- 2023 #  Previous FGH report year
  update_tag <- "20250522"

# Directories
  RAW <- get_path("UNFPA", "raw")
  FIN <- get_path("UNFPA", "fin")
  OUT <- get_path("UNFPA", "output")

# paths that doesn't use get_path function
  WD <- paste0(j, "FILEPATH")
  INT <- paste0(WD, "FILEPATH")
  CODE <- paste0(WD, "FILEPATH")

# Files
  country_codes <- data.frame(read_dta(paste0(CODE, "/countrycodes_official.dta")))
  ebola_data <- data.frame(read_csv(paste0("FILEPATH/ebola_all_collapsed.csv")))
  income_sector_type <- data.frame(read_csv(paste0(WD, "FILEPATH/income_sector_and_type_assignments.csv")))

# Derived Macros
  data_yr <- report_yr -1
  short_data_yr <- substr(as.character(data_yr), 3, 4)

    
# ALLOCATE from Source to Channel to Health Focus Area (merge ADB and PDB)
    combined_cleaned <- fread(file.path(FIN, "INC_EXP_FINAL.csv"))
    ## aggregate over budget type
    combined_cleaned <- combined_cleaned[, .(
        OUTFLOW = sum(OUTFLOW, na.rm = TRUE)
    ), by = .(YEAR, DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE, DONOR_COUNTRY, CHANNEL)]
  
    temp_rc <- country_codes %>%
      rename(ISO3_RC = iso3)
    
  # Ebola data is continually updated
    prep_ebola <- ebola_data %>%
      filter(channel == "UNFPA" & contributionstatus == "paid") %>%
      group_by(year, source, channel, recipient_country, HFA) %>%
      summarize(amount = sum(amount), .groups = "drop") %>%
      rename(YEAR = year, DONOR_NAME = source, CHANNEL = channel, oid_ebz_DAH = amount, country_lc = recipient_country) %>%
      mutate(REPORTING_AGENCY = "EBOLA")
    
    prep_ebola$source <- "ebola"
    temp_rc$source <- "temp"
    unfpa_ebola <- full_join(prep_ebola, temp_rc[, c("country_lc","ISO3_RC","source")], by = "country_lc") %>%
    mutate(merge_col = case_when(
      !is.na(source.x) & is.na(source.y) ~ "ebola_only",
      !is.na(source.x) & !is.na(source.y) ~ "both",
      TRUE ~ "temp_only"
    ))

    unfpa_ebola <- unfpa_ebola %>%
      select(-source.x, -source.y) %>%
      filter(merge_col == "ebola_only" | merge_col == "both") %>%
      select(-merge_col) %>%
      mutate(ISO3_RC = case_when(
        country_lc == "QMA" ~ "QMA",
        TRUE ~ ISO3_RC
      ))
    prep_ebola$source <- NULL
    temp_rc$source <- NULL
    
  # Fill in source info
    unfpa_ebola <- unfpa_ebola %>%
      mutate(ISO_CODE = str_replace(DONOR_NAME, "BIL_", "")) %>%
      mutate(INCOME_SECTOR = case_when(
        str_detect(DONOR_NAME, "BIL") | DONOR_NAME == "EC" ~ "PUBLIC",
        DONOR_NAME %in% c("PRIVATE", "NGO", "US_FOUND", "INT_FOUND") ~ "PRIVATE",
        DONOR_NAME == "UNSP_UN" ~ "UNALL",
        DONOR_NAME == "WB" ~ "OTHER",
        TRUE ~ NA_character_
      ))
    
  # Format data
    unfpa_ebola <- unfpa_ebola %>%
      mutate(WB_REGION = "Sub-Saharan Africa",
             WB_REGIONCODE = "SSA",
             LEVEL = "COUNTRY",
             EBOLA = 1) %>%
      mutate(LEVEL = case_when(
        ISO3_RC == "QMA" ~ "REGIONAL",
        TRUE ~ LEVEL
      ))
    
    unfpa_ebola <- unfpa_ebola %>%
      group_by(YEAR, DONOR_NAME, ISO_CODE, EBOLA, INCOME_SECTOR) %>%
      summarise(oid_ebz_DAH = sum(oid_ebz_DAH), .groups = "drop")
    
    combined_ebola_prep <- combined_cleaned %>%
      mutate(DONOR_COUNTRY = case_when(
        ISO_CODE == "NA" ~ "NA",
        ISO_CODE == "KOR" ~ "Republic of Korea",
        ISO_CODE == "GBR" ~ "United Kingdom",
        DONOR_COUNTRY == "" ~ "NA",
        TRUE ~ DONOR_COUNTRY
      ))
    combined_ebola_prep <- combined_cleaned %>%
        group_by(YEAR, CHANNEL, INCOME_SECTOR, INCOME_TYPE,
                 DONOR_NAME, DONOR_COUNTRY, ISO_CODE) %>%
        summarise(OUTFLOW = sum(OUTFLOW), .groups = "drop")
    
    combined_ebola_merge <- full_join(combined_ebola_prep, unfpa_ebola, 
                by = c("YEAR", "DONOR_NAME", "INCOME_SECTOR", "ISO_CODE"))
    
    combined_ebola_merge$EBOLA <- ifelse(is.na(combined_ebola_merge$EBOLA), 0, combined_ebola_merge$EBOLA)
    combined_ebola_merge$oid_ebz_DAH <- ifelse(is.na(combined_ebola_merge$oid_ebz_DAH), 0, combined_ebola_merge$oid_ebz_DAH)
    combined_ebola_merge$OUTFLOW <- ifelse(is.na(combined_ebola_merge$OUTFLOW), 0, combined_ebola_merge$OUTFLOW)
    
    combined_ebola_merge <- combined_ebola_merge %>%
      mutate(OUTFLOW = OUTFLOW - oid_ebz_DAH)

    write_dta(combined_ebola_merge, paste0(FIN, "UNFPA_INC_EXP_1990_", data_yr, "_ebola_fixed_", update_tag, "_alt.dta"))    

  # Calculate DAH by health focus area
    UNFPA_HFA <- data.frame(read.csv(paste0(RAW, "/UNFPA_exp_by_HFA_FGH", report_yr, ".csv")))
    # Collapse (sum) by HFA and year to get the total fraction of expenditure 
    # in each HFA by year
    UNFPA_HFA <- UNFPA_HFA %>%
      group_by(HFA, YEAR) %>%
      summarize(FRCT_HFA = sum(FRCT_HFA), .groups = "drop")
    ## ensure fractions sum exactly to 1
    UNFPA_HFA <- UNFPA_HFA %>%
      group_by(YEAR) %>%
      mutate(FRCT_HFA = FRCT_HFA / sum(FRCT_HFA)) %>%
      ungroup()
    
    UNFPA_HFA <- UNFPA_HFA %>%
      filter(!is.na(YEAR))
    
    UNFPA_HFA_wide <- UNFPA_HFA %>%
      pivot_wider(names_from = HFA, values_from = FRCT_HFA, names_prefix = "", values_fill = list(FRCT_HFA = 0)) %>%
      rename_with(~paste0(., "_frct"), .cols = -YEAR)
    
    UNFPA_HFA_wide <- UNFPA_HFA_wide %>%
      mutate(across(c(starts_with("rmh"), starts_with("hiv_prev")), ~ifelse(is.na(.), 0, .)))
    
  # Get earlier years (1990-1995)
  # 3-year weighted average
  # there is no HIV or HSS funding in earlier years
    early_years <- UNFPA_HFA_wide %>%
      filter(YEAR < 1999)

    early_years <- early_years %>%
      mutate(across(c(rmh_fp_frct, rmh_mh_frct, rmh_other_frct), ~case_when(
        YEAR == 1998 ~ .x / 2,
        YEAR == 1997 ~ .x / 3,
        YEAR == 1996 ~ .x / 6,
        TRUE ~ .x
      )))
    
    early_years <- early_years %>%
      summarise(across(c(rmh_fp_frct, rmh_mh_frct, rmh_other_frct), sum, na.rm = TRUE), .groups = "drop")
      
    early_years <- early_years %>%
      uncount(6)

    early_years$YEAR <- 1990:1995      
    
    UNFPA_HFA_append <- bind_rows(UNFPA_HFA_wide, early_years)
    UNFPA_HFA_append <- UNFPA_HFA_append %>%
      mutate(across(c(starts_with("rmh"), starts_with("hiv")), ~ifelse(is.na(.), 0, .)))
    
  # Merge with DAH data and calculate expenditure by HFA:
    # Pulling in stata-version as well to break some character values in
    # Donor name and donor country
    HFA_EBOLA <- merge(UNFPA_HFA_append, combined_ebola_merge, by = "YEAR")
    HFA_EBOLA <- HFA_EBOLA%>%
      rename(DAH = OUTFLOW)
  # Name cleaning
    HFA_EBOLA <- HFA_EBOLA %>%
      mutate(DONOR_NAME = case_when(
        ISO_CODE == "ARE" ~ "United Arab Emirates",
        ISO_CODE == "ARG" ~ "Argentina",
        ISO_CODE == "AUS" ~ "Australia",
        ISO_CODE == "BGD" ~ "Bangladesh",
        ISO_CODE == "BHS" ~ "Bahamas",
        ISO_CODE == "BIH" ~ "Bosnia and Herzegovina",
        ISO_CODE == "BOL" ~ "Bolivia",
        ISO_CODE == "BWA" ~ "Botswana",
        ISO_CODE == "CAF" ~ "Central African Republic",
        ISO_CODE == "CHE" ~ "Switzerland",
        ISO_CODE == "CHN" ~ "China",
        ISO_CODE == "CIV" ~ "Côte d'Ivoire",
        ISO_CODE == "COD" ~ "Democratic Republic of the Congo",
        ISO_CODE == "COG" ~ "Congo",
        ISO_CODE == "COK" ~ "Cook Islands",
        ISO_CODE == "COL" ~ "Colombia",
        ISO_CODE == "COM" ~ "Comoros",
        ISO_CODE == "CPV" ~ "Cape Verde",
        ISO_CODE == "CZE" ~ "Czechia",
        ISO_CODE == "DEU" ~ "Germany",
        ISO_CODE == "DOM" ~ "Dominican Republic",
        ISO_CODE == "DZA" ~ "Algeria",
        ISO_CODE == "EGY" ~ "Egypt",
        ISO_CODE == "FSM" ~ "Micronesia",
        (ISO_CODE == "GBR" & INCOME_SECTOR == "PUBLIC" & DONOR_NAME != "United Kingdom Trust Fund for Reproductive Health Commodities Security Programme") ~ "United Kingdom",
        ISO_CODE == "GMB" ~ "Gambia",
        ISO_CODE == "GNB" ~ "Guinea-Bissau",
        ISO_CODE == "GNQ" ~ "Equatorial Guinea",
        ISO_CODE == "IDN" ~ "Indonesia",
        ISO_CODE == "IRL" ~ "Ireland",
        ISO_CODE == "IRN" ~ "Iran (Islamic Republic of)",
        ISO_CODE == "KNA" ~ "Saint Kitts and Nevis",
        ISO_CODE == "KOR" ~ "Republic of Korea",
        ISO_CODE == "LAO" ~ "Lao People's Democratic Republic",
        ISO_CODE == "LBN" ~ "Lebanon",
        ISO_CODE == "LBR" ~ "Liberia",
        ISO_CODE == "LBY" ~ "Libya",
        ISO_CODE == "LCA" ~ "Saint Lucia",
        ISO_CODE == "LIE" ~ "Liechtenstein",
        ISO_CODE == "LKA" ~ "Sri Lanka",
        ISO_CODE == "LSO" ~ "Lesotho",
        ISO_CODE == "LUX" ~ "Luxembourg",
        ISO_CODE == "MDA" ~ "Moldova",
        ISO_CODE == "MDG" ~ "Madagascar",
        ISO_CODE == "MEX" ~ "Mexico",
        ISO_CODE == "MHL" ~ "Marshall Islands",
        ISO_CODE == "MKD" ~ "North Macedonia",
        ISO_CODE == "MOZ" ~ "Mozambique",
        ISO_CODE == "MYS" ~ "Malaysia",
        ISO_CODE == "NER" ~ "Niger",
        (ISO_CODE == "NLD" & DONOR_NAME == "Netherlands (the)") ~ "Netherlands",
        ISO_CODE == "PSE" ~ "Palestine",
        ISO_CODE == "PAK" ~ "Pakistan",
        ISO_CODE == "SDN" ~ "Sudan",
        ISO_CODE == "SLE" ~ "Sierra Leone",
        ISO_CODE == "SLV" ~ "El Salvador",
        ISO_CODE == "STP" ~ "Sao Tome and Principe",
        ISO_CODE == "SUR" ~ "Suriname",
        ISO_CODE == "SVK" ~ "Slovakia",
        ISO_CODE == "SVN" ~ "Slovenia",
        ISO_CODE == "SWE" ~ "Sweden",
        ISO_CODE == "SWZ" ~ "Eswatini",
        ISO_CODE == "SYC" ~ "Seychelles",
        ISO_CODE == "SYR" ~ "Syrian Arab Republic",
        ISO_CODE == "TKL" ~ "Tokelau",
        ISO_CODE == "TLS" ~ "Timor-Leste",
        ISO_CODE == "TZA" ~ "United Republic of Tanzania",
        (ISO_CODE == "USA" & DONOR_NAME == "United States of America (the)") ~ "United States of America",
        (ISO_CODE == "USA" & DONOR_NAME == "Finland") ~ "United States of America",
        ISO_CODE == "VCT" ~ "Saint Vincent and the Grenadines",
        ISO_CODE == "VEN" ~ "Venezuela (Bolivarian Republic of)",
        ISO_CODE == "VGB" ~ "Argentina",
        ISO_CODE == "VNM" ~ "Viet Nam",
        ISO_CODE == "YEM" ~ "Yemen",
        ISO_CODE == "WB" ~ "World Bank",
        ISO_CODE == "ZAF" ~ "South Africa",
        TRUE ~ DONOR_NAME
      )) %>%
    mutate(DONOR_COUNTRY = case_when(
        ISO_CODE == "SWE" ~ "Sweden",
        ISO_CODE == "LUX" ~ "Luxembourg",
        ISO_CODE == "PSE" ~ "Palestine",
        TRUE ~ DONOR_COUNTRY
      )
    )
    income_sector_type <- income_sector_type %>%
     mutate(DONOR_NAME = str_to_title(DONOR_NAME)
              ,ISO_CODE = case_when(
              is.na(ISO_CODE) ~ "NA",
              TRUE ~ ISO_CODE
            )) %>%
      distinct(DONOR_NAME, INCOME_SECTOR, INCOME_TYPE, ISO_CODE)
   
    # More manual fixes:
    HFA_EBOLA <- HFA_EBOLA %>%
      mutate(DONOR_NAME = case_when(
        DONOR_NAME == "IPPF/AGFUND" ~ "Ippf Agfund",
        DONOR_NAME == "Microcomputer Data­ base on Women, Population and Development"  ~ "Microcomputer Data Base On Women Population And Development",
        DONOR_NAME == "Rafael M. Satas Endowment Fund *" ~ "Rafael M Salas Endowment Fund",
        DONOR_NAME == "IPPF/ AGFUND" ~ "Ippf Agfund",
        DONOR_NAME == "Exchange adjustments on collection of contributions" ~ "Exchange Adjustments On Collection Of Contributions",
        DONOR_NAME == "UNICEF" ~ "Unicef",
        DONOR_NAME == "NGO (Indo - Hilfe-Peru)" ~ "Ngo Indo Hilfe Peru",
        DONOR_NAME == "NGO (Incio-Hilfe-Peru)" ~ "Ngo Incio Hilfe Peru",
        DONOR_NAME == "Canadian Public Health Assoc." ~ "Canadian Public Health Association",
        DONOR_NAME == "Global Contraceptive Commodity Programme - Note 9" ~ "Global Contraceptive Commodity Programme",
        DONOR_NAME == "Miscellaneous income (expenditure) - net" ~ "Miscellaneous Income Expenditure Net",
        DONOR_NAME == "NGO(lndo-Hilfe-Peru)" ~ "Ngo Indo Hilfe Peru",
        DONOR_NAME == "AGFUND" ~ "Agfund",
        DONOR_NAME == "IPRF/AGFUND" ~ "Iprf Agfund",
        DONOR_NAME == "UNFIP" ~ "Unfip",
        DONOR_NAME == "Exchange Adjustments on collection of contributions" ~ "Exchange Adjustments On Collection Of Contributions",
        DONOR_NAME == "International Conference on Population and Development *5" ~ "International Conference On Population And Development",
        DONOR_NAME == "Office of Results-based Management (ORM)" ~ "Office Of Results Based Management Orm",
        DONOR_NAME == "US Committee for UNFPA Trust Fund" ~ "Us Committee For Unfpa",
        DONOR_NAME == "USAID" ~ "Usaid",
        DONOR_NAME == "UN Foundation Support Office (UNFSO)" ~ "Un Foundation Support Office Unfso",
        DONOR_NAME == "NGO (Indo-Hilfe-Peru)" ~ "Ngo Indo Hilfe Peru",
        DONOR_NAME == "NGO  (lndo-Hife-Peru)" ~ "Ngo Indo Hilfe Peru",
        DONOR_NAME == "NORAD/Malawi" ~ "Norad Malawi",
        DONOR_NAME == "NGO & Paliamentary Activities" ~ "Ngo Paliamentary Activities",
        DONOR_NAME == "SHELL" ~ "Shell",
        DONOR_NAME == "Unspecified Contributions for JPO program" ~ "Unspecified Contributions To The Jpo Program",
        DONOR_NAME == "Macro lnternational" ~ "Macro International Inc",
        DONOR_NAME == "UN FoundationSupport Office (UNFSO)" ~ "",
        DONOR_NAME == "CIDA-Kosovo" ~ "Cida Kosovo",
        DONOR_NAME == "Arab Gulf Programme for United Nations Organizations" ~ "Arab Gulf Program For United Naitons Development Organizations",
        DONOR_NAME == "NORAD" ~ "Norad",
        DONOR_NAME == "NORAD / Malawi" ~ "Norad Malawi",
        DONOR_NAME == "United Nations Al DS" ~ "United Nations Aids",
        DONOR_NAME == "UN Foundation Support Office Note 11" ~ "Un Foundation Support Office Unfso",
        DONOR_NAME == "CIDA/ Kosovo" ~ "Cida Kosovo",
        DONOR_NAME == "Japanese Trust Fund for Inter-Country NGO and Parliamentary Activities" ~ "Japanese Trust Fund For Inter Country Ngo And Parliamentary Activities",
        DONOR_NAME == "International Conference on Population and Development +5" ~ "International Conference On Population And Development",
        DONOR_NAME == "Marie Stopes International" ~ "Marie Stopes International Unk",
        DONOR_NAME == "OXFAM" ~ "Oxfam",
        DONOR_NAME == "US Committee for UNFPA" ~ "Us Committee For Unfpa",
        DONOR_NAME == "CIDA/Kosovo" ~ "Cida Kosovo",
        DONOR_NAME == "The Global Fund to Fight AIDS, Tuberculosis and Malaria" ~ "Global Fund To Fight Aids Tuberculosis And Malaria",
        DONOR_NAME == "UNIFEM" ~ "Unifem",
        DONOR_NAME == "Procurement - Income (Unspecified Contributions)" ~ "Procurement Income Unspecified Contributions",
        DONOR_NAME == "Multi-Donor - Thematic Trust Fund for Obstetric Fistula" ~ "Multi Donor Thematic Trust Fund For Obstetric Fistula",
        DONOR_NAME == "Multi-Donor - Thematic Trust Fund for Reproductive Health Commodity Security" ~ "Multi Donor Thematic Trust Fund For Reproductive Health Commodity Security",
        DONOR_NAME == "Multi-Donor - Thematic Trust Fund for Tsunami" ~ "Multi Donor Thematic Trust Fund For Tsunami",
        DONOR_NAME == "FEMAP" ~ "Femap",
        DONOR_NAME == "MacArthur Foundation" ~ "John And Catherine T Macarthur Foundatlon",
        DONOR_NAME == "CHECCHI" ~ "Checchi",
        DONOR_NAME == "Humanitarian Coordinator/OCHA" ~ "Humanitarian Coordinator Ocha",
        DONOR_NAME == "UNAIDS" ~ "Unaids",
        DONOR_NAME == "UNDESA" ~ "Undesa",
        DONOR_NAME == "UNHCR" ~ "Unhcr",
        DONOR_NAME == "UNHSF" ~ "Unhsf",
        DONOR_NAME == "Americans for UNFPA" ~ "Americans For Unfpa",
        DONOR_NAME == "CHECCHI Consulting" ~ "Checchi Consulting",
        DONOR_NAME == "Unspecified Contributions to the JPO program" ~ "Unspecified Contributions To The Jpo Program",
        DONOR_NAME == "Multi-Donor  - Thematic Trust Fund for Maternal Health" ~ "Multi Donor Thematic Trust Fund For Maternal Health",
        DONOR_NAME == "Multi-Donor  - Thematic Trust Fund for Obstetric Fistula" ~ "Multi Donor Thematic Trust Fund For Obstetric Fistula",
        DONOR_NAME == "Multi-Donor  - Thematic Trust Fund for Reproductive Health Commodity Security" ~ "Multi Donor Thematic Trust Fund For Reproductive Health Commodity Security",
        DONOR_NAME == "Multi-Donor  - Thematic Trust Fund for Tsunami" ~ "Multi Donor Thematic Trust Fund For Tsunami",
        DONOR_NAME == "Multi-Donor - Female Genital Mutilation/Cutting" ~ "Multi Donor Female Genital Mutilation Cutting",
        DONOR_NAME == "Multi-Donor - Office of Results-Based Management" ~ "Multi Donor Office Of Results Based Management",
        DONOR_NAME == "Japanese Trust Fund for Inter-Country NGO & Parliamentary Activities" ~ "Japanese Trust Fund For Inter Country Ngo And Parliamentary Activities",
        DONOR_NAME == "Arab Gulf Programme for United Nations Organisations (AGFUND)" ~ "Arab Gulf Programme For The United Nations Development Organizations Agfund",
        DONOR_NAME == "Global Fund to Fight AIDS, Tuberculosis and Malaria" ~ "Global Fund To Fight Aids Tuberculosis And Malaria",
        DONOR_NAME == "Asian forum of Paliamentarian on Population and Development" ~ "Asian Forum Of Paliamentarian On Population And Development",
        DONOR_NAME == "United Nations Fund for International Partnerships (UNFIP)" ~ "United Nations Fund For International Partnerships Unfip",
        DONOR_NAME == "ONUCI" ~ "Onuci",
        DONOR_NAME == "The Humanitarian Coordinator" ~ "Humanitarian Coordinator Ocha",
        DONOR_NAME == "UN DESA" ~ "Un Desa",
        DONOR_NAME == "UN Foundation Support Office" ~ "Un Foundation Support Office Unfso",
        DONOR_NAME == "Other private contributions" ~ "Other Private Sector",
        DONOR_NAME == "Russian Federation (the)" ~ "Russia",
        DONOR_NAME == "Philippines (the)" ~ "Philippines",
        DONOR_NAME == "Government local costs and other" ~ "Government Local Costs And Other",
        DONOR_NAME == "Democratic People's Rep. of Korea (the)" ~ "Democratic People S Republic Of Korea",
        DONOR_NAME == "UNSP_UN" ~ "Unsp Un",
        DONOR_NAME == "Democratic People's Rep. of Korea" ~ "Democratic People S Republic Of Korea",
        DONOR_NAME == "Government contribution to local office costs" ~ "Government Contribution To Local Office Costs",
        DONOR_NAME == "Anonymous" ~ "Anonymous Donor",
        DONOR_NAME == "Takeda Pharmaceutical Company Limited" ~ "Takeda Pharmaceutical",
        DONOR_NAME == "WHO" ~ "World Health Organization",
        TRUE ~ DONOR_NAME
      ))
    
    HFA_EBOLA <- HFA_EBOLA %>%
      left_join(income_sector_type, by = c("DONOR_NAME", "ISO_CODE")) %>%
      mutate(
        INCOME_SECTOR = if_else(is.na(INCOME_SECTOR.y), INCOME_SECTOR.x, INCOME_SECTOR.y),
        INCOME_TYPE = if_else(is.na(INCOME_TYPE.y), INCOME_TYPE.x, INCOME_TYPE.y)
      ) %>%
      select(-c(INCOME_SECTOR.x, INCOME_TYPE.x, INCOME_SECTOR.y, INCOME_TYPE.y))
    
    
  # NEW FOR FGH_2024 and onward! Adding a section that subtracts covid numbers from HFAs
    covid_prepped <- data.table(read.csv(paste0(FIN, "COVID_prepped.csv")))
    covid_prepped <- covid_prepped[YEAR <= 2021]
    covid_prepped <- covid_prepped %>%
      select(YEAR, donor_name, donor_country, INCOME_SECTOR, INCOME_TYPE, iso_code, total_amt) %>%
      mutate(iso_code = case_when(
        is.na(iso_code) ~ "NA",
        TRUE ~ iso_code
      ),
      donor_name = case_when(
        donor_name == "" ~ "Unspecified",
        donor_name == "Australia, Government of" ~ "Australia",
        donor_name == "United Kingdom, Government of" ~ "United Kingdom",
        donor_name == "United Nations Children's Fund" ~ "United Nations Children S Fund",
        donor_name == "Sweden, Government of" ~ "Sweden",
        donor_name == "Denmark, Government of" ~ "DENMARK",
        donor_name == "European Commission's Humanitarian Aid and Civil Protection Department" ~ "EUROPEAN COMMISSION",
        donor_name == "Democratic Republic of the Congo Humanitarian Fund" ~ "Democratic Republic Of The Congo Humanitarian Fund",
        donor_name == "Canada, Government of" ~ "CANADA",
        donor_name == "Finland, Government of" ~ "FINLAND",
        donor_name == "Iceland, Government of" ~ "Iceland",
        donor_name == "Norway, Government of" ~ "NORWAY",
        donor_name == "UN Women" ~ "Un Women",
        donor_name == "UN Women" ~ "Un Women",
        donor_name == "Japan, Government of" ~ "JAPAN",
        donor_name == "Austria, Government of" ~ "Austria",
        donor_name == "Switzerland, Government of" ~ "Switzerland",
        donor_name == "International Organization for Migration" ~ "International Organization For Migration",
        donor_name == "China, Government of" ~ "China",
        donor_name == "Luxembourg, Government of" ~ "Luxembourg",
        donor_name == "occupied Palestinian territory" ~ "Palestine",
        donor_name == "Bulgaria, Government of" ~ "Bulgaria",
        donor_name == "Italy, Government of" ~ "ITALY",
        TRUE ~ donor_name
      ))
    names(covid_prepped)[names(covid_prepped) == "donor_name"] <- "DONOR_NAME"
    names(covid_prepped)[names(covid_prepped) == "donor_country"] <- "DONOR_COUNTRY"
    names(covid_prepped)[names(covid_prepped) == "iso_code"] <- "ISO_CODE"
    
    covid_prepped <- covid_prepped %>%
      mutate(DONOR_NAME = case_when(
        DONOR_NAME == "UN COVID-19 Response and Recovery Fund" ~ "Un Covid 19 Response And Recovery Fund",
        DONOR_NAME == "UN Multi-Partner Trust Fund for Sustaining Peace in Colombia" ~ "Un Multi Partner Trust Fund For Sustaining Peace In Colombia",
        DONOR_NAME == "Private (individuals & organizations)" ~ "Private Individuals Organizations",
        DONOR_NAME == "Korea, Republic of, Government of" ~ "Republic of Korea",
        DONOR_NAME == "United Nations Human Settlements Programme (UN-HABITAT)" ~ "United Nations Human Settlements Program Un Habitat",
        DONOR_NAME == "United Nations Joint Programme on HIV/AIDS" ~ "United Nations Joint Programme On Hiv Aids",
        
        TRUE ~ DONOR_NAME
      )) %>%
      mutate(DONOR_COUNTRY = case_when(
        DONOR_COUNTRY == "" ~ "NA",
        TRUE ~ DONOR_COUNTRY
      )) %>%
      mutate(ISO_CODE = case_when(
          ISO_CODE == "" ~ "NA",
          TRUE ~ ISO_CODE
      )
    )
  
    covid_prepped <- covid_prepped %>%
      left_join(income_sector_type, by = c("DONOR_NAME", "ISO_CODE")) %>%
      mutate(
        INCOME_SECTOR = if_else(is.na(INCOME_SECTOR.y), INCOME_SECTOR.x, INCOME_SECTOR.y),
        INCOME_TYPE = if_else(is.na(INCOME_TYPE.y), INCOME_TYPE.x, INCOME_TYPE.y)
      ) %>%
      select(-c(INCOME_SECTOR.x, INCOME_TYPE.x, INCOME_SECTOR.y, INCOME_TYPE.y))

    setDT(HFA_EBOLA); setDT(covid_prepped)
    nrow(HFA_EBOLA)
    nrow(unique(HFA_EBOLA[, .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE)]))
    ## aggregate so that now the rows are uniquely identified by the grouping variables
    HFA_EBOLA[DONOR_NAME %in% c("ANONYMOUS", "UNSPECIFIED"),
         `:=`(
           DONOR_NAME = "Unspecified", DONOR_COUNTRY = NA, ISO_CODE = NA,
           INCOME_SECTOR = "UNALL", INCOME_TYPE = "UNALL"
         )]
    HFA_EBOLA[DONOR_COUNTRY == "NA" | DONOR_COUNTRY == "", DONOR_COUNTRY := NA_character_]
    HFA_EBOLA[ISO_CODE == "NA" | ISO_CODE == "", ISO_CODE := NA_character_]
    HFA_EBOLA_2 <- HFA_EBOLA[, .(DAH = sum(DAH, na.rm = TRUE),
                     oid_ebz_DAH = sum(oid_ebz_DAH, na.rm = TRUE)),
                 by = .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE,
                        INCOME_SECTOR, INCOME_TYPE, CHANNEL, EBOLA)]
    # these are also not equal which again means rows are not uniquely identified by the
    #  grouping variables.
    nrow(covid_prepped)
    nrow(unique(covid_prepped[, .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE)]))
    ## aggregate so that now the rows are uniquely identified by the grouping variables
    covid_prepped[DONOR_COUNTRY == "NA", DONOR_COUNTRY := NA_character_]
    covid_prepped[ISO_CODE == "NA", ISO_CODE := NA_character_]
    covid_prepped <- covid_prepped[, .(oid_covid_DAH = sum(total_amt, na.rm = TRUE)),
                   by = .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE,
                          INCOME_SECTOR, INCOME_TYPE)]
    un_donors <- covid_prepped[DONOR_NAME %ilike% "united nations" |
                         DONOR_NAME %ilike% "humanitarian fund" |
                         DONOR_NAME %ilike% "emergency response fund" |
                         DONOR_NAME %ilike% "un covid 19" |
                         DONOR_NAME %ilike% "un women" |
                         DONOR_NAME %ilike% "un multi partner trust fund" |
                         DONOR_NAME %ilike% "international organization for migration" |
                         DONOR_NAME == "World Health Organization",
                       unique(DONOR_NAME)]
    ec_donors <- covid_prepped[DONOR_NAME %ilike% "european commission",
                       unique(DONOR_NAME)]
    covid_prepped[DONOR_NAME %in% un_donors, `:=`(
      DONOR_NAME = "UNITED NATIONS DEVELOPMENT PROGRAMME", ## need to assign to a representative UN donor
      DONOR_COUNTRY = NA, ISO_CODE = NA, INCOME_SECTOR = "OTHER", INCOME_TYPE = "UN"
    )]
    covid_prepped[DONOR_NAME %in% ec_donors, `:=`(
      DONOR_NAME = "EUROPEAN COMMISSION",
      DONOR_COUNTRY = NA, ISO_CODE = NA, INCOME_SECTOR = "PUBLIC", INCOME_TYPE = "EC"
    )]
    covid_prepped[DONOR_NAME %in% c("Austria", "Bulgaria", "Iceland", "Palestine", "Italy, Goverment Of"),
          `:=`(
            DONOR_NAME = "Unspecified", DONOR_COUNTRY = NA, ISO_CODE = NA,
            INCOME_SECTOR = "UNALL", INCOME_TYPE = "UNALL"
          )]
    ## world bank only in contributions in 2020
    covid_prepped[DONOR_NAME == "World Bank", `:=`(
        oid_covid_DAH = sum(oid_covid_DAH),
        DONOR_NAME = "WORLD BANK"
    )]
    covid_prepped <- covid_prepped[! (DONOR_NAME == "WORLD BANK" & YEAR == 2021)]
    covid_prepped[DONOR_NAME == "World Bank", `:=`(
      DONOR_NAME = "WORLD BANK", DONOR_COUNTRY = NA, ISO_CODE = NA,
      INCOME_SECTOR = "OTHER", INCOME_TYPE = "DEVBANK"
    )]
    covid_prepped[DONOR_NAME == "WORLD BANK" & YEAR == 2021, `:=`(
      DONOR_NAME = "OTHER", DONOR_COUNTRY = NA, ISO_CODE = NA,
      INCOME_SECTOR = "OTHER", INCOME_TYPE = "OTHER"
    )]
    covid_prepped[DONOR_NAME == "Private Individuals Organizations", `:=`(
      DONOR_NAME = "PRIVATE CONTRIBUTIONS", DONOR_COUNTRY = NA, ISO_CODE = NA,
      INCOME_SECTOR = "PRIVATE", INCOME_TYPE = "OTHER"
    )]
    ## re-aggregate now that grouping variables are modified
    covid_prepped <- covid_prepped[, .(oid_covid_DAH = sum(oid_covid_DAH, na.rm = TRUE)),
                   by = .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE,
                          INCOME_SECTOR, INCOME_TYPE)]
   
    HFA_EBOLA_2_COV <- merge(
      HFA_EBOLA_2, covid_prepped,
      by = c("YEAR", "DONOR_NAME", "DONOR_COUNTRY", "ISO_CODE",
             "INCOME_SECTOR", "INCOME_TYPE"),
      all.x = TRUE  ## left-join the covid values onto the main data set
    )
    nrow(HFA_EBOLA_2_COV) == nrow(HFA_EBOLA_2)  
    HFA_EBOLA_2_COV[!is.na(oid_covid_DAH), .N] == nrow(covid_prepped)  
    
    # now proceed with covid subtraction
    setnafill(HFA_EBOLA_2_COV, fill = 0, cols = "oid_covid_DAH")
    HFA_EBOLA_2_COV[EBOLA == 0, oid_covid_DAH := fifelse(
        oid_covid_DAH > DAH, DAH, oid_covid_DAH
    )]
    HFA_EBOLA_2_COV[, DAH := DAH - oid_covid_DAH]
    
    # Adding back the _frct rows
    key_vars <- c("YEAR", "DONOR_NAME", "DONOR_COUNTRY", "INCOME_SECTOR", 
                  "INCOME_TYPE", "ISO_CODE", "CHANNEL", "EBOLA")
    HFA_EBOLA_3 <- merge(HFA_EBOLA_2_COV, HFA_EBOLA[, -c("DAH", "oid_ebz_DAH")],
                         by = key_vars, all.x = TRUE)
    HFA_EBOLA_3 <- unique(HFA_EBOLA_3[, .(YEAR, DONOR_NAME, DONOR_COUNTRY, ISO_CODE, INCOME_SECTOR, INCOME_TYPE, CHANNEL, DAH, EBOLA, oid_ebz_DAH, oid_covid_DAH, hiv_other_frct, hiv_prev_frct, rmh_fp_frct, rmh_hss_hrh_frct, rmh_hss_other_frct, rmh_mh_frct, rmh_other_frct)])
    rm(HFA_EBOLA_2_COV, HFA_EBOLA_2)
    
    frct_columns <- grep("_frct$", names(HFA_EBOLA_3), value = TRUE)
    for(col in frct_columns) {
      new_col_name <- sub("_frct$", "_DAH", col)
      HFA_EBOLA_3[[new_col_name]] <- HFA_EBOLA_3[[col]] * HFA_EBOLA_3$DAH
    }
    
    HFA_EBOLA_3$ISO3_RC <- "NA"
    HFA_EBOLA_3 <- HFA_EBOLA_3[order(HFA_EBOLA_3$YEAR),]
    HFA_EBOLA_3$gov <- 0
    
    # Add ebola back in since we previously adjusted DAH to account for it
    # Also set the COVID amount to "oid_covid_DAH". Then you add the covid amount back
    HFA_EBOLA_3 <- HFA_EBOLA_3 %>%
      mutate(DAH = DAH + oid_ebz_DAH + oid_covid_DAH)
    
    # Subtract out negative disbursements from the year before
    HFA_EBOLA_3$DONOR_COUNTRY <- ifelse(HFA_EBOLA_3$DONOR_COUNTRY == "", "NA", HFA_EBOLA_3$DONOR_COUNTRY)
    HFA_EBOLA_3$DONOR_NAME <- ifelse(HFA_EBOLA_3$DONOR_NAME == "", "NA", HFA_EBOLA_3$DONOR_NAME)
    
    # Group to create id
    HFA_EBOLA_3 <- HFA_EBOLA_3 %>%
      group_by(DONOR_NAME, DONOR_COUNTRY, INCOME_SECTOR) %>%
      mutate(temp_id = cur_group_id()) %>%
      ungroup()
    
    
    # replace with other minus other of the previous year, and set the negative other to 0
    dah_columns <- grep("_DAH$", names(HFA_EBOLA_3), value = TRUE)
    for (var in dah_columns) {
      # For each year from 'data_yr' - 1 to 1991, perform the conditional adjustments
      for (year in (data_yr-1):1991) {
        # Adjust values based on the conditions provided
        HFA_EBOLA_3 <- HFA_EBOLA_3 %>%
          arrange(temp_id, YEAR) %>%
          mutate(
            !!var := case_when(
              YEAR == year & !!sym(var) < 0 ~ 0,
              TRUE ~ !!sym(var)
            )
          )
      }
      
      # Additional condition for the year 1990
      HFA_EBOLA_3 <- HFA_EBOLA_3 %>%
        mutate(
          !!var := if_else(YEAR == 1990 & !!sym(var) < 0, 0, !!sym(var))
        )
    }

  # calculate a new total as the sum of the HFAs with no negatives
    HFA_EBOLA_3$DAH <- NULL
    HFA_EBOLA_3 <- HFA_EBOLA_3 %>%
      rowwise() %>%
      mutate(DAH = sum(c_across(ends_with("_DAH")), na.rm = TRUE)) %>%
      ungroup()
    
    
  # Add in-kind 
    inkind_r <- as.data.frame(read_excel(paste0(RAW, "/UNFPA_INKIND_RATIOS_1990_", report_yr, ".xlsx")))
    inkind_r <- inkind_r %>%
      select(YEAR, INKIND_RATIO)
    
    total <- HFA_EBOLA_3 %>%
      left_join(inkind_r, by = "YEAR") %>%
      mutate(across(c(DAH, ends_with("_DAH")), ~ .x * (1 - INKIND_RATIO)))    
    
    inkind_amt <- HFA_EBOLA_3 %>%
      left_join(inkind_r, by = "YEAR") %>%
      mutate(across(c(DAH, ends_with("_DAH")), ~ .x * INKIND_RATIO)) %>%
      mutate(INKIND = 1)

    total_inkind_append <- bind_rows(total, inkind_amt)

    total_inkind_append <- total_inkind_append %>%
      mutate(INKIND = case_when(
        is.na(INKIND) ~ 0,
        TRUE ~ INKIND
      ))
    
    # applying upper-case conversion to just the DONOR_NAMES without broken characters
    # the full merge s
    safe_toupper <- function(x) {
      sapply(x, function(char) {
        tryCatch(toupper(char), error = function(e) char)
      }, USE.NAMES = FALSE)
    }
    
    total_inkind_append$DONOR_NAME <- sapply(total_inkind_append$DONOR_NAME, safe_toupper)

    inkind_2021_update <- as.data.frame(read_dta("FILEPATH/income_sector_and_type_assignments_2021_unfpa update.dta"))
    
    total_inkind_append$source <- "total"
    inkind_2021_update$source <- "update"
    total_unpdate_merge <- full_join(total_inkind_append, inkind_2021_update, by = "DONOR_NAME") %>%
      mutate(merge_col = case_when(
        !is.na(source.x) & is.na(source.y) ~ "total_only",
        !is.na(source.x) & !is.na(source.y) ~ "both",
        TRUE ~ "update_only"
      ))
    
    total_unpdate_merge <- total_unpdate_merge %>%
      select(-source.x, -source.y) %>%
      filter(merge_col == "total_only" | merge_col == "both") 
    
    total_unpdate_merge <- total_unpdate_merge %>%
      mutate(INCOME_SECTOR = case_when(
        INCOME_SECTOR == "" ~ INCOME_SECTOR2,
        INCOME_SECTOR != INCOME_SECTOR2 & merge_col == "both" ~ INCOME_SECTOR2,
        TRUE ~ INCOME_SECTOR
      )) %>%
      mutate(INCOME_TYPE = case_when(
        INCOME_TYPE == "" ~ INCOME_TYPE2,
        INCOME_TYPE != INCOME_TYPE2 & merge_col == "both" ~ INCOME_TYPE2,
        TRUE ~ INCOME_TYPE
      ))
    
    total_inkind_append$source <- NULL
    inkind_2021_update$source <- NULL
    total_unpdate_merge$merge_col <- NULL

  # tag double counting
    total_unpdate_merge <- total_unpdate_merge %>%
      mutate(DONOR_NAME = case_when(
        DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA" ~ "GFATM",
        DONOR_NAME == "THE GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA" ~ "GFATM",
        DONOR_NAME == "GLOBAL FUND" ~ "GFATM",
        DONOR_NAME == "AFRICAN DEVELOPMENT BANK" ~ "AfDB",
        DONOR_NAME == "ASIAN DEVELOPMENT BANK" ~ "AsDB",
        DONOR_NAME == "ASIAN DEVELOPMENT BANK (ADB)" ~ "AsDB",
        DONOR_NAME == "INTER-AMERICAN DEVELOPMENT BANK" ~ "IDB",
        DONOR_NAME == "WORLD BANK" ~ "WB",
        DONOR_NAME == "INTERNATIONAL BANK FOR RECONSTRUCTION AND DEVELOPMENT" ~ "WB_IBRD",
        TRUE ~ DONOR_NAME
      ))
    
    
    # NOTES:
    # (1) If UNDP gets added as a channel:
    #   replace DONOR_NAME = "UNDP" if DONOR_NAME == "UNDP, NEW YORK"
    # replace DONOR_NAME = "UNDP" if DONOR_NAME == ///
    #   regexm(DONOR_NAME, "UNITED NATIONS DEVELOPMENT PROGRAMME")
    # 
    # (2) No PAHO, GAVI, BMGF
    # 
    # (3) No need to tag Unitaid, Wellcome, or CRS (European Commission) - these 
    # are already handled
    # 
    # (4) No need to tag AfDB, IDB, or WB, since these development banks data 
    # did not include transfers in the first place; ie they do not report
    # expenditure if that money is a transfer.
    # replace ELIM_CH = 1 if DONOR_NAME == "AfDB"
    # replace ELIM_CH = 1 if DONOR_NAME == "IDB"
    # replace ELIM_CH = 1 if DONOR_NAME == "WB_IBRD" | DONOR_NAME=="WB"
    # replace ELIM_CH = 1 if DONOR_NAME == "UNDP" 
    
    total_unpdate_merge$ELIM_CH <- 0
    total_unpdate_merge <- total_unpdate_merge %>%
      mutate(ELIM_CH = case_when(
        DONOR_NAME == "UNICEF" ~ 1,
        DONOR_NAME == "UNAIDS" ~ 1,
        DONOR_NAME == "WHO" ~ 1, 	
        DONOR_NAME == "GFATM" ~ 1,
        DONOR_NAME == "AsDB" ~ 1,
        TRUE ~ELIM_CH 
      ))
    
    total_unpdate_merge$SOURCE_CH <- ifelse(total_unpdate_merge$ELIM_CH == 1, total_unpdate_merge$DONOR_NAME, "")

    total_unpdate_merge <- total_unpdate_merge %>%
      mutate(CHANNEL = case_when(
        CHANNEL == "" ~ "UNFPA",
        TRUE ~ CHANNEL
      )) %>%
      select(-EBOLA, -temp_id)
    
    write_dta(total_unpdate_merge, paste0(FIN, "/UNFPA_ADB_PDB_FGH", report_yr, "_ebola_fixed_includesDC_", update_tag, ".dta"))
    write_dta(total_unpdate_merge, paste0(FIN, "/UNFPA_ADB_PDB_FGH", report_yr, "_includesDC.dta"))

    cat("Done.\n")
    
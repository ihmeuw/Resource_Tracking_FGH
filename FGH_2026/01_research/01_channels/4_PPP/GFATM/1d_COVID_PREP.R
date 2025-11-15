#### #----#                        Docstring                         #----# ####
#' Project:         FGH
#'    
#' Purpose:         COVID data cleaning
#------------------------------------------------------------------------------#

####################### #----# ENVIRONMENT SETUP #----# ########################
rm(list=ls())
if (!exists("code_repo")) {
  code_repo <- 'FILEPATH'
}

## Source functions

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))
source(paste0(code_repo, "FUNCTIONS/helper_functions.R"))
pacman::p_load(crayon, stringr, readstata13, readxl, magrittr)

## Local CONSTANTS
# get_fgh_round_ids()
FGH_ROUND_ID <- 16
# NOTE: Update the path below when newer versions are downloaded

DOWNLOADS <- list(
    "2021" = paste0(
      dah.roots$j,
      "FILEPATH",
      "FGH_2021/covid_approved_funding_report_2022131.xlsx"
    ),
    "2022" = paste0(
      dah.roots$j,
      "FILEPATH",
      "FGH_2022/downloaded_files/covid_approved_funding_report_20221115.xlsx"
    ),
    "2023" = paste0(
      dah.roots$j,
      "FILEPATH",
      "FGH_2023/downloaded_files/covid_approved_funding_report_202425.xlsx"
    ),
    "2024" = paste0(
      dah.roots$j,
      "FILEPATH",
      "FGH_2024/downloaded_files/covid_approved_funding_report_2025117.xlsx"
    )
)
#------------------------------------------------------------------------------#


############################## #----# MAIN #----# ##############################
cat("\n\n")
cat(green(" ###############################\n"))
cat(green(" #### GFATM COVID DATA PREP ####\n"))
cat(green(" ###############################\n\n"))

#------------------------------------------------------------------------------#

#### #----#                     Processes 2020-2023 reports            #----# ####
process_report <- function(path, is.2020 = FALSE) {
  if (is.2020 == TRUE) {
    dt <- setDT(read_excel(path,
      sheet = "COVID-19 RM 2020", # RM -> Response Mechanism
      skip = 17)) # Table starts at line 18
    setnames(dt,
             c("Country/Multicountry", "Currency", "Total\r\nimmediate\r\nfunding for 2020"),
             c("country", "currency", "total"))
  } else {
    dt <- setDT(read_excel(path,
      sheet = "COVID-19 RM 2021", # RM -> Response Mechanism
      skip = 17)) # Table starts at line 18
    setnames(dt,
             c("Country/Multicountry", "Currency", "Total immediate funding for 2021"),
             c("country", "currency", "total"))
  }
    # Subset and rename columns - 2021
    dt <- dt[, .(country, currency, total)]
    total_row_position <- which(dt$country == "Total")
    dt <- dt[1:(total_row_position - 1)]
    dt[, total := as.numeric(total)]
    dt <- dt[rowSums(is.na(dt)) != ncol(dt)]
    if (is.2020 == TRUE) {dt[, YEAR := 2020]}
    else {dt[, YEAR := 2021]}
    return(dt)
}

#### #----#             Compute differences between reports          #----# ####
# To get the `report_year`'s new COVID funding from the document's 2021 tab,
# need to compute the difference between the current numbers and the previous
# report update's numbers.

compute_data_diff <- function(dt_new, dt_old) {
    new <- data.table::melt(dt_new[, - "YEAR"],
                            id.vars = c("country", "currency"),
                            value.name = "raw_value")
    
    old <- data.table::melt(dt_old[, - "YEAR"],
                            id.vars = c("country", "currency"),
                            value.name = "old_value")
    
    new <- merge(new, old,
                 by = c("country", "currency", "variable"),
                 all.x = TRUE)
    
    # if no funds for this country previously existed, set to 0
    new[is.na(old_value), old_value := 0]
    
    # calc amount of new funding
    new[, new_value := raw_value - old_value]
    
    # if new_value < 0, funds were subtracted, so remove from previous year
    new[new_value < 0, `:=` (old_value = old_value + new_value,
                             new_value = 0)]
    
    if (nrow(new[old_value < 0]) != 0) {
        stop("Error adjusting for decreases in grant amounts.")
    }
    
    # convert data back to wide format
    dt_old <- data.table::dcast(new[, .(country, currency, variable, old_value)],
                                 country + currency ~ variable,
                                 value.var = "old_value")
    
    dt_new <- data.table::dcast(new[, .(country, currency, variable, new_value)],
                                 country + currency ~ variable,
                                 value.var = "new_value")
    return(list(new = dt_new, old = dt_old))
}

# PREP DATA
# The 2020 data is retrieced from the 2021 year report
dt_2020 <- process_report(DOWNLOADS[["2021"]], is.2020 = TRUE)
dt_2021 <- process_report(DOWNLOADS[["2021"]])
dt_2022 <- process_report(DOWNLOADS[["2022"]])
dt_2023 <- process_report(DOWNLOADS[["2023"]])

dt_2024 <- setDT(read_excel(
  DOWNLOADS[["2024"]],
  sheet = "C19RM Awards",
  skip = 19 # Table starts at line 18
))
# Subset and rename columns
dt_2024 <- dt_2024[, .(`Country or Multicountry`, `...7`, ...10)]
setnames(dt_2024, 
         c("Country or Multicountry", "...7", "...10"),
         c("country", "total2021", "totaladditional"))
dt_2024 <- dt_2024[2:132]
dt_2024 <- dt_2024[, lapply(.SD, function(x) ifelse(is.na(x), 0, x))]
dt_2024 <- dt_2024[, total := as.numeric(total2021) + as.numeric(totaladditional)]
dt_2024 <- dt_2024[, c("total2021","totaladditional") := NULL]
dt_2024[, YEAR := 2021]
dt_2024[, currency := "USD"]


# finalize datasets
dt_2020[, oid_covid_amt := total]
dt_2021[, oid_covid_amt := total]


# COMPUTE DIFFERENCES
# Essentially we downloaded the same spreadsheet at different points in time,
# so the totals from t0 to t1 are attributed to 2021, from t1 to t2 go to 2022,
# and from t2 to t3 go to 2023 (t0 = start of funding round, t1 = end of 2021,
# t2 = end of 2022, t3 = end of 2023)
# allocate this funding as 2022, covid
o <- compute_data_diff(dt_2022, dt_2021)
dt_2022_cov <- o$new
dt_2022_cov[, oid_covid_amt := total]
dt_2022_cov[, YEAR := 2022]


# website $547 mil aug 22-jan 23 (so mostly 22) -- matches our 547 amt perfectly.
# downloaded in june 23.
# allocate this funding as 2022
o <- compute_data_diff(dt_2023, dt_2022)
dt_2022_hss <- o$new
dt_2022_hss[, swap_hss_other_amt := .5*total]
dt_2022_hss[, swap_hss_pp_amt := .5*total]
dt_2022_hss[, YEAR := 2022]

# website--$320 mil btwn march and december; $347 sum
o <- compute_data_diff(dt_2024, dt_2023)
dt_2023_hss <- o$new
dt_2024_hss <- copy(dt_2023_hss)
dt_2023_hss[, YEAR := 2023]
scale <- 320000000/sum(dt_2023_hss$total)
# scale 2023 values
dt_2023_hss[, total_scaled := total * scale]
dt_2023_hss[, swap_hss_pp_amt := .5*total_scaled]
dt_2023_hss[, swap_hss_other_amt := .5*total_scaled]
dt_2023_hss[, total := total_scaled]
dt_2023_hss[, total_scaled := NULL]

# scale 2024 values
dt_2024_hss[, YEAR := 2024]
dt_2024_hss[, total_scaled := total * (1-scale)]
dt_2024_hss[, swap_hss_other_amt := .5*total_scaled]
dt_2024_hss[, swap_hss_pp_amt := .5*total_scaled]
dt_2024_hss[, total := total_scaled]
sum(dt_2024_hss$total_scaled)
dt_2024_hss[, total_scaled := NULL]



#### #----#                   Combined COVID data                    #----# ####
## Concatenate all years
dt <- rbind(dt_2020, dt_2021, dt_2022_cov, dt_2022_hss, dt_2023_hss, dt_2024_hss, fill = TRUE)
rm(dt_2020, dt_2021, dt_2022, dt_2022_cov, dt_2022_hss, dt_2023_hss, dt_2024_hss, dt_2023, dt_2024)
dt <- dt[total > 150]

## Grant flexibilities for both 2020 and 2021
flex <- setDT(read_excel(
  DOWNLOADS[["2021"]], #check this is the right year...
  sheet = "Grant Flexibilities",
  skip = 13 # Table starts row 14
))
# Subset and rename columns
flex <- flex[1:(nrow(flex) - 1), c("Country/Multicountry", "USD equivalent")]
colnames(flex) <- c("country", "repurposed_money")
#------------------------------------------------------------------------------#

#### #----#                 Combined data formatting                 #----# ####
cat("  Data formatting\n")
# replace NA with 0
dt <- dt[, lapply(.SD, function(x) ifelse(is.na(x), 0, x))]

# Our Global Fund contact let us know that $500m from the Covid Funding Report
# is actually repurposed money, so the following code creates a fraction
# to account for that
scale <- 500000000/sum(dt$total, na.rm=T)

repurposed  <- dt %>%
  filter(YEAR %in% 2020:2021) %>%
  mutate(total = total * scale) %>%
  mutate(oid_covid_amt = total) %>%
  mutate(money_type = "repurposed_money")

new_money  <- dt %>%
  filter(YEAR %in% 2020:2021) %>%
  mutate(total = total * (1-scale)) %>%
  mutate(oid_covid_amt = total)



# Fill in purpose for flex as "Grant Flexibilities"
flex[, purpose := "Grant Flexibilities"]
# divide money into 2 years (2020 and 2021)
flex[, YEAR_2020 := repurposed_money / 2]
flex[, YEAR_2021 := repurposed_money / 2]
flex_long <- melt.data.table(flex[, -"repurposed_money"],
                             id.vars = c("country"),
                             measure.vars = c("YEAR_2020", "YEAR_2021"))
flex_long[variable == "YEAR_2020", YEAR := 2020]
flex_long[variable == "YEAR_2021", YEAR := 2021]
flex_long[, variable := NULL]
flex_long[, total := value]
setnames(flex_long, "value", "oid_covid_amt")
flex_long[, money_type := "repurposed_money"]
rm(flex)

# Append flex on dt to combine data
dt <- dt %>% filter(YEAR %ni% 2020:2021)
dt <- rbind(dt, new_money, repurposed, flex_long, fill = T)
dt <- dt %>% 
  mutate(money_type = ifelse(is.na(money_type), "new_money", money_type)) %>%
  mutate(channel = "GFATM")
setnames(dt, c("country", "total"), c("recipient_country", "total_amt"))

# Recode recipient country names
dt[recipient_country == "Bolivia (Plurinational State)", recipient_country := "Bolivia (Plurinational State of)"]
dt[recipient_country == "Iran (Islamic Republic)", recipient_country := "Iran (Islamic Republic of)"]
dt[recipient_country == "Lao (Peoples Democratic Republic)", recipient_country := "Lao People's Democratic Republic"]
dt[recipient_country == "Moldova", recipient_country := "Republic of Moldova"]
dt[recipient_country %in% c("Tanzania (United Republic)", "Zanzibar"), recipient_country := "United Republic of Tanzania"]
dt[recipient_country == "Venezuela", recipient_country := "Venezuela (Bolivarian Republic of)"]
dt[recipient_country == "Congo (Democratic Republic)", recipient_country := "Democratic Republic of the Congo"]
dt[recipient_country == "Korea (Democratic Peoples Republic)", recipient_country := "Democratic People's Republic of Korea"]
#------------------------------------------------------------------------------# 

#### #----#                      Merge metadata                      #----# ####
cat("  Merge metadata\n")
# Location IDs
isos <- fread(paste0(
  get_path("meta", "locs"), 
  "fgh_location_set.csv"
))[level == 3, c("location_name", "ihme_loc_id", "region_name")]
dt <- merge(dt, isos, by.x="recipient_country", by.y="location_name", all.x = T)
dt[recipient_country == "Kosovo", `:=`(ihme_loc_id = "KSV", region_name = "Central Europe")]
rm(isos)

# Income groups
dt <- get_ig(dt, fgh_round_id = FGH_ROUND_ID)

setnames(dt, c("ihme_loc_id", "region_name", "income_group"),
         c("iso3_rc", "gbd_region", "INC_GROUP"))
#------------------------------------------------------------------------------#

#### #----#                  Save out COVID dataset                  #----# ####
dt[, `:=`(donor_name = "GFATM")]
setnames(dt, "YEAR", "year")
dt <- dt[, c("year",
             "channel",
             "donor_name",
             "iso3_rc",
             "recipient_country",
             "gbd_region",
             "INC_GROUP",
             "oid_covid_amt",
             "swap_hss_other_amt",
             "swap_hss_pp_amt",
             "total_amt",
             "money_type"), with=F]
cat("  Save out COVID dataset\n")
save_dataset(dt, "COVID_prepped_pre_donor", "GFATM", "fin")
#------------------------------------------------------------------------------#


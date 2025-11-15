#### #----#                    Docstring                    #----# ####
# Project:  FGH
# Purpose:  Prep DAC data - calculate coverage ratio of DAC commitments 
#           to CRS disbursements
#---------------------------------------------------------------------#

#----# Environment Prep #----# ####
rm(list=ls(all.names = TRUE))
if (!exists("code_repo"))  {
  code_repo <- 'FILEPATH'
}

report_year <- 2024

source(paste0(code_repo, '/FGH_', report_year, '/utils.R'))
pacman::p_load(crayon, readstata13, tidyr)

#---------------------------------------------------------------------#

cat('\n\n')
cat(green(' ############################\n'))
cat(green(' #### CRS CLEAN DAC DATA ####\n'))
cat(green(' ############################\n\n'))


cat('  Read input data\n')
#----# Read input data #----# ####
dac <- fread(get_path("dac", "raw", "OECD.DCD.FSD,DSD_DAC1@DF_DAC5.csv"))
# filter years
dac <- dac[TIME_PERIOD >= 1990]

# filter sectors
keep_sectors <- c(
    "Education",
    "Health",
    "Population policies/Programmes & reproductive health",
    "Water supply & sanitation",
    "Government and civil society",
    "Other social infrastructure and services",
    "Economic infrastructure and services",
    "Production sectors",
    "Multi-sector / Cross-cutting",
    "Commodity aid / General programme assistance",
    "Action relating to debt",
    "Humanitarian aid",
    "Unallocated / unspecified"
)
dac <- dac[Sector %in% keep_sectors]
stopifnot( length(unique(dac$Sector)) == length(keep_sectors) )


# filter donors
dac[Donor == "Korea", Donor := "S Korea"]
dac <- dac[Donor %in% dah_cfg$crs$donors]
stopifnot( length(unique(dac$Donor)) == length(dah_cfg$crs$donors) )

setnames(dac,
         c("Price base", "Donor", "Sector", "SECTOR"),
         c("curr_type",  "donor", "sector", "sector_code"))

#---------------------------------------------------------------------#

cat('  Pull location metadata\n')
#----# Pull location metadata #----# ####
locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
locs <- locs[level == 3, .(ihme_loc_id, location_name)]
#---------------------------------------------------------------------#

cat('  Clean data\n')
#----# Clean data #----# ####

# Correct location names and merge with locations
locs[, location_name := fcase(
    location_name == "Republic of Korea", "S Korea",
    location_name == "Slovakia", "Slovak Republic",
    location_name == "United States of America", "United States",
    rep_len(TRUE, .N), location_name
)]

dac_iso <- merge(dac, locs,
                 by.x = "donor", by.y = "location_name", all.x = TRUE)

# Replace EC names and Drop multilaterals other than the EU
dac_iso[donor == "EU Institutions", donor := "EC"]
dac_iso[donor == "EC", ihme_loc_id := "EC"]

# Clean dataset (could make some of these more efficient)
dac_iso[curr_type == "Constant prices", curr_type := "cons"]
dac_iso[curr_type == "Current prices", curr_type := "curr"]


n_dac <- length(dah_cfg$crs$donors)
if (length(unique(dac_iso$ihme_loc_id)) != n_dac) {
      cat(red(italic(paste('\t ERROR: Ensure your data contains only the', n_dac, 'bilaterals and EC\n'))))
      cat(red(paste0('\t Missing ISOS: \t', bold(dac_isos[! dac_isos %in% unique(dac_iso$ihme_loc_id)]), "\n")))
      stop("Missing DAC members")
}

# filter to ODA and calculate total ODA
dt <- dac_iso[Measure %in% c("Bilateral ODA commitments")]
dt <- dt[, .(value = sum(OBS_VALUE)),
         by = .(donor, ihme_loc_id,
                sector, sector_code,
                year = TIME_PERIOD,
                curr_type,
                mult = `Unit multiplier`,
                measure = `Unit of measure`)]

# cast wide so that we have constant and current prices as columns and not rows
dt <- data.table::dcast(dt,
                        donor + ihme_loc_id +
                            sector + sector_code + year +
                            mult + measure
                            ~ curr_type,
                        value.var = "value")

setnames(dt,
         c("cons", "curr", "ihme_loc_id"),
         c("dac_oda_commcons", "dac_oda_commcurr", "isocode"))
#---------------------------------------------------------------------#
cat('  Save Stage 1a Data\n')
#----# Save Stage 1a data #----# ####
save_dataset(dt, '1a_DAC_CLEAN', 'DAC', 'int')
#---------------------------------------------------------------------#

cat('\n\n\t ###############################\n\t #### file  CRS 1h complete ####\n\t ###############################\n\n')

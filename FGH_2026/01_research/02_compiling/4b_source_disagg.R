#
# Disaggregate donors which should not appear as sources in our framework
#  because they are channels of funding.
#
# This happens because we track DAH at the channel level, and these channels may
# report receiving funds from other channels.
# For example, WHO contributes to several other channels, so they appear as a
# source of funding in our data. However, in our framework, WHO does not actually
# have money of its own, because it relies on contributions from countries or
# private foundations. Thus, we can disaggregate these flows further using the
# sources of WHO's funding.
#
# For each of our channels (including EC, but not including bilaterals or Gates),
# we:
# * create channel-specific data for channel A, which is used to calculate 
#   empirical source shares for channel A based on our compiled estimates.
# * create source-specific data, which consists of flows where the source is
#    currently marked as channel A. This can require some creative filtering to
#    tag relevant flows, because DONOR_NAME is not a standardized variable.
#
# Implemented for first time in FGH 2024
#
rm(list = ls(all.names = TRUE))

code_repo <- 'FILEPATh'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

dah_yr <- paste0("DAH_", dah_cfg$abrv_year)
last_complete_donor_year <- 2023 ## last year with no prelim estimates 


disagg_source <- function(dt_chann, dt_source) {
    # The channel data.table should contain all flows of DAH through the given
    # channel. The source data.table should contain all flows of DAH where the
    # given channel appears as a source.
    # The dt_chann is used to calculate source fractions for the channel, i.e.,
    # where does money flowing through that channel originate. Fractions are
    # computed for each year.
    # These fractions are then merged on and applied to the source data.table,
    # so that the channel no longer appears as a source of funding (it is
    # disaggregated into its own sources).
    # This expands the number of rows, since we take a single flow and explode
    # it into multiple flows, one for each new source.
    
    # calculate source fractions for the channel
    fr <- dt_chann[ELIM_CH == 0 & ELIM_DONOR == 0,
                   .(dah = sum(get(dah_yr))),
                   by = .(YEAR,
                          INCOME_SECTOR, INCOME_TYPE,
                          ISO_CODE, DONOR_COUNTRY)]
    fr[, yr_tot := sum(dah, na.rm = TRUE), by = YEAR]
    fr[, donor_frac := dah / yr_tot]
    fr[, c("dah", "yr_tot") := NULL]
    
    # merge with the source data
    dt_source[, c("INCOME_SECTOR", "INCOME_TYPE",
                  "ISO_CODE", "DONOR_COUNTRY") := NULL]
    dt_source <- merge(dt_source,
                       fr,
                       by = "YEAR",
                       all.x = TRUE,
                       # allow cartesian since merging on multiple donors to each flow
                       allow.cartesian = TRUE)
    
    # apply donor fractions to source data
    dah_cols <- grep("DAH", names(dt_source), value = TRUE)
    dt_source[, (dah_cols) := lapply(.SD, \(x) x * donor_frac),
        .SDcols = dah_cols]
    dt_source[, donor_frac := NULL]
    
    return(dt_source)
}



adb <- fread(get_path("compiling", "int", "adb_pre_disagg.csv"))



#
# First, we disaggregate OTHER PUBLIC for preliminary years ====
#
main_pub_locs <- c(names(dah_cfg$crs$donors), "CHN")
adb[, otherpub := INCOME_SECTOR == "PUBLIC" &
        DONOR_COUNTRY == "UNALLOCABLE" &
        YEAR > last_complete_donor_year]
## calculate proportion of public funding going to non-main donors
## based on last 3 years of complete donor data
donor_fracs <- adb[
    INCOME_SECTOR == "PUBLIC" & ! ISO_CODE %in% main_pub_locs &
        between(YEAR, last_complete_donor_year - 2, last_complete_donor_year),
    .(dah = sum(DAH_24, na.rm = TRUE)),
    by = .(ISO_CODE, CHANNEL)
]
donor_fracs[, frac := dah / sum(dah), by = CHANNEL]


otherpub_disagg <- adb[otherpub == TRUE]
otherpub_disagg[, c("ISO_CODE", "DONOR_NAME", "DONOR_COUNTRY",
                    "INCOME_SECTOR", "INCOME_TYPE") := NULL]
otherpub_disagg[, row := .I]
otherpub_disagg <- merge(
    otherpub_disagg,
    donor_fracs[, .(CHANNEL, ISO_CODE, frac)],
    by = "CHANNEL",
    all.x = TRUE,
    allow.cartesian = TRUE
)
stopifnot(
    ## ensure fractions sum to 1
    otherpub_disagg[, sum(frac), by = row][abs(V1 - 1) > 1e-6, .N] == 0
)
## apply fractions to all DAH cols
dah_cols <- grep("DAH", names(otherpub_disagg), value = TRUE)
otherpub_disagg[, (dah_cols) := lapply(.SD, function(x) x * frac), .SDcols = dah_cols]
otherpub_disagg[, c("frac", "row") := NULL]
## input other donor info
otherpub_disagg[, `:=`(
    INCOME_SECTOR = "PUBLIC",
    INCOME_TYPE = "CENTRAL"
)]
otherpub_disagg <- merge(
    otherpub_disagg,
    unique(adb[, .(ISO_CODE, DONOR_COUNTRY)]),
    by = "ISO_CODE",
    all.x = TRUE
)
otherpub_disagg[, DONOR_NAME := DONOR_COUNTRY]

## re-combine with main adb
adb <- rbind(
    adb[otherpub == FALSE | YEAR <= last_complete_donor_year],
    otherpub_disagg
)
adb[, otherpub := NULL]



#
# disaggregate source EC ====
#
adb[, src := ""]
adb[INCOME_TYPE == "EC", src := "EC"]
chn_ec <- adb[CHANNEL == "EC"]
src_ec <- adb[src == "EC"]
src_ec <- disagg_source(chn_ec, src_ec)
src_ec[, `:=`(SOURCE_CH = "EC", DONOR_NAME = "EC_Disaggregated")]
adb <- rbind(
    adb[src != "EC"],
    src_ec
)
rm(chn_ec, src_ec)

#
# disaggregate source WHO ====
#
adb[, src := ""]
adb[DONOR_NAME == "WHO", src := "WHO"]
chn_who <- adb[CHANNEL == "WHO"]
src_who <- adb[src == "WHO"]
src_who <- disagg_source(chn_who, src_who)
src_who[, `:=`(SOURCE_CH = "WHO", DONOR_NAME = "WHO_Disaggregated")]
adb <- rbind(
    adb[src != "WHO"],
    src_who
)
rm(chn_who, src_who)


#
# disaggregate source World Bank - IDA ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c("WB", "WB_IDA", "WORLD BANK"), src := "WB_IDA"]
### dont try to disaggregate flows from IBRD to IDA 
adb[CHANNEL == "WB_IBRD", src := ""]
chn_wb <- adb[CHANNEL == "WB_IDA"]
src_wb <- adb[src == "WB_IDA"]
src_wb <- disagg_source(chn_wb, src_wb)
src_wb[, `:=`(SOURCE_CH = "WB_IDA", DONOR_NAME = "WB_IDA_Disaggregated")]
adb <- rbind(
    adb[src != "WB_IDA"],
    src_wb
)
rm(chn_wb, src_wb)

#
# disaggregate source World Bank - IBRD ====
#
adb[, src := ""]
adb[DONOR_NAME == "WB_IBRD", src := "WB_IBRD"]
### dont try to disaggregate flows from IDA to IBRD
adb[CHANNEL == "WB_IDA", src := ""]
chn_wb <- adb[CHANNEL == "WB_IBRD"]
src_wb <- adb[src == "WB_IBRD"]
src_wb <- disagg_source(chn_wb, src_wb)
src_wb[, `:=`(SOURCE_CH = "WB_IBRD", DONOR_NAME = "WB_IBRD_Disaggregated")]
adb <- rbind(
    adb[src != "WB_IBRD"],
    src_wb
)
rm(chn_wb, src_wb)


#
# disaggregate source AFDB ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c("AFDB", "AFRICAN DEVELOPMENT BANK GROUP"), src := "afdb"]
chn_af <- adb[CHANNEL == "AfDB"]
src_af <- adb[src == "afdb"]
src_af <- disagg_source(chn_af, src_af)
src_af[, `:=`(SOURCE_CH = "AfDB", DONOR_NAME = "AfDB_Disaggregated")]
adb <- rbind(
    adb[src != "afdb"],
    src_af
)
rm(chn_af, src_af)


#
# disaggregate source ASDB ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c("ASDB", "ADB"), src := "asdb"]
chn_as <- adb[CHANNEL == "AsDB"]
src_as <- adb[src == "asdb"]
src_as <- disagg_source(chn_as, src_as)
src_as[, `:=`(SOURCE_CH = "AsDB", DONOR_NAME = "AsDB_Disaggregated")]
adb <- rbind(
    adb[src != "asdb"],
    src_as
)
rm(chn_as, src_as)


#
# disaggregate source IDB ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c("IDB"), src := "idb"]
chn_idb <- adb[CHANNEL == "IDB"]
src_idb <- adb[src == "idb"]
src_idb <- disagg_source(chn_idb, src_idb)
src_idb[, `:=`(SOURCE_CH = "IDB", DONOR_NAME = "IDB_Disaggregated")]
adb <- rbind(
    adb[src != "idb"],
    src_idb
)
rm(chn_idb, src_idb)


#
# disaggregate source GFATM  ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "GFATM",
    "THE GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)",
    "GLOBAL FUND TO FIGHT AIDS TUBERCULOSIS AND MALARIA GFATM"
), src := "gfatm"]
chn_gf <- adb[CHANNEL == "GFATM"]
src_gf <- adb[src == "gfatm"]
src_gf <- disagg_source(chn_gf, src_gf)
src_gf[, `:=`(SOURCE_CH = "GFATM", DONOR_NAME = "GFATM_Disaggregated")]
adb <- rbind(
    adb[src != "gfatm"],
    src_gf
)
rm(chn_gf, src_gf)


#
# disaggregate source GAVI ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "GAVI", "GAVI ALLIANCE", "GAVI, THE VACCINE ALLIANCE",
    "GAVI THE VACCINE ALLIANCE", "GAVI,  THE VACCINE ALLIANCE"
), src := "gavi"]
chn_gavi <- adb[CHANNEL == "GAVI"]
src_gavi <- adb[src == "gavi"]
src_gavi <- disagg_source(chn_gavi, src_gavi)
src_gavi[, `:=`(SOURCE_CH = "GAVI", DONOR_NAME = "GAVI_Disaggregated")]
adb <- rbind(
    adb[src != "gavi"],
    src_gavi
)
rm(chn_gavi, src_gavi)


#
# disaggregate UNAIDS ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "UNAIDS",
    "JOINT UNITED NATIONS PROGRAM ON HIV AIDS UNAIDS"
), src := "unaids"]
chn_unaids <- adb[CHANNEL == "UNAIDS"]
src_unaids <- adb[src == "unaids"]
src_unaids <- disagg_source(chn_unaids, src_unaids)
src_unaids[, `:=`(SOURCE_CH = "UNAIDS", DONOR_NAME = "UNAIDS_Disaggregated")]
adb <- rbind(
    adb[src != "unaids"],
    src_unaids
)
rm(chn_unaids, src_unaids)


#
# disaggregate PAHO ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "PAHO",
    "PAN AMERICAN HEALTH ORGANIZATION (PAHO)"
), src := "paho"]
chn_paho <- adb[CHANNEL == "PAHO"]
src_paho <- adb[src == "paho"]
src_paho <- disagg_source(chn_paho, src_paho)
src_paho[, `:=`(SOURCE_CH = "PAHO", DONOR_NAME = "PAHO_Disaggregated")]
adb <- rbind(
    adb[src != "paho"],
    src_paho
)
rm(chn_paho, src_paho)


#
# disaggregate UNICEF ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "UNICEF",
    "UNITED NATIONS CHILDREN'S FUND (UNICEF)"
), src := "unicef"]
chn_unicef <- adb[CHANNEL == "UNICEF"]
src_unicef <- adb[src == "unicef"]
src_unicef <- disagg_source(chn_unicef, src_unicef)
src_unicef[, `:=`(SOURCE_CH = "UNICEF", DONOR_NAME = "UNICEF_Disaggregated")]
adb <- rbind(
    adb[src != "unicef"],
    src_unicef
)
rm(chn_unicef, src_unicef)


#
# disaggregate UNFPA ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "UNFPA",
    "UNITED NATIONS POPULATION FUND (UNFPA)"
), src := "unfpa"]
chn_unfpa <- adb[CHANNEL == "UNFPA"]
src_unfpa <- adb[src == "unfpa"]
src_unfpa <- disagg_source(chn_unfpa, src_unfpa)
src_unfpa[, `:=`(SOURCE_CH = "UNFPA", DONOR_NAME = "UNFPA_Disaggregated")]
adb <- rbind(
    adb[src != "unfpa"],
    src_unfpa
)
rm(chn_unfpa, src_unfpa)


#
# disaggregate UNITAID ====
#
adb[, src := ""]
adb[DONOR_NAME %in% c(
    "UNITAID"
), src := "unitaid"]
adb[src == "unitaid" & YEAR < 2007, `:=`( ## founded in 2006, no disb until 2007
    INCOME_SECTOR = "UNALL", INCOME_TYPE = "UNALL",
    DONOR_NAME = "UNALLOCABLE", ISO_CODE = "QZA", DONOR_COUNTRY = "UNALLOCABLE",
    src = ""
)]
chn_unitaid <- adb[CHANNEL == "UNITAID"]
src_unitaid <- adb[src == "unitaid"]
src_unitaid <- disagg_source(chn_unitaid, src_unitaid)
src_unitaid[, `:=`(SOURCE_CH = "UNITAID", DONOR_NAME = "UNITAID_Disaggregated")]
adb <- rbind(
    adb[src != "unitaid"],
    src_unitaid
)
rm(chn_unitaid, src_unitaid)




#
# finalize, test, and save =====
#

## Finally, we don't want debt repayments to show up as a source for any
## non-development banks
adb[INCOME_SECTOR == "DEBT" & ! CHANNEL %in% c("WB_IBRD", "WB_IDA", "AfDB", "AsDB", "IDB"),
    `:=`(
        INCOME_SECTOR = "OTHER",
        INCOME_TYPE = "OTHER"
    )]

## Simplify rows
adb[INCOME_SECTOR == "PUBLIC", DONOR_NAME := DONOR_COUNTRY]
adb[INCOME_SECTOR == "BMGF", DONOR_NAME := "Gates Foundation"]



## Test that no funds were lost in disaggregation
adb_pre <- fread(get_path("compiling", "int", "adb_pre_disagg.csv"))

tst <- merge(
    adb_pre[, .(pre = sum(get(dah_yr), na.rm = TRUE)), by = YEAR],
    adb[, .(post = sum(get(dah_yr), na.rm = TRUE)), by = YEAR],
    by = "YEAR", all = TRUE
)
tst[, diff := post - pre]
if (tst[abs(diff) > 0.1, .N] > 0) {
    stop("Source disaggregation resulted in loss of funds. Please investigate.")
}
adb[, src := NULL]

save_dataset(
    adb,
    "adb_post_disagg.csv",
    channel = "compiling",
    stage = "int"
)

cat("* Done\n")

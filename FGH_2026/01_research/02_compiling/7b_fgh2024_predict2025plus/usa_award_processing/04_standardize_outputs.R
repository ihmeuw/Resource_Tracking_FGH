#
# Use ForeignAssistance.gov summary with terminated awards to estimate DAH cuts
# by recipient country and sector (ie, health focus areas/program areas)

code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


# load foreign assistance summary
fa_summ <- fread(get_path("compiling", "int", "foreign_assistance_summary.csv"))

# filter to health related sectors, and focus on recently active projects only
fa_obl <- fa_summ[was_active == TRUE &
                      international_sector_name %in% c(
                          "Non-communicable diseases (NCDs)",
                          "Maternal and Child Health, Family Planning",
                          "Basic Health",
                          "Health, General",
                          "HIV/AIDS"
                      ),
                  .(amt = as.numeric(sum(assigned_obligation))),
                  by = .(
                      international_sector_name,
                      international_purpose_name,
                      country_code,
                      country_name,
                      terminated
                  )]
fa_obl[, year := report_year] ## dummy

# load general disbursements
fa_disb <- fread(get_path("compiling", "int", "fa_projects_post_kws.csv"))

## based on our analysis, this project should be under the operating expenses
##   sector, but it is instead under HIV/AIDS, so we exclude it here
fa_disb <- fa_disb[activity_project_number != "N/A ADMIN - PEPFAR"]
fa_disb <- fa_disb[
    year >= 2020 & multilat == FALSE,
    .(amt = as.numeric(sum(disb))),
    by = .(
        year,
        hfa_pa = pa,
        country_code,
        country_name
    )
]
fa_disb[, terminated := FALSE] ## dummy
setorder(fa_disb, year)


#
# Map sectors/purposes to HFAs/PAs =============================================
#

# Map each purpose code to an HFA or PA. If mapped to HFA, it will apply to all
# PAs under the HFA, unless that PA already has a specific PA mapping.
gen_hfa_pa <- function(international_purpose_name) {
    hfa_pa <- fcase(
        international_purpose_name == "Basic nutrition",
        "pa_nch_cnn",
        international_purpose_name == "Reproductive health care",
        "hfa_rmh",
        international_purpose_name == "Family planning",
        "pa_rmh_fp",
        international_purpose_name == "Health policy and administrative management",
        "hfa_swap_hss",
        international_purpose_name == "STD control including HIV/AIDS",
        "hfa_hiv",
        international_purpose_name == "Malaria control",
        "hfa_mal",
        international_purpose_name == "Infectious disease control",
        "hfa_oid",
        international_purpose_name == "Population policy and administrative management",
        "hfa_swap_hss",
        international_purpose_name == "Tuberculosis control",
        "hfa_tb",
        international_purpose_name == "Basic health care",
        ## based on OECD definition, related to basic & primary health care, supply
        ## of medicines and vaccines... seems like a reasonable fit
        "hfa_nch",
        international_purpose_name == "COVID-19 control",
        "pa_oid_covid",
        international_purpose_name == "Medical research",
        "hfa_other",
        international_purpose_name == "Health education",
        "hfa_other",
        international_purpose_name == "Medical services",
        "hfa_other",
        international_purpose_name == "Noncommunicable diseases (NCDs) control, general",
        "hfa_ncd",
        international_purpose_name == "Other prevention and treatment of NCDs",
        "hfa_ncd",
        international_purpose_name %like% "Promotion of mental health and well-being",
        "pa_ncd_mental",
        international_purpose_name == "Health personnel development",
        "pa_swap_hss_hrh",
        international_purpose_name == "Basic health infrastructure",
        "hfa_swap_hss"
    )
    return(hfa_pa)
}

fa_obl[, hfa_pa := gen_hfa_pa(international_purpose_name)]
stopifnot(fa_obl[is.na(hfa_pa), .N] == 0)

# re-aggregate
fa_obl <- fa_obl[,
                 .(amt = sum(amt)),
                 by = .(year, country_code, country_name, hfa_pa, terminated)]

#
# Standardize locations and distribute regional amounts ====
#

# load GBD locations for country metadata
# (note that FA.gov doesn't provide their own region-country mappings, so will
#  assume GBD mappings apply)
gbd_locs <- fread(get_path("meta", "locs", "fgh_location_set.csv"))
gbd_locs <- gbd_locs[level == 3]

country_region_map <- unique(rbind(
    fa_disb[, .(country_code, country_name)],
    fa_obl[, .(country_code, country_name)]
))
stopifnot(
    country_region_map[, .N, by = country_code][N > 1, .N] == 0
)
country_region_map <- merge(
    ## merge on gbd location names to determine which FA "countries" are actually
    ## regions (if they merge with gbd_locs, they are a country)
    country_region_map,
    gbd_locs[, .(country_code = ihme_loc_id, location_name)],
    by = "country_code",
    all.x = TRUE
)
country_region_map <- country_region_map[
    is.na(location_name),
    .(country_code, fa_region = country_name)
]
country_region_map <- country_region_map[
    ## exclude non-regions which don't map to GBD locations
    ! fa_region %in% c("Kosovo", "Sudan (former)", "World"),
]

# assign FA.gov's regional recipients to countries based on GBD regions
country_region_map[, country_code := fcase(
    # highest level regions (level 1) ====
    fa_region == "Eurasia Region",
    paste(unique(
        gbd_locs[super_region_name %like% "Asia", ihme_loc_id],
    ), collapse = ","),
    fa_region == "Western Hemisphere Region",
    paste(unique(
        gbd_locs[super_region_name == "Latin America and Caribbean",
                 ihme_loc_id]
    ), collapse = ","),
    # level 2 ====
    fa_region == "Asia Region",
    paste(unique(
        gbd_locs[region_name %like% "Asia", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Middle East and North Africa Region",
    paste(unique(
        gbd_locs[super_region_name == "North Africa and Middle East", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Latin America and Caribbean Region",
    paste(unique(
        gbd_locs[super_region_name == "Latin America and Caribbean", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Sub-Saharan Africa Region",
    paste(unique(
        gbd_locs[super_region_name == "Sub-Saharan Africa", ihme_loc_id]
    ), collapse = ","),
    # level 3 ====
    fa_region == "East Asia and Oceania Region",
    paste(unique(
        gbd_locs[super_region_name == "Southeast Asia, East Asia, and Oceania", ihme_loc_id]
    ), collapse = ","),
    fa_region == "North and Central America Region",
    paste(unique(
        gbd_locs[region_name == "Central Latin America", ihme_loc_id]
    ), collapse = ","),
    # level 4 =====
    fa_region == "Central America Region",
    paste(unique(
        gbd_locs[region_name == "Central Latin America", ihme_loc_id]
    ), collapse = ","),
    fa_region == "South America Region",
    paste(unique(
        gbd_locs[region_name %like% "Latin America" & region_name != "Central Latin America",
                 ihme_loc_id]
    ), collapse = ","),
    fa_region == "Caribbean Region",
    paste(unique(
        gbd_locs[region_name == "Caribbean", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Eastern Europe Region",
    paste(unique(
        gbd_locs[region_name == "Eastern Europe", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Eastern Asia Region",
    paste(unique(
        gbd_locs[region_name == "East Asia", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Oceania Region",
    paste(unique(
        gbd_locs[region_name == "Oceania", ihme_loc_id]
    ), collapse = ","),
    fa_region == "South East Asia Region",
    paste(unique(
        gbd_locs[region_name == "Southeast Asia", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Middle East Region",
    paste(unique(
        gbd_locs[
            region_name == "North Africa and Middle East" & ! ihme_loc_id %in% c(
                "SDN", "MAR", "LBY", "DZA", "TUN", "EGY"
            ),
            ihme_loc_id
        ]
    ), collapse = ","),
    fa_region == "Eastern Africa Region",
    paste(unique(
        gbd_locs[region_name == "Eastern Sub-Saharan Africa", ihme_loc_id]
    ), collapse = ","),
    fa_region == "Southern Africa Region",
    paste(unique(
        gbd_locs[region_name == "Southern Sub-Saharan Africa", ihme_loc_id]
    ), collapse = ","),
    fa_region == "West Africa Region",
    paste(unique(
        gbd_locs[region_name == "Western Sub-Saharan Africa", ihme_loc_id]
    ), collapse = ","),
    # default ====
    default = NA
)]
stopifnot(country_region_map[is.na(country_code), .N] == 0)
## explode:
country_region_map <- country_region_map[
    ,
    .(country_code = unlist(strsplit(country_code, ","))),
    by = .(fa_region)
]
## multi-level, since some regions encapsulate others
country_region_map[, level := fcase(
    fa_region %in% c(
        "Eurasia Region",
        "Western Hemisphere Region"
    ),
    1,
    fa_region %in% c(
        "Asia Region",
        "Middle East and North Africa Region",
        "Latin America and Caribbean Region",
        "Sub-Saharan Africa Region"
    ),
    2,
    fa_region %in% c(
        "East Asia and Oceania Region",
        "North and Central America Region"
    ),
    3,
    default = 4
)]
stopifnot(country_region_map[, .N, by=.(country_code, level)][N > 1, .N] == 0)
    
    


standardize_locs <- function(dt) {
    dt[, reg_split := fifelse(
        country_name %in% unique(country_region_map$fa_region),
        TRUE,
        FALSE
    )]
    dt_s <- dt[reg_split == TRUE, -"country_code"]
    
    
    # Calculate weights for disaggregation and apply them to regional DAH amounts
    for (lvl in sort(unique(country_region_map$level))) {
        ## merge PA dah amounts with region-country mapping
        reg_pa_fracs <- merge(
            dt[, .(amt = sum(amt)), by = .(year, country_code, hfa_pa)],
            country_region_map[level == lvl, .(country_code, fa_region)],
            by = "country_code",
            all.x = TRUE
        )
        ## calculate fraction of HFA-specific dah to the region received by
        ## each country in that region
        reg_pa_fracs[, frac := amt / sum(amt), by = .(year, fa_region, hfa_pa)]
        stopifnot(
            ## ensure fracs sum to 1
            reg_pa_fracs[, sum(frac), by = .(year, fa_region, hfa_pa)][abs(V1 - 1)>1e-6, .N] == 0
        )
        
        ## calculate general dah amounts to each region
        reg_fracs <- merge(
            dt[, .(amt = sum(amt)), by = .(year, country_code)],
            country_region_map[level == lvl, .(country_code, fa_region)],
            by = "country_code",
            all.x = TRUE
        )
        reg_fracs[, frac := amt / sum(amt), by = .(year, fa_region)]
        stopifnot(
            ## ensure fracs sum to 1
            reg_fracs[, sum(frac), by = .(year, fa_region)][abs(V1 - 1)>1e-6, .N] == 0
        )
        
        ## merge weights onto region-level dah data and disaggregate
        ### first merge PA-specific region member fractions
        dt_s <- merge(
            dt_s,
            reg_pa_fracs[, .(year, country_name = fa_region,
                             hfa_pa, country_code, frac)],
            by = c("year", "country_name", "hfa_pa"),
            all.x = TRUE,
            ## each region row explodes into multiple country rows
            allow.cartesian = TRUE
        )
        ### for region-PAs with no PA-specific fractions, merge on general
        ### region member fractions
        dt_s2 <- merge(
            dt_s[is.na(frac), -c("country_code", "frac")],
            reg_fracs[, .(year, country_name = fa_region,
                           country_code, frac)],
            by = c("year", "country_name"),
            all.x = TRUE,
            allow.cartesian = TRUE
        )
        dt_s <- rbind(
            dt_s[!is.na(frac)],
            dt_s2,
            fill = TRUE
        )
        
        setnafill(dt_s, fill = 1, cols = "frac")
        dt_s[, amt := amt * frac]
        
        setnames(dt_s, "country_code", paste0("country_code_", lvl))
        dt_s[, frac := NULL]
    
    }
    
    for (lvl in sort(unique(country_region_map$level), decreasing = TRUE)) {
        cc_lvl <- paste0("country_code_", lvl)
        dt_s[!is.na(get(cc_lvl)), country_code := get(cc_lvl)]
    }
    
    ## Ensure total amount is preserved 
    stopifnot(
        abs(dt[reg_split == TRUE, sum(amt)] - dt_s[, sum(amt)]) < 1e-6
    )
    
    ## Drop rows which could not be assigned a country code
    ### This occurs if none of the region's member countries actually received
    ### DAH individually in the given year. This is rare but does happen. For now,
    ### we ignore these amounts.
    drop <- dt_s[is.na(country_code), unique(country_name)]
    warning(
        paste(
            "Dropping the following regions which could not be assigned country codes:",
            paste(drop, collapse = ", ")
        )
    )
    dt_s <- dt_s[!is.na(country_code)]
    
    # Re-combine region split data with original data
    dt <- rbind(
        dt[reg_split == FALSE,
           .(year, country_code, hfa_pa, terminated, amt, reg_split)],
        dt_s[, .(year, country_code, hfa_pa, terminated, amt, reg_split)]
    )
    return(dt)
}

fa_obl <- standardize_locs(fa_obl)
fa_disb <- standardize_locs(fa_disb)

# Re-aggregate finally
fa_obl <- fa_obl[, .(amt = sum(amt)), by = .(year, country_code, hfa_pa, terminated)]
fa_disb <- fa_disb[, .(amt = sum(amt)), by = .(year, country_code, hfa_pa, terminated)]


fwrite(
    fa_obl,
    get_path("compiling", "int", "usa_bilateral_fa_terminations.csv")
)


fwrite(
    fa_disb[, -"terminated"],
    get_path("compiling", "int", "usa_bilateral_fa_disbursed.csv")
)




if (FALSE) {
    

# Compare FA.gov DAH distributions with our historical/estimated DAH distributions
dah_cpr <- fread("FILEPATH/dah_by_channel_pa_recip_1990_2100.csv")
dah_cpr <- dah_cpr[year <= 2025]
dah_cpr[, hfa := tstrsplit(pa, "_", keep = 1)]
dah_cpr[hfa == "swap", hfa := "swap_hss_total"]

fa_disb_2 <- copy(fa_disb)
setnames(fa_disb_2, "country_code", "recip")
fa_disb_2[recip == "WLD", recip := "UNALLOCABLE_DAH_WLD"]
fa_disb_2[recip == "CS-KM", recip := "UNALLOCABLE_DAH_QZA"] ## kosovo
fa_disb_2 <- fa_disb_2[
    ## dropping countries not in our dah_cpr data (typically high-income)
    recip %in% dah_cpr[year >= 2023, unique(recip)]
]
fa_disb_2[, hfa := tstrsplit(hfa_pa, "_", keep = 1)]
fa_disb_2[hfa == "swap", hfa := "swap_hss_total"]
fa_disb_2[, pa := hfa_pa]
fa_disb_2[, hfa_pa := NULL]


usfa24 <- fa_disb_2[year == 2024]
usfa24[, pa_frac := amt/sum(amt), by = .(year, pa)]
uscpr24 <- dah_cpr[year == 2024 & channel == "BIL_USA" & recip != "UNALLOCABLE_DAH_INK",
                   .(dah = sum(dah)),
                   by = .(year, pa)]
usdah24 <- merge(
    usfa24,
    uscpr24,
    by = c("year", "pa"),
    all.x = TRUE
)
stopifnot(
    usdah24[, sum(dah * pa_frac, na.rm = TRUE)] - uscpr24[, sum(dah)] < 1e-6
)



pdf("~/DAH FA 2023 disbursement comparisons.pdf", width = 16, height = 10)
cmp1 <- merge(
    dah_cpr[year %in% 2023:2024 & channel == "BIL_USA" & recip != "UNALLOCABLE_DAH_INK",
            .(dah = sum(dah)),
            by = .(year, recip)],
    fa_disb_2[year  %in% 2023:2024, .(fa = sum(amt)), by = .(year, recip)],
    by = c("year", "recip"),
    all = TRUE
)
cmp1[, `:=`(
    dah_frac = dah/sum(dah, na.rm = TRUE),
    fa_frac = fa/sum(fa, na.rm = TRUE)
), by = year]
ggplot(
    cmp1[! recip %like% "UNALLOC" & year == 2023],
    aes(x = dah_frac, y = fa_frac)
) +
    geom_abline(slope = 1, intercept = 0, color = "red") +
    geom_text(aes(label = recip), alpha = 0.5, size = 3) +
    theme_bw() +
    labs(
        title = "Recipient shares of total DAH - FA.gov vs. DAH-team estimates",
        subtitle = "USA bilateral disbursements",
        x = "2023 DAH-team estimated share",
        y = "2023 FA.gov estimated share"
    ) +
    theme_bw()


cmp2 <- merge(
    dah_cpr[year %in% 2023:2024 & channel == "BIL_USA" & recip != "UNALLOCABLE_DAH_INK",
            .(dah = sum(dah)),
            by = .(year, hfa, recip)],
    fa_disb_2[year  %in% 2023:2024,
              .(fa = sum(amt)),
              by = .(year, hfa, recip)],
    by = c("year", "hfa", "recip"),
    all = TRUE
)
cmp2[, `:=`(
    dah_frac = dah/sum(dah, na.rm = TRUE),
    fa_frac = fa/sum(fa, na.rm = TRUE)
), by = .(year, hfa)]

setnafill(cmp2, fill = 0, cols = c("dah_frac", "fa_frac"))
ggplot(
    cmp2[! recip %like% "UNALLOC" & year == 2023],
    aes(x = dah_frac, y = fa_frac)
    ) +
    geom_abline(slope = 1, intercept = 0, color = "red") +
    geom_text(
        aes(label = recip),
        alpha = 0.5, size = 3
    ) +
    facet_wrap(~ hfa, scales = "free") +
    labs(
        title = "Recipient shares of HFA DAH - FA.gov vs. DAH-team estimates",
        subtitle = "USA bilateral disbursements",
        x = "2023 DAH-team estimated share",
        y = "2023 FA.gov estimated share"
    ) +
    theme_bw()


cmp3 <- merge(
    dah_cpr[year == 2023 & channel == "BIL_USA" & recip != "UNALLOCABLE_DAH_INK",
            .(dah = sum(dah)),
            by = .(hfa, recip)],
    fa_disb_2[year  == 2024,
              .(fa = sum(amt)),
              by = .(hfa, recip)],
    by = c("hfa", "recip"),
    all = TRUE
)
cmp3[, `:=`(
    dah_frac = dah/sum(dah, na.rm = TRUE),
    fa_frac = fa/sum(fa, na.rm = TRUE)
), by = .(hfa)]

setnafill(cmp3, fill = 0, cols = c("dah_frac", "fa_frac"))
ggplot(
    cmp3[! recip %like% "UNALLOC"],
    aes(x = dah_frac, y = fa_frac)
    ) +
    geom_abline(slope = 1, intercept = 0, color = "red") +
    geom_text(
        aes(label = recip),
        alpha = 0.5, size = 3
    ) +
    facet_wrap(~ hfa, scales = "free") +
    labs(
        title = "Recipient shares of HFA DAH - FA.gov vs. DAH-team estimates",
        subtitle = "USA bilateral disbursements",
        x = "2023 DAH-team estimated share",
        y = "2024 FA.gov estimated share"
    ) +
    theme_bw()



cmp4 <- cmp2[year == 2024]
cmp4[, hfa_tot := sum(dah, na.rm = TRUE), by = hfa]

p <- ggplot(
    cmp4[! recip %like% "UNALLOC"],
    aes(x = hfa_tot * dah_frac/1e6, y = hfa_tot * fa_frac/1e6)
    ) +
    geom_abline(slope = 1, intercept = 0, color = "red") +
    geom_text(
        aes(label = recip),
        alpha = 0.5, size = 3
    ) +
    facet_wrap(~ hfa, scales = "free") +
    labs(
        title = "HFA DAH by Recipient - FA.gov-based vs. DAH-team estimates",
        subtitle = "USA bilateral disbursements",
        x = "2024 DAH-team forecast (millions USD)",
        y = "2024 FA.gov-based estimate (millions USD)"
    ) +
    theme_bw()

p

dev.off()





# ...
fa_obl <- dcast(fa_obl,
                hfa_pa + country_code ~
                    ifelse(terminated == TRUE, "term", "act"),
                value.var = "amt",
                fill = 0)
fa_obl[, total := act + term]
fa_obl[, pct_chg := act/total - 1]

fa_obl[, hfa_pa := gsub("hfa_|pa_", "", hfa_pa)]
fa_obl[country_code == "WLD", country_code := "UNALLOCABLE_DAH_WLD"]

fa_obl_pa <- fa_obl[, .(total = sum(total), act = sum(act)), by = hfa_pa]
fa_obl_pa[, pct_chg := act/total - 1]



## merge with dah ests
dah_sc <- fread("FILEPATH/dah_by_source_channel_1990_2100.csv")
dah_sc[, source_frac := dah/sum(dah), by = .(channel, year)]
dah_sc <- dah_sc[year <= 2025]
dah_cpr <- fread("FILEPATH/dah_by_channel_pa_recip_1990_2100.csv")
dah_cpr <- dah_cpr[year <= 2025]
dah_cpr <- merge(
    ## merge on us source frac to disaggregate NGO channel
    dah_cpr,
    dah_sc[source == "USA", .(year, channel, us_source_frac = source_frac)],
    by = c("year", "channel"),
    all.x = TRUE
)
setnafill(dah_cpr, fill = 0, cols = "us_source_frac")
dah_bil_usa <- dah_cpr[
    between(year, 2000, 2025) &
        channel %in% c("BIL_USA", "NGO"),
    .(dah = sum(dah * us_source_frac)),
    by = .(channel, year, recip, pa)
]
dah_bil_usa[, hfa := unlist(lapply(strsplit(pa, "_"), `[`, 1))]
dah_bil_usa[hfa == "swap", hfa := "swap_hss"]


## merge on percent changes by country and hfa/pa
### - if we have PA specific cut for the country, use that,
##    else use HFA specific cut for the country, else
##    use overall HFA/PA cut (not country specific)
dah_bil_usa <- merge(
    dah_bil_usa,
    fa_obl[, .(hfa = hfa_pa, recip = country_code, hfa_loc_chg = pct_chg)],
    by = c("hfa", "recip"),
    all.x = TRUE
)
dah_bil_usa <- merge(
    dah_bil_usa,
    fa_obl[, .(pa = hfa_pa, recip = country_code, pa_loc_chg = pct_chg)],
    by = c("pa", "recip"),
    all.x = TRUE
)
dah_bil_usa[, pct_chg := fcase(
    !is.na(pa_loc_chg), pa_loc_chg,
    !is.na(hfa_loc_chg), hfa_loc_chg,
    default = NA
)]
setnafill(dah_bil_usa, fill = 0, cols = "pct_chg")

## calculate new dah for 2025 based on pct_chg from 2024 to 2025
dah_bil_usa[order(year),
            dah_lag := shift(dah),
            by = .(channel, pa, recip)]
dah_bil_usa[year == 2025,
            new_dah := dah_lag * (1 + pct_chg)]

## scale to match overall bil_usa growth
### calculate pa-recip fraction of channel's total new_dah
dah_bil_usa[year == 2025, frac := new_dah / sum(new_dah), by = channel]
## multiply pa-recip fraction by channel's total actual dah
dah_bil_usa[year == 2025, new_dah := sum(dah) * frac, by = channel]
dah_bil_usa[, pc := new_dah/dah_lag - 1]


dah_bil_usa_fin <- dah_bil_usa[, .(
    year, channel, pa, recip, dah = fifelse(year < 2025, dah, new_dah)
)]

## re-combine with non-bil_usa dah
dah_fin <- copy(dah_cpr)
dah_fin[, fin_dah := fifelse(
    channel %in% c("BIL_USA", "NGO"),
    dah * (1 - us_source_frac),
    dah
)]

dah_fin[, src := "OTH"]
dah_bil_usa_fin[, src := "USA"]
dah_fin <- rbind(
    dah_bil_usa_fin,
    dah_fin[, .(year, src, channel, pa, recip, dah = fin_dah)]
)
setorder(dah_fin, year)



#
#
#
## Figures/tables =============================================================
#
#
#

# Compare old vs new percent changes by country/hfa
### all channels
cmp1 <- merge(
    dah_cpr[, .(old = sum(dah)/1e9), by = .(year, recip)],
    dah_fin[, .(new = sum(dah)/1e9), by = .(year, recip)],
    by = c("year", "recip"),
    all.x = TRUE
)
cmp1[, channel := "All channels"]
cmp2 <- merge(
    dah_cpr[channel %in% c("BIL_USA"), .(old = sum(dah * us_source_frac)/1e9), by = .(year, recip)],
    dah_fin[src == "USA" & channel %in% c("BIL_USA"), .(new = sum(dah)/1e9), by = .(year, recip)],
    by = c("year", "recip"),
    all.x = TRUE
)
cmp2[, channel := "US Bilateral"]

cmp <- rbind(cmp1, cmp2)
cmp <- cmp[between(year, 2024, 2025)]
setorder(cmp, year)

cmp[order(year), `:=`(
    nl = shift(new),
    ol = shift(old)
), by = .(channel, recip)]
cmp[, `:=`(
    old_chg = old/ol - 1,
    new_chg = new/nl - 1
)]
cmp <- cmp[year == 2025]

p1 <- ggplot(cmp[! recip %like% "UNALLOC"],
       aes(x = -1 * old_chg, y = -1 * new_chg)) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
    geom_text(
        aes(label = recip), alpha = 0.25
    ) +
    scale_x_continuous(labels = scales::percent) +
    scale_y_continuous(labels = scales::percent) +
    facet_wrap(~channel) +
    labs(
        title = "Comparison of old vs new percent changes in DAH by recipient country",
        subtitle = "2024 to 2025",
        x = "Old percent decrease",
        y = "New percent decrease"
    ) +
    theme_bw(16)

p2 <- copy(p1)
p2$data <- p2$data[channel == "All channels"]

p3 <- copy(p1)
p3$data <- p3$data[channel == "US Bilateral"]


pdf("~/Percent Change Scatters.pdf", width = 12, height = 8)
print(p1)
print(p2)
print(p3)
dev.off()






# STACKED BARS
dah_fin[, hfa := unlist(lapply(strsplit(pa, "_"), `[`, 1))]
dah_fin[hfa == "swap", hfa := "swap_hss"]


hfa_color_map <- c(
    "rmh" = "#1f77b4",
    "nch" = "#ff7f0e",
    "ncd" = "#2ca02c",
    "hiv" = "#d62728",
    "mal" = "#9467bd",
    "tb" = "#8c564b",
    "oid" = "#e377c2",
    "swap_hss" = "#bcbd22",
    "other" = "#17becf",
    "unalloc" = "#7f7f7f"
)
hfa_name_map <- c(
    "rmh" = "Reproductive and Maternal Health",
    "nch" = "Newborn and Child Health",
    "ncd" = "Non-Communicable Diseases",
    "hiv" = "HIV/AIDS",
    "mal" = "Malaria",
    "tb" = "Tuberculosis",
    "oid" = "Other Infectious Diseases",
    "swap_hss" = "Health Systems Strengthening",
    "other" = "Other",
    "unalloc" = "Unallocated"
)

hfa_dt <- dah_fin[, .(dah = sum(dah)), by = .(year, hfa)]
hfa_seq <- dah_fin[hfa != "unalloc", sum(dah), by = hfa][order(V1), hfa]
hfa_dt[, hfa := factor(
    hfa,
    levels = c("unalloc", hfa_seq)
)]


p <- ggplot(hfa_dt, aes(x = year, y = dah/1e9, fill = hfa)) +
    geom_col() +
    scale_x_continuous(breaks = seq(1990, 2025, by = 5),
                   expand = c(0.02, 0.02)) +
    scale_fill_manual(values = hfa_color_map,
                      labels = hfa_name_map) +
    theme_bw(24) +
    labs(
        title = "DAH by Health Focus Area, 1990-2025",
        x = "",
        y = paste("Billions of", 2023, "USD"),
        fill = "Health Focus Area"
    ) +
    theme(
        panel.grid = element_blank(),
    )

hfa_dt[order(year), lag1 := shift(dah, n = 1), by = hfa]
hfa_dt[, diff := dah - lag1]
hfa_dt[, pct_chg := 100 * diff / lag1]

x <- hfa_dt[year == 2025 & hfa != "unalloc"]
x[, hfa := factor(hfa, levels = hfa[order(-diff)])]
p_hfa_diff <- ggplot(x, aes(x = hfa, y = diff/1e6, fill = hfa)) +
    geom_col() +
    coord_flip() +
    scale_y_continuous(
        labels = scales::comma,
        expand = expansion(mult = c(0.25, 0.2))
    ) +
    scale_fill_manual(
        labels = hfa_name_map,
        values = hfa_color_map
    ) +
    scale_x_discrete(
        labels = \(x) hfa_name_map[x]
    ) +
    labs(
        title = "Change in DAH by HFA, 2024-2025",
        subtitle = paste("Change in DAH (Millions of", 2023, "USD)"),
        x = "",
        y = "",
        fill = "Health Focus Area"
    ) +
    theme_bw(24) +
    theme(
        legend.position = "none",
        panel.grid = element_blank(),
    )


p_hfa_pct <- ggplot(x, aes(x = hfa, y = pct_chg, fill = hfa)) +
    geom_col() +
    coord_flip() +
    scale_y_continuous(
        labels = scales::comma,
        expand = expansion(mult = c(0.2, 0.2))
    ) +
    scale_fill_manual(
        labels = hfa_name_map,
        values = hfa_color_map
    ) +
    labs(
        title = "",
        x = "",
        y = "",
        subtitle = paste("Percent Change in DAH (%)"),
        fill = "Health Focus Area"
    ) +
    theme_bw(24) +
    theme(
        legend.position = "none",
        axis.text.y = element_blank(),
        panel.grid = element_blank(),
    )

p_hfa_chg <- p_hfa_diff + p_hfa_pct



pdf(file.path(OUTPUT_DIR, "dah_by_hfa.pdf"), width = 12, height = 8)
print(p_hfa + theme(text = element_text(size = 16)))
print(p_hfa + theme(legend.position = "none"))
dev.off()





ex1 <- copy(dah_bil_usa)
ex1[, pr_dah := fifelse(year < 2025, dah, new_dah)]
ex1 <- ex1[, .(dah = sum(pr_dah)), by = .(year, recip)]
ex1[order(year), dah_lag := shift(dah), by = .(recip)]
ex1[, pc := dah/dah_lag - 1]
ex1[year == 2025][order(pc)]
ex1[, share := dah/sum(dah), by = .(year)]


top_recips <- ex1[, sum(dah), by = recip][order(-V1), ][1:11, recip]
ex1[, recip2 := fifelse(recip %in% top_recips, recip, "Other Country")]
ex1.1 <- ex1[, .(dah = sum(dah)), by = .(year, recip = recip2)]
ex1.1[, share := dah/sum(dah), by = .(year)]
ggplot(ex1.1, aes(x = year, y = dah/1e9, fill = recip)) +
    geom_col() +
    scale_fill_brewer(palette = "Set3") +
    labs(
        title = "US Bilateral DAH Estimates by recipient (Top 12)",
        x = "",
        y = "Billions of 2023 USD",
        fill = "Recipient"
    ) +
    theme_bw(16)

ex2 <- copy(dah_bil_usa)
ex2[, pr_dah := fifelse(year < 2025, dah, new_dah)]
ex2 <- ex2[, .(dah = sum(pr_dah)), by = .(year, hfa)]
ex2[order(year), dah_lag := shift(dah), by = .(hfa)]
ex2[, pc := dah/dah_lag - 1]
ex2[year == 2025][order(-pc)]
ex2[, share := dah/sum(dah), by = .(year)]

ggplot(ex2, aes(x = year, y = dah/1e9, fill = hfa)) +
    geom_col() +
    scale_fill_brewer(palette = "Set3") +
    labs(
        title = "US Bilateral DAH Estimates by HFA",
        x = "",
        y = "Billions of 2023 USD",
        fill = "HFA"
    ) +
    theme_bw(16)


cc <- gbd_locs[, .(recip = ihme_loc_id, location_name)]
ex1 <- merge(ex1, cc, by = "recip", all.x = TRUE)
dah_bil_usa <- merge(dah_bil_usa, cc, by = "recip", all.x = TRUE)

ex1_out <- ex1[year == 2025, .(recip, location_name, percent_change = pc)][order(percent_change)]
ex2_out <- ex2[year == 2025, .(hfa, percent_change = pc)][order(percent_change)]
ex3_out <- dah_bil_usa[year == 2025, .(percent_change = sum(new_dah)/sum(dah_lag) - 1),
                       by = .(hfa, pa)][order(hfa, pa)]
ex4_out <- dah_bil_usa[year == 2025, .(percent_change = sum(new_dah)/sum(dah_lag) - 1),
                       by = .(hfa, recip, location_name)][order(hfa, recip)]
ex5_out <- dah_bil_usa[year == 2025, .(percent_change = sum(new_dah)/sum(dah_lag) - 1),
                       by = .(hfa, pa, recip, location_name)][order(hfa, pa, recip)]


openxlsx::write.xlsx(
    list(
        by_recipient = ex1_out,
        by_hfa = ex2_out,
        by_hfa_pa = ex3_out,
        by_hfa_recipient = ex4_out,
        by_hfa_pa_recipient = ex5_out
    ),
    "~/US Bilateral DAH Cuts by Country and HFA.xlsx"
)

}


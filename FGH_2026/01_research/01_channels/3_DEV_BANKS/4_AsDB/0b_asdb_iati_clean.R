#' Simplify IATI transaction data from iati-aggregator for use in AsDB estimates 
#'
#' This script helps simplify the IATI transaction level data collected for an
#' individual organization. The work is all done by one function. It can be sourced
#' into another script or the script can be run as a standalone, e.g. by Rscript
#' (see the "main" section at the bottom).
#'
#' The transaction level data is collected from IATI by the `1_iati_extract.py`
#' script which uses the "iati-aggregator" library. The iati-aggregator tries to
#' be fairly generic, so the transaction level data it produces includes a lot
#' more info then we really need for this particular project. So this script
#' just simplifies it a bit.
#'
library(data.table)
library(logger)


code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))


.__META <- list(
    incoming_funds = c(
        "Incoming Funds",
        "Incoming Commitment",
        "Incoming Pledge",
        "Interest Payment",
        "Loan Repayment",
        "Reimbursement",
        "Sale of Equity"
    ),
    outgoing_funds = c(
        "Outgoing Commitment",
        "Credit Guarantee",
        "Outgoing Pledge",
        "Disbursement",
        "Expenditure",
        "Purchase of Equity"
    )
)


#' A very specific function to simplify the IATI data a bit, with the particular
#' goals of this project in mind, before further processing.
#'
#' @param org_name The name of the organization whose transaction level data
#'   is to be processed.
#' @param data_path The path to the IATI transaction level data file.
#' @param output_path The path to save the simplified data to.
#' @param verbose Whether to print log INFO messages.
iati_simplify <- function(org_name,
                          data_path,
                          output_path,
                          verbose = TRUE) {
    lglvl <- log_threshold()
    if (!verbose) log_threshold(logger::WARN)
    log_info("Loading and processing '{org_name}' transactions...")
    
    # load transaction data
    ## and prevent NA (Namibia's ISO-2) from being read as a missing value
    iati <- fread(data_path, na.strings = c(""), colClasses = "character")
    iati[, names(iati) := lapply(.SD, function(x) {
        x[x == "None"] <- ""
        x[is.na(x)] <- ""
        return(x)
    }), .SDcols = names(iati)]
    
    # store names of final columns
    use_cols <- c("pub_code") 
    
    #
    # reporting agency
    #
    iati[, `:=`(
        reporting_org = reporting_org_ref_cname,
        reporting_org_ref = reporting_org_ref,
        reporting_org_type = reporting_org_type_cname
    )]
    
    use_cols <- c(use_cols, "reporting_org", "reporting_org_ref", "reporting_org_type")
    
    
    #
    # determine recipient country / region
    #
    #
    ## supposed to be declared either for all transactions in an activity or once
    ## at the activity level.
    iati[, `:=`(recip = "", recip_code = "", recip_pct = NA_character_)]
    #
    ## prioritize transaction-level recipient info
    ### if declared for a transaction, only one recipient country is allowed,
    ### or recipient region if country is not specified
    iati[recip == "", `:=`(
        recip = transaction_recipient_country_cname,
        recip_code = transaction_recipient_country_code
    )]
    iati[recip == "", `:=`(
        recip = transaction_recipient_region_cname,
        recip_code = transaction_recipient_region_code
    )]
    ## use activity level when transaction level is missing
    ### If declared for an activity, then 
    ### need to combine country and region level recipient since percentage can
    ### be reported across both levels.
    ### According to the IATI Standard, "Recipient-region must not be used merely
    ### to describe the region of a country reported in recipient-country, but
    ### ONLY if the region is a recipient IN ADDITION to the country."
    ### This would allow us to simply combine the reported country and regions,
    ### knowing that the reported percentages should add up to 100.
    about <- \(x, abt, eps) abs(x - abt) < eps
    iati[, country_pct := vapply(recipient_country_percentage, function(x) {
        sum(as.numeric(strsplit(x, "&")[[1]]))
    }, numeric(1))]
    iati[, region_pct := vapply(recipient_region_percentage, function(x) {
        sum(as.numeric(strsplit(x, "&")[[1]]))
    }, numeric(1))]
    iati[, total_pct := country_pct + region_pct]
    ## just use country if they make up 100%
    iati[about(country_pct, 100, 0.1),
         `:=`(
             rn = recipient_country_cname,
             rc = recipient_country_code,
             rp = recipient_country_percentage
         )]
    ## just use region if they make up 100%
    iati[about(country_pct, 0, 0.1) & about(region_pct, 100, 0.1),
         `:=`(
             rn = recipient_region_cname,
             rc = recipient_region_code,
             rp = recipient_region_percentage
         )]
    ## if country + region pcts make up 100% but not all country or all region,
    ##  then combine
    iati[about(total_pct, 100, 0.1) & is.na(rn),
         `:=`(
             rn = paste0(
                 recipient_country_cname, "<SEP>", recipient_region_cname
             ),
             rc = paste0(
                 recipient_country_code, "&", recipient_region_code
             ),
             rp = paste0(
                 recipient_country_percentage, "&", recipient_region_percentage
             )
         )]
    ## now, deal with cases where percentages are not reported correctly.
    ## if total percent is > 100 and country percentages are provided but don't
    ## add up to 100, then
    ## use country percentages anyways and ignore region percentages
    ##   (will have to modify percentages to add up to 100)
    iati[total_pct > 100 & is.na(rn) & !about(country_pct, 0, 0.1),
         `:=`(
             rn = recipient_country_cname,
             rc = recipient_country_code,
             rp = recipient_country_percentage
         )]
    
    # if no recipient percent provided, then there is only one recipient and
    # percentage is 100
    iati[total_pct == 0 & is.na(rn), `:=`(
        rn = recipient_country_cname,
        rc = recipient_country_code,
        rp = 100
    )]
    iati[rn == "", rn := NA_character_]
    iati[total_pct == 0 & is.na(rn), `:=`(
        rn = recipient_region_cname,
        rc = recipient_region_code,
        rp = 100
    )]
    
    # use this activity level recipient info when transaction level is missing
    iati[recip == "", `:=`(
        recip = rn,
        recip_pct = rp,
        recip_code = rc
    )]
    #
    # If percentages are not provided, it is because only one recipient country
    # is listed and thus the percentage is 100.
    # At transaction level, only one recipient country is allowed to be provided so
    # percentage is always 100.
    iati[is.na(recip_pct), recip_pct := 100]
    
    
    use_cols <- c(use_cols, "recip", "recip_code", "recip_pct")
    
    
    
    #
    # determine transaction provider
    #
    iati[, `:=`(
        provider = "",
        provider_ref = "",
        provider_is_narr_flag = FALSE
    )]
    
    iati[, provider_type := transaction_provider_type_cname]
    iati[, `:=`(
        provider = transaction_provider_ref_cname,
        provider_ref = transaction_provider_ref
    )]
    ## if provider ref doesn't match to one of the org refs in the database,
    ## we can use its narrative instead - will require further parsing
    iati[provider == "",
         `:=`(
             provider = transaction_provider_narr,
             provider_is_narr_flag = TRUE
         )]
    
    # If omitted on outgoing funds the reporting-org is assumed.
    # https://iatistandard.org/en/iati-standard/201/activity-standard/iati-activities/iati-activity/transaction/provider-org/
    iati[provider == "" & transaction_type_cname %in% .__META$outgoing_funds,
         `:=`(
             provider = reporting_org,
             provider_ref = reporting_org_ref,
             provider_type = reporting_org_type,
             provider_is_narr_flag = FALSE
         )]
    
    
    use_cols <- c(use_cols,
                  "provider", "provider_ref", "provider_type", "provider_is_narr_flag")
    
    
    #
    # determine transaction receiver
    #
    iati[, `:=` (
        receiver = "",
        receiver_ref = "",
        receiver_is_narr_flag = FALSE
    )]
    iati[, receiver_type := transaction_receiver_type_cname]
    iati[, `:=`(
        receiver = transaction_receiver_ref_cname,
        receiver_ref = transaction_receiver_ref
    )]
    iati[receiver == "",
         `:=`(
             receiver = transaction_receiver_narr,
             receiver_is_narr_flag = TRUE
         )]
    
    # if omitted on incoming funds then the receiver organisation is assumed to be the reporting organisation 
    # https://iatistandard.org/en/iati-standard/201/activity-standard/iati-activities/iati-activity/transaction/receiver-org/
    iati[receiver == "" & transaction_type_cname %in% .__META$incoming_funds,
         `:=`(
             receiver = reporting_org,
             receiver_ref = reporting_org_ref,
             receiver_type = reporting_org_type,
             receiver_is_narr_flag = FALSE
         )]
    
    use_cols <- c(use_cols,
                  "receiver", "receiver_ref", "receiver_type", "receiver_is_narr_flag")
    
    
    #
    # determine transaction sector
    # - supposed to be declared either at transaction or activity level
    #
    iati[, `:=`(
        scode = as.character(transaction_sector_code),
        sector_vocab = as.character(transaction_sector_vocab_cname),
        sector = as.character(transaction_sector_cname),
        sector_pct = "",
        snarr = as.character(transaction_sector_narr)
    )]
    ## if reported for transactions, only 1 sector can be reported per sector vocabulary
    iati[sector != "", sector_pct := "100"]
    
    ## else, reported at activity level, where you can have multiple sectors
    iati[sector == "", `:=`(
        scode = as.character(sector_code),
        sector_vocab = as.character(sector_vocab_cname),
        sector = as.character(sector_cname),
        sector_pct = as.character(sector_percentage),
        snarr = as.character(sector_narr)
    )]
    setnames(iati, c("scode", "snarr"), c("sector_code", "sector_narr"))
    use_cols <- c(use_cols,
                  "sector_code", "sector_vocab", "sector", "sector_pct", "sector_narr")
    
    
    #
    # CRS Policy Marker
    #
    iati[, `:=`(
        crs_policy_marker = as.character(policy_marker_crs_cname),
        crs_policy_marker_significance = as.character(policy_marker_crs_significance_cname)
    )]
    
    use_cols <- c(use_cols, "crs_policy_marker", "crs_policy_marker_significance")
    
    
    #
    # collaboration type
    #
    iati[, `:=`(
        collaboration_type = as.character(collaboration_type_cname)
    )]
    use_cols <- c(use_cols, "collaboration_type")
    
    
    #
    # determine aid type
    #
    iati[, `:=` (
        aid_type = as.character(transaction_aid_type_cname)
    )]
    iati[aid_type == "", `:=` (
        aid_type = default_aid_type_cname
    )]
    use_cols <- c(use_cols, "aid_type")
    
    
    #
    # determine transaction type
    #
    iati[, `:=` (
        trans_type = as.character(transaction_type_cname)
    )]
    use_cols <- c(use_cols, "trans_type")
    
    
    #
    # determine finance and flow type
    #
    iati[, `:=` (
        finance_type = as.character(transaction_finance_type_cname),
        flow_type = as.character(transaction_flow_type_cname)
    )]
    iati[finance_type == "",
         finance_type := as.character(default_finance_type_cname)]
    iati[flow_type == "",
         flow_type := as.character(default_flow_type_cname)]
    
    use_cols <- c(use_cols, "finance_type", "flow_type")
    
    
    #
    # determine transaction date
    #
    iati[, `:=` (
        trans_date = as.character(transaction_value_date)
    )]
    iati[trans_date == "", `:=` (
        trans_date = as.character(transaction_date)
    )]
    iati[, trans_year := data.table::year(trans_date)]
    
    use_cols <- c(use_cols, "trans_date", "trans_year")
    
    
    #
    # determine transaction value and currency
    #
    iati[, `:=` (
        trans_value = as.numeric(transaction_value),
        trans_currency = as.character(transaction_value_currency)
    )]
    iati[trans_currency == "",
         trans_currency := as.character(default_currency)]
    
    use_cols <- c(use_cols, "trans_value", "trans_currency")
    
    
    #
    # activity-level info
    #
    iati[, `:=` (
        activity_status = as.character(activity_status_cname),
        activity_scope = as.character(activity_scope_cname),
        activity_date_start_planned = as.character(activity_date_start_planned),
        activity_date_end_planned = as.character(activity_date_end_planned),
        activity_date_start_actual = as.character(activity_date_start_actual),
        activity_date_end_actual = as.character(activity_date_end_actual),
        hierarchy = as.character(hierarchy)
    )]
    use_cols <- c(
        "iati_identifier",
        "activity_status",
        "activity_scope",
        "activity_date_start_planned",
        "activity_date_end_planned",
        "activity_date_start_actual",
        "activity_date_end_actual",
        "hierarchy",
        use_cols
    )
    
    #
    # natural language / free-text items
    #
    iati[, `:=`(
        trans_description_narr = as.character(transaction_description_narr),
        trans_description_lang = as.character(transaction_description_lang)
    )]
    use_cols <- c(
        use_cols,
        "title_narr",
        "title_lang",
        "description_general_narr",
        "description_general_lang",
        "description_long_narr",
        "description_long_lang",
        "description_objectives_narr",
        "description_objectives_lang",
        "description_target_group_narr",
        "description_target_group_lang",
        "trans_description_narr",
        "trans_description_lang"
    )
    
    
    #
    # finalize output
    #
    out <- iati[, ..use_cols]
    setcolorder(out, use_cols)
    setnames(out, "recip_code", "recip_iso2")
    #
    # flag transactions as outgoing or incoming funds for convenience
    out[, outgoing_flag := fifelse(
        trans_type %in% .__META$outgoing_funds,
        TRUE,
        FALSE
    )]
    #
    # save
    log_info("Saving clean '{org_name}' data: {output_path}")
    dir.create(dirname(output_path), showWarnings = FALSE, recursive = TRUE)
    fwrite(out, output_path)
    
    log_threshold(lglvl) # restore
    return(invisible(NULL))
}



#
# main ========================================================================
#
._main <- function() {
    data_path <- get_path("asdb", "int", "asdb_iati_scrape.csv")
    output_path <- get_path("asdb", "int", "asdb_iati_clean.csv")
    
    log_threshold(logger::INFO)
    log_info("Simplify IATI transaction data")
    
    iati_simplify("asdb", data_path, output_path)
    
    log_info("")
    log_info("Done.")
    return(invisible(NULL))
}


if (sys.nframe() == 0L) ## dont run if sourced
    ._main()

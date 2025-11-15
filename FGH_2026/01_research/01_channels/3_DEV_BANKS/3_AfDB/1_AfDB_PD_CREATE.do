********************************************************************************
// Project: FGH 
// Purpose: Estimate AfDB disbursements using updated project-level data
********************************************************************************


** *****************************************************************************
// SETUP
** *****************************************************************************
	clear all
	set more off
	set maxvar 32000
	
	// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" | c(os) == "MacOSX" {
		global j "/home/j"
        global h "/homes/`c(username)'"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

	set scheme s1color
	
** *****************************************************************************
// USER INPUTS
** *****************************************************************************
	local report_yr = 2024		// FGH report year
	local previous_yr = 2023 // `report_yr' - 1		// Previous FGH report year
	local corr_date = "20250228"	// YYYYMMDD of processed correspondence data date (from script 0 which intercepts COVID funding)
	local corr_date1 = "20240228"	// last year's corr date
	
	local update_tag = "20241231"	// round_date
	local adf_repl_years /// 	   ADF replenishment years
		"2002_2004 2005_2007 2008_2010 2011_2013 2014_2016 2017_2019"
    local defl_mmyy = "0424"

	// Directories
	local WD			"FILEPATH"
	local RAW 			"`WD'/FILEPATH/FGH_`report_yr'/"
	local RAW_PREV		"`WD'/FILEPATH/FGH_`previous_yr'/"
	local INT 			"`WD'/FILEPATH/FGH_`report_yr'/"
	local FIN 			"`WD'/FILEPATH/FGH_`report_yr'/"
	local FIN_PREV  	"`WD'/FILEPATH/FGH_`previous_yr'/"
	local OUT 			"`WD'/FILEPATH/FGH_`report_yr'/"
	local INCGRPS 		"`WD'/FILEPATH/FGH_`report_yr'/"
	local DEFL			"`WD'/FILEPATH/FGH_`report_yr'/"
	local XCHANGE		"`WD'/FILEPATH/FGH_`report_yr'/"
	local HFA   		"FILEPATH"
	local INCOME   		"`WD'/FILEPATH"
	local CODE_DIR		"FILEPATH"
	
	// Files
	local deflator_file "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta"
	local country_codes "`WD'/FILEPATH/FGH_`report_yr'/countrycodes_official.dta"

	// Derived locals
	local wb_historical_incgrps "`INCGRPS'/wb_historical_incgrps"

	// Runs
	run "`HFA'/Health_ADO_master.ado" // master keyword search function
	//run "`WD'/FILEPATH/TT_smooth_2018.ado"
	run "`CODE_DIR'/TT_smooth.ado"
		
** *****************************************************************************
// STEP 1: LOAD PROJECT-LEVEL DATA
** *****************************************************************************	

/* NOTE: 
	Project level data is obtained through correspondence since FGH 2017.
	In the past it was updated by hand from: 
	http://www.afdb.org/en/projects-and-operations/project-portfolio/
*/

    // corresdpondent data modified by covid intercept script
	insheet using "`RAW'/M_AfDB_PD_`corr_date'updated.csv", comma names clear

	keep if subsector == "Health" 

	/* NOTE:
		In FGH 2018, we also investigated projects with 
		subsector == "Population and Nutrition". There were 3 population projects
		that when looked up on their respective project sites (see 
		http://projectsportal.afdb.org/dataportal/VProject/list) did not seem 
		directly health related, so we excluded them.
	*/

	rename name country_lc
	keep sourceoffinancing country_lc projectid loannumber projecttitle sector ///
		subsector is_covid srchstr disbursementsfor*
	// reshape long disbursementsfor, i(sourceoffinancing country_lc projectid loannumber projecttitle sector subsector is_covid srchstr) j(year)
    reshape long disbursementsfor, i(country_lc loannumber projecttitle is_covid) j(year)
	
/*	COMMENTED OUT FOR FGH2021-2024 SINCE VAR IS ALREADY NUMERIC
*/
// Destring disbursements variable
	// foreach var of varlist disbursementsfor {
	// 	replace `var' = subinstr(`var',",","",.)
	// 	replace `var' = subinstr(`var',"-","",.)
	// 	replace `var' = subinstr(`var',".00","",.)
	// 	destring `var', replace force
    // } 

/* NOTE:
	Projects that disbursed at some point but not in the observed years are 
	dropped, since we later add in envelopes for pre-2002 data
*/

	drop if disbursementsfor == .

	// Correct country names
	replace country_lc = "Democratic Republic of the Congo" if ///
		country_lc == "Dem Rep Congo"
	replace country_lc = "Equatorial Guinea" if country_lc == "Eq Guinea"
	replace country_lc = "The Gambia" if country_lc == "Gambia"

	// Convert commitments and disbursements from UAC to USD
	preserve
		insheet using "`RAW'/UAC to USD Exchange Rate 1990-`report_yr'.csv", ///
			names clear
		tempfile xr
		save `xr', replace
	restore

	sort year
	merge m:1 year using `xr', keep(1 3) keepusing(uac_exchange_rate) nogen
	rename disbursementsfor DISBURSEMENT_UAC
	gen double DISBURSEMENT = DISBURSEMENT_UAC * uac_exchange_rate

	tempfile disb_by_proj
	save `disb_by_proj', replace

	// Merge country codes
	merge m:1 country_lc using "`country_codes'", keep(1 3) keepusing(iso3)
	drop _merge
	rename iso3 ISO3_RC
    replace ISO3_RC = "SWZ" if country_lc == "Eswatini"
    replace ISO3_RC = "CIV" if country_lc == "Côte D'Ivoire"
    replace ISO3_RC = "CAF" if country_lc == "Centrafrique"
	replace ISO3_RC = "QMA" if country_lc == "Multinational"
	replace ISO3_RC = "QMA" if country_lc == "Multi-Countries"
	rename country_lc RECIPIENT_COUNTRY

    // CHECK FOR MISSING ISO CODES
    levelsof RECIPIENT_COUNTRY if ISO3_RC == ""

	// Dropping High Income recipient countries 
	/* NOTE:
		This only drops Equatorial Guinea in certain years when it was high income
	*/
	rename year YEAR
	sort ISO3_RC YEAR
	merge m:1 ISO3_RC YEAR using "`wb_historical_incgrps'.dta", ///
		keepusing(INC_GROUP) keep(1 3)
	drop _merge 
	drop if INC_GROUP == "H" 
		
** *****************************************************************************	
// STEP 2: Prepare Version of Database that can be merged into the PDB
** *****************************************************************************		
	gen gov = 1
	gen CHANNEL = "AfDB"
	gen FUNDING_TYPE = "LOAN"
	gen ELIM_CH = 0

	// Determine whether it is a country- or regional-level project
	gen LEVEL = "COUNTRY"
	replace LEVEL = "REGIONAL" if ISO3_RC == "QMA"

	rename ///
		(projectid projecttitle sector subsector sourceoffinancing loannumber) ///
		(PROJECT_ID PROJECT_NAME SECTOR SUBSECTOR agency LOAN_NUMBER)

	save "`INT'/project_data_`update_tag'.dta", replace

** *****************************************************************************
// INFLATE correspondence data for this year

	//COMMENT OUT FOR FGH 2019 ROUND 2 UPDATE
	//NOTE THAT THIS SHOULD CALCULATE TO AN INFLTION FACTOR = 1 WHEN WE HAVE
	//THE ENTIRE YEAR FROM CORRESPONDENCE! WE SIMPLY COMMENTED IT OUT FOR
	//SPEED PURPOSES. IN FGH2020, CHECK THAT THIS IS WORKING CORRECTLY!
/*
	// Calculate end-of-year date
	local eoy = date("12/31/`report_yr'", "MDY", 2050)
	
	// Calcualte previous three disbursement years
	local year_disb1 = `report_yr' - 1
	local year_disb2 = `report_yr' - 2
	local year_disb3 = `report_yr' - 3
	
	// Calculate end-of-year dates for previous three disbursement years
	local eoy1 = date("12/31/`year_disb1'", "MDY", 2050)
	local eoy2 = date("12/31/`year_disb2'", "MDY", 2050)
	local eoy3 = date("12/31/`year_disb3'", "MDY", 2050)
	
	// Calculate number of days after correspondence data this year
	local post_corr_days = (`eoy' - date(substr("`corr_date'",1,4) + "/" + ///
				substr("`corr_date'",5,2) + "/" + substr("`corr_date'",7,2), ///
				"YMD", 2050))

	// Inflation factor from three years ago
	preserve
		// Correspondence 
		use "`INT'/project_data_`corr_date3'.dta", clear
		keep if YEAR == `year_disb3'
		collapse (sum) DISBURSEMENT
		local corr_disb3 = DISBURSEMENT[1]

		// Actual from next year's correspondence
		use "`INT'/project_data_`corr_date2'.dta", clear
		keep if YEAR == `year_disb3'
		collapse (sum) DISBURSEMENT
		local actual_disb3 = DISBURSEMENT[1]

		// Calculate correspondence fraction
		local corr_frct3 = `actual_disb3' / `corr_disb3'

		// Calculate inflation
		local disb_infl3 = 1 + ((`corr_frct3' - 1) * `post_corr_days' / ///
			(`eoy3' - date(substr("`corr_date3'",1,4) + "/" + ///
				substr("`corr_date3'",5,2) + "/" + substr("`corr_date3'",7,2), ///
				"YMD", 2050)))
	restore

	// Inflation factor from two years ago
	preserve
		// Correspondence 
		use "`INT'/project_data_`corr_date2'.dta", clear
		keep if YEAR == `year_disb2'
		collapse (sum) DISBURSEMENT
		local corr_disb2 = DISBURSEMENT[1]

		// Actual from next year's correspondence
		use "`INT'/project_data_`corr_date1a'.dta", clear
		keep if YEAR == `year_disb2'
		collapse (sum) DISBURSEMENT
		local actual_disb2 = DISBURSEMENT[1]

		// Calculate correspondence fraction
		local corr_frct2 = `actual_disb2' / `corr_disb2'

		// Calculate inflation
		local disb_infl2 = 1 + ((`corr_frct2' - 1) * `post_corr_days' / ///
			(`eoy2' - date(substr("`corr_date2'",1,4) + "/" + ///
				substr("`corr_date2'",5,2) + "/" + substr("`corr_date2'",7,2), ///
				"YMD", 2050)))
	restore

	// Inflation factor from last year
	preserve
		// Correspondence 
		use "`INT'/project_data_`corr_date1a'.dta", clear
		keep if YEAR == `year_disb1'
		collapse (sum) DISBURSEMENT
		local corr_disb1 = DISBURSEMENT[1]

		// Actual from next year's correspondence
		use "`INT'/project_data_`corr_date'.dta", clear
		keep if YEAR == `year_disb1'
		collapse (sum) DISBURSEMENT
		local actual_disb1 = DISBURSEMENT[1]

		// Calculate correspondence fraction
		local corr_frct1 = `actual_disb1' / `corr_disb1'

		// Calculate inflation
		local disb_infl1 = 1 + ((`corr_frct1' - 1) * `post_corr_days' / ///
			(`eoy1' - date(substr("`corr_date1a'",1,4) + "/" + ///
				substr("`corr_date1a'",5,2) + "/" + substr("`corr_date1a'",7,2), ///
				"YMD", 2050)))
	restore

	gen disb_infl1 = `disb_infl1'
	gen disb_infl2 = `disb_infl2'
	gen disb_infl3 = `disb_infl3'
	gen disb_infl = disb_infl1 / 3 + disb_infl2 / 3 + disb_infl3 / 3

	replace DISBURSEMENT = DISBURSEMENT * disb_infl if YEAR == `report_yr'

	drop disb_infl*
	*/

** *****************************************************************************	
// STEP 3: Keyword search
** *****************************************************************************	

	// Manually fix one project description that will not be picked up correctly
	replace PROJECT_NAME = "PROJET D'APPUI A lA LUTTE CONTRE VIH/SIDA" if ///
		PROJECT_NAME == "PROJET D'APPUI A lA LUTTE CONTREVIH/SID"

	// Perform keyword search on PROJECT_NAME in English and French
	HFA_ado_master srchstr, language(english french)
	
	// Allocate commitments and disbursements across all health focus areas using weights
	foreach var of varlist final*frct {
		local healthfocus = subinstr("`var'", "final_", "", .)
		local healthfocus = subinstr("`healthfocus'", "_frct", "", .)
		gen double `healthfocus'_DAH = `var' * DISBURSEMENT
	}

	keep YEAR PROJECT_ID LOAN_NUMBER PROJECT_NAME RECIPIENT_COUNTRY SECTOR ///
		SUBSECTOR DISBURSEMENT gov FUNDING_TYPE LEVEL ISO3_RC is_covid *_DAH agency

    replace is_covid = "FALSE" if YEAR == 2023 // one project receiving disbursements in 2023 tagged as COVID, but has been repurposed for other activities
    replace is_covid = "FALSE" if YEAR == 2024 // one project receiving disbursements in 2024 tagged as COVID, but has been repurposed for other activities
			
    // COVID adjustment:
    // any disbursements tagged as COVID should go entirely to COVID
    foreach var of varlist *_DAH { // includes "total_DAH"
        if ("`var'" != "total_DAH") {
            replace `var' = 0 if is_covid == "TRUE"
        }
    }
    gen oid_covid_DAH = 0
    replace oid_covid_DAH = total_DAH if is_covid == "TRUE"


    // ensure components sum exactly to total_DAH - note, this 
    rename total_DAH total
    egen new_total = rowtotal(*_DAH)
    
    // ensure maximum difference isn't too large - some slight difference (<10$) is okay/expected due to inprecision of TT smooth
    egen max_diff = max(abs(total - new_total))
    count if max_diff > 10
    if r(N) != 0 {
        di "Error! DAH components do not sum to a value close to total DAH!"
        hfa_distribution_fail_error
    }
    
    rename new_total total_DAH
    drop total
    



** *****************************************************************************	
// ADD 1990-2001 DATA
/* NOTES:
	For the African Development Bank, we use 2 sources of DAH data:
		1990-2001:	AfDB's compendium of statistics data (collected in 2010)
		2002+: 		Project-level loan disbursement data (received directly
					from AfDB's loan department)
*/

	// Merge with the compendium of statistics data that was collected in 
	// 2010 and use as the total envelope for 1990-2001
	preserve
		collapse (sum) DISBURSEMENT, by(YEAR)
		** DO NOT CHANGE THE BELOW FILEPATH **
		merge 1:1 YEAR using "FILEPATH/M_AfDB_INC_DISB_FINAL_0910.dta", keep(2) nogen
		
		replace DISBURSEMENT = OUTFLOW
		gen PROJECT_ID = "N/A"
		gen PROJECT_NAME = "Dummy project"
		gen RECIPIENT_COUNTRY = "N/A"
		gen ISO3_RC = "QZA"
		drop OUTFLOW DONOR_NAME DONOR_COUNTRY ISO_CODE CHANNEL INCOME_SECTOR ///
			INCOME_TYPE INCOME_ALL GHI SOURCE

		tempfile compend_data
		save `compend_data', replace
		
	restore

	append using `compend_data'
	sort YEAR ISO3_RC

/* NOTE:
	DAH for QZA is unallocable in the dummy_proj tempfile and the tempfile 
	didn't include the *DAH variables for HFAs, so replacing unalloc_DAH 
	equal to total 
*/

	gen unalloc_DAH = DISBURSEMENT if ISO3_RC == "QZA" &  YEAR < 2002
	replace LEVEL = "GLOBAL" if ISO3_RC == "QZA" 
	replace gov = 0 if ISO3_RC == "QZA" // gov is assigned to unallocable for unallocable projects
	replace agency = "UNSP" if YEAR < 2002
	rename DISBURSEMENT DAH 

	tempfile disbursements
	save `disbursements', replace

** *****************************************************************************	
// APPEND AND PROCESS ADF DONOR DATA
// We use replenishment reports to split funding by donor

	// Import files
	local allfiles: dir "`INCOME'" files "*.xlsx", respectcase
   	foreach file in `allfiles' {
        import excel using "`INCOME'/`file'", firstrow clear
        levelsof YEAR, clean local(yr)
        tempfile replenishment_`yr'
        save `replenishment_`yr''
    } 		
    clear

    // Append
    local years `adf_repl_years'
    foreach year of local years {
    	append using `replenishment_`year''
    }

    // Clean 
    replace STATEPARTICIPANTS = proper(STATEPARTICIPANTS)
    drop if regexm(STATEPARTICIPANTS, "Asterisk") // Unallocated accounts
    replace STATEPARTICIPANTS = "South Korea" if STATEPARTICIPANTS == "Korea"
    ren STATEPARTICIPANTS country_lc
    merge m:1 country_lc using "`country_codes'", keep(1 3) keepusing(iso3) nogen
    ren (country_lc iso3) (DONOR_NAME ISO_CODE)

    replace SUBSCRIPTIONSINUA = 0 if SUBSCRIPTIONSINUA == .
    drop if SUBSCRIPTIONSINUA == 0
    keep YEAR DONOR_NAME ISO_CODE SUBSCRIPTIONSINUA

    // Split funding evenly across years
    split YEAR, parse("_")
    destring YEAR*, replace
    gen YEAR3 = YEAR1 + 1
    drop YEAR
    expand 3
    bysort YEAR1 DONOR_NAME: gen n = _n
    gen YEAR = YEAR1 if n == 1 
    replace YEAR = YEAR2 if n == 2
    replace YEAR = YEAR3 if n == 3
    drop YEAR1 YEAR2 YEAR3
    replace SUBSCRIPTIONSINUA = SUBSCRIPTIONSINUA / 3
    sort YEAR ISO_CODE
    drop n
    order YEAR ISO_CODE DONOR_NAME SUBSCRIPTIONSINUA

    // Create donor proportions
    bysort YEAR: egen double total_subscriptions = sum(SUBSCRIPTIONSINUA)
    gen double donor_prop = SUBSCRIPTIONSINUA / total_subscriptions
    keep DONOR_NAME ISO_CODE donor_prop YEAR
    gen agency = "ADF"
    gen INCOME_SECTOR = "PUBLIC"
    /* NOTE:
		We only use observed donor subscriptions through 2016 and ttsmooth 
		2017-2019 because there are large differences for certain donor 
		subscriptions between the ADF 13 and ADF 14. This is documented at 
		"FILEPATH\AfDB donor disaggregation".

		There are also other issues with the replenishment tables, such as some 
		data being incomplete for certain donors and years (see the notes at the 
		bottom of the tables) and supplemental donors at the bottom. 

		We would like a contact to help us clarify some of these questions.
    */
	keep if YEAR <= 2016
	tempfile replenishment_props
	save `replenishment_props'

	// Add proportions for 2017-2019 from subscripritions extracted from annual financial report
	// insheet using "FILEPATH/AFDB_INCOME_SHARES_2017_2020.csv", comma clear 
	insheet using "FILEPATH/FGH_`report_yr'/AFDB_INCOME_SHARES_2017_`report_yr'.csv", comma clear 
	ren (year donor_name income_sector currency amount_paid total_amount pct_calc income_share) (YEAR DONOR_NAME INCOME_SECTOR CURRENCY AMOUNT_PAID TOTAL_AMOUNT PCT_CALC INCOME_SHARE)

	// Fix some country names for merging
	ren DONOR_NAME country_lc
	replace country_lc = "Central African Republic" if country_lc == "Centralafrican Republic"
	replace country_lc = "Cote d Ivoire" if country_lc == "Cote D'ivoire"
	replace country_lc = "Gambia" if country_lc == "Gambia,The"
    replace country_lc = "United States" if country_lc == "U.S.A."
    replace country_lc = "United Kingdom" if country_lc == "U.K."
    merge m:1 country_lc using "`country_codes'", keep(1 3) keepusing(iso3) nogen
 	replace iso3 = "SWZ" if country_lc == "Eswatini"
 	ren (iso3 country_lc) (ISO_CODE DONOR_NAME)

 	// Final cleaning and append to previous year proportions
 	gen donor_prop = INCOME_SHARE / 100
 	gen agency = "ADF"
 	keep DONOR_NAME ISO_CODE donor_prop YEAR agency INCOME_SECTOR
 	append using `replenishment_props'

 	sort YEAR ISO_CODE
    tempfile adf_props
    save `adf_props'

    // Add debt fraction props for ADB projects
	insheet using "`RAW'/AfDB_DEBT.csv", comma clear
    keep year debt_frac
    rename year YEAR
    rename debt_frac val1
    gen val2 = 1 - val1
    reshape long val, i(YEAR) j(don)

    gen DONOR_NAME = "DEBT Repayments" if don == 1
    replace DONOR_NAME = "Other" if don == 2
    gen INCOME_SECTOR = "DEBT" if don == 1
    replace INCOME_SECTOR = "OTHER" if don == 2
    gen INCOME_TYPE = "OTHER"
    gen ISO_CODE = ""
    gen agency = "ADB"

    drop don
    rename val donor_prop

    append using `adf_props'
    tempfile donor_props
    save `donor_props'
   
    
	save "`FIN'/AfDB_DONOR_PROPS.dta", replace

** *****************************************************************************	
// DISAGGREGATE DAH PER DONOR
/* NOTES:
	- ADF: Add donor information based on replenishments.
		FGH 2021: we updated this to be based on subscriptions from the AfDB financial reports
		since the ADF replenishments had not been updated since 2016 
	- ADB: We were unable to disaggregate ADB donor information; set to other. 
	- NTF: Assign all to Nigeria.

	 Pre-2002 data is all assigned to unallocable.
*/

	use `disbursements', clear
	// Outer join adf_props
	joinby YEAR agency using `donor_props', unmatched(master) 
	
	// ADF
	replace INCOME_SECTOR = "UNSP" if _m == 1 & agency == "ADF"
	replace DONOR_NAME = "UNSP" if _m == 1 & agency == "ADF"
	replace ISO_CODE = "NA" if _m == 1 & agency == "ADF"

	// NTF
	replace INCOME_SECTOR = "PUBLIC" if agency == "NTF"
	replace DONOR_NAME = "Nigeria" if agency == "NTF"
	replace ISO_CODE = "NGA" if agency == "NTF"

	// // ADB
	// replace INCOME_SECTOR = "OTHER" if agency == "ADB"
	// replace DONOR_NAME = "UNSP" if agency == "ADB"
	// replace ISO_CODE = "NA" if agency == "ADB"

	// Pre-2002
	replace INCOME_SECTOR = "UNSP" if _m == 1 & YEAR < 2002
	replace DONOR_NAME = "UNSP" if _m == 1 & YEAR < 2002
	replace ISO_CODE = "NA" if _m == 1 & YEAR < 2002

	drop _m
	order agency YEAR ISO3_RC ISO_CODE *
	sort agency YEAR ISO3_RC ISO_CODE

	// Multiply DAH by donor proportions where available
	foreach var of varlist *DAH {
		replace `var' = `var' * donor_prop if donor_prop != .
	}
	
	gen temp_agency = "ALL"
	tempfile pre_inkind
	save `pre_inkind', replace

** *****************************************************************************	
// Add in-kind
	preserve
		import excel using "`RAW'/INKIND_RATIO_1990_`report_yr'.xlsx", firstrow clear
		keep year agency inkind_ratio
		ren (inkind_ratio agency year) (INKIND_RATIO temp_agency YEAR)
		merge 1:m YEAR temp_agency using `pre_inkind', nogen keep(3 2)
		foreach var of varlist DAH *_DAH {
			replace `var' = `var' * INKIND_RATIO
		}
		gen INKIND = 1
		tempfile inkind
		save `inkind', replace
	restore
	
	use `pre_inkind', clear
	append using `inkind'
	replace INKIND = 0 if INKIND == .
	drop if DAH == . | DAH == 0
	gen CHANNEL = "AfDB"
	
	tempfile newink
	save `newink'

	save "`FIN'/M_AfDB_INTPDB_`update_tag'.dta", replace
		
** *****************************************************************************
// STEP 4: Save Version of Database that can be merged into the ADB
** *****************************************************************************
	collapse (sum) DAH (mean) INKIND_RATIO, by(YEAR INKIND gov)
	gen CHANNEL = "AfDB" 
	gen SOURCE = "Data obtained through correspondence" if YEAR > 2001
	replace SOURCE = "Compendium data" if YEAR < 2002
	rename DAH OUTFLOW
	tab YEAR

	save "`FIN'/M_AfDB_INC_DISB_FINAL_`update_tag'.dta", replace

** *****************************************************************************
// STEP 5: Save ADB/PDB
** *****************************************************************************
	use "`FIN'/M_AfDB_INTPDB_`update_tag'.dta", clear

	collapse (sum) *DAH, by(YEAR ISO3_RC gov LEVEL DONOR_NAME ISO_CODE ///
		INCOME_SECTOR INKIND CHANNEL agency)

	save "`FIN'/M_AfDB_ADB_PDB_FGH`report_yr'_withoutdonorpreds_`update_tag'.dta", replace
	
** *****************************************************************************
// Predict out newest ADF donor data while preserving recipient and HFA amounts
// Uses TTsmooth
    /* NOTE:
		We only use observed donor subscriptions through 2016 and ttsmooth 
		2017-2019 because there are large differences for certain donor 
		subscriptions between the ADF 13 and ADF 14. This is documented at 
		"FILEPATH\AfDB donor disaggregation".

		There are also other issues with the replenishment tables, such as some 
		data being incomplete for certain donors and years (see the notes at the 
		bottom of the tables) and supplemental donors at the bottom. 

		We would like a contact to help us clarify some of these questions.
    */
** *****************************************************************************
	
	// Find non-zero HFAs for ADF 2017-19
	use  "`FIN'/M_AfDB_ADB_PDB_FGH`report_yr'_withoutdonorpreds_`update_tag'.dta", clear
	drop if ISO_CODE == "NA" & YEAR < 2002
	drop total_DAH
	preserve
		drop if YEAR < `report_yr' 
		drop if agency != "ADF"

		// Drop all all-zero HFAs for 2017-2019
		//ssc install findname
		//findname, all(@ == 0)
		//drop `r(varlist)'
		keep *DAH
		drop DAH
		ds

		local hfas_nom `r(varlist)'
	restore
	local hfas_nom swap_hss_hrh_DAH nch_cnn_DAH other_DAH // are the last two items needed?

/* NOTE:
	Temporarily reassign 2017-2019 NTF and ADB projects so we can just 
	predict ADF. We decided to use subscriptions and replace 2017 observed 
	proportions because it looked so different between subscriptions and 
	contributions.

	This had not been updated correctly for FGH2019-FGH2020. We now pull subscription 
	proportions from the annual financial statement which is usually lagged
	1 year and then we predict only the report year.
*/
	
	// Calculate total DAH envelope for ADF 2017-2019
	preserve
		replace YEAR = 9999 if YEAR == `report_yr' & inlist(agency, "NTF", "ADB")
		collapse (sum) `hfas_nom' DAH, by(YEAR ISO3_RC)
		tempfile totaldah
		save `totaldah'
	restore

	// Reshape for ttsmoothing
	replace YEAR = 9999 if YEAR == `report_yr' & inlist(agency, "NTF", "ADB")
		// Group donor isos for ttsmooth
		replace ISO_CODE = "OTHER" if !inlist(ISO_CODE, "NGA", "JPN", "USA", "DEU", "EGY", "MAR", "ZAF", "CAN", "DZA") & !inlist(ISO_CODE, "FRA", "CIV", "ITA", "LBY", "GBR", "SWE", "GHA", "KEN", "ZWE") & !inlist(ISO_CODE, "CHN", "NOR", "DNK", "ETH", "ESP", "NA")
        // Group recipients
        replace ISO3_RC = "QZA" if ISO3_RC == ""
	collapse (sum) `hfas_nom', by(YEAR ISO_CODE CHANNEL ISO3_RC)
	reshape wide `hfas_nom', i(YEAR CHANNEL ISO3_RC) j(ISO_CODE) string
	merge 1:1 YEAR ISO3_RC using `totaldah', nogen keepusing(DAH)

	// Remove 2017-2019 ADF DAH values
	foreach var of varlist *_DAH* {
		replace `var' = 0 if `var' == . & YEAR < `report_yr'
		replace `var' = . if YEAR >= `report_yr'
	}

	// Merge in predicted DAH by HFA I
	merge 1:1 YEAR ISO3_RC using `totaldah', keepusing(`hfas_nom') nogen

	drop if YEAR == 9999
    replace ISO3_RC = "QZA" if ISO3_RC == ""
	reshape wide *DAH*, i(YEAR CHANNEL) j(ISO3_RC) string
    collapse (sum) *DAH*, by(YEAR) // channel is just == AfDB, causes issues

	// Predict by health focus area
	//local hfas = subinstr("`hfas_nom'", "_DAH", "", .)
	local hfas "swap_hss_hrh nch_cnn other"

	// Fill in CPV's 2017&2018 data with very small values so TT_Smooth doesn't error out.
	// NOTE: CHANGE THIS IN THE FUTURE IF CPV REPORTS 2017/2018 DISBURSEMENT
//check_note_above
	foreach var of varlist other*CPV swap*CPV DAHCPV {
		replace `var' = 1 if YEAR == `report_yr' 
	}

	preserve
		drop if YEAR < `report_yr'

		// Drop all-zero HFAs
        // ssc install findname
		findname, all(@ == 0)
		capture drop `r(varlist)'

		// Drop empty HFAs
		findname, all(@ == .)
		capture drop `r(varlist)'

		// Keep the country-specific envelopes for ttsmooth
		keep DAH* /**/

		// Extract countries with envelopes
		ds
		local countries = subinstr("`r(varlist)'", "DAH", "", .)
		// local countries "ETH KEN QMA RWA SDN TZA UGA"
	restore

// note - this code is needed only if you need to predict (TT smooth) the report-year, but we often have report-year data from correspondence
if (2 < 1) {
** *****************************************************************************
// TTSMOOTH 
// We smooth ADF spending across non-NA donors.
	// 1-year forecast (report year)
	// Have to decide on top donors local daccountriesdah "`hfa'_DAHAUT`iso' `hfa'_DAHBEL`iso' `hfa'_DAHCAN`iso' `hfa'_DAHCHE`iso' `hfa'_DAHCHN`iso' `hfa'_DAHDEU`iso' `hfa'_DAHDNK`iso' `hfa'_DAHESP`iso' `hfa'_DAHFIN`iso' `hfa'_DAHFRA`iso' `hfa'_DAHGBR`iso' `hfa'_DAHIRL`iso' `hfa'_DAHITA`iso' `hfa'_DAHJPN`iso' `hfa'_DAHKOR`iso' `hfa'_DAHLUX`iso' `hfa'_DAHNLD`iso' `hfa'_DAHNOR`iso' `hfa'_DAHPRT`iso' `hfa'_DAHSWE`iso' `hfa'_DAHUSA`iso' `hfa'_DAHOTHER`iso' `hfa'_DAHNA`iso'"
//manual_changes_below
	foreach hfa in `hfas' {
		foreach iso in `countries' {
			preserve  
				local daccountriesdah "`hfa'_DAHNGA`iso' `hfa'_DAHJPN`iso' `hfa'_DAHUSA`iso' `hfa'_DAHDEU`iso' `hfa'_DAHEGY`iso' `hfa'_DAHMAR`iso' `hfa'_DAHZAF`iso' `hfa'_DAHCAN`iso' `hfa'_DAHDZA`iso' `hfa'_DAHFRA`iso' `hfa'_DAHCIV`iso' `hfa'_DAHITA`iso' `hfa'_DAHLBY`iso' `hfa'_DAHGBR`iso' `hfa'_DAHSWE`iso' `hfa'_DAHGHA`iso' `hfa'_DAHKEN`iso' `hfa'_DAHZWE`iso' `hfa'_DAHCHN`iso' `hfa'_DAHNOR`iso' `hfa'_DAHDNK`iso' `hfa'_DAHETH`iso' `hfa'_DAHESP`iso' `hfa'_DAHOTHER`iso' `hfa'_DAHNA`iso'"
				keep YEAR `hfa'_DAH`iso' `daccountriesdah' 
				replace `hfa'_DAH`iso' = 1 if `hfa'_DAH`iso' == 0

				TT_smooth `hfa'_DAH`iso' `daccountriesdah', ///
					time(YEAR) forecast(1) test(0)

				tempfile pr_`hfa'_`iso'
				save `pr_`hfa'_`iso'', replace
			restore
		}	
	}
//foreach iso in AGO COD ETH GMB LSO MOZ NGA SEN UGA BEN CPV GAB GNB MAR MRT QMA SLE ZMB BFA DJI GHA GNQ MDG MWI RWA TCD CMR EGY GIN KEN MLI NER SDN {
		
	// Compile predictions	
	use `pr_other_TZA', clear
	foreach hfa in `hfas' {
		foreach iso in `countries' {
			merge 1:1 YEAR using `pr_`hfa'_`iso''
			drop _m
		}
	}
	// merge 1:1 YEAR using `pr_swap_hss_hrh_TZA', nogen
	// merge 1:1 YEAR using `pr_nch_cnn_TZA', nogen

	// Assign predictions
//manual_changes_below
	foreach hfa in `hfas' {
		foreach iso in `countries' {
			local daccountriesdah "`hfa'_DAHNGA`iso' `hfa'_DAHJPN`iso' `hfa'_DAHUSA`iso' `hfa'_DAHDEU`iso' `hfa'_DAHEGY`iso' `hfa'_DAHMAR`iso' `hfa'_DAHZAF`iso' `hfa'_DAHCAN`iso' `hfa'_DAHDZA`iso' `hfa'_DAHFRA`iso' `hfa'_DAHCIV`iso' `hfa'_DAHITA`iso' `hfa'_DAHLBY`iso' `hfa'_DAHGBR`iso' `hfa'_DAHSWE`iso' `hfa'_DAHGHA`iso' `hfa'_DAHKEN`iso' `hfa'_DAHZWE`iso' `hfa'_DAHCHN`iso' `hfa'_DAHNOR`iso' `hfa'_DAHDNK`iso' `hfa'_DAHETH`iso' `hfa'_DAHESP`iso' `hfa'_DAHOTHER`iso' `hfa'_DAHNA`iso'"
			foreach source in `daccountriesdah' {
				replace `source' = pr_`source' if YEAR == 2021 
			}
		}	
	}
	drop pr_* 

	// Format and reshape
	gen CHANNEL = "AfDB"
	foreach country in `countries' {
		drop *_DAH`country'
	}

	reshape long `hfas_nom', i(YEAR CHANNEL) j(ISO_CODEISO3_RC) string	
	keep if YEAR >= `report_yr'
	gen agency = "ADF"
	gen ISO_CODE = substr(ISO_CODEISO3_RC, 1, 3)
	gen ISO3_RC = substr(ISO_CODEISO3_RC, 4, 3)
	drop ISO_CODEISO3_RC
	gen INCOME_SECTOR = "PUBLIC"
	gen gov = 1
	gen LEVEL = "COUNTRY"
	gen INKIND = 2
	replace other_DAH = 0 if other_DAH == .
	replace swap_hss_hrh_DAH = 0 if swap_hss_hrh_DAH == .
	replace nch_cnn_DAH = 0 if nch_cnn_DAH == .
	gen DAH = other_DAH + swap_hss_hrh_DAH + nch_cnn_DAH
	tempfile preds_src_hfa
	save `preds_src_hfa'
}

	use "`FIN'/M_AfDB_ADB_PDB_FGH`report_yr'_withoutdonorpreds_`update_tag'.dta", clear

if (2 < 1) {
	drop if YEAR >= 2021 & agency == "ADF"
	append using `preds_src_hfa'
}
	drop agency total_DAH
	foreach var of varlist *_DAH* {
		replace `var' = 0 if `var' == . 
	}

	save "`INT'/M_AfDB_ADB_PDB_FGH`report_yr'_20220817.dta", replace
	save "`FIN'/M_AfDB_ADB_PDB_FGH`report_yr'.dta", replace

** END OF FILE **

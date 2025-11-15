** *****************************************************************************
// Project: 	FGH 
// Purpose: 	Estimating US Foundation DAH using new grant-level data from the Foundation Center
** *****************************************************************************

** *****************************************************************************
// SETUP
** *****************************************************************************

	set more off
	clear all
	set maxvar 32000
// Define drives for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix"
		global j "FILEPATH"
		global h "FILEPATH"
		set odbcmgr unixodbc
	}

** *****************************************************************************
// USER INPUTS
	local report_yr = 2024							// FGH report year
	local update_tag "20250429"	            	    // round_date
	local defl_mmyy = "0424"						//check FILEPATH for most recent
/* -----------------------------------------------------------------------------
NOTE:
	`hfareas' (below) currently doesn't contain ebola because we don't want 
	TT Smooth to predict ebola funding
----------------------------------------------------------------------------- */
	local hfareas 			"rmh_fp_DAH rmh_mh_DAH nch_cnn_DAH nch_cnv_DAH hiv_treat_DAH hiv_prev_DAH hiv_pmtct_DAH hiv_ct_DAH hiv_care_DAH hiv_ovc_DAH hiv_amr_DAH mal_treat_DAH mal_diag_DAH mal_con_nets_DAH mal_con_irs_DAH mal_con_oth_DAH mal_comm_con_DAH mal_amr_DAH tb_treat_DAH tb_diag_DAH tb_amr_DAH oid_zika_DAH oid_amr_DAH ncd_tobac_DAH ncd_mental_DAH swap_hss_other_DAH swap_hss_hrh_DAH swap_hss_pp_DAH rmh_other_DAH nch_other_DAH mal_other_DAH hiv_other_DAH tb_other_DAH ncd_other_DAH oid_other_DAH rmh_hss_other_DAH nch_hss_other_DAH mal_hss_other_DAH hiv_hss_other_DAH tb_hss_other_DAH oid_hss_other_DAH ncd_hss_other_DAH rmh_hss_hrh_DAH nch_hss_hrh_DAH mal_hss_hrh_DAH hiv_hss_hrh_DAH tb_hss_hrh_DAH oid_hss_hrh_DAH ncd_hss_hrh_DAH other_DAH"
		
	// Filepaths
	local working_dir 		"FILEPATH"
	local DEFL				"FILEPATH"
	local FIN 				"FILEPATH"
 	local INT 				"FILEPATH"
 	local RAW 				"FILEPATH"
	local DEFL	 			"FILEPATH"

 	// Files
	local LDI_file 			"FILEPATH"
	local ttsmooth_file		"FILEPATHo"
	local two_years_ago = `report_yr' - 2
	local adb_pdb_two_yrs "FILEPATH
	local deflator_file "FILEPATH"

	// Derived macros
	local abrv_yr = substr("`report_yr'", 3, 4)		// abbreviated FGH report year
	local previous_yr = `report_yr' - 1				// Previous FGH report year
	
** *****************************************************************************
// IMPORT DATASETS 
** *****************************************************************************
	// In-kind 
	import excel using "FILEPATH",  clear firstrow // Don't change this path - files not brought forward
	keep YEAR DONOR_NAME INKIND_RATIO
	tempfile old_ik
	save `old_ik'
	import delimited using "FILEPATH", clear varn(1)
	rename year YEAR
	rename donor_name DONOR_NAME
	rename inkind_ratio INKIND_RATIO
	keep YEAR DONOR_NAME INKIND_RATIO
	append using `old_ik', force


** -----------------------------------------------------------------------------
	// CHECK TOP AND BOTTOM 10
// manual_inspection

	// See the top and bottom 10 US Foundations in the current inkind ratios dataset
	di "These are the current donors used for inkind ratios:"
	destring YEAR, replace
	list DONOR_NAME if YEAR == `report_yr' - 2

	// Load clean names
	preserve
		import excel using "`RAW'/../Cleaning Raw Names.xlsx", firstrow case(upper) clear
		tempfile clean_names 
		save `clean_names'
	restore 

* -----------------------------------------------------------------------------

	collapse (mean) INKIND_RATIO 
	expand `report_yr' - 1990 + 1
	gen n = _n
	gen YEAR = 1989 + n
	drop n
	tempfile ik_ratios 
	save `ik_ratios'

** -----------------------------------------------------------------------------

	use "FILEPATH", clear
	append using "FILEPATH", force

	preserve
	keep if covid == 1
	gen oid_covid_DAH = amount_split
	foreach var of varlist *_DAH {
    if "`var'" != "oid_covid_DAH" {
        replace `var' = 0
    	}
	}

	tempfile covidrows
	save `covidrows'
	restore
	
	drop if covid == 1
	append using `covidrows'


	preserve
		merge m:1 YEAR using `ik_ratios', nogen keepusing(INKIND_RATIO) keep(1 3)
		foreach var of varlist amount_split *_DAH { 
			replace `var' = `var' * INKIND_RATIO
		}
		gen INKIND = 1
		replace ELIM_CH = 0 if INKIND == 1	// we want to include the administrative costs of projects that are double counted

		tempfile inkind
		save `inkind', replace
	restore
		

	append using `inkind'	
	replace INKIND = 0 if INKIND == .

	tempfile F_USA_intermediate_newdata
	save `F_USA_intermediate_newdata'
	
	// For creating final dataset	
		
	drop if ELIM_CH == 1
	collapse (sum) amount_split *_DAH, by(YEAR)
	gen iso3 = "USA"
	rename YEAR year

	tempfile envelope_2002_2014_new
	save `envelope_2002_2014_new', replace
	
	rename amount_split DAH_newdata
	
	keep year DAH_newdata
	
	tempfile newdata_2002_14
	save `newdata_2002_14', replace
		
	// Pull in old foundation data - this needs to be done seprately because the 
	// variable names are different
	use  "FILEPATH", clear
		
	// Drop double counting and add in-kind	
		
	preserve
		merge m:1 YEAR using `ik_ratios', nogen keepusing(INKIND_RATIO) keep(1 3)
		foreach var of varlist amount_split *_DAH { 
			replace `var' = `var' * INKIND_RATIO
		}
		gen INKIND = 1
		replace ELIM_CH = 0 if INKIND == 1	// we want to include the administrative costs of projects that are double counted
		
		tempfile inkind
		save `inkind', replace
	restore
	
	append using `inkind'
	replace INKIND = 0 if INKIND == .

	// For creating final dataset
	tempfile F_USA_intermediate_olddata
	save `F_USA_intermediate_olddata'

	drop if ELIM_CH == 1
	collapse (sum) amount_split *_DAH , by(YEAR)
	gen iso3 = "USA"
	rename YEAR year
	
	tempfile envelope_1992_2012_old
	save `envelope_1992_2012_old', replace
	
	keep year amount_split 
	rename amount_split DAH_olddata
	merge 1:1 year using `newdata_2002_14'

	// Method #2: (method used in previous years)
	// 3-year weighted average to predict comparable DAH for new data classification from 1992 to 2001
	tsset year
	
	gen double dah_frct = DAH_newdata / DAH_olddata
		
	// Use the three most relevant			
	gen dah_frct_2002 = dah_frct if year == 2002
	egen dah_frct_2002_2 = total(dah_frct_2002)
	
	gen dah_frct_2003 = dah_frct if year == 2003
	egen dah_frct_2003_2 = total(dah_frct_2003)
	
	gen dah_frct_2004 = dah_frct if year == 2004
	egen dah_frct_2004_2 = total(dah_frct_2004)
	
	drop dah_frct_2002 dah_frct_2003 dah_frct_2004
	
	gen double wgt_avg_frct_2 = 1/2 * (dah_frct_2002_2) + ///
		1/3 * (dah_frct_2003_2) + 1/6 * (dah_frct_2004_2)
	gen double wgt_avg_frct_out_16_2 = wgt_avg_frct_2 * DAH_olddata	
	
	gen double final_DAH = wgt_avg_frct_out_16_2
	replace final_DAH = DAH_newdata if year > 2001
		
	keep year final_DAH
	
	tempfile finalDAH
	save `finalDAH', replace

*********************************************************************************	
// Add final disbursement values to aggregate data
*****************************************************************************
	// OLD DATA 
    use `envelope_1992_2012_old', clear
	keep if year < 2002
	
	append using `envelope_2002_2014_new'
	
	merge m:1 year using `finalDAH'
	drop if _m == 2
	drop _m
	
	rename final_DAH DAH
	foreach var of varlist *_DAH {
		gen `var'frct = `var' / amount_split
		replace `var' = `var'frct * DAH
	}

	tempfile final_1992_2014
	save `final_1992_2014', replace 

*********************************************************************************	
// Make predictions using GDP per capita
*****************************************************************************	

// separate out covid data
	preserve
	keep if oid_covid_DAH > 0
	tempfile covidremovepredict
	save `covidremovepredict'
	restore
//update_ldi_file!
	//MAKE SURE you are using an updated version of this dataset!
	use "`LDI_file'", clear
	ren IHME_usd_gdppc_b2018 IHME_usd_gdppc_b2023

	keep iso3 year IHME_usd_gdppc_b`previous_yr'
	ren year YEAR
	merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", ///
		keepusing(GDP_deflator_2018) nogen keep(3)
	replace IHME_usd_gdppc_b`previous_yr' = ///
		IHME_usd_gdppc_b`previous_yr' * GDP_deflator_2018
	drop GDP_deflator_2018
	ren YEAR year

	keep if iso3 == "USA"
	keep if year >= 1989 & year <= `report_yr'
	encode iso3, g(iso3_n)
	
	merge 1:1 iso3 year using `final_1992_2014', nogen 
	drop iso3

	// to prevent predicting based on covid
	//replace DAH = DAH - oid_covid_DAH 

	tsset year
	gen ln_DAH = ln(DAH)
	gen ln_gdppc = ln(IHME_usd_gdppc)

	//this is what is predicting the envelope! Make sure to understand this
	foreach var in ln_DAH {
		reg `var' l.ln_gdppc year, r
		predict pr_`var', xb
		replace pr_`var' = exp(pr_`var')
		replace `var' = pr_`var' if year == `report_yr' | year==`previous_yr' | ///
			year == 1990 | year == 1991
	}

	rename DAH dah
	g DAH = dah
	replace DAH = pr_ln_DAH if DAH == .
	drop ln_DAH pr_*
	
	tempfile int_prediction_1990_2016
	save `int_prediction_1990_2016', replace 
	
	ren year YEAR
	merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", ///
		keepusing(GDP_deflator_`report_yr')
	keep if _merge == 3 
	drop _merge 

	// Deflate to real usd
    ren YEAR year
	foreach var of varlist *_DAH DAH dah {
		replace `var' = `var' / GDP_deflator_`report_yr'
	}
	drop GDP_deflator_`report_yr'
	drop if year < 1990

	egen double check = rowtotal(*_DAH)
	replace DAH = check if !inlist(year, 1990, 1991, `previous_yr', `report_yr')
	gen diff = check - DAH if !inlist(year, 1990, 1991, `previous_yr', `report_yr')
	sum diff
	if `r(max)' > 30 {
		di in red "sum of program areas do not equal DAH"
	}
	else if `r(min)' < -30 {
		di in red "sum of program areas do not equal DAH"
	}
	else {
		drop diff check
	}

	// Predict DAH by health focus area for this year & previous year and 1990 & 1991
	do "`ttsmooth_file'"
	TT_smooth_revised DAH *_DAH, forecast(2) time(year) test(0)

	foreach var in `hfareas' {
		replace `var' = pr_`var' if year == `previous_yr' | year == `report_yr'
	}

	sum year, d
	gen temp_year = r(mean) + (r(mean) - year)
	sort temp_year
	drop pr_*


	TT_smooth_revised DAH *_DAH, forecast(2) time(temp_year) test(0)
	foreach var in `hfareas' { 
		replace `var' = pr_`var' if temp_year == `previous_yr' | temp_year == `report_yr'
	}
	drop pr_* temp_year 

	// Add Covid back in MAKE SURE TO UPDATE!
	//drop if year >= 2020 & year <= 2022
	//append using `covidremovepredict'
	//gsort -year

	gen CHANNEL = "US_FOUND"
	rename year YEAR
	drop *_DAHfrct ln_gdppc iso3_n IHME_usd_gdppc dah DAH amount_split

	ren *_DAH *_DAH_`abrv_yr'
	egen double DAH_`abrv_yr' = rowtotal(*_DAH_`abrv_yr')

	save "FILEPATH", replace

	// Create final datasets

	//need to back cast from 1992-2001 using a dataset with double counting included (for ADB_PDB)
	// Prep datasets to back cast 
	use `F_USA_intermediate_olddata', clear
	collapse (sum) amount_split, by(YEAR)
	rename amount_split DAH_olddata
	tempfile olddata 
	save `olddata'
	use `F_USA_intermediate_newdata', clear
	collapse (sum) amount_split , by(YEAR)
	rename amount_split DAH_newdata
	merge 1:1 YEAR using `olddata'
	drop _m
	sort YEAR
	ren YEAR year

	// 3-year weighted average to backwards predict comparable DAH for new data classification from 1992 to 2001
	tsset year
	gen double dah_frct=DAH_newdata/DAH_olddata
	
	// Use the three most relevant			
	gen dah_frct_2002=dah_frct if year == 2002
	egen dah_frct_2002_2 = total(dah_frct_2002)
	
	gen dah_frct_2003=dah_frct if year == 2003
	egen dah_frct_2003_2 = total(dah_frct_2003)
	
	gen dah_frct_2004=dah_frct if year == 2004
	egen dah_frct_2004_2 = total(dah_frct_2004)
	
	drop dah_frct_2002 dah_frct_2003 dah_frct_2004
	
	gen double wgt_avg_frct_2 = 1/2 * (dah_frct_2002_2) + ///
		1/3 * (dah_frct_2003_2) + 1/6 * (dah_frct_2004_2)
	gen double wgt_avg_frct_out_16_2 = wgt_avg_frct_2 * DAH_olddata	
	
	gen double final_DAH=wgt_avg_frct_out_16_2
	replace final_DAH = DAH_newdata if year > 2001
	
	keep year final_DAH
	
	tempfile finalDAH_DC
	save `finalDAH_DC', replace

	use `F_USA_intermediate_olddata', clear
	keep if YEAR < 2002
	append using `F_USA_intermediate_newdata', force
	
	rename YEAR year
	sort year
	merge m:1 year using `finalDAH_DC'
	drop if _m==2
	drop _m

	// Mapping finalDAH to old dataset 
	bysort year: egen double old_amount = total(amount_split)
	gen double newfrct = amount_split/old_amount
	gen double final_amtsplit = newfrct * final_DAH
	bysort year: egen double new_amount = total(final_amtsplit)
	
	// Reallocate disbursements across all health focus areas using weights
	foreach var of varlist final*frct {
		// we do not want to include these umbrella terms
		if "`var'" != "final_total_frct"  { 
			local healthfocus = subinstr("`var'", "final_", "", .)
			local healthfocus = subinstr("`healthfocus'", "_frct", "", .)
			gen double `healthfocus'_DAH2 = `var' * final_amtsplit
		}
	}
			
	rename year YEAR
		
	// Generating variables:
	** For ADB
	gen ISO_CODE = "USA"
	gen DONOR_NAME = gm_name
	gen DONOR_COUNTRY = "United States"
	gen OUTFLOW = final_amtsplit
	gen INCOME_SECTOR = "PRIVATE"
	gen INCOME_TYPE = "FOUND"	
	gen SOURCE_DOC = "Foundation Center"	
	gen GHI = "US_FOUND"
	gen INCOME_ALL = .
	** For PDB
	gen DATA_SOURCE = "Foundation Center"
	gen FUNDING_TYPE = "GRANT"
	gen PROJECT_ID = grant_key
	gen PROJECT_DESCRIPTION = description
	gen PROJECT_PURPOSE = activity_override_tran
	gen FUNDING_COUNTRY = "United States"
	gen ISO3_FC = "USA" 
	gen FUNDING_AGENCY = gm_name
	gen FUNDING_AGENCY_TYPE = "FOUND"
	gen FUNDING_AGENCY_SECTOR = "CSO"
	gen RECIPIENT_COUNTRY = countryname
	gen RECIPIENT_AGENCY = agency
	gen DISBURSEMENT = final_amtsplit
	
	gen gov = .
	replace gov = 0 if RECIPIENT_AGENCY_SECTOR == "OTH"
	replace gov = 1 if RECIPIENT_AGENCY_SECTOR == "GOV" //FGH 2017: none are GOV
	replace gov = 2 if RECIPIENT_AGENCY_SECTOR == "NGO"
	
	tab gov RECIPIENT_AGENCY_SECTOR
	tab recipient if ISO3_RC == "QZA" 
	tab recipient if ISO3_RC == "" 
/* NOTE:
	These recipients will sometimes list an individual country but it is always 
	followed by "Developing Countries".
	These projects have a wider reach than a single country but are not actually 
	nallocable - will be reassigned to global.
*/
	replace ISO3_RC = "WLD" if ISO3_RC == "QZA" | ISO3_RC == ""
	ren ISO3_RC iso3 
	gen ISO3_RC = subinstr(iso3, " ", "", .)
	drop iso3 

		
** ***
// Step 5: Save ADB	
** ***	
	preserve
		keep YEAR ISO_CODE INCOME_ALL DONOR_NAME DONOR_COUNTRY OUTFLOW ///
			INCOME_SECTOR INCOME_TYPE CHANNEL SOURCE_DOC ELIM_CH GHI INKIND
		save "FILEPATH", replace
	restore	


** ***
// Step 6: Save PDB	
** ***	
	preserve
		drop if INKIND == 1
		keep YEAR FUNDING_TYPE DATA_SOURCE PROJECT_DESCRIPTION PROJECT_PURPOSE ///
			FUNDING_COUNTRY ISO3_FC FUNDING_AGENCY FUNDING_AGENCY_SECTOR ///
			RECIPIENT_COUNTRY ISO3_RC RECIPIENT_AGENCY RECIPIENT_AGENCY_SECTOR ///
			DISBURSEMENT *_DAH2 ELIM_CH
		rename *_DAH2 *_DAH
		save "FILEPATH", replace
	restore

** *** 
// Step 7: Save ADB/PDB input	
** ***	
	rename DISBURSEMENT DAH
	keep YEAR INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO3_RC ///
		CHANNEL SOURCE_DOC DAH *_DAH2 INKIND ELIM_CH gov
	collapse (sum) DAH *_DAH2, ///
		by(YEAR INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO3_RC ///
			CHANNEL SOURCE_DOC INKIND ELIM_CH gov)
	rename *_DAH2 *_DAH
	
	save "FILEPATH", replace
	
** END OF FILE **


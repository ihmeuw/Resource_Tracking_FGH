** *****************************************************************************
// Project: FGH
// Purpose: Preliminary research on prediction of DAH from budgetary numbers 
//			for UNFPA
// Key updates: Corrected file paths
//				Changed deflator year to 2024 for FGH2024
//				Changed the way merges are done.
** *****************************************************************************

** *****************************************************************************
// SETUP
** *****************************************************************************
	set more off
	clear all
	if c(os) == "Unix" {
		global j "FILEPATH"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

** *****************************************************************************
// USER INPUTS

	local noDC = 1 // only set to 1 after first stage of COMPILING is complete
	/* NOTE: 
		Starting in FGH 2018, a double-counting fix was implemented for the
		UNAIDS, UNFPA, UNICEF, GAVI, PAHO, and WHO channels that involves
		outputting the ADB_PDB and PREDS files, running them partially through
		compiling until the point those channels' _noDC ADB_PDB files are output,
		and then re-running each channel's PREDS code using those _noDC files.

		Reference:https://hub.ihme.washington.edu/display/FGH/Eliminating+Double+Counting

		The 'noDC' flag before is used to ensure the correct files are
		used. 
			* noDC=0: Double counting still exists and has NOT been accounted for
			* noDC=1: Double counting has been accounted for and _noDC ADB_PDBs
				have been created from compiling
	*/

	local report_yr = 2024	// FGH report year	
	local update_tag "20250522"
	local update_tag_noDC "20250522"
    local defl_mmyy "0424"
	
	// Directories
	local RAW 		"FILEPATH/RAW"
	local FIN 		"FILEPATH/FGH_`report_yr'"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local DEFL		"FILEPATH/FGH_`report_yr'"

	// Datasets
	local deflator_file "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta"

	// Derived marcros
	local data_yr = `report_yr' - 1				// Last year of data
	local report_YY = substr("`report_yr'",3,4)	// Report year in YY format

	// Runs
	run "FILEPATH/TT_smooth_2018.ado"

** *****************************************************************************
// STEP 1: Pull in UNFPA data
** *****************************************************************************
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
	    use "`FIN'/UNFPA_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNFPA_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}

	collapse (sum) DAH oid_ebz_DAH oid_covid_DAH, by (YEAR) fast
    replace DAH = DAH - oid_covid_DAH
    drop oid_covid_DAH
	// Note that the above collapse drops the double counting by adding in the 
	// negative DAH values from compiling
	
	// In-kind is already included in UNFPA estimates so no need to tag it 
	// separately for the PREDS datasets 
	sort YEAR

	// Get current year revenue estimates from funding commitments sheets
	preserve
		import delimited using "`RAW'/revenue_estimates_FGH_`report_yr'.csv", ///
			clear varnames(1) case(preserve)
		rename year, upper
		keep YEAR prel_rev_est
		drop if YEAR == .
		tempfile preliminary
		save `preliminary'
	restore

	// merge on
	merge 1:1 YEAR using `preliminary', nogen 
		
	// Estimate current year's DAH
	tsset YEAR
	gen double dah_frct = DAH / prel_rev_est

	// 3-year weighted average fraction for current year
	gen  wgt_avg_frct = 1/2 * dah_frct[_n-1] + 1/3 * dah_frct[_n-2] + ///
	1/6 * dah_frct[_n-3] if YEAR == `report_yr'

	gen double wgt_avg_frct_out = wgt_avg_frct * prel_rev_est
	replace DAH = wgt_avg_frct_out if YEAR == `report_yr'
	
	// FGH 2019 - use 2018 fraction - replaced with above for FGH2021
	* gen wgt_avg_frct = dah_frct[_n-1] if YEAR == `report_yr'
	* gen double wgt_avg_frct_out = wgt_avg_frct * prel_rev_est
	* replace DAH = wgt_avg_frct_out if YEAR == `report_yr'

	// Drop what I don't need
	drop prel_rev_est dah_frct wgt_avg_fr*

	// Deflate to constant USD
	merge m:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	keep if _m == 3 | _m == 1
	drop _m

	foreach var of varlist DAH oid_ebz_DAH {
		gen double `var'_`report_YY' = `var' / GDP_deflator_2024
	}
		
	tempfile outflow
	save `outflow', replace
	
** *****************************************************************************
// STEP 2: Calculate outflow by health focus area
** *****************************************************************************

	import delimited using "`RAW'/UNFPA_exp_by_HFA_FGH`report_yr'.csv", ///
		varnames(1) clear

	// Collapse (sum) by HFA and year to get the total fraction of expenditure 
	// in each HFA by year
	collapse (sum) frct_hfa, by(hfa year)
	
    drop if year == .
	reshape wide frct_hfa, i(year) j(hfa) string
	rename frct_hfa* *

	foreach var of varlist hiv* rmh* {
		replace `var' = 0 if `var' == .
	}
		
	rename year YEAR 

	// 3-year weighted average of fractions
	set obs `=_N+1'
	replace YEAR = `report_yr' in `=_N'

//manual_hfas_below
	local hfas = "hiv_other hiv_prev rmh_fp rmh_hss_hrh rmh_hss_other rmh_mh rmh_other"
	foreach var of varlist `hfas' {
		replace `var' = (`var'[_n-1] / 2) + (`var'[_n-2] / 3) + (`var'[_n-3] / 6) if ///
		YEAR == `report_yr'
	}

	// Set hiv_prev = 0
//manual_hfas_below
	replace hiv_prev = 0 if YEAR == `report_yr'

	// Set hiv_other, rmh_hrh, and rmh_hss = last year
//manual_hfas_below
	foreach var of varlist hiv_other rmh_hss_hrh {
		replace `var' = `var'[_n-1] if YEAR == `report_yr'
	}

	// Scale predicted fractions to sum to 1
	egen double total = rowtotal (hiv* rmh*)
	foreach var of varlist `hfas' {
		replace `var' = `var' / total if YEAR == `report_yr'
	}
	drop total

	// Check that hfa fractions sum to 1
	egen double total = rowtotal (hiv* rmh*)
	quietly sum total
	if `r(min)' < 0.999 {
		di "Fractions don't sum to 1!"
		hfas_dont_sum_to_1
	}
	else if `r(min)' > 1 {
		di "Fractions don't sum to 1!"
		hfas_dont_sum_to_1
	}
	else {
		drop total
	}
		
	// Get earlier years
	preserve
		keep if YEAR < 1999
		collapse (mean) `hfas'
		expand 6
		gen YEAR = _n + 1989
		tempfile early_years
		save `early_years', replace
	restore

	append using `early_years' 
			
	// Merge with outflow data and calculate expenditure by HFA
	merge 1:1 YEAR using `outflow', nogen
	sort YEAR

	// Don't get an ebola fraction from annual report so exclude first then add 
	// in after creating fractions for other HFAs
	replace oid_ebz_DAH_`report_YY' = 0 if oid_ebz_DAH_`report_YY' == . 
		// Note that if we don't do the above replace then subtracting a missing 
		// value from DAH makes DAH missing

	replace DAH_`report_YY' = DAH_`report_YY' - oid_ebz_DAH_`report_YY'

	foreach var in `hfas' {
		gen double `var'_DAH_`report_YY' = DAH_`report_YY' * `var'
	}

	replace DAH_`report_YY' = DAH_`report_YY' + oid_ebz_DAH_`report_YY'

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", replace
		save "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_includesDC_fin.dta", replace
	}
	else if `noDC' == 1 {
		//save "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed_`update_tag_noDC'.dta"
		save "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed.dta", replace
	}
	
** *****************************************************************************
// Extend and correct source data
** *****************************************************************************
	// NOTE: Do NOT use the _noDC version here!
	use "`FIN'/UNFPA_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", clear
    replace oid_covid_DAH = 0 if oid_covid_DAH == .
    replace DAH = DAH - oid_covid_DAH
    drop oid_covid_DAH
	
	// Fill in income sector and income type to destinguish sources
	replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
	replace INCOME_SECTOR = "BMGF" if CHANNEL == "BMGF"

	replace INCOME_SECTOR = SOURCE_CH if inlist(SOURCE_CH, "GFATM", "AsDB")

	replace INCOME_SECTOR = "USA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "USA"
	replace INCOME_SECTOR = "UK" if  INCOME_SECTOR == "PUBLIC" & ISO_CODE == "GBR"
	replace INCOME_SECTOR = "DEU" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "DEU"
	replace INCOME_SECTOR = "FRA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "FRA"
	replace INCOME_SECTOR = "CAN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CAN" 
	replace INCOME_SECTOR = "AUS" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "AUS" 
	replace INCOME_SECTOR = "JPN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "JPN" 
	replace INCOME_SECTOR = "NOR" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NOR" 
	replace INCOME_SECTOR = "ESP" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "ESP" 
	replace INCOME_SECTOR = "NLD" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NLD" 
	replace INCOME_SECTOR = "AUT" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "AUT"
	replace INCOME_SECTOR = "BEL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "BEL"
	replace INCOME_SECTOR = "DNK" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "DNK"
	replace INCOME_SECTOR = "FIN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "FIN"
	replace INCOME_SECTOR = "GRC" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "GRC" 
	replace INCOME_SECTOR = "IRL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "IRL" 
	replace INCOME_SECTOR = "ITA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "ITA" 
	replace INCOME_SECTOR = "KOR" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "KOR" 
	replace INCOME_SECTOR = "LUX" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "LUX" 
	replace INCOME_SECTOR = "NZL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NZL"
	replace INCOME_SECTOR = "PRT" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "PRT"
	replace INCOME_SECTOR = "SWE" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "SWE" 
	replace INCOME_SECTOR = "CHE" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CHE" 
	replace INCOME_SECTOR = "CHN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CHN" 

	replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR == "MULTI"
	//replace INCOME_SECTOR = "PRIVATE_INK" if INCOME_SECTOR == "INK"
	replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR == "OTHER_PRIV"

	collapse (sum) *_DAH, by(YEAR INCOME_SECTOR)
	reshape long @_DAH, i(YEAR INCOME_SECTOR) j(hfa) string

	// Calculate fractions of year totals by income_sector/hfa
	bysort YEAR: egen tot = total(_DAH)
	gen frct = _DAH / tot
	keep YEAR INCOME_SECTOR hfa frct 
	tempfile fractions 
	save `fractions'

	// Now use the actual DAH data 
	/* NOTE:
		We need to do this because we want to apply hfa and income_sector fractions 
		to the TOTAL yearly DAH amounts (with double counted subtracted out)
		If we tried to simply subtract out health focus area double counting there 
		would be a lot of negative values.
	*/

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
	    use "`FIN'/UNFPA_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNFPA_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}

	collapse (sum) DAH, by(YEAR)
		// Note that the above collapse drops the double counting by adding in 
		// the negative DAH values from compiling
	merge 1:m YEAR using `fractions', nogen

	// Deflate
	merge m:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	keep if _m == 3 | _m == 1
	drop _m
	gen double DAH_`report_YY' = DAH / GDP_deflator_2024
	
	// Calculate program area disbursements
	replace DAH_`report_YY' = DAH_`report_YY' * frct
	drop GDP_deflator_* DAH frct

	// Reshape 	
	reshape wide DAH_`report_YY', i(YEAR INCOME_SECTOR) j(hfa) string
	rename DAH_`report_YY'* *_DAH_`report_YY'
	reshape wide *_DAH_`report_YY', i(YEAR) j(INCOME_SECTOR) string

	keep if YEAR <= `data_yr'

	// Manual adjustments

	// Temporarily rename long variable names
	ren rmh_hss_other_* rmh_hss_o_*

	// Manually adjust for problem program areas
//manual_hfas_below
	preserve
		keep YEAR hiv_other_DAH_* hiv_prev_DAH_* rmh_hss_hrh_DAH_*
		tempfile problem_pas
		save `problem_pas'
	restore

//manual_hfas_below
	drop hiv_other_DAH_* hiv_prev_DAH_* rmh_hss_hrh_DAH_*

	// Total by HFA
//manual_hfas_below
	local hfas2 = "rmh_fp rmh_mh rmh_other rmh_hss_o oid_ebz"
	
	foreach hfa in `hfas2' {
		egen double `hfa'_tot = rowtotal(`hfa'_DAH_`report_YY'*) // 
	}

	foreach var of varlist *_DAH* *_tot {
		replace `var' = 0 if `var' == .
	}

	collapse (sum)  *_DAH* *_tot, by (YEAR)
	
	// merge in predicted DAH by HFA I
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		merge 1:1 YEAR using  "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed_includesDC_`update_tag'.dta"
	}
	else if `noDC' == 1 {
		merge 1:1 YEAR using  "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed.dta"
	}

	drop _m
	rename rmh_hss_other_* rmh_hss_o_* 
		// Some var names too long for Stata when we do TT Smooth 
	
	gen CHANNEL = "UNFPA"

	foreach var in `hfas2' {
		replace `var'_tot = `var'_DAH_`report_YY' if YEAR == `report_yr' 
	}

	drop *_DAH_`report_YY'

	tempfile check1
	save `check1'
		
	// Predict report year

/* -----------------------------------------------------------------------------
NOTE
	FGH2019, 2020 and 2021 sees the use of the new Strategy Plan 2018-2021. 
	The change in UNFPA outcomes and outputs means a change in program area 
	disbursements starting in 2018. Specifically, we now have hiv_other and
	rmh_hss_hrh program areas which we did not have prior to FGH 2018.
	Since ttsmooth needs 3 years of data and these three program areas only have
	one, we set 2019 equal to them.

	hiv_prev gets no disbursements starting in 2018 so we manually set that to 0.

	TTsmooth the rest!
----------------------------------------------------------------------------- */

	use `check1', clear

	foreach hfa in `hfas2' {
		preserve 
			keep YEAR `hfa'_tot `hfa'_DAH_`report_YY'*
			
			TT_smooth_revised `hfa'_tot `hfa'_DAH_`report_YY'*, time(YEAR) forecast(1) test(0)
			
			tempfile pr_`hfa'
			save `pr_`hfa'', replace
		restore
	}

//update_the_tempfiles_below!
	use `pr_oid_ebz', clear
	merge 1:1 YEAR using `pr_rmh_fp', nogen
	merge 1:1 YEAR using `pr_rmh_hss_o', nogen
	merge 1:1 YEAR using `pr_rmh_mh', nogen
	merge 1:1 YEAR using `pr_rmh_other', nogen
		
	// Format
//update_the_variables_below!
	foreach var of varlist ///
			oid_ebz_DAH_`report_YY'* ///
			rmh_fp_DAH_`report_YY'* ///
			rmh_hss_o_DAH_`report_YY'* ///
			rmh_mh_DAH_`report_YY'* ///
			rmh_other_DAH_`report_YY'* { 
		replace `var' = pr_`var' if  YEAR == `report_yr'
	}

	drop pr*

	// Add back US and program areas that were not ttsmoothed
	merge 1:1 YEAR using `problem_pas', nogen


//update_us_spending! -- set to 0 for 2018-2020 only
	foreach var of varlist *USA {
		replace `var' = 0 if YEAR == 2020 | YEAR == 2019 | YEAR == 2018
	}
	
//update_problem_pa_spending!
	foreach var of varlist hiv_other_DAH* rmh_hss_hrh_DAH* {
		replace `var' = `var'[_n-1] if YEAR == `report_yr'
	}

//update_problem_pa_spending!
	foreach var of varlist hiv_prev_* {
		replace `var' = 0 if YEAR == `report_yr'
	}

	// Clean up
	gen CHANNEL = "UNFPA"
	drop *_tot

	local hfas3 = "oid_ebz" + " `hfas'"
	rename rmh_hss_o* rmh_hss_other*
		// Back to original name

//manual_pa_below
	reshape long ///
			oid_ebz_DAH_`report_YY' ///
			rmh_fp_DAH_`report_YY' ///
			rmh_hss_other_DAH_`report_YY' ///
			rmh_mh_DAH_`report_YY' ///
			rmh_other_DAH_`report_YY' ///
			hiv_other_DAH_`report_YY' ///
			hiv_prev_DAH_`report_YY' ///
			rmh_hss_hrh_DAH_`report_YY', ///
		i(YEAR CHANNEL) j(INCOME_SECTOR)string

	rename *_DAH_`report_YY' pr_*_DAH_`report_YY'

	tempfile check2
	save `check2'

	// Deflate the problem program areas (those that were not TT smoothed)
	// to properlyl fit the known envelope
	use `check2', clear

	// Determine current prediction of problem program areas
	preserve
		collapse (sum) pr_*, by (YEAR)
		egen double total_pr_DAH_`report_YY' = rowtotal(pr*)
//manual_hfas_below
		keep YEAR total_pr_DAH* pr_hiv_other_DAH* pr_hiv_prev_DAH* pr_rmh_hss_hrh_DAH*
		egen double total_problem_pa_`report_YY' = rowtotal(pr*)
		tempfile problem_pa_dah
		save `problem_pa_dah'
	restore

	// Determine problem program area envelope
	preserve
		// Double-counting file check
		assert `noDC' == 0 | `noDC' == 1
		if `noDC' == 0 {
			use "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", clear
		}
		else if `noDC' == 1 {
			use "`FIN'/UNFPA_PREDS_DAH_1990_`report_yr'_ebola_fixed.dta", clear
		}
			
		keep YEAR *_`report_YY'
//manual_hfas_below
		keep YEAR hiv_other_DAH* rmh_hss_hrh_DAH* hiv_prev_DAH*
		egen double problem_pa_preds_`report_YY' = rowtotal(*_DAH_`report_YY')
		merge m:1 YEAR using `problem_pa_dah'
		drop _m
		gen defl_factor = problem_pa_preds_`report_YY' / total_problem_pa_`report_YY' if YEAR == `report_yr'
		replace defl_factor = 1 if YEAR < `report_yr'
		keep YEAR defl_factor
		tempfile problem_pa_factors
		save `problem_pa_factors'
	restore

	merge m:1 YEAR using `problem_pa_factors'
	drop _m

//manual_hfas_below
	foreach var of varlist pr_hiv_other_DAH* pr_rmh_hss_hrh_DAH* pr_hiv_prev_DAH* {
		replace `var' = `var' * defl_factor if YEAR == `report_yr'
	}

	drop defl_factor

	
	// Now we can add income sector back in for donor channels that we needed to make predictions for
	gen SOURCE_CH = INCOME_SECTOR if inlist(INCOME_SECTOR, "GFATM", "AsDB")
	replace INCOME_SECTOR = "OTHER" if inlist(SOURCE_CH, "AsDB", "GFATM")
	
	gen ISO_CODE = INCOME_SECTOR 
	replace ISO_CODE = "OTHER" if INCOME_SECTOR == "PUBLIC"
	replace ISO_CODE = "GBR" if INCOME_SECTOR == "UK"
	egen double DAH_`report_YY' = rowtotal(*_DAH_`report_YY')

	
// manually set DAH for USA 2021

	sort INCOME_SECTOR YEAR
	
	// total DAH
	replace DAH_`report_YY' = 32500000 if INCOME_SECTOR == "USA" & YEAR == 2021
	
	// program area proportions
	replace pr_rmh_fp_DAH_`report_YY' = 10223964/38489955 if INCOME_SECTOR == "USA" & YEAR == 2021
	replace pr_rmh_hss_other_DAH = 5776184.3/38489955 if INCOME_SECTOR == "USA" & YEAR == 2021
	replace pr_rmh_mh_DAH_`report_YY' = 13036548/38489955 if INCOME_SECTOR == "USA" & YEAR == 2021
	replace pr_rmh_other_DAH_`report_YY' = 8449841.7/38489955 if INCOME_SECTOR == "USA" & YEAR == 2021
	replace pr_hiv_prev_DAH_`report_YY' = 1003416.6/38489955 if INCOME_SECTOR == "USA" & YEAR == 2021
		
	// amounts for each program area
	foreach v of varlist pr_rmh_fp_DAH_`report_YY'-pr_hiv_prev_DAH_`report_YY' {
		replace `v' = `v'*DAH_`report_YY' if INCOME_SECTOR == "USA" & YEAR == 2021
		}
		

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNFPA_PREDS_DAH_BY_SOURCE_1990_`report_yr'_ebola_fixed_includesDC_`update_tag'.dta", replace
		save "`FIN'/UNFPA_PREDS_DAH_BY_SOURCE_1990_`report_yr'_includesDC_fin.dta", replace
	}
	else if `noDC' == 1 {
		save "`FIN'/UNFPA_PREDS_DAH_BY_SOURCE_1990_`report_yr'_ebola_fixed_`update_tag_noDC'.dta", replace
		save "`FIN'/UNFPA_PREDS_DAH_BY_SOURCE_1990_`report_yr'_ebola_fixed.dta", replace
	}
	
** END OF FILE **

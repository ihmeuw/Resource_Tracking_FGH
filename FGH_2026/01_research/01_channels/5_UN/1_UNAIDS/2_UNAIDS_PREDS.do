** *****************************************************************************
** Project:		FGH
** Purpose: 	Estimate UNAIDS non-UBW budget
**			 	Use ratio of UBW to non-UBW income as a proxy for the ratio of 
**				UBW to non-UBW budget
** *****************************************************************************

** *****************************************************************************
** SETUP
** *****************************************************************************
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
        global h "/homes/`c(username)'"
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

** *****************************************************************************
** USER INPUTS
	local noDC = 1 // only should be 1 after first stage of COMPILING is run
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

	local report_yr = 2024 	// FGH report year
	local update_tag_frac = "20241112"
	local update_tag = "20241112"
	local defl_tag = "0424" //check FILEPATH\FGH_[yr]\ for most recent

	// File paths
	local RAW 		"FILEPATH/RAW"
	local INT 		"FILEPATH/1_UNAIDS/DATA/INT"
	local FIN 		"FILEPATH/FGH_`report_yr'"
	local OUT 		"FILEPATH/OUTPUT"
	local CODES 	"FILEPATH/COUNTRY FEATURES"
	local DEFL	 	"FILEPATH/FGH_`report_yr'/"

	// Files
	local deflator_file = "`DEFL'/imf_usgdp_deflators_`defl_tag'.dta"	// Newest deflator file

** *****************************************************************************
** DERIVED MACROS
	local previous_yr = `report_yr' - 1				// Previous FGH report year
	local report_YY = substr("`report_yr'",3,4)		// Report year in YY format
	
** *****************************************************************************
** LOAD DATA
	//run "FILEPATH\TT_smooth_2018.ado"
	run "FILEPATH/TT_smooth.ado"

** *****************************************************************************
** STEP 1: Pull in UNAIDS data
** *****************************************************************************

	// (1) DAH with double counting removed
	// Calculate expenditure after accounting for double counting

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		use "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_includesDC.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}

	ren DAH OUTFLOW
	collapse (sum) OUTFLOW, by(YEAR)
	
	merge m:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	
	keep if _m == 3 | _m == 1
	drop _m
	gen double OUTFLOW_`report_YY' = OUTFLOW / GDP_deflator_`report_yr'
	
	// Double-counting file check
	//label var OUTFLOW_`report_YY' "UNAIDS outflow, accounts for double counting"
	label var OUTFLOW_`report_YY' "UNAIDS outflow"

	drop if YEAR >= `report_yr'	// We want to predict current year

	// Save tempfile
	tempfile outflow_dc
	save `outflow_dc', replace
		
	// (2) Budget data
	insheet using "`RAW'/UNAIDS budget_2002_`report_yr'.csv", ///
		comma names case clear
	destring UBW_budget, replace ignore(",")
	
	// Deflate to real
	merge 1:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	keep if _m == 3 | _m == 1
	drop _m
	
	gen double ubw_budget_`report_YY' = UBW_budget/ GDP_deflator_`report_yr'
		
** *****************************************************************************
** STEP 2. Compile all the data
** *****************************************************************************
	merge m:1 YEAR using `outflow_dc'
	drop _m
	replace CHANNEL = "UNAIDS" if CHANNEL == ""
	sort YEAR
	drop if YEAR < 1996

** *****************************************************************************
** STEP 3: Predict expenditure for the most recent year
** *****************************************************************************
	tsset YEAR // Declares YEAR as time series data
	
	// 3-year weighted average of DAH/Actual Core Budget
	gen double dah_frct = OUTFLOW_`report_YY'/ubw_budget_`report_YY'

	//gen double wgt_avg_frct_`report_yr' = ///
	//	1/2*(l.dah_frct) + ///
	//	1/3*(l2.dah_frct) + ///
	//	1/6*(l3.dah_frct)

	// FGH 2021 trying a 2 year weighted average - since covid caused an increase in 2020
	// !! Return to 3 year weighted average in FGH 2025
	gen double wgt_avg_frct_`report_yr' = ///
		2/3*(l.dah_frct) + ///
		1/3*(l2.dah_frct)
	gen double wgt_avg_frct_out = wgt_avg_frct_`report_yr' * ubw_budget_`report_YY'
		
	drop if YEAR > `report_yr'
	
	foreach var of varlist OUTFLOW_`report_YY' ubw_budget_`report_YY' wgt_avg_frct_out {
		replace `var' = `var'/ 1000000
	}
	
	// Save temp
	tempfile outflow
	save `outflow', replace
		
** *****************************************************************************
** STEP 4: Calculate outflow by health focus area
** *****************************************************************************
	// Merge on health focus area fractions
	preserve

		use "`INT'/hfa_fractions_`report_yr'_`update_tag_frac'.dta", clear
		keep if year <= 2000
		collapse (mean) fraction*, by(channel)
		expand 2
		gen year = 1995 + _n

		// Save tempfile
		tempfile fractions
		save `fractions' 

		use "`INT'/hfa_fractions_`report_yr'_`update_tag_frac'.dta", clear
		append using `fractions'
		drop if year == .
		rename year YEAR
		rename channel CHANNEL

		// Save tempfile
		save `fractions', replace

	restore
		
	// Generate disbursement and health focus area estimates by source of funding:
	merge m:1 YEAR CHANNEL using `fractions', assert(2 3) nogen keep(3)
	
	local hfas hiv_care hiv_hss hiv_ovc hiv_prev hiv_treat hiv_other ///
		hiv_pmtct hiv_ct hiv_amr tb_diag tb_treat tb_other tb_hss tb_amr

	foreach hfa of local hfas {
		gen `hfa'_DAH_`report_YY' = OUTFLOW_`report_YY' * fraction_`hfa'
		replace `hfa'_DAH_`report_YY' = wgt_avg_frct_out * fraction_`hfa' ///
			if YEAR > `previous_yr'
	}
			
	// Test
	egen test = rowtotal(hiv* tb*)
	gen diff = test - OUTFLOW_`report_YY'
	quietly summ diff if YEAR < `report_yr'
	if `r(mean)' > .1 {
		wearebroken
	}
	drop test diff
		
	// Rename fractions
	rename fraction_* *_frct
 
 	// Save out
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'_includesDC_`update_tag'.dta", replace
	}
	else if `noDC' == 1 {
		save "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'_`update_tag'.dta", replace
		save "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'.dta", replace
		save "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'.dta", replace
	}
	
** *****************************************************************************	
** STEP 5. PREDICTIONS for the most recent years by source
** *****************************************************************************	
	// NOTE: do NOT use the _noDC file here - it should be the one that still 
	// includes double counting!
	use "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_includesDC.dta", clear

	keep if YEAR < `report_yr'
	
	// Fill in income sector to destinguish sources
	replace DONOR_NAME = "BMGF" if DONOR_NAME == "BILL AND MELINDA GATES FOUNDATION"
	replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
	replace INCOME_SECTOR = "BMGF" if CHANNEL == "BMGF"

	replace INCOME_SECTOR=SOURCE_CH if inlist(SOURCE_CH, "GFATM", "AsDB", "AfDB", "IDB")

	replace INCOME_SECTOR = "USA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "USA"
	replace INCOME_SECTOR = "UK" if  INCOME_SECTOR == "PUBLIC" & ISO_CODE == "GBR"
	replace INCOME_SECTOR = "DEU" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "DEU"
	replace INCOME_SECTOR = "FRA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "FRA"
	replace INCOME_SECTOR = "CAN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CAN" 
	replace INCOME_SECTOR = "AUS" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "AUS" 
	replace INCOME_SECTOR = "JPN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "JPN" 
	replace INCOME_SECTOR = "NOR" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NOR" 
	replace INCOME_SECTOR = "ESP" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "ESP" 
	
	// Make donor list more comprehensive
	replace INCOME_SECTOR = "CHN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CHN"
	replace INCOME_SECTOR = "CHE" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "CHE"
	replace INCOME_SECTOR = "GRC" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "GRC"
	replace INCOME_SECTOR = "DNK" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "DNK"
	replace INCOME_SECTOR = "NZL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NZL"
	replace INCOME_SECTOR = "BEL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "BEL"
	replace INCOME_SECTOR = "SWE" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "SWE"
	replace INCOME_SECTOR = "LUX" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "LUX"
	replace INCOME_SECTOR = "KOR" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "KOR"
	replace INCOME_SECTOR = "IRL" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "IRL"
	replace INCOME_SECTOR = "ITA" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "ITA"
	replace INCOME_SECTOR = "PRT" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "PRT"
	replace INCOME_SECTOR = "NLD" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NLD"
	replace INCOME_SECTOR = "AUT" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "AUT"
	replace INCOME_SECTOR = "FIN" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "FIN"

	replace INCOME_SECTOR = "OTHER" if (INCOME_SECTOR == "MULTI" | ///
		INCOME_SECTOR == "UNSP" | INCOME_SECTOR == "DEVBANK" | ///
		INCOME_SECTOR == "NA") | INCOME_SECTOR == "UNALL"
	replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR == "OTHER_PRIV"
	replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR == "PRIV" 

	
	collapse (sum) *_DAH, by(YEAR INCOME_SECTOR)
	reshape long @_DAH, i(YEAR INCOME_SECTOR) j(hfa) string

	// Calculate fractions of year totals by income_sector/hfa
	bysort YEAR: egen tot=total(_DAH)
	gen frct=_DAH/tot
	keep YEAR INCOME_SECTOR hfa frct 

	// Save tempfile
	tempfile fractions 
	save `fractions'

	// Now use the actual DAH data 
	/* =========
	NOTES:
	We need  to do this because we want to apply hfa and income_sector 
	fractions to the TOTAL yearly DAH amounts (with double counted 
	subtracted out)

	If we tried to simply subtract out health focus area double counting ///
	there would be a lot of negative values 
	========= */

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		use "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_includesDC.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}
	
	collapse (sum) DAH, by(YEAR)  // this drops the double counting by adding in the negative DAH values from compiling
	merge 1:m YEAR using `fractions', nogen

	// Deflate
	merge m:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	keep if _merge==3 | _merge==1
	drop _m

	gen double DAH_`report_YY' = DAH / GDP_deflator_`report_yr'
	
	// Reshape 	
	replace DAH_`report_YY' = DAH_`report_YY' * frct
	drop GDP_deflator_* DAH frct
	reshape wide DAH_`report_YY', i(YEAR INCOME_SECTOR) j(hfa) string
	ren DAH_`report_YY'* *_DAH_`report_YY'
	// Rename hss variable to make it shorter in order for TT smooth to run 
	// (it breaks with variables with many characters)
	rename hiv_hss_other_DAH* hiv_hss_DAH*
	rename tb_hss_other_DAH* tb_hss_DAH*
    replace INCOME_SECTOR = "NA" if INCOME_SECTOR == ""
	reshape wide *DAH_`report_YY', i(YEAR) j(INCOME_SECTOR) string
		
	// Merge in predicted DAH by HFA I
	preserve
		// Double-counting file check
		assert `noDC' == 0 | `noDC' == 1
		if `noDC' == 0 {
			use "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'_includesDC_`update_tag'.dta", clear
		}
		else if `noDC' == 1 {
			use "`FIN'/UNAIDS_PREDS_DAH_1996_`report_yr'_`update_tag'.dta", clear
		}

		keep hiv_*_DAH_`report_YY' tb*DAH_`report_YY' YEAR

	 	// Save tempfile
	 	tempfile estimation
	 	save `estimation'
 	restore

	merge 1:1 YEAR using `estimation'
	
	foreach var of varlist *DAH_`report_YY' {
		replace `var' = `var' * 1000000
	}
		
	// Zero out missing values
	foreach var of varlist *DAH_`report_YY'* {
		replace `var' = 0 if `var' == . & YEAR < `report_yr'
	}
	
	// Predict
	local hfas hiv_care hiv_hss hiv_ovc hiv_prev hiv_treat hiv_other ///
		hiv_pmtct hiv_ct tb_treat tb_diag tb_other tb_hss 
	// hiv_amr and tb_amr are all 0 in FGH 2019 so not ttsmoothed because it 
	// will break. check in future years for these	
	// (sbachmei) Possible update: Can we replace all-zero columns with very 
	// small ~0 numbers?

	// Check that all _amr_ are Zero
	quietly sum(*amr*)
	if r(sum) != 0 {
		amr_not_all_zero
	}
	
	// (sbachmei) Potential update: look for a way to automate this list of donors?
	foreach var of local hfas {

		preserve
			keep YEAR ///
				`var'_DAH_`report_YY'AUS `var'_DAH_`report_YY'AUT ///
				`var'_DAH_`report_YY'BEL `var'_DAH_`report_YY'BMGF ///
				`var'_DAH_`report_YY'CAN `var'_DAH_`report_YY'CHE ///
				`var'_DAH_`report_YY'CHN `var'_DAH_`report_YY'DEU ///
				`var'_DAH_`report_YY'DNK `var'_DAH_`report_YY'ESP ///
				`var'_DAH_`report_YY'FIN `var'_DAH_`report_YY'FRA ///
				`var'_DAH_`report_YY'GFATM `var'_DAH_`report_YY'GRC ///
				`var'_DAH_`report_YY'IRL `var'_DAH_`report_YY'ITA ///
				`var'_DAH_`report_YY'JPN `var'_DAH_`report_YY'KOR ///
				`var'_DAH_`report_YY'LUX `var'_DAH_`report_YY'NLD ///
				`var'_DAH_`report_YY'NOR `var'_DAH_`report_YY'NZL ///
				`var'_DAH_`report_YY'OTHER `var'_DAH_`report_YY'PRIVATE ///
				`var'_DAH_`report_YY'INK `var'_DAH_`report_YY'PPP ///
				`var'_DAH_`report_YY'PRT `var'_DAH_`report_YY'PUBLIC ///
				`var'_DAH_`report_YY'SWE `var'_DAH_`report_YY'UK ///
				`var'_DAH_`report_YY'USA  ///
				`var'_DAH_`report_YY' 

			egen sum_org = rowtotal( ///
				`var'_DAH_`report_YY'AUS `var'_DAH_`report_YY'AUT ///
				`var'_DAH_`report_YY'BEL `var'_DAH_`report_YY'BMGF ///
				`var'_DAH_`report_YY'CAN `var'_DAH_`report_YY'CHE ///
				`var'_DAH_`report_YY'CHN `var'_DAH_`report_YY'DEU ///
				`var'_DAH_`report_YY'DNK `var'_DAH_`report_YY'ESP ///
				`var'_DAH_`report_YY'FIN `var'_DAH_`report_YY'FRA ///
				`var'_DAH_`report_YY'GFATM `var'_DAH_`report_YY'GRC ///
				`var'_DAH_`report_YY'IRL `var'_DAH_`report_YY'ITA ///
				`var'_DAH_`report_YY'JPN `var'_DAH_`report_YY'KOR ///
				`var'_DAH_`report_YY'LUX `var'_DAH_`report_YY'NLD ///
				`var'_DAH_`report_YY'NOR `var'_DAH_`report_YY'NZL ///
				`var'_DAH_`report_YY'OTHER `var'_DAH_`report_YY'PRIVATE ///
				`var'_DAH_`report_YY'INK `var'_DAH_`report_YY'PPP ///
				`var'_DAH_`report_YY'PRT `var'_DAH_`report_YY'PUBLIC ///
				`var'_DAH_`report_YY'SWE `var'_DAH_`report_YY'UK ///
				`var'_DAH_`report_YY'USA)
			gen diff = `var'_DAH_`report_YY' - sum_org
			di diff
			//TT_smooth_revised `var'_DAH_18 `var'_DAH_18AUS `var'_DAH_18BMGF `var'_DAH_18CAN `var'_DAH_18DEU `var'_DAH_18ESP `var'_DAH_18FIN `var'_DAH_18FRA `var'_DAH_18JPN `var'_DAH_18NLD `var'_DAH_18NOR `var'_DAH_18AUT `var'_DAH_18CHN `var'_DAH_18CHE `var'_DAH_18GRC `var'_DAH_18DNK `var'_DAH_18NZL `var'_DAH_18BEL ///
			//`var'_DAH_18SWE `var'_DAH_18LUX `var'_DAH_18KOR `var'_DAH_18IRL `var'_DAH_18ITA `var'_DAH_18PRT `var'_DAH_18OTHER `var'_DAH_18PRIVATE `var'_DAH_18PUBLIC `var'_DAH_18UK `var'_DAH_18USA `var'_DAH_18GFATM, time(YEAR) forecast(2) test(0)
			TT_smooth `var'_DAH_`report_YY' ///
				`var'_DAH_`report_YY'AUS `var'_DAH_`report_YY'AUT ///
				`var'_DAH_`report_YY'BEL `var'_DAH_`report_YY'BMGF ///
				`var'_DAH_`report_YY'CAN `var'_DAH_`report_YY'CHE ///
				`var'_DAH_`report_YY'CHN `var'_DAH_`report_YY'DEU ///
				`var'_DAH_`report_YY'DNK `var'_DAH_`report_YY'ESP ///
				`var'_DAH_`report_YY'FIN `var'_DAH_`report_YY'FRA ///
				`var'_DAH_`report_YY'GFATM `var'_DAH_`report_YY'GRC ///
				`var'_DAH_`report_YY'IRL `var'_DAH_`report_YY'ITA ///
				`var'_DAH_`report_YY'JPN `var'_DAH_`report_YY'KOR ///
				`var'_DAH_`report_YY'LUX `var'_DAH_`report_YY'NLD ///
				`var'_DAH_`report_YY'NOR `var'_DAH_`report_YY'NZL ///
				`var'_DAH_`report_YY'OTHER `var'_DAH_`report_YY'PRIVATE ///
				`var'_DAH_`report_YY'INK `var'_DAH_`report_YY'PPP ///
				`var'_DAH_`report_YY'PRT `var'_DAH_`report_YY'PUBLIC ///
				`var'_DAH_`report_YY'SWE `var'_DAH_`report_YY'UK ///
				`var'_DAH_`report_YY'USA, ///
				time(YEAR) forecast(2) test(0)
			
			// Save tempfiles
			tempfile pr_`var'
			save `pr_`var'', replace
		restore
	}
	preserve

	foreach var in `hfas' {
		if "`var'" == "hiv_care" {
			use `pr_`var'', clear
		}
		else {
			merge 1:1 YEAR using `pr_`var''
			drop _m
		}
		foreach variable of varlist ///
				`var'_DAH_`report_YY'AUS `var'_DAH_`report_YY'AUT ///
				`var'_DAH_`report_YY'BEL `var'_DAH_`report_YY'BMGF ///
				`var'_DAH_`report_YY'CAN `var'_DAH_`report_YY'CHE ///
				`var'_DAH_`report_YY'CHN `var'_DAH_`report_YY'DEU ///
				`var'_DAH_`report_YY'DNK `var'_DAH_`report_YY'ESP ///
				`var'_DAH_`report_YY'FIN `var'_DAH_`report_YY'FRA ///
				`var'_DAH_`report_YY'GFATM `var'_DAH_`report_YY'GRC ///
				`var'_DAH_`report_YY'IRL `var'_DAH_`report_YY'ITA ///
				`var'_DAH_`report_YY'JPN `var'_DAH_`report_YY'KOR ///
				`var'_DAH_`report_YY'LUX `var'_DAH_`report_YY'NLD ///
				`var'_DAH_`report_YY'NOR `var'_DAH_`report_YY'NZL ///
				`var'_DAH_`report_YY'OTHER `var'_DAH_`report_YY'PRIVATE ///
				`var'_DAH_`report_YY'INK `var'_DAH_`report_YY'PPP ///
				`var'_DAH_`report_YY'PRT `var'_DAH_`report_YY'PUBLIC ///
				`var'_DAH_`report_YY'SWE `var'_DAH_`report_YY'UK ///
				`var'_DAH_`report_YY'USA {
			replace `variable' = pr_`variable' if YEAR == `report_yr'
		}
	}
	drop pr* 
	drop *_DAH_`report_YY'
	gen CHANNEL = "UNAIDS"
	reshape long hiv_care_DAH_`report_YY' hiv_hss_DAH_`report_YY' ///
		hiv_ovc_DAH_`report_YY' hiv_prev_DAH_`report_YY' hiv_treat_DAH_`report_YY' ///
		hiv_other_DAH_`report_YY' hiv_pmtct_DAH_`report_YY' hiv_ct_DAH_`report_YY' ///
		tb_treat_DAH_`report_YY' tb_diag_DAH_`report_YY' tb_other_DAH_`report_YY' ///
		tb_hss_DAH_`report_YY', ///
	i(YEAR CHANNEL) j(INCOME_SECTOR)string
	
	replace INCOME_SECTOR = "PRIV_INK" if INCOME_SECTOR == "INK"

	//Rename hss hfa names to original
	rename hiv_hss_DAH_`report_YY' hiv_hss_other_DAH_`report_YY'
    rename tb_hss_DAH_`report_YY' tb_hss_other_DAH_`report_YY'
	rename *DAH_`report_YY' pr_*DAH_`report_YY'
		
	gen SOURCE_CH = INCOME_SECTOR if ///
		inlist(INCOME_SECTOR, "GFATM", "IDB", "AsDB", "AfDB")
	replace INCOME_SECTOR = "PRIVATE" if SOURCE_CH == "GFATM"
	replace INCOME_SECTOR = "UNALL" if inlist(SOURCE_CH, "IDB", "AsDB", "AfDB")
	gen ISO_CODE = INCOME_SECTOR 
	replace ISO_CODE = "OTHER" if INCOME_SECTOR == "PUBLIC"
	replace ISO_CODE = "GBR" if INCOME_SECTOR == "UK"
	egen double wgt_avg_frct_out = rowtotal(*_DAH_`report_YY')
	
	// Save out
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNAIDS_PREDS_BY_SOURCE_DAH_1996_`report_yr'_includesDC_`update_tag'.dta", replace
		save "`FIN'/UNAIDS_PREDS_BY_SOURCE_DAH_1996_`report_yr'_includesDC.dta", replace
	}
	else if `noDC' == 1 {
		save "`FIN'/UNAIDS_PREDS_BY_SOURCE_DAH_1996_`report_yr'_`update_tag'.dta", replace
		save "`FIN'/UNAIDS_PREDS_BY_SOURCE_DAH_1996_`report_yr'.dta", replace
	}
	
** END OF FILE**

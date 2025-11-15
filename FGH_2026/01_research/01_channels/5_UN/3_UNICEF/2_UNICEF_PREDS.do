** *****************************************************************************
// Project:		FGH
// Purpose:		Preliminary research on prediction of DAH from budgetary numbers
** *****************************************************************************

** *****************************************************************************
// SETUP
** *****************************************************************************
	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

	set scheme s1color

** *****************************************************************************
// USER INPUTS
	local noDC = 1
		// noDC=0: Double counting still exists and has NOT been accounted for
		// noDC=1: Double counting has been accounted for and _noDC ADB_PDBs
			// have been created from compiling

	/* NOTE: 
		Starting in FGH 2018, a double-counting fix was implemented for the
		UNAIDS, UNFPA, UNICEF, GAVI, PAHO, and WHO channels that involves
		outputting the ADB_PDB and PREDS files, running them partially through
		compiling until the point those channels' _noDC ADB_PDB files are output,
		and then re-running each channel's PREDS code using those _noDC files.

		Reference:https://hub.ihme.washington.edu/display/FGH/Eliminating+Double+Counting

		The 'noDC' flag before is used to ensure the correct files are used. 
	*/

	local report_yr = 2024	// FGH report year
	local dateTag: display %tdCCYYNNDD =daily("`c(current_date)'", "DMY")
	local defl_MMYY = "0424"
	
	// Filepaths
	local RAW	"FILEPATH/RAW"
	local INT 	"FILEPATH/INT"
	local FIN 	"FILEPATH/FIN/FGH_`report_yr'"
	local OUT 	"FILEPATH/OUTPUT"
	local DEFL	 "FILEPATH/DEFLATORS/FGH_`report_yr'"

	// Files
	local deflator_file = "`DEFL'/imf_usgdp_deflators_`defl_MMYY'.dta"

	// Runs
	run "FILEPATH/TT_smooth_2018.ado"

	// Derived macros
	local data_yr = `report_yr' - 1				// Last year of data
	local report_YY = substr("`report_yr'",3,4)	// Short year format
	local data_YY = substr("`data_yr'",3,4)		// Short year format

** *****************************************************************************
// STEP 1: Remove double counting
** *****************************************************************************
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}
	
	collapse (sum) DAH, by(YEAR)
	/* NOTE:
		The above collapse drops the double counting by adding in the negative 
		DAH values from compiling
	*/

** *****************************************************************************
// STEP 2: Deflate
** *****************************************************************************
	merge 1:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')
	drop if YEAR < 1990 | YEAR > `report_yr'
	drop _m
	
	gen double DAH_`report_YY' = DAH / GDP_deflator_`report_yr'
		
** *****************************************************************************
// STEP 3: Adding total budget data 
** *****************************************************************************	
	preserve

		insheet using "`RAW'/UNICEF_budget_2001-2017.csv", comma names clear
		ren (fyofreport budget totalgrossexpenditureincludingwr) (YEAR budget_yr budget)
		destring budget, replace ignore("$" ",") force
		replace budget_yr = substr(budget_yr,6,10)
		destring budget_yr, replace
		keep YEAR budget_yr budget
		drop if budget_yr > 2014
		merge m:1 YEAR using "`deflator_file'", ///
			keepusing(GDP_deflator_`report_yr')
		drop if _m == 2
		drop _m
		gen budget_`report_YY' = budget / GDP_deflator_`report_yr'
		keep budget_yr budget budget_`report_YY'
		ren budget_yr YEAR
		merge 1:1 YEAR using "`deflator_file'", ///
			keepusing(GDP_deflator_`report_yr')
		drop if _m == 2
		replace budget = budget_`report_YY' * GDP_deflator_`report_yr'
		keep YEAR budget
		tempfile budget_2001_2014
		save `budget_2001_2014'

		insheet using "`RAW'/FGH_`report_yr'/UNICEF_budget_2015-`report_yr'.csv", comma names clear
		destring totalgrossexpenditureincludingwr, gen(budget) ignore("$" ",") force
		ren year YEAR
		keep YEAR budget
		tempfile budget_2015_current
		save `budget_2015_current', replace

		append using `budget_2001_2014'
		sort YEAR
		tempfile budget
		save `budget'

	restore
	
	merge 1:1 YEAR using `budget'
	gen double budget_`report_YY' = budget / GDP_deflator_`report_yr'
	drop if YEAR > `report_yr'
	drop _m
		
** *****************************************************************************
// STEP 4: Predict expenditure for the report year 
** *****************************************************************************	
	// 3-Year Weighted Average of DAH/Budget
	tsset YEAR
	gen double dah_frct = DAH_`report_YY'/budget_`report_YY'
	gen double wgt_avg_frct = ///
		1/2*(l.dah_frct) + 1/3*(l2.dah_frct) + 1/6*(l3.dah_frct)
	gen double DAH_wgt_avg_frct_`report_YY' = wgt_avg_frct * budget_`report_YY'
	replace DAH_`report_YY' = DAH_wgt_avg_frct_`report_YY' if YEAR==`report_yr'
			
** *****************************************************************************
// STEP 5: Calculate outflow by health focus area
** *****************************************************************************

	tempfile compile
	save `compile'

	import excel using "`RAW'/FGH_`report_yr'/UNICEF_EXP_BYYEAR_1990-`data_yr'.xlsx", ///
		sheet("Stata - HFA summary") firstrow clear
	drop check
	ren year YEAR

	ds YEAR, not
	local hfas = "`r(varlist)'"

	merge 1:m YEAR using `compile'
	drop _m

	// Use data year's HFA % for report year:
	sort YEAR

	foreach var in `hfas' {
		replace `var' = `var'[_n-1] if YEAR == `report_yr'
	}
	
	/* NOTE: 
		FGH 2018: It turns out there is Ebola funding for UNICEF in 2018, but 
		TT Smooth won't predict it so we will use actual numbers pulled from 
		FTS UNOCHA 
	*/
	preserve
		use "`INT'/UNICEF_EBOLA_FGH`report_yr'.dta", clear
		collapse (sum) oid_ebz_DAH, by(YEAR)
		merge m:1 YEAR using "`deflator_file'", ///
			keepusing(GDP_deflator_`report_yr') nogen keep(1 3)
		gen oid_ebz_DAH_`report_YY' = oid_ebz_DAH/GDP_deflator_`report_yr'
		drop oid_ebz_DAH
		tempfile unicef_ebola 
		save `unicef_ebola'
	restore

	merge 1:1 YEAR using `unicef_ebola', nogen keep(1 3)
	replace oid_ebz_DAH_`report_YY' = 0 if oid_ebz_DAH_`report_YY' == .
	// Replace total estimated DAH minus ebola amounts so can calculate other HFAs
	gen temp_DAH_`report_YY' = DAH_`report_YY' - oid_ebz_DAH_`report_YY'

	foreach hfa in `hfas' {
		gen `hfa'_DAH_`report_YY' = `hfa' * temp_DAH_`report_YY'
	}

	drop temp_DAH_`report_YY'

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNICEF_PREDS_DAH_1990_`report_yr'_includesDC.dta", replace
		save "`FIN'/archive/UNICEF_PREDS_DAH_1990_`report_yr'_includesDC_`dateTag'.dta", replace
	}
	else if `noDC' == 1 {
		save "`FIN'/UNICEF_PREDS_DAH_1990_`report_yr'.dta", replace
		save "`FIN'/archive/UNICEF_PREDS_DAH_1990_`report_yr'_`dateTag'.dta", replace
	}

** *****************************************************************************
// Extend source through report year 
** *****************************************************************************
	// NOTE: don't use the _noDC version here!
	use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC.dta", clear

	replace INCOME_SECTOR = SOURCE_CH if inlist(SOURCE_CH, "AsDB")
	replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
	
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

	//replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR=="MULTI"
	replace INCOME_SECTOR = "PRIVATE_INK" if INCOME_SECTOR == "INK"
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
		We need to do this because we want to apply hfa and income_sector 
		fractions to the TOTAL yearly DAH amounts (with double counted 
		subtracted out). If we tried to simply subtract out health focus area 
		double counting there would be a lot of negative values 
	*/

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC.dta", clear
	}
	else if `noDC' == 1 {
		use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	}

	collapse (sum) DAH, by(YEAR)
	merge 1:m YEAR using `fractions', nogen
	
	// Deflate
	merge m:1 YEAR using "`deflator_file'", ///
		keepusing(GDP_deflator_`report_yr') nogen keep(1 3)
	gen double DAH_`report_YY' = DAH / GDP_deflator_`report_yr'
	
	// Reshape 	
	replace DAH_`report_YY' = DAH_`report_YY' * frct
	drop GDP_deflator_* DAH frct
	reshape wide DAH_`report_YY', i(YEAR INCOME_SECTOR) j(hfa) string
	ren DAH_`report_YY'* *_DAH_`report_YY'
	drop if YEAR == .
	reshape wide *_DAH_`report_YY', i(YEAR) j(INCOME_SECTOR) string

	keep if YEAR <= `data_yr'
			
	// Merge in predicted DAH by HFA
	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		merge 1:1 YEAR using "`FIN'/UNICEF_PREDS_DAH_1990_`report_yr'_includesDC.dta", ///
		keepusing(*_DAH_`report_YY')
	}
	else if `noDC' == 1 {
		merge 1:1 YEAR using "`FIN'/UNICEF_PREDS_DAH_1990_`report_yr'.dta", ///
		keepusing(*_DAH_`report_YY')
	}

	drop _m oid_ebz_DAH* // Ebola real numbers will be added in after TT Smooth
	ren *_DAH_`report_YY' *_preds_DAH
	
	foreach var of varlist *DAH_`report_YY'* {
		quietly replace `var' = 0 if `var' == . & YEAR != `report_yr'
	}

	// Shorten long variable names
	ren nch_hss_other* nchhssother* 
	local hfas = subinstr("`hfas'", "nch_hss_other", "nchhssother", .)
		// This doesn't include Ebola because TT Smooth won't predict Ebola correctly

	foreach hfa in `hfas' {  
		egen double `hfa'_tot = rowtotal(`hfa'_DAH_`report_YY'*)
		replace `hfa'_tot = `hfa'_preds_DAH if YEAR==`report_yr'
	}

	drop *_preds_DAH
	collapse (sum)  *_DAH* *_tot, by (YEAR)
	
	// subtract the swap_hss_other and ncd_mental amounts from their total, while processing
	// other hfas as normal.
	
	foreach var of varlist *DAH_`report_YY'* {
		// Check if the variable name contains specific patterns and adjust accordingly
		if regexm("`var'", "swap_hss_other") {
			quietly replace `var' = swap_hss_other_tot - `var' if YEAR == `report_yr'
		}
		else if regexm("`var'", "ncd_mental") {
			quietly replace `var' = ncd_mental_tot - `var' if YEAR == `report_yr'
		}
		else {
			quietly replace `var' = . if YEAR == `report_yr'
		}
	
	}

	// skip the tt_smooth for swap_hss_other and ncd_mental
	foreach hfa in `hfas' {
		if "`hfa'" != "swap_hss_other" & "`hfa'" != "ncd_mental" {
			preserve  
				keep YEAR `hfa'_tot `hfa'_DAH_`report_YY'*
				
				TT_smooth_revised `hfa'_tot `hfa'_DAH_`report_YY'*, time(YEAR) forecast(1) test(0)
				
				tempfile pr_`hfa'
				save `pr_`hfa'', replace
			restore
		}
	}

	// Creating constants for 2024 for the two hfas, and adding back the total
	foreach hfa in `hfas' {
		if "`hfa'" == "swap_hss_other" | "`hfa'" == "ncd_mental" {
			preserve
				keep if YEAR == 2023 | YEAR == 2024
				keep YEAR `hfa'_tot `hfa'_DAH_`report_YY'* 
				gen pr_`hfa'_tot  = cond(YEAR == 2023, `hfa'_tot, .)
				
				ds `hfa'_DAH_`report_YY'* 
				di "`r(varlist)'"
			
				local hfa_vars "`r(varlist)'"
				foreach var of local hfa_vars {
					gen pr_`var' = cond(YEAR == 2023, `var', .)
				}
				
				
				foreach var of local hfa_vars {
					* Generate a temporary variable for the year 2023 value and id
					// gen id = _n
					// gen t_`var' =  `var' // cond(YEAR == 2023, `var', .)

					* Carry forward the 2023 value to 2024, assuming `id` uniquely identifies rows that should match between years
					
					sort YEAR
					replace pr_`var' = `var'[_n-1] if YEAR == 2024
					// replace `var' = t_`var'[_n - 1] if YEAR == 2024

					* Drop the temporary variable and id
					// drop id
					// drop t_`var'
					// drop wantedValue
				}
				/*
				foreach var in `hfa'_DAH_`report_YY'*  {
					local value = `var'[`id' if YEAR == 2023]
					replace `var' = `value' if id == `id' & YEAR == 2024
				}
				*/
				local hfa_vars `r(varlist)'

				foreach var in `hfa_vars' {
					replace `hfa'_tot = `hfa'_tot + `var'
				}
				
				tempfile pr_`hfa'
				save `pr_`hfa'', replace
				
			restore
		}
	}


* manual_entries_below
	di "`hfas'"
	use `pr_nch_cnv', clear
	merge 1:1 YEAR using `pr_nch_other', nogen
	merge 1:1 YEAR using `pr_nchhssother', nogen
	merge 1:1 YEAR using `pr_rmh_other', nogen
	merge 1:1 YEAR using `pr_hiv_prev', nogen
	merge 1:1 YEAR using `pr_hiv_treat', nogen
	merge 1:1 YEAR using `pr_hiv_care', nogen
	merge 1:1 YEAR using `pr_hiv_pmtct', nogen
	merge 1:1 YEAR using `pr_hiv_ovc', nogen
	merge 1:1 YEAR using `pr_hiv_other', nogen
	merge 1:1 YEAR using `pr_nch_cnn', nogen
	merge 1:1 YEAR using `pr_swap_hss_other', nogen
	merge 1:1 YEAR using `pr_ncd_mental', nogen
	// Format
* manual_entries_below
	foreach var of varlist nch_cnv_DAH_`report_YY'* nch_other_DAH_`report_YY'* ///
			nchhssother_DAH_`report_YY'* rmh_other_DAH_`report_YY'* ///
			hiv_prev_DAH_`report_YY'* hiv_treat_DAH_`report_YY'* ///
			hiv_care_DAH_`report_YY'* hiv_pmtct_DAH_`report_YY'* ///
			hiv_ovc_DAH_`report_YY'* hiv_other_DAH_`report_YY'* ///
			nch_cnn_DAH_`report_YY'* ncd_mental_DAH_`report_YY'*  ///
			swap_hss_other_DAH_`report_YY'* {
		replace `var' = pr_`var' if  YEAR == `report_yr' 
	}

	gen CHANNEL = "UNICEF"		
	drop pr* 

	// Add in Ebola
	preserve
		use "`INT'/UNICEF_EBOLA_FGH`report_yr'.dta", clear
		replace ISO_COD = "UNALL" if ISO_CODE=="NA" | ISO_CODE=="EC"
		collapse (sum) oid_ebz_DAH, by(YEAR ISO_CODE)
		reshape wide oid_ebz_DAH, i(YEAR) j(ISO_CODE) string
		merge m:1 YEAR using "`deflator_file'", ///
			keepusing(GDP_deflator_`report_yr') nogen keep(1 3)
		foreach var of varlist *_DAH* {
			replace `var' = `var' / GDP_deflator_`report_yr'
		}
		ren *_DAH* *_DAH_`report_YY'*
		tempfile unicef_ebola 
		save `unicef_ebola'
	restore

	merge 1:1 YEAR using `unicef_ebola', nogen

* manual_entries_below
	reshape long nch_cnv_DAH_`report_YY' nch_other_DAH_`report_YY' ///
			nchhssother_DAH_`report_YY' rmh_other_DAH_`report_YY' ///
			hiv_prev_DAH_`report_YY' hiv_treat_DAH_`report_YY' ///
			hiv_care_DAH_`report_YY' hiv_pmtct_DAH_`report_YY' ///
			hiv_ovc_DAH_`report_YY' hiv_other_DAH_`report_YY' ///
			nch_cnn_DAH_`report_YY' oid_ebz_DAH_`report_YY' ///
			ncd_mental_DAH_`report_YY' swap_hss_other_DAH_`report_YY', ///
		i(YEAR CHANNEL) j(INCOME_SECTOR)string
	
	ren nchhssother_* nch_hss_other_*
	rename *_DAH_`report_YY' pr_*_DAH_`report_YY'

	// Add income sector back in for donor channels that we needed to make predictions for
	gen SOURCE_CH = INCOME_SECTOR if inlist(INCOME_SECTOR, "AsDB")
	replace INCOME_SECTOR = "OTHER" if SOURCE_CH == "AsDB"
		
	gen ISO_CODE = INCOME_SECTOR 
	replace ISO_CODE = "OTHER" if INCOME_SECTOR == "PUBLIC"
	replace ISO_CODE = "GBR" if INCOME_SECTOR == "UK"
	// fgh 2019: WB disbursed for ebola
	replace INCOME_SECTOR = "OTHER" if ISO_CODE=="WB"
	egen double DAH_`report_YY' = rowtotal(pr_*)
	drop *tot GDP_deflator*

	// Double-counting file check
	assert `noDC' == 0 | `noDC' == 1
	if `noDC' == 0 {
		save "`FIN'/UNICEF_PREDS_DAH_BY_SOURCE_1990_`report_yr'_includesDC.dta", replace
		save "`FIN'/archive/UNICEF_PREDS_DAH_BY_SOURCE_1990_`report_yr'_includesDC_`dateTag'.dta", replace
	}
	else if `noDC' == 1 {
		save "`FIN'/UNICEF_PREDS_DAH_BY_SOURCE_1990_`report_yr'.dta", replace
		save "`FIN'/archive/UNICEF_PREDS_DAH_BY_SOURCE_1990_`report_yr'_`dateTag'.dta", replace
	}
	
** END OF FILE **

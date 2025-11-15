// Goal: get HFA fractions for UNAIDS.

** ******************************************************
** INITIAL SETUP
** ******************************************************
// Unix/Windows filepaths
	clear all
	set more off
// Define drives for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "/home/j"
		global h "/homes/`c(username)'"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
		global h "H:"
	}
	
** ******************************************************
** User inputs
	local report_yr = 2024	// FGH report year
	local update_tag = "20241112"
	local defl_tag = "0424" //check FILEPATH\FGH_[yr]\ for most recent

	// File paths 
	local RAW 		"FILEPATH/RAW"
	local INT 		"FILEPATH/INT"
	local FIN 		"FILEPATH/FIN"
	local OUT 		"FILEPATH/OUTPUT"
	local CODES 	"FILEPATH/COUNTRY FEATURES"
	local DEFL	 	"FILEPATH/FGH_`report_yr'/"
	local KWS 		"FILEPATH/inst/"

	// Files
	local deflator_file = "`DEFL'/imf_usgdp_deflators_`defl_tag'.dta"	// Newest deflator file
	local keyword_list "`KWS'/HEALTH_FOCUS_AREAS_all.do"

** ******************************************************
** Run files
	run "`KWS'/Health_ADO_master.ado"

** ******************************************************
** Derived macros
	local report_YY = substr("`report_yr'",3,4)		// Report year (YY format)

** ******************************************************
** BUDGET INFORMATION
** ******************************************************
	// Import 'budget_by_HFA_<year>.xlsx' file (include firstrow clear) 
	// Note that this file goes as far back as 2008-09.
	import excel using "`RAW'/budget_by_HFA_`report_yr'.xlsx", firstrow clear 

** ******************************************************
** Clean
	keep YEAR CHANNEL Goal KeyOutput PrincipalOutcome TotalUSD publish_year
	rename KeyOutput OUTPUT
	rename PrincipalOutcome OUTPUT2
	rename TotalUSD BUDGET
	rename *, lower
	destring budget, replace ignore(",")
	capture drop if year == ""

** ******************************************************
** Deflate budget to report year's USD
	rename year holder
	rename publish_year YEAR
	
	// Merge deflator file	
	merge m:1 YEAR using "`deflator_file'", keepusing(GDP_deflator_`report_yr')		
	keep if _m == 3 | _m == 1
	drop _m
	
	// Deflate	
	gen budget_`report_YY' = budget / GDP_deflator_`report_yr'
	
	// Clean
	drop GDP_deflator_`report_yr' YEAR
	rename holder year 
	
** ******************************************************
** KEYWORD SEARCH
** ******************************************************

** ******************************************************
** (A) Clean text
	create_upper_vars goal output output2

	tempfile post_clean
	save `post_clean'
		
** ******************************************************
** (B) Keyword searches
/* NOTE
	This is a very customized keyword search which is why we do it locally
	here rather than running from Health_ADO_master.ado.
*/
	do "`keyword_list'"
		
	// Create local with only English hfas
	local hfa $hfa // Copy global keywords
	local hfas rmh_level rmh_fp rmh_mh nch_level nch_cnn nch_cnv hiv_level hiv_treat hiv_prev ///
		hiv_pmtct hiv_ct hiv_care hiv_ovc hiv_amr mal_level mal_treat mal_diag  mal_con_nets ///
		mal_con_irs mal_con_oth mal_comm_con mal_amr tb_level tb_treat tb_diag tb_amr oid_level ///
		oid_ebz oid_zika oid_amr ncd_level ncd_tobac ncd_mental swap_hss_level swap_hss_hrh ///
		swap_hss_pp swap_hss_me
				
	foreach healthfocus in `hfas'{
		local `healthfocus' $`healthfocus'
		// di `"``healthfocus''"'                                                                //"
	}
				
	// Create keyword count matrix
	local vars
	foreach healthfocus of local hfas {
		local i = 1
		local n : word count ``healthfocus''
		di in red `"``healthfocus''"'
		while `i' <= `n' {
			local srchstr : word `i' of ``healthfocus''
			di `"`srchstr'"'
			egen `healthfocus'_`i'_goal = noccur(upper_goal), string(`"`srchstr'"') 
			egen `healthfocus'_`i'_out = noccur(upper_output), string(`"`srchstr'"') 
			egen `healthfocus'_`i'_out2 = noccur(upper_output2), string(`"`srchstr'"') 
			local vars "`vars' `healthfocus'_`i'_*"
			local i = `i' + 1
		}
		egen `healthfocus' = rowtotal(`vars')
		drop `vars' 
		tab `healthfocus'
		local vars
	}
			
	// Test whether we are getting hits for other health focus areas but not HIV
	rename swap_hss_level hiv_hss
	rename tb_level hiv_tb
	egen total_HIV = rowtotal(hiv*)
	foreach var of varlist rmh* nch* mal* ncd* oid* {
		quietly count if `var' > 0 & total_HIV == 0
		if `r(N)' {
			display in red "For `var' we have `r(N)' hits with no HIV hits"
		}
		replace hiv_level = 1 if `var' > 0 & total_HIV == 0
	}
	drop total_HIV

	tempfile post_kws
	save `post_kws'

** ******************************************************
** (C) Post-keyword fixes
** This will be different from the general approach, as we want all DAH to go to HIV. 

/* NOTE
	This is a very customized keyword search which is why we do it locally
	here rather than running from Health_ADO_master.ado.
*/
		
	// Categorize HIV prevention-PMTCT and treatment-PMTCT as PMTCT only 
	foreach var of varlist hiv_treat hiv_prev {
		replace `var' = max(`var' - hiv_pmtct, 0) if hiv_pmtct >= 1
	}
		
	// Drop non-HIV HFAs	
	drop rmh* nch* mal* ncd* oid* swap*

	// Fixes for Counseling and Testing, Strength and Capacity, and/or Care and Support
	// Note that this makes replacements in both upper_output and upper_output2
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "CAPACIT") & regexm(upper_output, "STRENGTH")
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "SYSTEM") & regexm(upper_output, "STRENGTH")
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "STRATEG") 
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "MONITOR") // added for addiiton of swap_hss_me in FGH2021
	replace hiv_care = 1 if hiv_care == 0 & regexm(upper_output, "CARE") & regexm(upper_output, "SUPPORT")
	replace hiv_ct = 1 if hiv_ct == 0 & regexm(upper_output, "COUNSELLING") & regexm(upper_output, "TESTING")
	replace hiv_ovc = 1 if hiv_ovc == 0 & regexm(upper_output, "CHILD") & regexm(upper_output, "INFECT")
		
	// Fix outlier outputs
	// Note that this makes replacements in both upper_output and upper_output2
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "ALLOCATE RESOURCES")
	replace hiv_hss = 1 if hiv_hss == 0 & regexm(upper_output, "SYNCHRONIZED AND ALIGNED")

	// Create program area total variable
	egen pas_total = rowtotal(hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_care hiv_ovc /// 
		hiv_amr hiv_tb tb_treat tb_diag tb_amr) // PA total

	// Other fixes
	replace hiv_level=0 if hiv_hss > 0 & hiv_level > 0 & pas_total==0

	tempfile post_kws_fixes
	save `post_kws_fixes'
		
** ******************************************************
** (D) Fractions	
** This section generates allocated health focus area (hfa) and program area (pa) fractions for each output

/* NOTE
	This is a very customized keyword search which is why we do it locally
	here rather than running from Health_ADO_master.ado.
*/
	rename hiv_tb tb_level 
	rename hiv_hss swap_hss
	egen hfa = rowtotal(hiv_level tb_level swap_hss)
	local hiv_vars hiv_level 
	local tb_vars tb_level
	local swap_hss_vars swap_hss
	local level1  hiv tb swap_hss

	// Calculate fractions
	foreach level1 in `level1' {
		egen `level1'_total = rowtotal(``level1'_vars')
		foreach var in ``level1'_vars' {
			gen level1_`var'_frct = `level1'_total/hfa
			replace level1_`var'_frct = 0 if hfa == 0
		}
	}
	foreach hfa in hiv tb {
		if 	"`hfa'" == "tb" {
				drop tb_total 
				egen tb_total = rowtotal(tb_treat tb_diag tb_amr) if tb_level != 0
			foreach pa in tb_treat tb_diag tb_amr {
				gen level2_`pa'_frct = `pa'/tb_total 
				replace level2_`pa'_frct = 0 if level2_`pa'_frct==.
				gen final_`pa'_frct = level1_tb_level_frct * level2_`pa'_frct 
			}
			gen final_tb_other_frct = level1_tb_level_frct if tb_total == 0 | tb_total == .
		}
		if "`hfa'" == "hiv" {
				drop hiv_total 
				egen hiv_total = rowtotal(hiv_treat hiv_prev hiv_pmtct hiv_ovc hiv_care hiv_ct hiv_amr) if hiv_level != 0 
			foreach pa in hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr {
				gen level2_`pa'_frct = `pa'/hiv_total 
				replace level2_`pa'_frct = 0 if level2_`pa'_frct==.
				gen final_`pa'_frct = level1_hiv_level_frct * level2_`pa'_frct 
			}
			gen final_hiv_other_frct = level1_hiv_level_frct if hiv_total == 0 | hiv_total == .
		}
	}

	// Rename hiv_tb tb
	rename swap_hss hiv_hss
	gen tb_hss = 0.5*hiv_hss if tb_level>0
	replace hiv_hss = 0.5*hiv_hss if tb_level>0
	replace tb_hss =0 if tb_hss == .

	// Generate final_hss frcts	
	foreach var of varlist hiv_hss tb_hss {
		gen final_`var'_frct = `var'/hfa 
	}	
		
	// Generate PA fractions
	egen hiv_pa_hss_total = rowtotal(hiv_treat hiv_prev hiv_pmtct hiv_ovc hiv_care /// 
		hiv_ct hiv_amr hiv_hss )
	foreach pa in hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_hss {
		replace final_`pa'_frct = `pa' / hiv_pa_hss_total if hiv_pa_hss_total>0 & hiv_level==0 	
	}		

	keep year channel output2 output budget budget_`report_YY' upper_output upper_output2 final_*_frct

	tempfile fractions
	save `fractions'
	
** ******************************************************
** If hiv_hss and another PA (or multiple other PAs) is assigned, then you assign no DAH to 
** hss, but instead put the DAH in the other PA. 
	egen test_total =rowtotal(final*)
	replace final_hiv_other_frct=1 if test_total==0
	drop test_total
	egen test_total =rowtotal(final*)
	quietly sum test_total
   	di in red "check that all values equal to 1 or close to 1"
   	if `r(min)' < 0.999 | `r(max)' > 1.001 {
   		noi di in err "Sum of program areas must equal 1"
		exit 999
   	}

** ******************************************************
** (E) Get budget money
	foreach var in hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_hss hiv_care hiv_amr ///
			tb_treat tb_diag tb_other tb_hss tb_amr hiv_other {
		gen budget_`var'_`report_YY' = final_`var'_frct * budget_`report_YY'
	}	

	// Sum across year
	collapse (sum) *_`report_YY', by(year channel) fast
		
	foreach var in hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_hss hiv_care ///
			hiv_amr tb_treat tb_diag tb_other tb_hss tb_amr hiv_other {
		gen fraction_`var' = budget_`var'_`report_YY' / budget_`report_YY'
	}

** ******************************************************
** (F) Start getting things right	
	// Expand fractions from bi-annual to annual		
	replace year = substr(year, 1, 4)
	destring(year), replace
	keep year channel fraction*
	expand 2, gen(expanded)
	replace year = year + 1 if expanded == 1
	drop expanded
	sort year
		
	// Tempfile out
	tempfile years_from_2008
	save `years_from_2008', replace

	keep if year <= 2010 
	tempfile for_average
	save `for_average', replace
		
	// Get older years (prior to 2008)
	import delimited "`RAW'/UNAIDS_exp_by_HFA_thru_2016_final_updated2019.csv", clear case(preserve)
	rename *, lower
	keep year channel hfa exp_hfa_unaids exp_total frct_hfa
	// Rename hiv_unid to hiv_other (starting 2018, the hfa name was updated to hiv_other)
	replace hfa="hiv_other" if hfa=="hiv_unid"	
	keep if year == "1998_99" | year == "2000_01" | year == "2002_03" | ///
		year == "2004_05" | year == "2006_07"

	// Expand bi-annual years to annual
	replace year = substr(year, 1, 4)
	destring(year), replace
	expand 2, gen(expanded)
	replace year = year + 1 if expanded == 1
	replace exp_hfa_unaids = exp_hfa_unaids / 2
	replace exp_total = exp_total / 2
	drop expanded
		
	// Collapse by year, channel, and hfa
	collapse (sum) exp_hfa_unaids, by(year channel hfa)
	rename exp* exp
	sort year
		
	// Split up proportional pieces
	preserve
		drop if hfa == "proportion"
		bysort year: egen total_exp = total(exp)
		gen prop = exp / total_exp
		keep year channel hfa prop
		tempfile prop
		save `prop'
	restore
	merge 1:1 year channel hfa using `prop', nogen
		
	// Sanity check
	bysort year: egen testing_total = total(exp)
	// (sbachmei) What is this doing? 
	foreach year in 1998 1999 2006 2007 {
		quietly summ exp if hfa == "proportion" & year == `year'
		gen temp = `r(mean)'
		replace exp = exp + temp*prop if year == `year'
		drop if hfa == "proportion" & year == `year'
		drop temp
	}
	bysort year: egen testing_total_after = total(exp)
	gen diff = testing_total - testing_total_after
	quietly summ diff
	if `r(mean)' > 0.1 {
		alliswrong
	}
	drop testing_tota* diff prop
		
	// Fix hfa "hiv_care hiv_treat" (split care and treatment into two HFAs)
	// Calculate treat and care proportions
	preserve
		use `for_average', clear
		keep year channel fraction_hiv_treat fraction_hiv_care
		egen tot = rowtotal(fraction*)
		gen frac_hiv_treat = fraction_hiv_treat / tot
		gen frac_hiv_care = fraction_hiv_care / tot
		keep year channel frac_*
		collapse (mean) frac_*, by(channel)
		save `for_average', replace
	restore
		
	// Split
	preserve
		keep if hfa == "hiv_care hiv_treat"
		merge m:1 channel using `for_average', assert(3) nogen
		gen exp_hiv_care = exp * frac_hiv_care
		gen exp_hiv_treat = exp * frac_hiv_treat
		keep year channel exp_* 
		reshape long exp, i(year channel) j(hfa) string
		replace hfa = subinstr(hfa, "_", "", 1)
		tempfile care_treat
		save `care_treat'
	restore
	drop if hfa == "hiv_care hiv_treat"
	append using `care_treat'
	collapse (sum) exp, by(year channel hfa)
		
	// Get fractions
	bysort year: egen tot = total(exp)
	gen frac = exp / tot
	drop exp tot
	
	// Reshape
	reshape wide frac, i(year channel) j(hfa) string
	rename frac* fraction_*
	append using `years_from_2008'
		
	// Clean .s
	foreach var of varlist fraction* {
		replace `var' = 0 if `var' == . 
	}
			
** ******************************************************
** SAVE
** ******************************************************	
	// Save
    drop if year > `report_yr'
	save "`INT'/hfa_fractions_`report_yr'_`update_tag'.dta", replace

** END OF FILE **

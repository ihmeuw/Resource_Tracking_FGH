***********************************************************
// Project: 	FGH 
// Purpose: 	Preliminary Research on Prediction of DAH from Budgetary Numbers for PAHO
*************************************************************
	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
        global h "/homes/`c(username)'"
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

	local defl_mmyy = "0424" //check FILEPATH\FGH_[yr]\ for most recent
	local report_yr = 2024					// FGH report year
	local previous_yr = `report_yr' - 1		// Previous FGH report year
	local update_yr = `report_yr' - 1		// Previous FGH report year
	local data_yr = `report_yr' - 1		// Last year of data
	local deflate_yr = 24               // Last two digits o
	local prev_defl_yr = 23				// Previous dflate year

	local working_dir 	"FILEPATH"
	local RAW 			"FILEPATH/RAW"
	local INT 		"FILEPATH/INT"
	local PREVFIN 		"FILEPATH/FGH_`previous_yr'"
	local FIN 		"FILEPATH/FGH_`report_yr'"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local DEFL	 	"FILEPATH/FGH_`report_yr'/"
	local HFA 			"FILEPATH/HEALTH FOCUS AREAS" 

** ****
// Get .ado files
** ****

	run "FILEPATH/TT_smooth_2018.ado"		


** ***
// Step 1:  Creating data set of PAHO expenditure/disbursement
** ***
	
	use "`FIN'/PAHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
    // drop covid
    replace oid_covid_DAH = 0 if oid_covid_DAH == .
    replace DAH = DAH - oid_covid_DAH

	collapse (sum) DAH, by(YEAR)   // this drops the double counting by adding in the negative DAH values from compiling
	
	// Import budget data
	preserve
	insheet using "`RAW'/FGH_`report_yr'/M_PAHO_BUDGET_`report_yr'.csv", comma names case clear
	destring (REG_BUDGET), replace ignore(",")
	tempfile budget
	save `budget', replace
	restore

	merge 1:1 YEAR using `budget'
	drop _merge
** ***
// Step 2: Get data from correspondence to predict 2015 numbers
// Pull in expenditure data for the most recent year from the Financial Statement to improve predictions
** ***
	// Pull in data form correspondence to get new expenditure numbers 
		preserve
		// pull in new data 
			import delimited "`RAW'/FGH_`report_yr'/correspondence_envelopes_update.csv", clear varnames(1) case(preserve)
			egen mean_percent = mean(percent_budget)
			replace DAH = DAH * mean_percent / 100 if budget == 1
			keep year DAH
			rename year YEAR
			rename DAH new_DAH
			replace new_DAH = new_DAH * 1000000
	
		// keep year we want
			keep if YEAR == `report_yr'
			rename new_DAH DAH
			gen CHANNEL = "PAHO"
			gen ISO3_RC = "NA"
			tempfile new_year
			save `new_year'
		restore
		
		merge 1:1 YEAR using `new_year', nogen update
		
		merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr')
			keep if _m == 3 | _m == 1
			drop _m
			
		foreach var of varlist DAH REG_BUDGET {
			gen double `var'_`deflate_yr' = `var' / GDP_deflator_`report_yr'
		}
		
	//Exploring using 3 year weighted average instead of average over years
		tsset YEAR

		// 3-Year Weighted Average
		gen double dah_frct = DAH_`deflate_yr'/ REG_BUDGET_`deflate_yr'
		
		gen double wgt_avg_frct = 1/2*(l.dah_frct) + 1/3*(l2.dah_frct) + 1/6*(l3.dah_frct)
		*gen double wgt_avg_frct = l.dah_frct //in case we want to only use 1-year weights
		replace dah_frct = wgt_avg_frct if YEAR == `report_yr'
		
		gen double wgt_avg_frct_`report_yr' = 1/2*(l.dah_frct) + 1/3*(l2.dah_frct) + 1/6*(l3.dah_frct)
		*gen double wgt_avg_frct_2018 = l.dah_frct //in case we want to only use 1-year weights
		replace dah_frct = . if YEAR == `report_yr'
		
		gen double wgt_avg_frct_out = wgt_avg_frct_`report_yr' * REG_BUDGET_`deflate_yr'
		replace DAH_`deflate_yr' = wgt_avg_frct_out if YEAR == `report_yr'
		
	// get health focus areas for report year
		merge 1:1 YEAR using "`INT'/all_hfas_`update_yr'.dta", nogen
		foreach var in hiv_other mal_other ncd_mental ncd_other nch_cnn nch_cnv nch_other oid_other other rmh_other swap_hss_hrh swap_hss_other swap_hss_pp tb_other {
			gen `var'_DAH_`deflate_yr' = frac_dah_`var' * DAH_`deflate_yr'
		}
			
		drop frac* wgt* dah_frct
		drop if YEAR > `report_yr'
		
*** ***	
// Step 3: Graph to see
*** ***
	preserve
// Rescale to millions of USD dollars
	rename DAH OUTFLOW
	rename DAH_`deflate_yr' OUTFLOW_`deflate_yr'
	foreach var of varlist OUTFLOW_`deflate_yr' REG_BUDGET_`deflate_yr' {
		replace `var' = `var'/ 1000000
		}
		

// Graph 
	keep if YEAR <= `report_yr'
	twoway (connected OUTFLOW_`deflate_yr' YEAR, mcolor(black) lcolor(black)) ///
		(scatter REG_BUDGET_`deflate_yr' YEAR, mcolor(gray)), ///
		legend(size(vsmall) label(1 "Observed DAH") label(2 "Program Budget Total")) xlabel(1990(2)`report_yr') ytitle("Millions `report_yr' USD") title("PAHO Predicted DAH") graphregion(color(white))
	
	gr export "`OUT'/PAHO disbursement estimates FGH `report_yr'_noDC.pdf", replace
	restore
	
	save "`FIN'/PAHO_PREDS_DAH_1990_`report_yr'_noDC.dta", replace
	
*** ***
// Graph and compare results from this year to last year
*** ***
	use "`FIN'/PAHO_PREDS_DAH_1990_`report_yr'_noDC.dta", clear
	// set previous year
		local previous_yr = `report_yr' - 1
		
	// Set colors:
	
		local col1 = "ebblue*1.3"
		local col2 = "midgreen*0.8"
		local col3 = "black"
		
	// This year's data:
		rename DAH_`deflate_yr' DAH_`report_yr'_`deflate_yr'
		keep YEAR DAH_`report_yr'_`deflate_yr'  GDP_deflator_`report_yr'
		* rename OUTFLOW_16 OUTFLOW_`report_yr'_16
		tempfile fgh_`report_yr'
		save `fgh_`report_yr'', replace
		
	// Last year's data:
	
		use "`PREVFIN'/PAHO_PREDS_DAH_1990_`previous_yr'_noDC.dta", clear
		
		merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`previous_yr')
			keep if _m == 3 | _m == 1
			drop _m

		merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr')
			keep if _m == 3 | _m == 1
			drop _m
		
		replace DAH_`prev_defl_yr' = ((DAH_`prev_defl_yr'*GDP_deflator_`previous_yr')/GDP_deflator_`report_yr')
		
		rename DAH_`prev_defl_yr' DAH_`previous_yr'_`deflate_yr'
		
	// Merge with this year's data and deflate to 2014 USD:
		
		merge 1:1 YEAR using `fgh_`report_yr''
			drop _m
    foreach year in `previous_yr' `report_yr'{
		replace DAH_`year'_`deflate_yr' = DAH_`year'_`deflate_yr'/ 1000000
	}
	
	// Graph to compare results:
	
		twoway (line YEAR YEAR, lcolor(white) yaxis(1)) /// Dummy for Axis 1
			(line DAH_`report_yr'_`deflate_yr' YEAR if YEAR <= `report_yr', yaxis(2) lwidth(*1.5) lcolor(`col1')) 	/// report year Estimates
			(connected DAH_`report_yr'_`deflate_yr' YEAR if YEAR > `previous_yr', yaxis(2) mcolor(`col1') lcolor(`col1') lwidth(*1.5) msize(medium) msymbol(O))	/// report year PE
			(line  DAH_`previous_yr'_`deflate_yr' YEAR if YEAR <= `previous_yr', yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3'))	/// previous year estimates
			(connected  DAH_`previous_yr'_`deflate_yr' YEAR if YEAR >= `previous_yr', yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3') msize(medium) mcolor(`col3') msymbol(Oh)),	/// previous year PE
			graphregion(fcolor(white)) ytitle("Millions of `report_yr' US Dollars", size(*0.7) axis(2)) ///
			xlabel(1990(2)`report_yr', angle(45)) ///
			ylabel(, axis(2) angle(0)) ///
			ylabel(none, axis(1)) ytitle("", axis(1)) ///
			legend(row(2) size(vsmall) position(6) order(2 3 4 5) region(lcolor(white)) label(3 "`report_yr' Preliminary Estimate") label(2 "`report_yr' Estimate") label(4 "`previous_yr' FGH Estimate") label(5 "`previous_yr' FGH Preliminary Estimate")) ///
			title("PAHO", size(*0.7))
			
		graph export "`OUT'/PAHO comparison FGH`report_yr' vs FGH`previous_yr' noDC.pdf", replace

		//Figure for collaborators
		twoway (line YEAR YEAR, lcolor(white) yaxis(1)) /// Dummy for Axis 1
			(line DAH_`report_yr'_`deflate_yr' YEAR if YEAR <= `report_yr', yaxis(2) lwidth(*1.5) lcolor(`col1')) 	/// report year Estimates
			(connected DAH_`report_yr'_`deflate_yr' YEAR if YEAR > `previous_yr', yaxis(2) mcolor(`col1') lcolor(`col1') lwidth(*1.5) msize(medium) msymbol(O)),	/// report year PE
			graphregion(fcolor(white)) ytitle("Millions of `report_yr' US Dollars", size(*0.7) axis(2)) ///
			xlabel(1990(2)`report_yr', angle(45)) ///
			ylabel(, axis(2) angle(0)) ///
			ylabel(none, axis(1)) ytitle("", axis(1)) ///
			legend(row(1) size(vsmall) position(6) order(2 3) region(lcolor(white)) label(3 "`report_yr' Preliminary Estimate") label(2 "`report_yr' Estimate")) ///
			title("PAHO", size(*0.7))
		gr export "`OUT'/FGH `report_yr' - PAHO Disbursements noDC.pdf", replace
			
** ****
// Extend source data through 2017
** ****
	use "`FIN'/PAHO_ADB_PDB_FGH`report_yr'.dta" if YEAR < `report_yr', clear
	
	replace DONOR_NAME = "United States" if DONOR_NAME == "United States of America"
	
	// fill in income sector to distinguish sources
		replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
		replace INCOME_SECTOR = "BMGF" if CHANNEL == "BMGF"
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
	// make donor list more comprehensive as per 2018 list	
		
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
        replace INCOME_SECTOR = "WHO" if DONOR_NAME == "WHO"
		
		replace INCOME_SECTOR = "OTHER" if (INCOME_SECTOR == "MULTI" | INCOME_SECTOR == "UNSP" | INCOME_SECTOR == "DEVBANK" | INCOME_SECTOR == "NA")
		replace INCOME_SECTOR = "PRIVATE_INK" if INCOME_SECTOR == "INK"
		replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR == "OTHER_PRIV"
		
		collapse (sum) *_DAH, by(YEAR INCOME_SECTOR)
		reshape long @_DAH, i(YEAR INCOME_SECTOR) j(hfa) string

		// Calculate fractions of year totals by income_sector/hfa
		bysort YEAR: egen tot=total(_DAH)
		gen frct=_DAH/tot
		keep YEAR INCOME_SECTOR hfa frct 
		tempfile fractions 
		save `fractions'

	// Now use the actual DAH data 
	// We need to do this because we want to apply hfa and income_sector fractions to the TOTAL yearly DAH amounts (with double counted subtracted out)
	// If we tried to simply subtract out health focus area double counting there would be a lot of negative values 
	use "`FIN'/PAHO_ADB_PDB_FGH`report_yr'_noDC.dta",clear
        // drop covid
        replace oid_covid_DAH = 0 if oid_covid_DAH == .
        replace DAH = DAH - oid_covid_DAH

		collapse (sum) DAH, by(YEAR)  // this drops the double counting by adding in the negative DAH values from compiling
		merge 1:m YEAR using `fractions', nogen
	
	// deflate
		merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr')
		keep if _merge==3 | _merge==1
		drop _m

		gen double DAH_`deflate_yr'=DAH/GDP_deflator_`report_yr'
	
	// Reshape 	
		replace DAH_`deflate_yr' = DAH_`deflate_yr'*frct
		drop GDP_deflator_* DAH frct
		reshape wide DAH_`deflate_yr', i(YEAR INCOME_SECTOR) j(hfa) string
		ren DAH_`deflate_yr'* *_DAH_`deflate_yr'
		
	// get list for reshaping long
		local command
		local hfas
		foreach var of varlist *_DAH_`deflate_yr' {
			local command `command' `var'
			local area = subinstr("`var'", "_DAH_`deflate_yr'", "", .)
			local hfas `hfas' `area'
		}
		keep if YEAR<`report_yr'	
		reshape wide *_DAH_`deflate_yr', i(YEAR) j(INCOME_SECTOR) string
			
	// merge in predicted DAH by HFA I
		merge 1:1 YEAR using  "`FIN'/PAHO_PREDS_DAH_1990_`report_yr'_noDC.dta"
		drop _m
		
		replace CHANNEL = "PAHO" if CHANNEL==""
			
	// predict report year
		local sources "CAN FRA NLD OTHER PUBLIC UK USA WHO"
		
		ren swap_hss_other_* swaphssother_*
		foreach hfa in hiv_other mal_other ncd_mental ncd_other nch_cnn nch_cnv nch_other oid_other other rmh_other swap_hss_pp swap_hss_hrh swaphssother tb_other {
			foreach source in `sources' {
				replace `hfa'_DAH_`deflate_yr'`source' = 0 if `hfa'_DAH_`deflate_yr'`source' == . & YEAR <= `previous_yr'
				}
			}
		
		foreach hfa in hiv_other mal_other ncd_mental ncd_other nch_cnn nch_cnv nch_other oid_other other rmh_other swap_hss_pp swap_hss_hrh swaphssother tb_other {
			preserve  
			keep YEAR `hfa'_DAH_`deflate_yr' `hfa'_DAH_`deflate_yr'CAN `hfa'_DAH_`deflate_yr'FRA `hfa'_DAH_`deflate_yr'NLD `hfa'_DAH_`deflate_yr'OTHER `hfa'_DAH_`deflate_yr'PUBLIC `hfa'_DAH_`deflate_yr'UK `hfa'_DAH_`deflate_yr'USA `hfa'_DAH_`deflate_yr'WHO
			
			cap TT_smooth_revised `hfa'_DAH_`deflate_yr' `hfa'_DAH_`deflate_yr'CAN `hfa'_DAH_`deflate_yr'FRA `hfa'_DAH_`deflate_yr'NLD `hfa'_DAH_`deflate_yr'OTHER `hfa'_DAH_`deflate_yr'PUBLIC `hfa'_DAH_`deflate_yr'UK `hfa'_DAH_`deflate_yr'USA `hfa'_DAH_`deflate_yr'WHO, time(YEAR) forecast(1) test(0)
			
			if _rc {
				//drop logit* 
				rename `hfa'_DAH_`deflate_yr' total
				foreach var of varlist `hfa'* {
					gen frac = `var' / total
					gen pr_`var' = frac[_n-1] * total if YEAR == `report_yr'
					//replace pr_`var' = frac[_n-1] * total if YEAR == `report_yr'
					drop frac
					}
				}
			tempfile pr_`hfa'
			save `pr_`hfa'', replace
			restore
			}
	
	// get everything together
		use `pr_hiv_other', clear
		local temp_hfas = subinstr("hiv_other mal_other ncd_mental ncd_other nch_cnn nch_cnv nch_other oid_other other rmh_other swap_hss_pp swap_hss_hrh swaphssother tb_other", "hiv_other", "", 1) // change local so it doesn't have mnch_mh, as we will merge on. 
		
		foreach hfa in `temp_hfas' {
			merge 1:1 YEAR using `pr_`hfa''
			drop _m
		}
		
	// format
		foreach hfa in hiv_other mal_other ncd_mental ncd_other nch_cnn nch_cnv nch_other oid_other other rmh_other swap_hss_pp swap_hss_hrh swaphssother tb_other {
			foreach var of varlist `hfa'_DAH_`deflate_yr'CAN `hfa'_DAH_`deflate_yr'FRA `hfa'_DAH_`deflate_yr'NLD `hfa'_DAH_`deflate_yr'OTHER `hfa'_DAH_`deflate_yr'PUBLIC `hfa'_DAH_`deflate_yr'UK `hfa'_DAH_`deflate_yr'USA `hfa'_DAH_`deflate_yr'WHO {
				replace `var' = pr_`var' if  YEAR == `report_yr'
			}
		}

			
		drop pr* 
		drop *_DAH_`deflate_yr'
		gen CHANNEL = "PAHO"
		rename swaphssother* swap_hss_other*
		reshape long `command', i(YEAR CHANNEL) j(INCOME_SECTOR)string
		rename *_DAH_`deflate_yr' pr_*_DAH_`deflate_yr'
		recode pr_*_DAH_`deflate_yr' (.=0)
			
        gen ISO_CODE = INCOME_SECTOR
        gen DONOR_NAME = INCOME_SECTOR
		replace ISO_CODE = "OTHER" if INCOME_SECTOR == "PUBLIC" | INCOME_SECTOR == "WHO"
		replace ISO_CODE = "GBR" if INCOME_SECTOR == "UK"
        replace DONOR_NAME = "WHO" if INCOME_SECTOR == "WHO"
		egen double DAH_`deflate_yr' = rowtotal(pr_*)
		
		save "`FIN'/PAHO_PREDS_DAH_BY_SOURCE_1990_`report_yr'_noDC.dta", replace
		

    ///////////////////////////
    // end of preds creation //
    ///////////////////////////


	//running Nafis' code to complete channel checks
	//need to deflate to compare years
	
	
	use "`FIN'/PAHO_PREDS_DAH_BY_SOURCE_1990_`report_yr'_noDC.dta", clear
    
	ren *, lower
			
	keep year iso* channel *_dah*
	ren pr_* *
	ren *_dah* dah*
	
	collapse (sum) dah*, by(year iso_code channel)
	
	reshape long dah, i(year iso_code channel) j(hfa_program_area) str
	rename dah dah_new
	sort year iso hfa_program_area
	
	tempfile fgh_`deflate_yr'
	save `fgh_`deflate_yr'', replace
	
	//bringing in last year's FGH numbers to compare to this year's new numbers
	use "`PREVFIN'/PAHO_PREDS_DAH_BY_SOURCE_1990_`previous_yr'_noDC.dta", clear
		*merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_0118.dta", keepusing(GDP_deflator_2018 GDP_deflator_2017)
		merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`previous_yr')
			keep if _m == 3 | _m == 1
			drop _m

		merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr')
			keep if _m == 3 | _m == 1
			drop _m

		ren *DAH_`prev_defl_yr' *DAH
		foreach var of varlist *DAH {
			replace `var' = (`var'*GDP_deflator_`previous_yr')/GDP_deflator_`report_yr'
			}

	ren *, lower
	
	//drop redundant pas and rename to match the new (from unid to other)
	/*
	drop pr_mnch_fp_dah pr_mnch_mh_dah pr_mnch_cnu_dah pr_hiv_treat_dah pr_hiv_prev_dah pr_hiv_pmtct_dah pr_hiv_ct_dah pr_hiv_ovc_dah pr_hiv_care_dah ///
	     pr_mal_diag_dah pr_mal_con_nets_dah pr_mal_con_irs_dah pr_mal_con_oth_dah pr_mal_treat_dah pr_mal_comm_con_dah pr_tb_treat_dah pr_tb_diag_dah ///
		 pr_oid_ebz_dah pr_oid_zika_dah pr_ncd_tobac_dah pr_swap_hss_pp_dah

     rename (pr_hiv_unid_dah pr_mal_unid_dah pr_tb_unid_dah) (pr_hiv_other_dah pr_mal_other_dah pr_tb_other_dah)

     rename pr_mnch_unid_dah pr_mnch_other_dah

*/

	
	keep year iso* channel *_dah
	ren pr_* *
	ren *_dah dah_old*
	
	collapse (sum) dah*, by(year iso_code channel)
	reshape long dah_old, i(year iso channel) j(hfa_program_area) str
	sort year iso hfa_program_area
	replace hfa_program_area= "oid" if hfa_program_area=="oid_other"
	merge 1:1 year iso hfa_program_area using `fgh_`deflate_yr''
	
	** Split a hfa and program area variable:
	split hfa_program_area, parse(_)
	
	ren (hfa_program_area1 hfa_program_area2) (hfa program_area)
	
	tempfile prepped
	save `prepped', replace
	
	
	// do "H:\repos\fgh\FUNCTIONS\fgh_graphs.ado"
	
	
	
	// fgh_graphs dah_new dah_old iso_code year, restore join_stack save_graphs savepath("FILEPATH") outlier_label outlier_pc("20")
	
	// fgh_graphs dah_new dah_old hfa_program_area year, restore join_stack save_graphs savepath("FILEPATH") outlier_label outlier_pc("20")
	
	// fgh_graphs dah_new dah_old hfa year, restore join_stack save_graphs savepath("FILEPATH") outlier_label outlier_pc("20")
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*
	
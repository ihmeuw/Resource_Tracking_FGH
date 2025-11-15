*** ************************************************
// Project:		FGH
// Purpose: 		Creating Dataset of Income and Expenditure for PAHO
*** *************************************************
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

	local report_yr = 2024					// FGH report year
    // WARNING: search for TODO
	local previous_yr = `report_yr' - 1		// Previous FGH report year
	local update_yr = `report_yr' - 1		// Previous FGH report year
	local data_yr = `report_yr' - 1		// Last year of data


	local working_dir 	"FILEPATH"
	local RAW 			"`working_dir'FILEPATH/RAW"
	local INT 		"`working_dir'/FILEPATH/INT"
	local FIN 		"`working_dir'/FILEPATH/FIN"
	local OUT 		"`working_dir'/FILEPATH/FGH_`report_yr'"
	local CODES 	"`working_dir'/FILEPATH/FGH_`report_yr'"
	local DEFL	 		"`working_dir'/FILEPATH/FGH_`report_yr'"
	local HFA 			"`working_dir'/FILEPATH/HEALTH FOCUS AREAS" 
** ****
// Get .ado files
** ****
	//run "FILEPATH/TT_smooth.ado"	
	run "FILEPATH/TT_smooth_2018.ado"
** ****
// Step 1: Expenditure
** ****
	import excel using "`RAW'/FGH_`report_yr'/M_PAHO_INC_EXP_1990_`update_yr'.xlsx", sheet("Stata") firstrow clear
	capture drop if YEAR ==""

	// Export data for WHO Analysis
		preserve
	 
		gen YEAR1 = substr(YEAR, 1, 5)
		gen YEAR2 = substr(YEAR, -2,.)
		replace YEAR2 = "" if length(YEAR1) == 4 // YEAR2 == "10" | YEAR2 == "11" | YEAR2 == "12" | YEAR2 == "13"
		drop YEAR
		gen YEAR = YEAR1 + YEAR2

		cap drop if YEAR ==""
 
		save "`INT'/FGH_`report_yr'/M_PAHO_INC_EXP_BIENNUM_1990-`update_yr'_WHO.dta", replace

		restore
		
		rename YEAR YEAR_drop
		split YEAR_drop, parse(_) generate(YEAR)
		gen id = _n
		reshape long YEAR, i(id) j(k)
		drop k YEAR_drop
		drop if YEAR == "" // 2010, 2011, 2012, and 2013 are interim financial statements, not biennium.
		destring YEAR, replace 
		
		foreach var of varlist INCOME_PAHO_REG_BUDGET INCOME_WHO INCOME_CENTERS INCOME_ELIMINATION TOTAL_INCOME EXPENDITURE_PAHO_REG_BUDGET EXPENDITURE_WHO EXPENDITURE_CENTERS EXPENDITURE_ELIMINATION TOTAL_EXPENDITURE RF_EXP {
			replace `var' = `var'/2 if YEAR < 2010 
			}


		gen INCOME_ALL = TOTAL_INCOME - RF_EXP 
		// CHANGE FGH 2012: Subtracted out revolving fund expenditure. In previous years, income column included income relating to rotating fund, which is not included as DAH.
		// Although expenditure does not exactly match revolving fund income, it will more closely approximate PAHO's income as it relates to DAH					   
		// DAH here doesn't count the rotating fund expenditure as these are country monies which are pooled together only for purchasing power; the procurement of drugs is direct to the country whose monies were invested
		
		gen OUTFLOW = TOTAL_EXPENDITURE - RF_EXP

	gen GHI = "PAHO"
	
/*
	preserve
		foreach var of varlist INCOME_ALL OUTFLOW INCOME_PAHO_REG_BUDGET INCOME_WHO EXPENDITURE_PAHO_REG_BUDGET EXPENDITURE_WHO EXPENDITURE_CENTERS INCOME_CENTERS {
			replace `var'= `var'/ 1000000
			}

		label var INCOME_ALL 					"Income (millions USD)"
		label var OUTFLOW						"Outflow (millions USD)"
		label var INCOME_PAHO_REG_BUDGET 		"Income PAHO (millions USD)"
		label var INCOME_WHO					"Income WHO (millions USD)"
		label var EXPENDITURE_PAHO_REG_BUDGET 	"Exp PAHO (millions USD)"
		label var EXPENDITURE_WHO				"Exp WHO (millions USD)"
		label var EXPENDITURE_CENTERS			"Exp centers (millions USD)"
		label var INCOME_CENTERS				"Income centers (millions USD)"
		
		sort YEAR
		
		twoway (conn INCOME_ALL YEAR) (conn OUTFLOW YEAR) (conn INCOME_PAHO_REG_BUDGET YEAR) (conn INCOME_WHO YEAR) (conn EXPENDITURE_PAHO_REG_BUDGET YEAR) (conn EXPENDITURE_WHO YEAR) (conn EXPENDITURE_CENTERS YEAR) (conn INCOME_CENTERS YEAR), legend( label(1 "Income All") label(2 "Expenditure All") label(3 "Income PAHO REG Budget") label(4 "Income from WHO") label(5 "Expenditure PAHO REG Budget") label(6 "Expenditure WHO") label(7 "Centers Expenditure") label(8 "Centers Income")) ytitle("Millions USD")
	 	graph export "`OUT'/PAHO_var_comparison_`update_yr'.pdf", replace
	restore
*/
	keep YEAR INCOME_PAHO_REG_BUDGET INCOME_WHO INCOME_CENTERS TOTAL_INCOME EXPENDITURE_PAHO_REG_BUDGET EXPENDITURE_WHO EXPENDITURE_CENTERS TOTAL_EXPENDITURE RF_EXP ADJ_EXP EXPENDITURE_ELIMINATION OUTFLOW
	
	tempfile exp
	save `exp', replace
	
	// Pull in data form correspondence / End-of-biennium assesments to get new expenditure numbers 
		// pull in new data 
			import delimited "`RAW'/correspondence_envelopes_update.csv", clear varnames(1) case(preserve)
		// percent_budget isn't doesn't seem to be heading in one specific direction, so let's take average
			egen mean_percent = mean(percent_budget)
			replace DAH = DAH * mean_percent / 100 if budget == 1
			keep year DAH
			rename year YEAR
			rename DAH new_DAH
			replace new_DAH = new_DAH * 1000000
			merge 1:1 YEAR using `exp', keep(2 3)

	
	// Get fraction, use weighted average loops to go back in time
		sort YEAR
		gen double fraction = new_DAH / OUTFLOW
		
		gen weighted_fraction = fraction
		forvalues i = 2007(-1)1990 {
			replace weighted_fraction = weighted_fraction[_n + 1]*.5 + weighted_fraction[_n + 2] * 1/3 + weighted_fraction[_n + 3] * 1/6 if YEAR == `i'
			}
			
	// replace weith new OUTFLOW
		gen new_OUTFLOW = weighted_fraction * OUTFLOW
		replace new_OUTFLOW = new_DAH if new_DAH != . 
		rename OUTFLOW old_OUTFLOW
		rename new_OUTFLOW OUTFLOW
	
	// save
		keep YEAR INCOME_PAHO_REG_BUDGET INCOME_WHO INCOME_CENTERS TOTAL_INCOME EXPENDITURE_PAHO_REG_BUDGET EXPENDITURE_WHO EXPENDITURE_CENTERS TOTAL_EXPENDITURE RF_EXP ADJ_EXP EXPENDITURE_ELIMINATION OUTFLOW old_OUTFLOW
		
		// Remove food safety spending and emergency health spending that are supposed to be dropped from total envelope by the correspondence HFA file but are just excluded from the HFA fractions. 
		// Manually added total money column in HFAs_FGH2019 excel spreadsheet. Used this to create proportions to be dropped by year for those we want to exclude from total envelope. Read in and remove here
		preserve 
		
		import delimited "`RAW'/FGH_`report_yr'/HFAs_FGH`report_yr'.csv", varnames(1) clear case(preserve) 
		keep if drop_portion != . 
		collapse(sum) drop_portion, by(year)
		
		replace year = subinstr(year, "-", "_", .)
		rename year year_drop
		split year_drop, parse(_) generate(YEAR)
		drop year_drop
		destring YEAR1, replace 
		destring YEAR2, replace
		reshape long YEAR, i(drop_portion) j(k)
		drop k
		sort YEAR
		
		tempfile non_dah
		save `non_dah'
		restore

		merge 1:1 YEAR using `non_dah'
		replace OUTFLOW = OUTFLOW - (OUTFLOW * drop_portion) if drop_portion != .
		drop drop_portion _merge

		save `exp', replace
	
** ****
// Step 2:  Income
** ****
	// import delimited "`RAW'/FGH_`report_yr'/M_PAHO_INC_DONOR_1990_`update_yr'.csv", clear varnames(1)
	import delimited "`RAW'/FGH_`report_yr'/paho_inc_donor_processed.csv", clear varnames(1)

	bysort year: egen double INCOME_ALL = total(income)
	label var INCOME_ALL "Total Regular Income by Year"
	replace source_doc = "Financial Statement - Statement 1" if source_doc==""

	rename channel CHANNEL
	rename year YEAR
	rename income_sector INCOME_SECTOR
	rename income_type INCOME_TYPE
	rename source_doc SOURCE_DOC
	rename donor_name country_lc

	sort country_lc
	replace country_lc=subinstr( country_lc, "  ", " ", .)
	replace country_lc=trim(country_lc)
	
	
	merge m:1 country_lc using "`CODES'/countrycodes_official.dta", keepusing(countryname_ihme iso3 gbd_region)
	keep if _merge==1 | _merge==3
	drop _merge

	rename country_lc DONOR_NAME
	rename countryname_ihme DONOR_COUNTRY
	replace iso_code = iso3 if iso_code==""
	rename iso_code ISO_CODE
	rename income INCOME
	rename expenditure EXPENDITURE_ALL
	drop iso3 gbd_region donor_country
	replace DONOR_NAME="NA" if DONOR_NAME==""
	replace DONOR_COUNTRY="NA" if DONOR_COUNTRY==""
	sort YEAR DONOR_NAME
	rename exp_reg EXP_REG
	
	// Minor fixes:
		replace DONOR_NAME = "CENTERS" if INCOME_TYPE == "CENTERS"
		replace INCOME_SECTOR = "PUBLIC" if INCOME_TYPE == "CENTERS"
		replace INCOME_TYPE = "CENTRAL" if INCOME_TYPE == "CENTERS"
		
		replace DONOR_NAME = "WHO" if INCOME_TYPE == "WHO"
		replace INCOME_SECTOR = "MULTI" if DONOR_NAME == "WHO"
		replace INCOME_TYPE = "UN" if DONOR_NAME == "WHO"
        gen SOURCE_CH = "WHO" if DONOR_NAME == "WHO"

        replace INCOME_SECTOR = "UNALL" if DONOR_NAME == "Voluntary Contributions"
        replace INCOME_TYPE = "UNALL" if INCOME_SECTOR == "UNALL"

	split YEAR, parse(-)
	gen id = _n
	drop YEAR
	reshape long YEAR, i(id) j(k)
	drop k id
	drop if YEAR == "" // Way of dropping duplicates since 2010 on are not a biennium.
	destring YEAR, replace 
	
	foreach var of varlist INCOME EXPENDITURE_ALL INCOME_ALL EXP_REG {
		replace `var' = `var'/2 if YEAR < 2010
	}
		
	preserve
		collapse (sum) INCOME (mean)  EXPENDITURE_ALL INCOME_ALL EXP_REG, by(YEAR INCOME_TYPE)
		destring YEAR, replace force
		drop if YEAR>`previous_yr'
		twoway (conn INCOME_ALL YEAR) (conn EXPENDITURE_ALL YEAR) (conn INCOME YEAR if INCOME_TYPE=="UN") (conn INCOME YEAR if INCOME_TYPE=="CENTRAL") (conn INCOME YEAR if INCOME_TYPE=="NA")
		graph export "`OUT'/PAHO_var_comparison_using_donor_data_`report_yr'.pdf", replace
	restore
	
	merge m:1 YEAR using `exp'
	keep if _merge==3
	drop _merge


** ***
// Step 3: Computing Income Shares for only Country Donors
** ***
	gen double INC_SHARE = INCOME/INCOME_ALL // divide by envelope
	recode INC_SHARE (. = 0)
	
	// Imputing Expenditures by Income Source - we want the total OUTFLOW that we multiply shares into not to include WHO and CENTERS data, e.g. subtract out EXPENDITURE_WHO and EXPENDITURE_CENTERS from OUTFLOW
	replace EXP_REG = INC_SHARE*(OUTFLOW - EXPENDITURE_WHO - EXPENDITURE_CENTERS) if EXP_REG == .
	label var EXP_REG "Imputed Regular Budget Expenditure by Income Source"

	// INCOME_TOTAL represents sum of income to PAHO by year.  However, for the other Channels, INCOME_TOTAL represents the income to the Channel by donor and year.
	// Changing INCOME_TOTAL to equal INCOME which is income to PAHO by donor and year.

	replace INCOME_ALL = INCOME
	rename OUTFLOW total_OUTFLOW			
	gen OUTFLOW = EXP_REG
	gen GHI = "PAHO"
	keep CHANNEL SOURCE_DOC INCOME_SECTOR INCOME_TYPE DONOR_NAME SOURCE_CH ISO_CODE INCOME EXPENDITURE_ALL EXP_REG INCOME_ALL OUTFLOW DONOR_COUNTRY YEAR GHI total_OUTFLOW
	sort YEAR

	// get rid of negative values 
		preserve
		gen tag=1 if OUTFLOW < 0
		collapse (sum) OUTFLOW, by(YEAR tag)
		sort YEAR tag
		// gen fraction that DAH would be decreased by if we were to subtract out negative values 
		gen frct=(OUTFLOW + OUTFLOW[_n+1])/OUTFLOW[_n+1] if tag==1
		keep if frct!=.
		keep YEAR frct
		tempfile frct 
		save `frct'
		restore

		merge m:1 YEAR using `frct', nogen
		drop if OUTFLOW < 0
		replace OUTFLOW=OUTFLOW*frct if frct != .

	// save tempfile
	* tempfile old_method
	* save `old_method'

	drop if YEAR>`previous_yr'
	
	save "`FIN'/FGH_`report_yr'/PAHO_INC_EXP_1990_`data_yr'.dta", replace
	
	// STOP, BEFORE CONTINUING, run the part 1 of the R code fix, generated the fix version,
	// AND THEN proceed with the rest of the code.

** ***	
// Step 4: Allocating from Source to Channel to Health Focus Area (merging ADB and PDB)
** ***
	use "`FIN'/FGH_`report_yr'/PAHO_INC_EXP_1990_`data_yr'_fix.dta",clear
	rename OUTFLOW DAH
	gen ISO3_RC = "NA"
	
	// tempfile data
		tempfile the_data
		save `the_data' 
		
	// We split up PAHO DAH using focus areas from correspondence 
		// get HFAS
			import delimited "`RAW'/FGH_`report_yr'/HFAs_FGH`report_yr'.csv", varnames(1) clear case(preserve) 
			drop total drop_portion v7
			drop if HFA == "drop"
			
			// collapse on HFA
			collapse (sum) money, by(HFA year)
			
			// Divide up multiple HFA's equally
			//	gen expanded = 3 if HFA == "hiv_unid tb_unid mal_unid"
			//	replace expanded = 2 if HFA == "mal_unid oid"
			//	expand expanded
			//	replace money = money / expanded if expanded != . 
			//	bysort year HFA expanded: gen number = _n
			//	replace HFA = "hiv_unid" if number == 1 & HFA == "hiv_unid tb_unid mal_unid"
			//	replace HFA = "tb_unid" if number == 2 & HFA == "hiv_unid tb_unid mal_unid"
			//	replace HFA = "mal_unid" if number == 3 & HFA == "hiv_unid tb_unid mal_unid"
			//	replace HFA = "mal_unid" if number == 1 & HFA == "mal_unid oid"
			//	replace HFA = "oid" if number == 2 & HFA == "mal_unid oid"
			//	replace HFA = "hiv_unid" if regexm(HFA, "hiv_unid")
				
				
			    gen expanded = 3 if HFA == "hiv_other tb_other mal_other"
				replace expanded = 2 if HFA == "mal_other oid_other"
				replace expanded = 2 if HFA == "rmh_other nch_other"
				expand expanded
				replace money = money / expanded if expanded != . 
				bysort year HFA expanded: gen number = _n
				replace HFA = "hiv_other" if number == 1 & HFA == "hiv_other tb_other mal_other"
				replace HFA = "tb_other" if number == 2 & HFA == "hiv_other tb_other mal_other"
				replace HFA = "mal_other" if number == 3 & HFA == "hiv_other tb_other mal_other"
				replace HFA = "mal_other" if number == 1 & HFA == "mal_other oid_other"
				replace HFA = "oid_other" if number == 2 & HFA == "mal_other oid_other"
				replace HFA = "rmh_other" if number == 1 & HFA == "rmh_other nch_other"
				replace HFA = "nch_other" if number == 2 & HFA == "rmh_other nch_other"
				replace HFA = "hiv_other" if regexm(HFA, "hiv_other")
				
				
			// divid moneys by two and make long on year
				forvalues year = 2008/`report_yr' {
					gen dah_`year' = money / 2 if regexm(year, "`year'")
					replace dah_`year' = 0 if dah_`year' == . 
				}
				drop year expanded number
				collapse (sum) dah* , by(HFA)
				rename dah_* dah*
				reshape long dah, i(HFA) j(year)
				collapse (sum) dah, by(year HFA)
				rename HFA, lower
				bysort year: egen total_dah = total(dah)

			gen frac_dah = dah / total_dah
			// test that this is correct
				rename year, upper
				bysort YEAR: egen year_total = total(frac_dah)
				quietly count if year_total < .99 | year_total > 1.01 
				if `r(N)' {
					display in red "the health focus area fractions do not add up to one"
					breaknow
				}
			// reshape and save to use later
				keep hfa YEAR frac_dah 
				rename frac_dah frac_dah_
				reshape wide frac_dah, i(YEAR) j(hfa) string
				tempfile hfa_fractions
				save `hfa_fractions'
			// get earlier years by iteravely running TT-smooth
				// get list of fractions
					local the_fractions
					foreach var of varlist frac_dah* {
						local the_fractions `the_fractions' `var'
					}
				
				// get empty tempfile to merge on
					preserve
					clear
                    set obs 18
                    gen YEAR = 1989 + _n
					tempfile new_years
					save `new_years'
					restore
					append using `new_years'
					
				// gen new year variable going in opposite direction
					gsort - YEAR
					gen new_year = _n 
					
				// get envelope (1) and then iteraviely runn through years
					gen one = 1
					
				// loop through - make sure to check the numbers when adding a new year because it will change the range (add one to second number with each year FGH update)
                // (so in fgh 2023, changed this from 14(2)33 to 14(2)34
					forvalues num = 14(2)36 {
						preserve
						if `num' == 14 {
							drop if new_year > `num'
							}
						else {
							drop if new_year > `num'
							merge 1:1 YEAR using `new_numbers', update nogen
							}
						TT_smooth_revised one `the_fractions', time(new_year) forecast(2) test(0)
						foreach var of varlist frac* {
							replace `var' = pr_`var' if `var' == . 
							}
						keep YEAR frac* new_year one
						if `num' == 14 {
							tempfile new_numbers
							save `new_numbers'
							}
						else {
							save `new_numbers', replace
							}
						restore
						}
						
					// clean up
						use `new_numbers', clear
						drop new_year one				
				
				
					save "`INT'/all_hfas_`update_yr'.dta", replace
					//drop if YEAR == 2015
					use "`INT'/all_hfas_`update_yr'.dta", clear
		// merge on and multiply through
			merge 1:m YEAR using `the_data' // ,  assert(3) nogen
			foreach var of varlist frac* {
				local new_var = subinstr("`var'", "frac_dah_", "", .)
				gen `new_var'_DAH = DAH * `var'
				}

		//making sure all the program areas are there
		//local pa mnch_fp_DAH mnch_mh_DAH mnch_cnn_DAH mnch_cnv_DAH mnch_cnu_DAH mnch_unid_DAH ///
		//mnch_hss_DAH hiv_treat_DAH hiv_prev_DAH hiv_pmtct_DAH hiv_unid_DAH hiv_ct_DAH hiv_ovc_DAH ///
		//hiv_care_DAH hiv_hss_DAH mal_diag_DAH mal_hss_DAH mal_con_nets_DAH mal_con_irs_DAH mal_con_oth_DAH ///
		//mal_treat_DAH mal_comm_con_DAH mal_unid_DAH tb_unid_DAH tb_treat_DAH tb_diag_DAH tb_hss_DAH ///
		//oid_hss_DAH oid_ebz_DAH oid_zika_DAH oid_other_DAH ncd_hss_DAH ncd_tobac_DAH ncd_mental_DAH ncd_other_DAH ///
		//swap_hss_DAH swap_hss_pp_DAH other_DAH
		
		//making sure all the program areas are there// update pa's in line with 2018 changes that exist within this particular channel
		local pa hiv_other_DAH mal_other_DAH ncd_mental_DAH ncd_other_DAH nch_cnn_DAH nch_cnv_DAH ///
		         nch_other_DAH oid_other_DAH other_DAH rmh_other_DAH swap_hss_pp swap_hss_hrh_DAH swap_hss_other_DAH tb_other_DAH, oid_covid_DAH
		
		
			foreach p in varlist `pa' {
				cap gen `p' = 0
				}
			//replace oid_other_DAH = oid_DAH
			//replace ncd_other_DAH = ncd_unid
			//drop oid_DAH ncd_unid
		
		    	
		
		// save
			capture drop _m
			gen gov =0
			drop frac*
			drop if YEAR==`report_yr'
		//
		
		save "`FIN'/FGH_`report_yr'/PAHO_ADB_PDB_FGH`report_yr'_prep.dta", replace
		// STOP, run the rest of the R code that adds back the covid amount, then resume the stata code
		use "`FIN'/FGH_`report_yr'/PAHO_ADB_PDB_FGH`report_yr'_prepfix.dta", clear
		
	// Add in-kind 
		preserve
		import excel using "`RAW'/FGH_`report_yr'/PAHO_INKIND_RATIOS_1990_`update_yr'.xlsx", firstrow clear 
		keep YEAR INKIND_RATIO
		drop if YEAR==.|INKIND_RATIO==.
		tempfile inkind_r 
		save `inkind_r'
		restore

		preserve
		merge m:1 YEAR using `inkind_r', nogen
		foreach var of varlist DAH *_DAH {
			replace `var'=`var'*(1-INKIND_RATIO) // In-kind amount is already included because we pull total regular & other resource expenditure from financial statements
		}
		tempfile total 
		save `total'
		restore

		merge m:1 YEAR using `inkind_r', nogen 
		foreach var of varlist DAH *_DAH {
			replace `var'=`var'*INKIND_RATIO // In-kind amount is already included because we pull total regular & other resource expenditure from financial statements
		}
		gen INKIND=1
		tempfile inkind_amt 
		save `inkind_amt'

		use `total', clear
		append using `inkind_amt', force
		replace INKIND=0 if INKIND==.	
	
		
		save "`FIN'/FGH_`report_yr'/PAHO_ADB_PDB_FGH`report_yr'.dta", replace


*** ***
// Graph and compare results from this year to last year
*** ***
	use "`FIN'/FGH_`report_yr'/PAHO_ADB_PDB_FGH`report_yr'.dta", clear
	// Set colors:
	
		local col1 = "ebblue*1.3"
		local col2 = "midgreen*0.8"
		local col3 = "black"
		
	// This year's data:
		collapse(sum) DAH, by(YEAR)
		ren DAH DAH_new
		tempfile fgh_`report_yr'
		save `fgh_`report_yr'', replace
		
	// Last year's data:
	
		use "`FIN'/FGH_`previous_yr'/PAHO_ADB_PDB_FGH`previous_yr'.dta", clear
		
		collapse(sum) DAH, by(YEAR)
		ren DAH DAH_old
		
		
	// Merge with this year's data and deflate to report year USD:
		
		merge 1:1 YEAR using `fgh_`report_yr''
			drop _m
    	replace DAH_old = DAH_old / 1000000
    	replace DAH_new = DAH_new / 1000000

		

// Graph to compare results:
	
		twoway (line YEAR YEAR, lcolor(white) yaxis(1)) /// Dummy for Axis 1
			(line DAH_new YEAR if YEAR <= `report_yr', yaxis(2) lwidth(*1.5) lcolor(`col1')) 	/// report year Estimates
			(connected DAH_new YEAR if YEAR > `previous_yr', yaxis(2) mcolor(`col1') lcolor(`col1') lwidth(*1.5) msize(medium) msymbol(O))	/// report year PE
			(line  DAH_old YEAR if YEAR <= `previous_yr', yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3'))	/// previous year estimates
			(connected  DAH_old YEAR if YEAR >= `previous_yr', yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3') msize(medium) mcolor(`col3') msymbol(Oh)),	/// previous year PE
			graphregion(fcolor(white)) ytitle("Millions of nominal US Dollars", size(*0.7) axis(2)) ///
			xlabel(1990(2)`report_yr', angle(45)) ///
			ylabel(, axis(2) angle(0)) ///
			ylabel(none, axis(1)) ytitle("", axis(1)) ///
			legend(row(2) size(vsmall) position(6) order(2 3 4 5) region(lcolor(white)) label(3 "`report_yr' Preliminary Estimate") label(2 "`report_yr' Estimate") label(4 "`previous_yr' FGH Estimate") label(5 "`previous_yr' FGH Preliminary Estimate")) ///
			title("PAHO", size(*0.7))
			
		graph export "`OUT'/PAHO comparison FGH`report_yr'_DC vs FGH`previous_yr'_DC.pdf", replace

		//Fig
	/*
	// Make figure by health focus area
	// 1.) collapse 
		use "`FIN'/PAHO_ADB_PDB_FGH`report_yr'.dta", clear
		collapse (sum) *_DAH DAH, by(YEAR)
	
	// 2.) Deflate
		//merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_0718.dta", keepusing(GDP_deflator_2018)
		merge 1:1 YEAR using "FILEPATH\imf_usgdp_deflators_0718.dta", keepusing(GDP_deflator_2018) 
		keep if _m==3 | _m==1
		drop _m

		foreach var of varlist DAH *_DAH  {
			gen `var'_18= (`var'/ GDP_deflator_2018)/1000000
			}
			
	// 3.) double check that the health focus areas account for all disbursements
		egen double tot_alloc = rowtotal(*_DAH_18)
		gen double tot_unalloc = DAH_18 - tot_alloc
		sum tot_unalloc
		
	
	// 4.) Level 2 HFAs
		// get label key
		preserve
		import delimited "`HFA'/labels_2017.csv", varnames(1) clear
		replace hfa = substr(hfa, 1, 32)
		tempfile labels
		save `labels'
		restore
	
	// get command
		local command 
		local legend_order
		local legend
		local bar
		local num = 1
		local hfas hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_hss hiv_care hiv_unid ebz tb mal_con_net mal_con_oth mal_con_irs mal_treat mal_diag mal_hss mal_unid mnch_fp mnch_mh mnch_cnn mnch_cnv mnch_cnu ncd_tobac ncd_mental ncd_unid oid swap_hss mnch_hss tb_hss oid_hss ncd_hss other 
		foreach var of local hfas {
			quietly lookfor `var'_DAH_18
			if regexm("`r(varlist)'", "`var'_DAH_18") {
				local command `command' `var'_DAH_18
				local legend_order `num' `legend_order'
				preserve
				use `labels', clear
				keep if hfa == "`var'"
				quietly levelsof label, local(the_label) clean
				local legend `legend' label(`num' `the_label')
				quietly levelsof color, local(the_color) clean
				local bar `bar' bar(`num', `the_color')
				local num = `num' + 1
				restore
				}
			}
			tostring(YEAR), replace
			replace YEAR = YEAR + "*" if YEAR == "`previous_yr'" | YEAR == "`report_yr'"
		// graph
			local command = subinstr("`command'", "final_DAH_18", "", .)
			graph bar `command', stack over(YEAR, gap(0) label(labsize(vsmall) angle(90))) ///
			ytitle("Millions of 2018 US Dollars", size(*0.7) margin(0 5 0 0)) ylabel(, labsize(small) angle(0)) ///
			legend(size(tiny) symxsize(1.5) position(3) c(1) order(`legend_order') `legend' ) ///
			`bar' ///
			graphregion(fcolor(white)) 
			graph export "`OUT'/PAHO_by_HFAII`report_yr'.pdf", replace
			
		// 5.) Level 1 HFAs
		egen hiv_DAH_18 = rowtotal(hiv_*_DAH_18)
		egen mal_DAH_16 = rowtotal(mal_*_DAH_18)
		egen mnch_DAH_18 = rowtotal(mnch_*_DAH_18)
		egen ncd_DAH_18 = rowtotal(ncd_*_DAH_18)
		
		graph bar hiv_DAH_18 tb_DAH_18 mal_DAH_18 mnch_DAH_18 ncd_DAH_18 oid_DAH_18 swap_hss_DAH_18 other_DAH_18, stack over(YEAR, gap(0) label(labsize(vsmall) angle(90))) ///
					ytitle("Millions of 2015 US Dollars", size(*0.7) margin(0 5 0 0)) ylabel(, labsize(small) angle(0)) ///
					legend(size(tiny) symxsize(1.5) position(3) c(1) order(8 7 6 5 4 3 2 1) label(1 "HIV") label(2 "TB") label(3 "Malaria") label(4 "MNCH") label(5 "NCD") label(6 "Other infectious diseases") label(7 "SWAPs/HSS") label(8 "Other")) ///
					bar(1, c(red*0.8))  bar(2, c(gold*1.1)) bar(3, c(ebblue*0.5)) bar(4, c(purple*0.8)) bar(5, c(orange*0.8))  bar(6, c(midgreen*0.7)) bar(7, c(lavender)) bar(8, c(gs9)) ///
					graphregion(fcolor(white))
			graph export "`OUT'/PAHO_by_HFAI`report_yr'.pdf", replace
		restore	

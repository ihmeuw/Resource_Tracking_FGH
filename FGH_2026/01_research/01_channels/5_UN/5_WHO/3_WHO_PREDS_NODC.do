** ****************************************************
// Project: 	FGH
// Purpose: 	Predicting WHO DAH for the most recent years	
** ********************************************************
	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
        global h "~"
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

**  // FGH report year
	local report_yr = 2024
**	// Previous FGH report year
	local previous_yr = `report_yr' - 1	
**	 // deflator year 
	local deflate_yr = "24"					
**  // deflator data update mmyy
    local defl_mmyy = "0424"
	
	local RAW		"FILEPATH/RAW/FGH_`report_yr'"
	local INT		"FILEPATH/INT/FGH_`report_yr'"
	local FIN		"FILEPATHFIN/FGH_`report_yr'"
	local FIN_PREV	"FILEPATH/FIN/FGH_`previous_yr'"
	local CODES 	"FILEPATH/FGH_`report_yr'"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local DEFL 		"FILEPATH/FGH_`report_yr'"
		
** ****
**// Get .ado files
** ****
    run "FILEPATH/TT_smooth.ado"

** ****
**// Step 1: Insheet program budget
** ***
	insheet using "`RAW'/WHO_BUDGET_TOTALS_1998-`report_yr'.csv", comma names case clear
	drop if CHANNEL == ""
	
**	// a.) Split into 1-year observations 
		expand 2
		sort YEAR
		drop if YEAR=="1998-1999"
		bysort YEAR: gen odd=_n

		split YEAR, parse(-)

		replace YEAR = YEAR1
		replace YEAR = YEAR2 if odd == 2

		destring YEAR, replace force
		drop YEAR1 YEAR2 odd
		
		rename PROGRAMBUDGETTOTAL PROGRAM_BUDGET_TOTAL
		destring PROGRAM_BUDGET_TOTAL, replace ignore(",")
		replace PROGRAM_BUDGET_TOTAL = PROGRAM_BUDGET_TOTAL/2

		tempfile budget
		save `budget', replace

** ***
**// Step 2: Bring in DAH
** ***
**	//If creating estimates with double counting (prior to compilation where double counting is taken care of) - make sure to save out files as DC (Double-counting)
    use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	**we want to make prediction based on non-Ebola/covid funding, so we will subtract out
    replace oid_ebz_DAH = 0 if oid_ebz_DAH == .
    replace oid_covid_DAH = 0 if oid_covid_DAH == .
    replace DAH = DAH - oid_ebz_DAH - oid_covid_DAH

    ren DAH OUTFLOW
	collapse (sum) OUTFLOW oid_ebz_DAH oid_covid_DAH, by(YEAR)


** ***
**// Step 3: Merge with budget data and deflate
** ***
	merge 1:1 YEAR using `budget'
		drop _merge

**	// Deflate
	merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr')
		keep if _m == 3 | _m == 1
		drop _m
	
	foreach var of varlist PROGRAM_BUDGET_TOTAL OUTFLOW {
		gen double `var'_`deflate_yr' = `var'/GDP_deflator_`report_yr'
	}
	
	drop if YEAR > `report_yr'
	
** ***
**// Step 4: Predict DAH for the most recent years
** ***
	tsset YEAR

**	// 3 yr weighted average.  dah_frct is outflow/total budget, wgt_avg_frct takes 3 yr weighted average.  due to removal of ebola/covid, fraction smaller in those yrs
	gen double dah_frct = OUTFLOW_`deflate_yr'/ PROGRAM_BUDGET_TOTAL_`deflate_yr'	
	gen double wgt_avg_frct_`report_yr' = 1/2*(l.dah_frct) + 1/3*(l2.dah_frct) + 1/6*(l3.dah_frct)
	gen double wgt_avg_frct_out = wgt_avg_frct_`report_yr' * PROGRAM_BUDGET_TOTAL_`deflate_yr'

	replace OUTFLOW_`deflate_yr'=wgt_avg_frct_out if YEAR==`report_yr'
	replace dah_frct = . if YEAR == `report_yr'

		
**	// Rescale to millions of USD dollars
	foreach var of varlist OUTFLOW_`deflate_yr' oid_ebz_DAH oid_covid_DAH PROGRAM_BUDGET_TOTAL_`deflate_yr' wgt_avg_frct_out {
		replace `var' = `var'/1e6 
	}
		
	tempfile outflow
	save `outflow', replace
		
** ****
**// Step 5: Calculate outflow by health focus area
** ****
**	//WHO has HFA buget for every two years, so 2015 HFA split is same as 2014, and 2016 should be checked not used in this year 
	use "`INT'/WHO_EXP_BY_HFA_FGH`report_yr'.dta", clear

**	// Merge with outflow data and calculate expenditure by HFA:
		merge 1:1 YEAR using `outflow'
			keep if _m==3
			drop _m
			cap rename mal_nets_frct mal_con_nets_frct

        foreach healthfocus in hiv_amr hiv_care hiv_ct hiv_hss_hrh hiv_hss_me hiv_hss_other hiv_other hiv_ovc hiv_pmtct hiv_prev hiv_treat mal_amr mal_comm_con mal_con_irs mal_con_nets mal_con_oth mal_diag mal_hss_hrh mal_hss_me mal_hss_other mal_other mal_treat ncd_hss_hrh ncd_hss_me ncd_hss_other ncd_mental ncd_other ncd_tobac nch_cnn nch_cnv nch_hss_hrh nch_hss_me nch_hss_other nch_other oid_amr oid_hss_hrh oid_hss_me oid_hss_other oid_other oid_zika other rmh_fp rmh_hss_hrh rmh_hss_me rmh_hss_other rmh_mh rmh_other swap_hss_hrh swap_hss_me swap_hss_other swap_hss_pp tb_amr tb_diag tb_hss_hrh tb_hss_me tb_hss_other tb_other tb_treat unalloc {
			cap gen `healthfocus'_frct = 0 
			gen double `healthfocus'_DAH = OUTFLOW_`deflate_yr' * `healthfocus'_frct
		}
		
		order *_frct *_DAH, last
		order dah_frct wgt_avg_frct_`report_yr', after(OUTFLOW_`deflate_yr')

		egen double rmh_DAH_`deflate_yr' = rowtotal(rmh_*_DAH)
		egen double nch_DAH_`deflate_yr' = rowtotal(nch_*_DAH)
		egen double hiv_DAH_`deflate_yr' = rowtotal(hiv_*_DAH)
		egen double tb_DAH_`deflate_yr' = rowtotal(tb_*_DAH)
		egen double mal_DAH_`deflate_yr' = rowtotal(mal_*_DAH)
		egen double ncd_DAH_`deflate_yr' = rowtotal(ncd_*_DAH)
		gen oid_ebz_DAH_`deflate_yr' = oid_ebz_DAH
		gen oid_covid_DAH_`deflate_yr' = oid_covid_DAH
		egen swap_hss_DAH_`deflate_yr' = rowtotal(swap_hss_*_DAH)
		gen other_DAH_`deflate_yr' = other_DAH
		replace oid_ebz_DAH_`deflate_yr' = 0 if oid_ebz_DAH_`deflate_yr' == .
		replace oid_covid_DAH_`deflate_yr' = 0 if oid_covid_DAH_`deflate_yr' == .
		drop if YEAR >`report_yr'
		gen DAH_`deflate_yr' = OUTFLOW_`deflate_yr' + oid_ebz_DAH_`deflate_yr' + oid_covid_DAH_`deflate_yr'
**		//replace DAH_18 = wgt_avg_frct_out if DAH_`deflate_yr' == .

		replace CHANNEL = "WHO" if CHANNEL == ""
		
** ****		
**// Step 6: Graph & Compare
** ****

	twoway (connected DAH_`deflate_yr' YEAR, mcolor(black) lcolor(black)) (scatter PROGRAM_BUDGET_TOTAL_`deflate_yr' YEAR, mcolor(gray)) (line wgt_avg_frct_out YEAR, lcolor(ebblue)), legend(size(vsmall) r(2) label(1 "Disbursements, DAH") label(2 "Program Budget Total") label(3 "Predicted Values: 3-Yr. Weighted Average")) xlabel(1990(2)`report_yr') ytitle("Millions `report_yr' USD") title("WHO Predicted Aid")  graphregion(color(white))
	gr export "`OUT'/WHO disbursement estimates FGH `report_yr'_noDC.pdf", replace

	save "`FIN'/WHO_PREDS_DAH_1990_`report_yr'_ebola_fixed_noDC.dta", replace
	
*** ***
// Graph and compare results from this year to last year
*** ***
	use "`FIN'/WHO_PREDS_DAH_1990_`report_yr'_ebola_fixed_noDC.dta", clear
	
	// Set colors:
		local col1 = "ebblue*1.3"
		local col2 = "midgreen*0.8"
		local col3 = "black"
		
		tempfile fgh_`report_yr'
		save `fgh_`report_yr'', replace
		
	// Last year's data:
		use "`FIN_PREV'/WHO_PREDS_DAH_1990_`previous_yr'_ebola_fixed_noDC.dta", clear

		merge 1:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`previous_yr')
			keep if _m == 3 | _m == 1
			drop _m

		keep YEAR OUTFLOW wgt_avg_frct_out 
		merge 1:1 YEAR using `fgh_`report_yr'', nogen assert(2 3)
		gen OUTFLOW_`deflate_yr'_FGH`previous_yr' = OUTFLOW / GDP_deflator_`report_yr' / 1e6
		gen wgt_avg_frct_out_`deflate_yr'_FGH`previous_yr' = wgt_avg_frct_out / GDP_deflator_`report_yr'
		gen PE_`deflate_yr'_FGH`previous_yr' = wgt_avg_frct_out_`deflate_yr'_FGH`previous_yr'
		gen PE_`deflate_yr'_FGH`report_yr' = wgt_avg_frct_out		
		replace OUTFLOW_`deflate_yr'_FGH`previous_yr' = PE_`deflate_yr'_FGH`previous_yr' if YEAR == `previous_yr'
		replace OUTFLOW_`deflate_yr' = PE_`deflate_yr'_FGH`report_yr' if YEAR == `report_yr'
		
	// Graph to compare results:
		twoway (line YEAR YEAR, lcolor(white) yaxis(1)) /// Dummy for Axis 1
			(connected OUTFLOW_`deflate_yr' YEAR if YEAR >= `previous_yr', yaxis(2) mcolor(`col1') msize(medium) msymbol(O) lwidth(*1.5) lcolor(`col1')) 	/// report year Estimates
			(line OUTFLOW_`deflate_yr' YEAR if YEAR < `report_yr', yaxis(2) mcolor(`col1') lcolor(`col1') lwidth(*1.5) msize(medium) msymbol(O))	/// report year PE
			(line OUTFLOW_`deflate_yr'_FGH`previous_yr' YEAR, yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3'))	/// previous year estimates
			(connected OUTFLOW_`deflate_yr'_FGH`previous_yr' YEAR if YEAR >= `previous_yr', yaxis(2) lpattern(dash) lwidth(*1.5) lcolor(`col3') msize(medium) mcolor(`col3') msymbol(Oh)),	/// previous year PE
			graphregion(fcolor(white)) ytitle("Millions of `report_yr' US Dollars", size(*0.7) axis(2)) ///
			xlabel(1990(1)`report_yr', angle(90)) ///
			ylabel(, angle(0) axis(2)) ///
			ylabel(none, axis(1)) ytitle("", axis(1)) ///
			legend(row(2) size(vsmall) position(6) order(3 2 4 5) region(lcolor(white)) label(3 "`report_yr' Estimate") label(2 "`report_yr' Preliminary Estimate") label(4 "`previous_yr' Estimate") label(5 "`previous_yr' Preliminary Estimate")) ///
			title("WHO", size(*0.7))
		graph export "`OUT'/WHO comparison FGH`report_yr' vs FGH`previous_yr'_noDC.pdf", replace

	// Graph to send to collaborators:
		use "`FIN'/WHO_PREDS_DAH_BY_SOURCE_1990_`report_yr'_noDC.dta", clear
		collapse (sum) DAH_`deflate_yr', by(YEAR)
		replace DAH_`deflate_yr' = DAH_`deflate_yr'/1e9
		ren DAH_`deflate_yr' ihme_dah 
		tempfile ihme_series 
		save `ihme_series'

		use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
		drop if INKIND==1 
		drop if YEAR==`report_yr'
		collapse (sum) DAH, by(YEAR)
		merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr') keep(1 3) nogen
		gen double DAH_`deflate_yr' =DAH/ GDP_deflator_`report_yr'/1e9
	
	merge 1:1 YEAR using `ihme_series', nogen

	// Graph
		twoway (connected DAH_`deflate_yr' YEAR, lcolor(ebblue*1.3) lpattern(solid) lwidth(medium) msymbol(i)) ///
		(connected ihme_dah YEAR, lcolor(black) lpattern(solid) lwidth(medium) msymbol(i)) ///
		(connected ihme_dah YEAR if YEAR==`report_yr', lcolor(black) lpattern(solid) lwidth(medium) msymbol(Oh) mcolor(black)), ///
		graphregion(fcolor(white)) ytitle(" ") ///
		ytitle("Billions of" "`report_yr' USD", size(*0.8) orientation(horizontal) justification(right)) ///
		ylabel(, angle(0) nogrid format(%9.1fc) labsize(small)) ///
		xlabel(1990(1)2022, angle(90) labsize(small)) xtitle("") ///
		legend(size(vsmall) position(11) ring(0) col(1) region(lcolor(white)) order(3 2 1) ///
		label(1 "Excludes administrative expenses" "and includes transfers" "to other agencies") ///
		label(2 "Includes administrative expenses" "and excludes transfers" "to other agencies") ///
		label(3 "IHME Preliminary Estimate")) ///
		title("WHO development assistance for health estimates, 1990-`report_yr'", size(*0.7))
	graph export "`OUT'/WHO Disb FGH`report_yr'.pdf", replace
		
** ****
**// Extend source data through report year
** ****
	use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	
**	// fill in income sector to destinguish sources
		replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
		replace INCOME_SECTOR = "BMGF" if CHANNEL == "BMGF"
**		//keep below part?
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
		replace INCOME_SECTOR = "NLD" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "NLD" 

		replace INCOME_SECTOR = "AUT" if INCOME_SECTOR == "PUBLIC" & ISO_CODE == "AUT"
		replace INCOME_SECTOR = "BEL" if  INCOME_SECTOR == "PUBLIC" & ISO_CODE == "BEL"
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

		replace INCOME_TYPE = "UN" if DONOR_NAME == "UNAIDS" & INCOME_SECTOR == "MULTI"
        replace INCOME_TYPE = "CORP" if INCOME_SECTOR == "DENKA SEIKEN CO LTD"
		replace INCOME_SECTOR = "INK" if INCOME_TYPE == "CORP"
		replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR == "MULTI"
		replace INCOME_SECTOR = "OTHER" if INCOME_TYPE == "UN"
        replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR == "PRVIATE"
        replace INCOME_SECTOR = "UNALL" if INCOME_SECTOR == "UNSP"
        replace INCOME_SECTOR = "UNALL" if INCOME_SECTOR == "NA"
        replace INCOME_SECTOR = "UN" if INCOME_TYPE == "UN"


        drop oid_covid_DAH
        drop oid_ebz_DAH
        
		collapse (sum) *_DAH, by(YEAR INCOME_SECTOR)

        foreach var of varlist *_DAH {
            quiet count if `var' < 0
            if `r(N)' > 0 {
                di "`var' has negatives"
            }
            quiet replace `var' = 0 if `var' < 0
        }

		reshape long @_DAH, i(YEAR INCOME_SECTOR) j(hfa) string
        

**		// Calculate fractions of year totals by income_sector/hfa
		bysort YEAR: egen tot=total(_DAH)
		gen frct=_DAH/tot
		keep YEAR INCOME_SECTOR hfa frct 

		tempfile fractions 
		save `fractions'

	**// Now use the actual DAH data 
	**// We need to do this because we want to apply hfa and income_sector fractions to the TOTAL yearly DAH amounts (with double counted subtracted out)
	**// If we tried to simply subtract out health focus area double counting there would be a lot of negative values
	**//changed because there is no DC taken out yet
        use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
        replace oid_ebz_DAH = 0 if oid_ebz_DAH == .
        replace oid_covid_DAH = 0 if oid_covid_DAH == .
        replace DAH = DAH - oid_ebz_DAH - oid_covid_DAH

		**// this drops the double counting by adding in the negative DAH values from compiling
		collapse (sum) DAH, by(YEAR)  
		merge 1:m YEAR using `fractions', nogen
        drop if hfa == ""
        drop if INCOME_SECTOR == ""
	
**	// deflate
		merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr') nogen keep(1 3)
		gen double DAH_`deflate_yr' = DAH / GDP_deflator_`report_yr'
	
**	// Reshape 	
		replace DAH_`deflate_yr' = DAH_`deflate_yr'*frct
		drop GDP_deflator_* DAH frct
		reshape wide DAH_`deflate_yr', i(YEAR INCOME_SECTOR) j(hfa) string
		ren DAH_`deflate_yr'* *_DAH_`deflate_yr'
		

** // save list of INCOME_SECTORs as a local
        levelsof INCOME_SECTOR, local(sources)

**	// get list for reshaping long
		local command
		local hfas
		foreach var of varlist *_DAH_`deflate_yr' {
			local command `command' `var'
			local area = subinstr("`var'", "_DAH_`deflate_yr'", "", .)
			local hfas `hfas' `area'
		}

		reshape wide *_DAH_`deflate_yr', i(YEAR) j(INCOME_SECTOR) string
		keep if YEAR<=`report_yr'
		
**	// merge in predicted DAH by HFA I
		merge 1:1 YEAR using  "`FIN'/WHO_PREDS_DAH_1990_`report_yr'_ebola_fixed_noDC.dta"
**		// Some HFA names too long 
		drop _m

		drop *_DAH_`deflate_yr'
		foreach var of varlist *DAH* {
			replace `var' = 0 if `var' ==.
		}

		drop DAH_`deflate_yr' PROGRAM_BUDGET_TOTAL_`deflate_yr'
		foreach hfa in `hfas' {
			egen `hfa'_DAH_`deflate_yr' = rowtotal(`hfa'_DAH_`deflate_yr'*)
			replace `hfa'_DAH_`deflate_yr' = `hfa'_DAH * 1e6 if YEAR == `report_yr'
		}

**	// Here we get a local with the hfa's present in WHO data. 
		 local real_hfas
		 foreach hfa of local hfas {
		 	quietly summ `hfa'_DAH
		 	if `r(mean)' != 0 {
		 		local real_hfas `real_hfas' `hfa'
		 	}
		 }


		
**	// predict report year
			
		foreach hfa in `real_hfas' {
			foreach source in `sources' {
				replace `hfa'_DAH_`deflate_yr'`source' = 0 if `hfa'_DAH_`deflate_yr'`source' == . & YEAR <= `previous_yr'
			}
		}

**		// Some HFA names too long 
        ren swap_hss_other_* swaphssother_*
        ren mal_con_oth_*  malconoth_*
        ren hiv_hss_other_* hivhssoth_* 
        ren mal_hss_other_* malhssoth_* 
        ren ncd_hss_other_* ncdhssoth_*
        ren nch_hss_other_* nchhssoth_*
        ren oid_hss_other_* oidhssoth_*
        ren rmh_hss_other_* rmhhssoth_*
        local real_hfas = subinstr("`real_hfas'", "swap_hss_other", "swaphssother", 1)
        local real_hfas = subinstr("`real_hfas'", "mal_con_oth", "malconoth", 1)
        local real_hfas = subinstr("`real_hfas'", "hiv_hss_other", "hivhssoth", 1)
        local real_hfas = subinstr("`real_hfas'", "mal_hss_other", "malhssoth", 1)
        local real_hfas = subinstr("`real_hfas'", "ncd_hss_other", "ncdhssoth", 1)
        local real_hfas = subinstr("`real_hfas'", "nch_hss_other", "nchhssoth", 1)
        local real_hfas = subinstr("`real_hfas'", "oid_hss_other", "oidhssoth", 1)
        local real_hfas = subinstr("`real_hfas'", "rmh_hss_other", "rmhhssoth", 1)
        di "`real_hfas'"
        local command = subinstr("`command'", "swap_hss_other", "swaphssother", 1)
        local command = subinstr("`command'", "mal_con_oth", "malconoth", 1)
        local command = subinstr("`command'", "hiv_hss_other", "hivhssoth", 1)
        local command = subinstr("`command'", "mal_hss_other", "malhssoth", 1)
        local command = subinstr("`command'", "ncd_hss_other", "ncdhssoth", 1)
        local command = subinstr("`command'", "nch_hss_other", "nchhssoth", 1)
        local command = subinstr("`command'", "oid_hss_other", "oidhssoth", 1)
        local command = subinstr("`command'", "rmh_hss_other", "rmhhssoth", 1)
        di "`command'"

        /*
            NOTE: if this errorrs, go back up to comment "check income sector"
            and verify that the income sector levels match the "sources" local
        */

		foreach hfa in `real_hfas' {
			di as red "hfa `hfa'"
			preserve  
            // filter down to necessary columns
			keep YEAR `hfa'_DAH_`deflate_yr'*
            gen hfa_target = `hfa'_DAH_`deflate_yr'
            drop `hfa'_DAH_`deflate_yr'
            egen hfa_total = rowtotal(`hfa'_DAH_`deflate_yr'*)
            replace hfa_target = hfa_total if YEAR < `report_yr'

            drop hfa_total
                // gen tst = `hfa'_target - `hfa'_DAH_`deflate_yr' // want 0 for prediction years
            // create list of all hfa sub-component columns 
            unab vars: `hfa'_DAH_`deflate_yr'*
            // replace any report year values with missing values
            foreach var of local vars {
			    replace `var' = . if YEAR ==`report_yr'
			}
            // TT smooth
            TT_smooth hfa_target `vars', time(YEAR) forecast(1) test(0)
            // save results
            drop hfa_target
			tempfile pr_`hfa'
			save `pr_`hfa'', replace
			restore
		}

		// use `pr_nch_mh', clear
**		// change local so it doesn't have mnch_mh, as we will merge on. 
		// local temp_hfas = subinstr("`real_hfas'", "rmh_mh", "", 1) 
        local first_hfa = word("`real_hfas'", 1)
        use `pr_`first_hfa'', clear
        local temp_hfas = subinstr("`real_hfas'", "`first_hfa'", "", 1)
		
		foreach hfa in `temp_hfas' {
			merge 1:1 YEAR using `pr_`hfa''
			drop _m
		}
		
**	// format
		foreach hfa in `real_hfas' {
            di "`hfa'"
            // create list of all hfa sub-component columns 
            unab allvars: `hfa'_DAH_`deflate_yr'*
            local vars ""
            foreach var of local allvars {
                if "`var'" != "`hfa'_DAH_`deflate_yr'" { // remove total col from list
                    local vars "`vars' `var'"
                }
            }
            // loop over vars and set the report-year to the predicted value
            foreach var of local vars {
                replace `var' = pr_`var' if  YEAR == `report_yr'
			}
		}

		gen CHANNEL = "WHO"
		capture drop pr* 
		reshape long `command', i(YEAR CHANNEL) j(INCOME_SECTOR) string
		
		rename *_DAH_`deflate_yr' pr_*_DAH_`deflate_yr'

	
**		// Now we can add income sector back in for donor channels that we needed to make predictions for
			gen SOURCE_CH=INCOME_SECTOR if inlist(INCOME_SECTOR, "GFATM", "AsDB", "AfDB", "IDB")
			replace INCOME_SECTOR="PRIVATE" if SOURCE_CH=="GFATM"
			replace INCOME_SECTOR="OTHER" if inlist(SOURCE_CH, "AsDB", "AfDB", "IDB")
	
		gen ISO_CODE = INCOME_SECTOR 
		replace ISO_CODE = "OTHER" if INCOME_SECTOR == "PUBLIC"
		replace ISO_CODE = "GBR" if INCOME_SECTOR == "UK"
		egen double DAH_`deflate_yr' = rowtotal(*_DAH_`deflate_yr')
		
		foreach var of varlist pr*{
			replace `var' = 0 if `var' == .
		}
        ren pr_swaphssother_* pr_swap_hss_other_*
        ren pr_malconoth_*   pr_mal_con_oth_*
        ren pr_hivhssoth_*   pr_hiv_hss_other_*
        ren pr_malhssoth_*   pr_mal_hss_other_*
        ren pr_ncdhssoth_*   pr_ncd_hss_other_*
        ren pr_nchhssoth_*   pr_nch_hss_other_*
        ren pr_oidhssoth_*   pr_oid_hss_other_*
        ren pr_rmhhssoth_*   pr_rmh_hss_other_*

        // The US owes $260m for membership dues in 2024-2025, which we assume it won't give (Trump administration).
        // We will reduce the US envelope by $130m.
        //
        ** convert hfas to fractions before adjusting envelope
        foreach var of varlist pr_* {
            quietly replace `var' = `var'/DAH_24
        }
        replace DAH_24 = DAH_24 - 130e+06 if ISO_CODE == "USA" & YEAR == 2024
        foreach var of varlist pr_* {
            quietly replace `var' = `var' * DAH_24
        }
        

		
		save "`FIN'/WHO_PREDS_DAH_BY_SOURCE_1990_`report_yr'_noDC.dta", replace	


// Graph by health focus areas for collaborators 
// We decided in FGH 2018 to exclude in-kind and include double counting in figures for collaborators to avoid confusion 
// For regular channel work and channel presentations don't show these figures 

	use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_noDC.dta", clear
	drop if INKIND==1
	drop if YEAR >= `report_yr'
	
	collapse (sum) *_DAH, by(YEAR)
	merge m:1 YEAR using "`DEFL'/imf_usgdp_deflators_`defl_mmyy'.dta", keepusing(GDP_deflator_`report_yr') nogen keep(1 3)
	foreach var of varlist *_DAH {
		gen double `var'_`deflate_yr' = `var'/GDP_deflator_`report_yr'
	}

	// gen HFA totals 
		egen hiv_DAH = rowtotal(hiv_*_DAH_`deflate_yr')
		egen mal_DAH = rowtotal(mal_*_DAH_`deflate_yr')
		egen rmh_DAH = rowtotal(rmh_*_DAH_`deflate_yr')
		egen nch_DAH = rowtotal(nch_*_DAH_`deflate_yr')
		egen ncd_DAH = rowtotal(ncd_*_DAH_`deflate_yr')
		egen tb_DAH = rowtotal(tb_*_DAH_`deflate_yr')
		egen oid_DAH = rowtotal(oid_*_DAH_`deflate_yr')
		egen swap_DAH=rowtotal(swap_*_DAH_`deflate_yr')
		drop other_DAH
		ren other_DAH_`deflate_yr' other_DAH

	collapse (sum) hiv_DAH mal_DAH rmh_DAH nch_DAH ncd_DAH tb_DAH oid_DAH swap_DAH other_DAH, by(YEAR)

	foreach var of varlist *_DAH {
		replace `var'=`var'/1e9
	}

	graph bar hiv_DAH tb_DAH mal_DAH rmh_DAH nch_DAH ncd_DAH oid_DAH swap_DAH other_DAH, ///
		stack over(YEAR, gap(0) label(labsize(small) angle(90))) ///
		ytitle("Billions of" "2018 USD", size(*0.8) orientation(horizontal) justification(right)) ///
		ylabel(, nogrid labsize(small) angle(0)) ///
		legend(size(*0.5) symxsize(1.5) position(11) ring(0) col(1) region(lcolor(white)) order(9 8 7 6 5 4 3 2 1) ///
		label(1 "HIV/AIDS") ///
		label(2 "Tuberculosis") ///
		label(3 "Malaria") ///
		label(4 "Reproductive and Maternal Health") ///
		label(5 "Newborn and child health") ///
		label(6 "Non-communicable diseases") ///
		label(7 "Other infectious diseases") ///
		label(8 "HSS/SWAps") ///
		label(9 "Other health focus area")) ///
		bar(1, c(red*0.8)) ///
		bar(2, c(gold*1.1)) ///
		bar(3, c(ebblue*0.5)) ///
		bar(4, c(purple*0.9)) ///
		bar(5, c(purple*0.6)) ///
		bar(5, c(dkgreen*0.8)) /// 
		bar(6, c(orange*0.7)) ///
		bar(7, c(lavender)) ///
		bar(8, c(ebblue*1.1)) ///
		bar(9, c(gs9)) ///
		graphregion(fcolor(white)) title("WHO DAH estimates by health focus areas, 1990-2021, noDC", size(*0.7))

	graph export "`OUT'/WHO DAH by HFA FGH`report_yr' noDC.pdf", replace


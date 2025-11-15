** **********************************************************
** Project:			FGH
** Purpose:			Generating dataset with unaids expenditures imputed by income source 
** **********************************************************

** **********************************************************
** SETUP
** **********************************************************
	clear all
	set more off
	if c(os) == "Unix" {
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

** **********************************************************
** User inputs
	local report_yr = 2024	// FGH report year
	local update_tag = "20241112"
	
	// File paths
	local RAW 		"FILEPATH/RAW"
	local INT 		"FILEPATH/INT"
	local FIN 		"FILEPATH/FIN"
	local OUT 		"FILEPATH/OUTPUT"
	local CODES 	"FILEPATH/COUNTRY FEATURES"

** **********************************************************
** Derived macros
	local previous_yr = `report_yr' - 1		// Previous FGH report year
	local data_yr =  `report_yr'			// Last year of data (for FGH 2024 is same as report year)
	local data_YY = substr("`data_yr'",3,4)	// Last year of data in YY format
	
** **********************************************************
** CLEAN DATA
** **********************************************************

** **********************************************************
** 1996-2001
	local years = "96_97 98_99 00_01"
	foreach year of local years {	
		di as red "Cleaning `year'"
		use "`INT'/unaids_`year'.dta", clear
		gen id = _n
		capture rename ISO_CODE DONOR_COUNTRY_ISO
		tab YEAR
		sort id

		// Split bi-annual into annual
		expand 2
		foreach var in CORE_UBW CORE_IPAA CORE NONCORE_ACT NONCORE_COUNTRY NONCORE_SUPP ///
				NONCORE JPO INCOME_ALL EXP_CORE EXP_NONCORE EXP_JPO {
			capture replace `var' = 0 if `var' == . 
			capture replace `var' = `var'/2
		}
		sort id
		gen odd = mod(_n,2) 
		gen yr = "`year'"
		split yr, p("_")
		replace YEAR = "19" + yr1 if odd == 0 & (yr1 == "96" | yr1 == "97" | ///
			 yr1 == "98" | yr1 == "99")
		replace YEAR = "19" + yr2 if odd == 1 & (yr2 == "96" | yr2 == "97" | ///
			yr2 == "98" | yr2 == "99")
		replace YEAR = "20" + yr1 if odd == 0 & regexm(yr1, "0") == 1
		replace YEAR = "20" + yr2 if odd == 1 & regexm(yr2, "0") == 1
		drop odd yr*
		destring YEAR, replace   

		// Re-label
		capture rename CORE_UBW CORE 
		capture rename NONCORE_NUBW NONCORE 

		// Save temp
		tempfile clean_`year'
		save `clean_`year'', replace
	}

** **********************************************************
** 2002-2007
	local years ="02_03 04_05 06_07"
	foreach year of local years {	
		di as red "Cleaning `year'"
		use "`INT'/unaids_`year'.dta", clear
		gen id = _n
		capture rename ISO_CODE DONOR_COUNTRY_ISO
		tab YEAR
		sort id

		// Clean labels
		if "`year'"=="02_03" {
			// fix incorrectly added total (NONCORE was included twice)
			replace CORE_UBW = 0 if CORE_UBW == . 
			replace JPO = 0 if JPO == . 
			replace INCOME_ALL = NONCORE + CORE_UBW + JPO
		}
		if "`year'"=="06_07" {
			// fix incorrectly added total (NONCORE was included twice)
			gen NONCORE = NONCORE_NUBW
		}

		// Split bi-annual into annual
		expand 2
		foreach var in CORE_UBW JPO INCOME_ALL NONCORE_NUBW NONCORE_EB NONCORE_SUPP_UBW NONCORE {
			capture replace `var' = 0 if `var' == . 
			capture replace `var' = `var'/2
		}
		sort id
		gen odd = mod(_n,2) 
		gen yr = "`year'"
		split yr, p("_")
		replace YEAR = "19" + yr1 if odd == 0 & (yr1 == "96" | yr1 == "97" | yr1 == "98" | yr1 == "99")
		replace YEAR = "19" + yr2 if odd == 1 & (yr2 == "96" | yr2 == "97" | yr2 == "98" | yr2 == "99")
		replace YEAR = "20" + yr1 if odd == 0 & regexm(yr1, "0") == 1
		replace YEAR = "20" + yr2 if odd == 1 & regexm(yr2, "0") == 1
		drop odd yr*
		destring YEAR, replace   

		// Re-label
		capture rename CORE_UBW CORE 

		// Save temp
		tempfile clean_`year'
		save `clean_`year'', replace
	}

** **********************************************************
** 2008-09
** Amended 8/23/10
	insheet using "`RAW'/unaids_0809.csv", comma names case clear 

	// Drop "Eliminations*" (refund to donors)
	drop if DONOR_NAME == "Eliminations*"

	tab YEAR
	gen id = _n
	sort id

	// Split bi-annual into annual
	expand 2
	foreach var in  CORE_UBW NONCORE_NUBW JPO OTHER INCOME_ALL EXP_CORE EXP_NONCORE EXP_JPO { 
		replace `var' = 0 if `var' == .
		replace `var' = `var'/2
	} 
	sort id
	gen odd = mod(_n,2) 
	replace YEAR = "2008" if odd == 0 
	replace YEAR = "2009" if odd == 1
	drop odd
	destring YEAR, replace

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_08_09
	save `clean_08_09', replace

** **********************************************************
** 2010-11
** Amended 7/17/12 by BPB
	insheet using "`RAW'/unaids_1011.csv", comma case clear 

	// Drop "Refund to donors and others"
	drop if DONOR_NAME == "Refund to donors and others"

	tab YEAR
	gen id = _n
	sort id

	// Split bi-annual into annual
	expand 2
	foreach var in  CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL { 
		replace `var' = 0 if `var' == .
		replace `var' = `var'/2
	} 
	sort id
	gen odd = mod(_n,2) 
	replace YEAR = "2010" if odd == 0 
	replace YEAR = "2011" if odd == 1
	drop odd
	destring YEAR, replace

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_10_11
	save `clean_10_11', replace
		
** **********************************************************
** 2012
** Amended 7/31/13 by CMG
	insheet using "`RAW'/unaids_12.csv", comma case clear

	// Drop "Refund to donors and others" and "Allowance for non-recovery"
	drop if DONOR_NAME == "Refund to donors and others" | ///
		DONOR_NAME == "Allowance for non-recovery"

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	} 

	// Drop blank channel observations
	drop if CHANNEL== ""

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_12
	save `clean_12', replace

** **********************************************************
** 2013
** Amended 8/18/14 by EKJ
	insheet using "`RAW'/unaids_13.csv", comma case clear

	// Drop "Refund to donors and others" and "Allowance for non-recovery"
	drop if DONOR_NAME == "Refund to donors and others" | ///
		DONOR_NAME == "Allowance for non-recovery"

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_13
	save `clean_13', replace

** **********************************************************
** 2014
** Amended 9/16/15 by MLB
	import delimited "`RAW'/unaids_14.csv", case(preserve) varnames(1) clear

	// Drop "Miscellaneous / Refund to Donor", "Allowance for non-recovery", 
	// and "Adjustments"
	drop if DONOR_NAME == "Miscellaneous / Refund to Donor" | ///
		DONOR_NAME == "Allowance for non-recovery" | ///
		DONOR_NAME == "Adjustments"

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_14
	save `clean_14', replace

** **********************************************************
** 2015
** Amended 10/25/16
	// (the csv version creates a strange YEAR variable that wont merge at the end)
	import delimited "`RAW'/unaids_15.csv", case(preserve) varnames(1) clear 
	// use "J:\Project\IRH\DAH\RESEARCH\CHANNELS\5_UN_AGENCIES\1_UNAIDS\DATA\RAW\unaids15.dta", clear
	
	// Drop "Miscellaneous / Refund to Donor"
	drop if DONOR_NAME == "Miscellaneous / Refund to Donor"

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_15
	save `clean_15', replace

** **********************************************************
** 2016
** Amended 09/20/17 
	import delimited using "`RAW'/unaids_16.csv", case(preserve) varnames(1) clear

	// Drop "Refund to donors"
	drop if DONOR_NAME == "Refund to donors"

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_16
	save `clean_16', replace

** **********************************************************
** 2017
** Amended 08/18
	import delimited using "`RAW'/unaids_17.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_17
	save `clean_17', replace

** **********************************************************
/** 2018	*/
	import delimited using "`RAW'/unaids_18.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_18
	save `clean_18', replace
	
	*********************************************************
	import delimited using "`RAW'/unaids_19.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_19
	save `clean_19', replace
	
	*********************************************************
	import delimited using "`RAW'/unaids_20.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_20
	save `clean_20', replace	


	*********************************************************
	import delimited using "`RAW'/unaids_21.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_21
	save `clean_21', replace	

	*********************************************************
	import delimited using "`RAW'/unaids_22.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_22
	save `clean_22', replace
	*********************************************************
	import delimited using "`RAW'/unaids_23.csv", case(preserve) varnames(1) clear

	// Clean empty cells
	foreach var in CORE_UBW EXP_CORE NONCORE_NUBW EXP_NONCORE INCOME_ALL {
		replace `var' = 0 if `var' == .
	}

	// Re-label
	rename CORE_UBW CORE
	rename NONCORE_NUBW NONCORE

	// Save temp
	tempfile clean_23
	save `clean_23', replace	

** **********************************************************
** Append years
	local years = "98_99 00_01 02_03 04_05 06_07 08_09 10_11 12 13 14 15 16 17 18 19 20 21 22 23"
	use `clean_96_97', clear
	foreach year of local years { 									
		append using `clean_`year''
	}
	sort YEAR

	// Adjustments
	rename DONOR_COUNTRY_ISO ISO_CODE
	replace DONOR_NAME = "Commission of the European Communities" if DONOR_NAME == "Commission of the European Communities  "
	replace DONOR_NAME = "United States" if DONOR_NAME == "United States of America"
	replace DONOR_NAME = "United Kingdom" if DONOR_NAME == "United Kingdom of Great Britain and Northern Ireland"
	replace DONOR_NAME = "United Kingdom" if DONOR_NAME == "United Kingdom of Great Britain"
	replace DONOR_NAME = "Italy" if DONOR_NAME == "italy"
    replace DONOR_NAME = "Cote d'Ivoire" if DONOR_NAME == "Cote dIvoire"
	replace DONOR_NAME = "Canton de Genève, Switzerland" if regexm(DONOR_NAME,"Canton de")
	replace DONOR_NAME = "African Society for Laboratory Medicine" if regexm(DONOR_NAME,"African Society for Laboratory")
	replace DONOR_NAME = "Autonomous Government of the Region of Extramadura" if regexm(DONOR_NAME,"Autonomous Government of the Region of")
    replace DONOR_NAME = " Germany GIZ" if regexm(DONOR_NAME,"GTZ")
	replace DONOR_NAME = " MDTF Office" if DONOR_NAME == "MPTF Office"
	replace DONOR_NAME = " Miscellaneous" if regexm(DONOR_NAME,"Miscellan")
	replace INCOME_TYPE = "DEVBANK" if INCOME_TYPE == "DEVBANKS"
	replace INCOME_TYPE = "IND" if INCOME_TYPE == "INDIV"
	replace INCOME_SECTOR = "MULTI" if INCOME_TYPE == "UN" & INCOME_SECTOR == "PRIVATE"
		
	// Replace DONOR names if alternative was listed: 
	replace DONOR_NAME = ALT_DONOR_NAME if ALT_DONOR_NAME != ""
	replace INCOME_SECTOR = ALT_INCOME_SECTOR if ALT_INCOME_SECTOR != ""
	replace INCOME_TYPE = ALT_INCOME_TYPE if ALT_INCOME_TYPE != "" 
	replace DONOR_COUNTRY = ALT_DONOR_COUNTRY if ALT_DONOR_COUNTRY != ""
	replace ISO_CODE = ALT_ISO_CODE if ALT_ISO_CODE != ""

	drop ALT_DONOR_NAME ALT_INCOME_SECTOR ALT_INCOME_TYPE ALT_DONOR_COUNTRY ALT_ISO_CODE

** **********************************************************
** INCOME SHARES (by major budgets Core, Non-Core, and Junior Professional Officers (JPO))
** **********************************************************
	// Calculate totals
	sort YEAR
	by YEAR: egen double CORE_INCOME_TOTAL_YR = total(CORE)
	label var CORE_INCOME_TOTAL_YR "Total Core Budget Income by Year"
	by YEAR: egen double NONCORE_INCOME_TOTAL_YR = total(NONCORE)
	label var NONCORE_INCOME_TOTAL_YR "Total Non Core Budget Income by Year"
	by YEAR: egen double JPO_INCOME_TOTAL_YR = total(JPO)
	label var JPO_INCOME_TOTAL_YR "Total JPO Budget Income by Year"
	
	// Calculate ratios
	gen double CORE_INCOME_SHARE = CORE/CORE_INCOME_TOTAL_YR
	label var CORE_INCOME_SHARE "Percent of Total UNAIDS Core Income Attributable to Row"
	gen double NONCORE_INCOME_SHARE = NONCORE/NONCORE_INCOME_TOTAL_YR
	label var NONCORE_INCOME_SHARE "Percent of Total UNAIDS Non Core Income Attributable to Row"
	gen double JPO_INCOME_SHARE = JPO/JPO_INCOME_TOTAL_YR
	label var JPO_INCOME_SHARE "Percent of Total UNAIDS JPO Income Attributable to Row"

	// Impute expenditure by row
	gen double EXP_CORE_IMP =  EXP_CORE * CORE_INCOME_SHARE
	label var EXP_CORE_IMP "Imputed Expenditure for Core Budget (Unified Budget)"
	gen double EXP_NONCORE_IMP = EXP_NONCORE * NONCORE_INCOME_SHARE
	label var EXP_NONCORE_IMP "Imputed Expenditure for Non Core Budget"
	gen double EXP_JPO_IMP = EXP_JPO*JPO_INCOME_SHARE
	label var EXP_JPO_IMP "Imputed Expenditure for JPO (Junior Professional Officers)"

	replace EXP_JPO_IMP = 0 if (YEAR>=2008)

	// Combine noncore budget income/expenditures with JPO expenditures to get NONCORE total. 
	replace NONCORE = NONCORE + JPO
	replace EXP_NONCORE_IMP = EXP_NONCORE_IMP + EXP_JPO_IMP
	
	gen EXPENDITURE_ALL = EXP_CORE_IMP + EXP_NONCORE_IMP
	gen OUTFLOW = EXPENDITURE_ALL
	label var OUTFLOW "Outflow (Disbursement or Expenditure)"

	// Save final database
	keep YEAR CHANNEL INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO_CODE ///
		CORE NONCORE INCOME_ALL EXP_CORE_IMP EXP_NONCORE_IMP EXPENDITURE_ALL OUTFLOW
	gen GHI = "UNAIDS"

	save "`FIN'/UNAIDS_INC_EXP_1996_`previous_yr'_`update_tag'.dta", replace

	// Allocate from Source to Channel to Health Focus Area (merging ADB and PDB)
	use "`FIN'/UNAIDS_INC_EXP_1996_`previous_yr'_`update_tag'.dta", clear
	rename OUTFLOW DAH
	gen ISO3_RC = "NA"
	
	// Merge on health focus area fractions
	preserve
		use "`INT'/hfa_fractions_`report_yr'_`update_tag'.dta", clear
			
		keep if year <= 2000
		collapse (mean) fraction*, by(channel)
		expand 2
		gen year = 1995 + _n
		tempfile 96_fractions
		save `96_fractions', replace
			
		use "`INT'/hfa_fractions_`report_yr'_`update_tag'.dta", clear
		
		drop if year > `report_yr'
		append using `96_fractions'
		rename year YEAR
		rename channel CHANNEL
		tempfile fractions
		save `fractions', replace
	restore
		
	// Generate disbursement and health focus area estimates by source of funding:
	replace CHANNEL = "UNAIDS" if CHANNEL == "UNAIS" // a quick fix. 
	merge m:1 YEAR CHANNEL using `fractions', assert(2 3) nogen keep(3)
		
	// Check that fractions add to one
	egen test = rowtotal(fraction*)
	gen diff = test - 1
	quietly summ diff
	if `r(max)' > 0.1 {
		wearebroken
	}
	drop diff test
		
	local hfas hiv_care hiv_hss hiv_ovc hiv_prev hiv_treat hiv_other hiv_pmtct hiv_ct ///
		hiv_amr tb_treat tb_diag tb_other tb_amr tb_hss
	foreach hfa of local hfas {
		gen double `hfa'_DAH = DAH * fraction_`hfa'
	}
			
	// Test
	egen test = rowtotal(hiv*)
	gen diff = test - DAH
	quietly summ diff
	if `r(mean)' > 0.1 {
		wearebroken
	}
	drop test diff
		
	// Rename fractions
	rename fraction_* *_frct
	
	gen gov = 0 //this is for DAHG analysis
	
   	drop CORE NONCORE INCOME_ALL EXP_NONCORE_IMP EXP_CORE_IMP GHI 

   	// CMSHYONG oops!  i actually forgot to fill this out yet.
	// Add in-kind 
	preserve
		import excel using "`RAW'/UNAIDS_INKIND_RATIOS_1996_`report_yr'.xlsx", firstrow clear 
		keep YEAR INKIND_RATIO
		drop if YEAR == . | INKIND_RATIO == .
		tempfile inkind_r 
		save `inkind_r'
	restore

	preserve
		merge m:1 YEAR using `inkind_r', nogen

		// Remove inkind spending (since we include all spending when extracting
		// data from financial statements)
		foreach var of varlist DAH *_DAH {
			replace `var'=`var'*(1-INKIND_RATIO) 
		}
		tempfile total 
		save `total'
	restore

	merge m:1 YEAR using `inkind_r', nogen 

	// Extract inkind spending
	foreach var of varlist DAH *_DAH {
		replace `var'=`var'*INKIND_RATIO 
	}

	gen INKIND=1
	tempfile inkind_amt 
	save `inkind_amt'

	use `total', clear
	append using `inkind_amt', force
	
	rename hiv_hss_frct hiv_hss_other_frct      
	rename tb_hss_frct tb_hss_other_frct
	rename hiv_hss_DAH hiv_hss_other_DAH
	rename tb_hss_DAH tb_hss_other_DAH
	
	replace INKIND=0 if INKIND==.

    replace DONOR_NAME = upper(DONOR_NAME)
	
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, NEW YORK" 
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFP"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "IATF_TC_UNFPA"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA NEW YORK"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA PROGRAMME SUPPORT SERVICES"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, HEW YORK"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNITED NATIONS POPULATION FUND (UNFPA)"
	
	replace DONOR_NAME = "UNICEF" if DONOR_NAME == "UNITED NATIONS CHILDREN'S FUND"
	replace DONOR_NAME = "UNICEF" if DONOR_NAME == "IATF_TC_UNICEF"
	replace DONOR_NAME = "UNICEF" if DONOR_NAME == "UNITED NATIONS CHILDREN’S FUND (UNICEF)"

	replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS"
	replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME"
	replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "UNITED NATIONS JOINT PROGRAMME"
	replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS (UNAIDS)"
	
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO. GENEVA"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO, GENEVA" 
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO.GENEVA"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (EXTRABUDGETARY FUNDS)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (REGULAR BUDGET)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (ROLL BACK MALARIA)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WORLD HEALTH ORGANIZATION"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WORLD HEALTH ORGANIZATION (WHO)"
	
	replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAHO, WASHINGTON"
	replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION (PAHO)"
	replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION"
	
	replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA"
	replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND"
	replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)"

	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINE"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINE"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINES"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINES"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI ALLIANCE"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINE IMMUNIZATION (GAVI)"
	
	gen ELIM_CH = 0
	replace ELIM_CH = 1 if DONOR_NAME == "UNFPA" 						
	replace ELIM_CH = 1 if DONOR_NAME == "UNICEF" 						
	replace ELIM_CH = 1 if DONOR_NAME == "UNAIDS"						
	replace ELIM_CH = 1 if DONOR_NAME == "WHO"  & CHANNEL != "PAHO"		
	replace ELIM_CH = 1 if DONOR_NAME == "PAHO"						
	replace ELIM_CH = 1 if DONOR_NAME == "GAVI" & CHANNEL != "GAVI"		
	replace ELIM_CH = 1 if DONOR_NAME == "GFATM" & CHANNEL != "GFATM"	
	gen SOURCE_CH = DONOR_NAME if ELIM_CH == 1
	drop ELIM_CH
	drop if YEAR == `report_yr'

	save "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_includesDC_`update_tag'.dta", replace
	save "`FIN'/UNAIDS_ADB_PDB_FGH`report_yr'_includesDC.dta", replace
		
	// Save PDB
	rename DAH OUTFLOW
	gen DISBURSEMENT = OUTFLOW
	collapse (sum) OUTFLOW DISBURSEMENT *DAH *frct, by(YEAR CHANNEL)
	gen ISO3_FC = "NA"
	gen FUNDING_TYPE = "GRANT"
	gen FUNDING_AGENCY = "UNAIDS"
	gen FUNDING_AGENCY_SECTOR = "MULTI"
	gen RECIPIENT_AGENCY_SECTOR = "UNSP"
	gen RECIPIENT_AGENCY_TYPE = "UNSP"
	gen rmh_fp = 0
	gen rmh_mh = 0
	gen rmh_other = 0
	gen nch_cnn = 0
	gen nch_cnv = 0
	gen nch_nch = 0
	gen mal_con_nets = 0
	gen mal_con_irs = 0
	gen mal_con_oth = 0 
	gen mal_treat = 0 
	gen mal_diag = 0
	gen mal_hss = 0
	gen mal_comm_con = 0
	gen mal_amr = 0
	gen ncd_tobac = 0
	gen ncd_mental = 0
	gen ncd_other = 0
	gen oid_ebz = 0
	gen oid_zika = 0
	gen oid_amr = 0
	gen oid_other = 0	
	gen swap_hss_hrh = 0
	gen swap_hss_pp = 0
	// All DAH is hiv_unid
	
	order hiv*, after(nch_nch)
	
	save "`FIN'/UNAIDS_INTPDB_1990_`data_yr'_`update_tag'.dta", replace
	
** END OF FILE **

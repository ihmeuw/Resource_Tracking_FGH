*** *********************************************************
// Project:	FGH
// Purpose: Generating GFF estimates and final datasets
*** ***********************************************************

	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

	local report_yr = 2018					// FGH report year

	local RAW		"FILEPATH"
	local FIN		"FILEPATH""
	local DEFL 		"FILEPATH""
	local CODES 	"FILEPATH

	import excel using "`RAW'/GFF Approved projects.xlsx", firstrow clear

	// Data was reported in millions
	replace GFFApprovedamount = GFFApprovedamount*amountmultiple

	keep Country GFFApprovedamount *_DAH start_date end_date
	ren GFFApprovedamount DAH 
	drop if DAH==. // countries that have not received disbursements yet

	// Split projects based on length 
	gen num_yr=(end_date-start_date)+1
	forvalues i=2015(1)2023 {
		gen split_`i'=DAH
		replace split_`i'=. if `i' < start_date | `i' > end_date
	}

	foreach var of varlist split_* {
		replace `var'=`var'/num_yr
	}

	drop DAH start_date end_date num_yr
	gen n=_n
	reshape long split_, i(Country n *_DAH) j(year)
	drop if split_ ==.
	keep if year <= `report_yr'
	ren split_ DAH 

	foreach var of varlist *_DAH {
		replace `var'=1/3 if `var'==.33
	}

	// Split out HFA amounts
	foreach var of varlist *_DAH {
		replace `var'=`var'*DAH
	}
	
	// Add country ISO codes 
	ren Country country_lc 
	replace country_lc = "Democratic Republic of the Congo" if country_lc=="DRC"
	merge m:1 country_lc using "`CODES'\countrycodes_official_2018.dta", keepusing(iso3 countryname_ihme)
	drop if _m==2 // make sure there are no entries for _m==1 
	drop _m 

	// Add donor information 
	preserve
	import delimited "`RAW'/GFF_INCOME.csv", varn(1) case(upper) clear
	collapse (sum) AMOUNT, by(DONOR_NAME INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY ISO_CODE)
	egen tot=total(AMOUNT)
	gen pct=AMOUNT/tot 
	keep DONOR_NAME INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY ISO_CODE pct 
	gen n=_n // there are 5 donors
	tempfile income 
	save `income'
	restore

	drop n 
	gen n=_n
	expand 5 // because there are 5 donors 
	ren n num 
	bysort num: gen n=_n 
	merge m:1 n using `income', nogen

	foreach var of varlist DAH *_DAH {
		replace `var'=`var'*pct
	}

	// Add in-kind 
	// GFF retains the administrative structure of the World Bank 
	// Until GFF releases a financial report that details administrative expenses, we will rely on WB_IDA in-kind ratios 
	preserve 
	import delimited using "FILEPATH\IDA_INKIND_1990-2018.csv", varn(1) case(upper) clear
	keep YEAR INKIND_RATIO 
	tempfile ik_ratio 
	save `ik_ratio'
	restore 

	ren year YEAR
	merge m:1 YEAR using `ik_ratio', nogen keep(1 3)
	gen INKIND=0

	preserve
	foreach var of varlist DAH *_DAH {
		replace `var'=`var'*INKIND_RATIO
	}
	replace INKIND=1
	tempfile with_ik 
	save `with_ik'
	restore

	append using `with_ik', force

	// Clean and save out datasets 
	ren countryname_ihme RECIPIENT_COUNTRY 
	ren iso3 ISO3_RC 
	
	keep YEAR RECIPIENT_COUNTRY ISO3_RC DONOR_NAME DONOR_COUNTRY INCOME_SECTOR INCOME_TYPE ISO_CODE DAH *_DAH INKIND 

	gen CHANNEL=="GFF"
	save "`FIN'/GFF_ADB_PDB_FGH`report_yr'.dta", replace
	
// No double counting - we are only extracting GFF money not IDA or IBRD money 

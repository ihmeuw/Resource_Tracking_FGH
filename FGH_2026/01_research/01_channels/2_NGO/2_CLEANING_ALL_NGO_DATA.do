**************************************************************
// Project:		FGH
// Purpose: 	Use 2015 data from Guidestar to create estimates for NGOs
**************************************************************
	set more off
	clear all

	if c(os) == "Unix" | c(os) == "MacOSX" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

	// FGH report year	
	local report_yr = 2024	
	local data_yr = 2022
	
	local INT  "FILEPATH"
	local FIN  "FILEPATH"
	
** ****
// Step 0: Preparing and updating data
** ****

// Get together all agency names and IDs
		import delimited "FILEPATH", clear
		append using "FILEPATH" 
		replace n = 0 if n == .
		replace id = 10000 + id if n == 0
		ren n usngo
		// clean some names
		preserve
		keep if regexm(agency, "-") | regexm(agency, "/") | regexm(agency, "'")
		replace agency = subinstr(agency, "-"," ",.)
		replace agency = subinstr(agency, "/"," ",.)
		replace agency = subinstr(agency, "'","",.)
		replace agency = stritrim(agency)
		duplicates tag agency, gen(dup)
		bysort agency: gen n = _n
		drop if dup > 0 & n > 1
		drop n dup
		tempfile cleaner
		save `cleaner'
		restore
		append using `cleaner'
		preserve
		keep if regexm(agency, ",") & !regexm(agency, ", INC.")
		replace agency = subinstr(agency, ",","",.)
		tempfile cleancommas
		save `cleancommas'
		restore
		append using `cleancommas'

		duplicates tag agency, gen(dup)
		bysort agency: gen n = _n
		drop if dup > 0 & n>1
		drop dup n
		replace agency = lower(agency)	
		tempfile agencies
		save `agencies'
** ****
// Step 1: Cleaning data
** ****	
		
	import delimited using "FILEPATH",  clear
    // ** FGH2024: drop data after 2018
    drop if year > 2018
    // **
	sort id year 
	duplicates list id year // make sure there aren't duplicates from adding in additional top ngos
	tempfile healthexp
	save `healthexp'
				
** ****
// Step 2: Merging BMGF and health expenditure data
** ****
	// Loading BMGF data: 
		insheet using "FILEPATH", comma clear
	// Dropping irrelevant cases and formatting data:
		drop if inkind == 1
		drop if usaid_ngo == 0
		keep year disbursement recipient_agency
		rename disbursement bmgf_grant_amnt
		rename recipient_agency agency
		
	// Merge agency ids:
		replace agency = subinstr(agency,",","",.)
		replace agency = subinstr(agency,".","",.)
		split agency, parse(" ")
			replace agency1 = "" if agency1 == "The"
			egen agency_new = concat(agency?), punct(" ")
			replace agency = agency_new if agency != agency_new
			drop agency? agency_new
		
		// Fixes for agency names:
			replace agency = "Cooperative for Assistance and Relief Everywhere" if regexm(agency,"CARE")
			replace agency = "The Carter Center" if regexm(agency,"Carter")
			replace agency = "Center for Strategic and International Studies" if agency == "Center for Strategic and International Studies (CSIS)"
			replace agency = "Children's Health Fund" if agency == "Childrens Health Fund"
			replace agency = "East  West Foundation" if agency == "East Meets West Foundation"
			replace agency = "Freedom from Hunger" if agency == "FREEDOM FROM HUNGER"
			replace agency = "German Marshall Fund of the United States" if agency == "German Marshall Fund of the US"
			replace agency = "International Center for Journalists" if agency == "International CENTER FOR JOURNALISTS"
			replace agency = "International Rescue Committee" if agency == "Internatlonal Rescue Committee Inc"
			replace agency = "Magee-Womens Health Corporation" if regexm(agency,"MAGEE") | regexm(agency,"Magee")
			replace agency = "Medical Education for South African Blacks" if agency == "Medical Education for South African Blacks Inc"
			replace agency = "Program for Appropriate Technology in Health" if regexm(agency,"PATH")
			replace agency = "Plan International USA" if agency == "PLAN International"
			replace agency = "Pathfinder International" if agency == "Pathfinder Intemational"
			replace agency = "Population Council" if agency == "Population Council Inc"
			replace agency = "Population Services International" if agency == "Population services International"
			replace agency = "Project HOPE - The People-to-People Health Foundation" if agency == "Project Hope The People to People Health Foundation Inc"
			replace agency = "Project Orbis" if agency == "Project Orbis International Inc"
			replace agency = "Rotary Foundation of Rotary International" if agency == "ROTARY FDN OF ROTARY INTL"
			replace agency = "Results Educational Fund" if agency == "Results Educational Fund Inc"
			replace agency = "Synergos Institute" if agency == "Synergos Institute Inc" | agency == "SYNERGOS INSTITUTE"
			replace agency = "Training Programs in Epidemiology and Public Health Interventions Network" if agency == "Training Programs In Epidemiology and Public Health Interventions Network"
			replace agency = "White Ribbon Alliance for Safe Motherhood" if agency == "WHITE RIBBON ALLIANCE FOR SAFE MOTHERHOOD"
			replace agency = "World Vision" if agency == "World Vision International"
			replace agency = "save the children federation, inc." if regexm(agency, "Save the Children")
			replace agency = "save the children federation, inc." if agency == "SAVE THE CHILDREN" 
			replace agency = "ipas inc" if regexm(agency, "IPAS-Protecting Women")

		
		replace agency = lower(agency)	
		merge m:m agency using `agencies'
		drop if _m == 2
		drop if id == . // general names from BMGF new and can't match?? 
		drop _m
			
		drop if id >= 10000	& id < 20000	// Drop international NGOs
			
	// Collapsing BMGF grants by recipient and year:
		collapse (sum) bmgf_grant_amnt, by(id year)
		sort id year
		
		tempfile bmgf
		save `bmgf', replace

	// Merging Volag data with BMGF grant database:
		import delimited "FILEPATH", clear
		gen existing = 1
        // ** FGH2024: drop years after 2018
        drop if year > 2018

		merge 1:1 id year using `bmgf'
			drop if _m == 2 	// Drop NGOs that received BMGF funds that are not in our database
			drop _m
		replace bmgf_grant_amnt=0 if bmgf_grant_amnt==.

	// Saving merged Volag and BMGF data:
		tempfile volag_with_extra_ngos_bmgf
		save `volag_with_extra_ngos_bmgf', replace		

** ****
// Step 3: Loading health expenditure data
** ****
	use `healthexp', clear
	sort id year
	
	foreach var of varlist random_sample top_ngo other_ngo {
		replace `var'= 0 if `var'==.
	}
	//drop v*
	keep id year health_expenditure_990
	
	tempfile health_exp
	save `health_exp', replace

	// Loading merged Volag and BMGF data:
	use `volag_with_extra_ngos_bmgf', clear
	drop top_ngo random
	
	// Merging US NGO data with health expenditure data:
	merge 1:1 id year using `health_exp' 
	drop if _m == 2
	drop _m

** ****
// Step 4: Prepping data for regression estimations
** ****
	
	// Formatting variables (recasting the variables ensure the variables retain their precision):
	recast double usaid_freight pl_480_freight pl_480_donated_food usaid_grants usaid_contracts other_usg_grants  ///
	other_usg_contracts other_govt_and_international_org inkind_contributions private_contributions private_revenue ///
	domestic_programs administrative_and_management fund_raising total_support_and_revenue overseas_programs total_expenses ///
	bmgf_grant_amnt health_expenditure_990 govt_grants_total

	// Renaming variables:
	rename other_govt_and_international_org other_gvmt
	rename administrative_and_management admin_man

// estimate US vs non-US spend proportions
preserve
	egen tot_US = rowtotal(usaid_freight  pl_480_freight pl_480_donated_food usaid_grants usaid_contracts other_usg_grants other_usg_contracts)
	replace govt_grants_total = tot_US + other_gvmt if govt_grants_total == 0 // fill in totals for VolAg data
	gen prop_us = tot_US/govt_grants_total
	gen prop_othergov = other_gvmt/govt_grants_total
	replace prop_us  = 0 if prop_us == .
	replace prop_othergov = 0 if prop_othergov == .

	//copy prev proportions from 2015-2018 when we were using guidestar data
	replace prop_us = prop_us[_n-1] if year == 2015
	replace prop_othergov = prop_othergov[_n-1] if year == 2015
	replace prop_us = prop_us[_n-1] if year == 2016
	replace prop_othergov = prop_othergov[_n-1] if year == 2016  // do I need both of these
	replace prop_us = prop_us[_n-1] if year == 2017
	replace prop_othergov = prop_othergov[_n-1] if year == 2017  // used again for 2020 because no time to figure any of this out
	replace prop_us = prop_us[_n-1] if year == 2018
	replace prop_othergov = prop_othergov[_n-1] if year == 2018  // used again for 2021 because no time to figure any of this out

	gen other_usg_grants_new = prop_us * govt_grants_total if year >= 2015 // should this be == 2016
	gen other_gvmt_new = prop_othergov * govt_grants_total if year >= 2015 // should this be == 2016

	keep if year >= 2015 // should this be year==2016
	keep year id agency other_usg_grants_new other_gvmt_new
	tempfile donorvals
	save `donorvals'
restore	
merge 1:1 year agency id using `donorvals'
replace other_usg_grants = other_usg_grants_new if _m == 3
replace other_gvmt = other_gvmt_new if _m ==3 
drop _m

	// Creating binary food variable to indicate whether an NGO's expenditure included food aid:
		gen food=0
		replace food=1 if pl_480_donated_food!=0

	// Creating total (revenue):
		egen new_tot_rev = rowtotal(other_revenue usaid_freight pl_480_freight pl_480_donated_food usaid_grants usaid_contracts other_usg_grants other_usg_contracts inkind_contributions private_contributions private_revenue other_gvmt)

	// Creating total (expenditure):
		egen new_tot_exp = rowtotal(overseas_programs domestic_programs admin_man fund_raising)
replace new_tot_exp = total_expenses if year>2014 	

	// Creating total US and private revenue variables:
		egen tot_US = rowtotal(usaid_freight  pl_480_freight pl_480_donated_food usaid_grants usaid_contracts other_usg_grants other_usg_contracts)
		egen new_priv = rowtotal(private_contributions private_revenue)

	// Creating fractions for regression:
		foreach var in inkind_contributions new_priv private_contributions tot_US other_gvmt { 
			gen `var'_frc = `var'/new_tot_rev
		}

		foreach var in overseas_programs { 
			gen `var'_frc = `var'/new_tot_exp
		}

	// Calculating health fraction:
		gen hlth_frct = health_expenditure/new_tot_exp

	// Cleaning overseas program fraction that was missing due to a 0 in the denominator (new_total_expenditure = 0):
		replace overseas_programs_frc = 0 if overseas_programs_frc == .

	// Correcting outliers:
		replace hlth_frct = 0 if hlth_frct < 0
		replace hlth_frct = 1 if hlth_frct > 1 & hlth_frct != .
		replace private_contributions_frc = 0 if private_contributions_frc < 0
		replace private_contributions_frc = 1 if private_contributions_frc > 1 & private_contributions_frc != .
		replace tot_US_frc = 0 if tot_US_frc < 0
		replace tot_US_frc = 1 if tot_US_frc > 1 & tot_US_frc != . 
		replace inkind_contributions_frc = 0 if inkind_contributions_frc < 0
		replace inkind_contributions_frc = 1 if inkind_contributions_frc > 1 & inkind_contributions_frc != .
		replace overseas_programs_frc = 0 if overseas_programs_frc < 0
		replace overseas_programs_frc = 1 if overseas_programs_frc > 1 & overseas_programs_frc != .

	// Cleaning data in preparation for logit transformations:
		replace hlth_frct = .0001 if hlth_frct == 0 | hlth_frct <  0
		replace hlth_frct = .9999 if hlth_frct == 1 | hlth_frct > 1 & hlth_frct != .

	// Modifying year variable and creating an "old" year variable.:
		drop if year > `data_yr'
		replace year = year - 1990

	// Logit transformation:
		gen lnhlth = ln(hlth_frct/(1-hlth_frct))

	// Creating total (private revenue, US public revenue, non-US public revenue):
		gen pubpriv_revenue = new_tot_rev

	// Creating total (US public revenue):
		egen USpub_rev_total = rowtotal(usaid_freight pl_480_donated_food pl_480_freight usaid_grants usaid_contracts other_usg_grants other_usg_contracts)

	// Creating total (non-US public revenue):
		gen nonUSpub_rev_total = other_gvmt

	// Creating total (private revenue):
		gen priv_rev_total = pubpriv_revenue - (USpub_rev_total + nonUSpub_rev_total) 
replace priv_rev_total = private_revenue + inkind_contributions if year > 24
		*** This is equal to inkind_contributions + private_contributions + private_revenue

	// Creating total (private revenue, non-BMGF):
		gen priv_tot_non_BMGF = priv_rev_total - bmgf_grant_amnt
		*** This is equal to inkind_contributions + private_contributions + private_revenue - bmgf_grant_amnt

	// Creating total private non-in-kind revenue:
		gen priv_rev_other = priv_rev_total - inkind_contributions

	// Creating total private non-in-kind, non-BMGF revenue:
		gen priv_other_non_BMGF = priv_rev_other - bmgf_grant_amnt

	// Creating fraction (private revenue/total revenue):
		gen priv_frct_non_BMGF = priv_tot_non_BMGF/pubpriv_revenue

	// Creating fraction (public US revenue/total revenue):
		gen USpub_rev_frct = USpub_rev_total/pubpriv_revenue

	// Creating fraction (public non-US revenue/total revenue):
		gen nonUSpub_rev_frct = nonUSpub_rev_total/pubpriv_revenue

	// Creating fraction (BMGF revenue/total revenue):
		gen BMGF_frct = bmgf_grant_amnt/pubpriv_revenue

	// Creating fraction (private in-kind revenue/total revenue):
		gen ink_frct = inkind_contributions/pubpriv_revenue

	// Creating fraction (private revenue total-BMGF/total revenue):
		gen other_priv_frct = priv_other_non_BMGF/pubpriv_revenue

	// Correcting outliers:
		replace priv_frct_non_BMGF = 1 if priv_frct_non_BMGF > 1
		replace priv_frct_non_BMGF = 0 if priv_frct_non_BMGF < 0
		replace USpub_rev_frct = 1 if USpub_rev_frct > 1
		replace USpub_rev_frct = 0 if USpub_rev_frct < 0
		replace nonUSpub_rev_frct = 1 if nonUSpub_rev_frct > 1
		replace nonUSpub_rev_frct = 0 if nonUSpub_rev_frct < 0
		replace BMGF_frct = 1 if BMGF_frct > 1
		replace BMGF_frct = 0 if BMGF_frct < 0
		replace ink_frct = 1 if ink_frct > 1
		replace ink_frct = 0 if ink_frct < 0
		replace other_priv_frct = 1 if other_priv_frct > 1
		replace other_priv_frct = 0 if other_priv_frct < 0

	
** ****
// Step 5: Saving out-of-sample data
** ****

	// Keeping only those observations with missing health exp., keeping all out-of-sample observations, creating sample variable:
		preserve
		keep if lnhlth == .
	// generating sample variable:
		gen sample = 0
		save "FILEPATH",replace
		restore
		
** ****
// Step 6: Saving in-sample data
** ****
	// Keeping only those NGOs in sample:
		drop if lnhlth == .
	// Correlation matrix:
		correlate private_contributions_frc tot_US_frc inkind_contributions_frc overseas_programs_frc year food lnhlth hlth_frct
		gen sample = 1
		save "FILEPATH",replace

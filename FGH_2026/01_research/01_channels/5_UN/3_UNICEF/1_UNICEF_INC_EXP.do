** *****************************************************************************
// Project: FGH
// Purpose: Generating dataset with UNICEF expenditures imputed by sources of 
//			income
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

** *****************************************************************************
// USER INPUTS
	local report_yr = 2024				// FGH report year
	local dateTag: display %tdCCYYNNDD =daily("`c(current_date)'", "DMY")
	
	// Directories
	local RAW 		"FILEPATHRAW/FGH_`report_yr'"
	local INT 		"FILEPATHINT"
	local FIN 		"FILEPATHFIN/FGH_`report_yr'"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local CODES 	"FIEPATH/FGH_`report_yr'"

	// Files
	local country_codes "`CODES'/fgh_location_set.csv"
	local ebola_data "FILEPATH/ebola_all_collapsed.csv"

	// Derived locals
	local data_yr = `report_yr' - 1		// Last year of data
	local report_YY =substr("`report_yr'",3,4)

	import delimited using "`country_codes'", varnames(1) clear
	keep if level == 3
	keep location_name ihme_loc_id
	replace location_name = "Cote d'Ivoire" if strpos(location_name, "Ivoire")
	local max = _N + 1
	set obs `max'
	replace location_name = "Liechtenstein" if location_name == ""
	replace ihme_loc_id = "LIE" if ihme_loc_id == ""
	gen countryname_ihme = location_name
	ren (location_name ihme_loc_id) (country_lc iso3)
	duplicates drop
	tempfile country_codes
	save `country_codes'
			
** *****************************************************************************
// STEP 1: Generate tempfile for newest update year
** *****************************************************************************
	import delimited using "`RAW'/UNICEF_INCOME_`data_yr'.csv", varnames(1) ///
		case(preserve) clear
	drop if YEAR != `data_yr'

	rename DONOR_COUNTRY country_lc
	replace country_lc = "Czechia" if country_lc == "Czech Republic"
	replace country_lc = "China" if country_lc == "Hong Kong, China"
	replace country_lc = "United States of America" if country_lc == "United States"
	replace country_lc = "Cote d'Ivoire" if strpos(country_lc, "Ivoire")
	replace country_lc = "Solomon Islands" if country_lc=="Solomn Islands"
	replace country_lc = "TÃ¼rkiye" if country_lc=="Türkiye"

	merge m:1 country_lc using `country_codes', keepusing(iso3 countryname_ihme)
	replace countryname_ihme = "NA" if countryname_ihme == ""

	// Check that there are no countries that did not merge w/ countrycodes
	// If there are, go in and manually fix them
	quietly count if _merge == 1 & (country_lc != "" & country_lc != "NA")
	if `r(N)' != 0 {
		list YEAR DONOR_NAME if _merge == 1 | _merge == 2
		go_fix_countrylocations_merge
	}

	drop if _m == 2
	drop _m country_lc

	rename countryname_ihme DONOR_COUNTRY
	rename iso3 ISO_CODE

	order YEAR CHANNEL SOURCE_DOC INCOME_SECTOR INCOME_TYPE DONOR_NAME ///
		DONOR_COUNTRY ISO_CODE
	
	destring INCOME_REG_* INCOME_SUPP_* INCOME_EMER_*, replace ignore(",")
	tostring YEAR, replace

	foreach var of varlist INCOME_REG_* INCOME_SUPP_* INCOME_EMER_* {
		replace `var' = 0 if `var' == .
	}
	
	gen id = _n
	save "`INT'/prelim_`data_yr'.dta", replace

** *****************************************************************************		
// STEP 2: Generate tempfile for all years
** *****************************************************************************
	// Recent (annual) report years
	local single_years = "12 13"
	forval year=2014/`data_yr' {
		local single_years "`single_years' `year'"
	}

	foreach year of local single_years {
		use "`INT'/prelim_`year'.dta", clear
		reshape long INCOME_REG_ INCOME_SUPP_ INCOME_EMER_, i(id) ///
			j(INCOME_SECTOR2) string
		
		foreach var in INCOME_REG_ INCOME_SUPP_ INCOME_EMER_ {
			replace `var' = 0 if `var' == .
		}
			
		egen TOTAL = rowtotal(INCOME_REG_ INCOME_SUPP_ INCOME_EMER_)
		drop if TOTAL == 0
		drop TOTAL
		
		replace DONOR_NAME="Other private donor from " + DONOR_NAME if ///
			INCOME_SECTOR2=="PRIVATE" & INCOME_SECTOR=="PUBLIC"

		replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR2 == "PRIVATE"
		replace INCOME_TYPE = "SOC" if INCOME_SECTOR2 == "PRIVATE"
		drop INCOME_SECTOR2
		
		tempfile temp_`year'
		save `temp_`year'', replace
	}
		
	// All other years: Break out biennia into two years		
	local years 9091 9293 9495 9697 9899 0001 0203 0405 0607 0809 1011
	foreach year of local years { 
		use "`INT'/prelim_`year'.dta", clear
		di "`year'"
	 
		reshape long INCOME_REG_ INCOME_SUPP_ INCOME_EMER_, ///
			i(id) j(INCOME_SECTOR2) string
	
		foreach var in  INCOME_REG_ INCOME_SUPP_ INCOME_EMER_ { 
			replace `var' = 0 if `var' == .
		} 
	
		egen TOTAL = rowtotal(INCOME_REG_ INCOME_SUPP_ INCOME_EMER_) 
		drop if TOTAL == 0
		drop TOTAL
	
		replace DONOR_NAME="Other private donor from " + DONOR_NAME if ///
			INCOME_SECTOR2=="PRIVATE" & INCOME_SECTOR=="PUBLIC"

		replace INCOME_SECTOR = "PRIVATE" if INCOME_SECTOR2 == "PRIVATE"
		replace INCOME_TYPE = "SOC" if INCOME_SECTOR2 == "PRIVATE"
		drop INCOME_SECTOR2
		
		replace id = _n
		sort id
		expand 2 	
		sort id
	
		foreach var in INCOME_REG_ INCOME_SUPP_ INCOME_EMER_ { 
			replace `var' = `var'/2	
		}
			
		sort id
		gen odd = mod(_n,2)
		tab YEAR
	
		replace SOURCE_DOC = ///
			"UNICEF Biennium Financial Report & Audited Financial Statements, " ///
			+ YEAR
		
		replace YEAR = "1990" if odd == 0 & YEAR == "1990_91"
		replace YEAR = "1991" if odd == 1 & YEAR == "1990_91"
		replace YEAR = "1992" if odd == 0 & YEAR == "1992_93"
		replace YEAR = "1993" if odd == 1 & YEAR == "1992_93"
		replace YEAR = "1994" if odd == 0 & YEAR == "1994_95"
		replace YEAR = "1995" if odd == 1 & YEAR == "1994_95"
		replace YEAR = "1996" if odd == 0 & YEAR == "1996_97"
		replace YEAR = "1997" if odd == 1 & YEAR == "1996_97"
		replace YEAR = "1998" if odd == 0 & YEAR == "1998_99"
		replace YEAR = "1999" if odd == 1 & YEAR == "1998_99"
		replace YEAR = "2000" if odd == 0 & YEAR == "2000_01"
		replace YEAR = "2001" if odd == 1 & YEAR == "2000_01"
		replace YEAR = "2002" if odd == 0 & YEAR == "2002_03"
		replace YEAR = "2003" if odd == 1 & YEAR == "2002_03"
		replace YEAR = "2004" if odd == 0 & YEAR == "2004_05"
		replace YEAR = "2005" if odd == 1 & YEAR == "2004_05"
		replace YEAR = "2006" if odd == 0 & YEAR == "2006_07"
		replace YEAR = "2007" if odd == 1 & YEAR == "2006_07"
		replace YEAR = "2008" if odd == 0 & YEAR == "2008_09"
		replace YEAR = "2009" if odd == 1 & YEAR == "2008_09"
		replace YEAR = "2010" if odd == 0 & YEAR == "2010_11"
		replace YEAR = "2011" if odd == 1 & YEAR == "2010_11"
		replace YEAR = "2012" if odd == 0 & YEAR == "2011_12"
		drop odd
	
		tempfile temp_`year'
		save `temp_`year'', replace
	}
	
** *****************************************************************************		
// STEP 3: Append datasets
** *****************************************************************************
	use `temp_9091', clear

	foreach year in 9293 9495 9697 9899 0001 0203 0405 0607 0809 1011 12 13 {
		append using `temp_`year''
	}

	forval year=2014/`data_yr' {
		append using `temp_`year''
	} 
	
	// Fix confusing donor names that are countries but actually private funding:
	gen country_lc = DONOR_NAME
	merge m:1 country_lc using "`country_codes'", keepusing(countryname_ihme)

	// Check that there are no countries that did not merge w/ countrycodes
	// If there are, go in and manually fix them
	quietly count if _merge == 1 & ///
		(ISO_CODE != "NA" & countryname_ihme != "")
	if `r(N)' != 0 {
		list YEAR DONOR_NAME if _merge == 1 | _merge == 2
		go_fix_countrylocations_merge
	}

	drop if _merge == 2
	replace DONOR_NAME="Other private donor from " + DONOR_NAME if ///
		INCOME_SECTOR=="PRIVATE" & _m==3
	drop _m countryname_ihme

	sort YEAR id
	replace ISO_CODE = "NA" if DONOR_COUNTRY == "NA"

	// Fix extracted values that were in thousands
	foreach var in  INCOME_REG_ INCOME_SUPP_ INCOME_EMER_ {
		replace `var' = `var'*1000 if YEAR == "2000" | YEAR == "2001" | ///
			YEAR == "2002" | YEAR == "2003"
	} 
	
	rename INCOME_REG_ INCOME_REG
	label var INCOME_REG "Regular Budget Income by Source and Year"
	
	rename INCOME_SUPP_ INCOME_SUPP
	label var INCOME_SUPP "Supplementary Budget Income by Source and Year" 
	
	rename INCOME_EMER_ INCOME_EMER
	label var INCOME_EMER "Emergency Budget Income by Source and Year"
	
	gen double INCOME_EXTRA = INCOME_SUPP + INCOME_EMER
	label var INCOME_EXTRA ///
		"Total Income by Source and Year from Non-Regular Budgets"
	
	gen double INCOME_ALL = INCOME_REG + INCOME_SUPP + INCOME_EMER
	label var INCOME_ALL "Total Income (all budgets) by Source & Year"
	
	destring(YEAR), replace
	
	// Minor fixes:
	replace INCOME_TYPE = "DEVBANK" if INCOME_TYPE == "DEVBANKS"
	replace INCOME_TYPE = "DEVBANK" if ///
		INCOME_SECTOR == "DEVBANK" & INCOME_TYPE == "MULTI"
	replace INCOME_TYPE = "OTHER" if ///
		INCOME_SECTOR == "OTHER" & INCOME_TYPE == "MULTI"
	replace INCOME_SECTOR = "MULTI" if INCOME_TYPE == "DEVBANK"
	
	replace ISO_CODE = "CZE" if ///
		(ISO_CODE=="CZE_FRMR" | ISO_CODE=="CZECH") & YEAR > 1992 // Assuming this is money from the Czech Republic and not Slovakia that had the wrong donor country name added
	replace ISO_CODE = "MKD" if ///
		(ISO_CODE=="YUG_FRMR" | ISO_CODE=="YUG") & YEAR > 1992

** *****************************************************************************		
// STEP 4:  Importing yearly expenditure amounts
** *****************************************************************************
/* Note:
 Expenditure comes from:
	1. audited Financial Statements or Interim Financial Statements (Statement V)
	2. correspondence for health fractions
*/

	preserve
		import excel using "`RAW'/UNICEF_EXP_BYYEAR_1990-`data_yr'.xlsx", ///
			sheet("Stata - expenditure") firstrow clear
		drop if YEAR == .
		tempfile exp
		save `exp', replace
	restore
	
	merge m:1 YEAR using `exp'
	drop _m
	
	label var EXP_REG "Total Regular Budget Expenditure by Year"
	label var EXP_SUPP "Total Supplementary budget Expenditure by Year"
	label var EXP_EMER "Total Emergency budget Expenditure by Year"
	label var TOT_EXP "Total Exp. (all budgets) by Year"
	label var TOTAL_HLTH_EXP "Total Health Exp. (all Budgets) by Year"
	
	rename HLTH_EXP_REG TOT_HLTH_EXP_REG
	label var TOT_HLTH_EXP_REG "Total Health Exp. from the Reg. Budget by Year"
	rename HLTH_EXP_SUPP TOT_HLTH_EXP_EXTRA
	label var TOT_HLTH_EXP_EXTRA ///
		"Total Health Exp. from the Supp. & Emer. Budget by Year"
			
** *****************************************************************************		
// STEP 5:  Imputing health expenditure from each source
** *****************************************************************************	
	
	// Generate fraction of income from each source	
	bysort YEAR: egen double TOTAL_REG_INCOME = total(INCOME_REG)
	label var TOTAL_REG_INCOME "Total Reg. budget Income by Year"
	
	bysort YEAR: egen double TOTAL_EXTRA_INCOME = total(INCOME_EXTRA)
	label var TOTAL_EXTRA_INCOME "Total Non Reg. Budget Income by Year"
	
	gen double frct_INC_REG = INCOME_REG/TOTAL_REG_INCOME
	label var frct_INC_REG "Fraction of Total reg Income attributed to row"
	
	gen double frct_INC_EXTRA = INCOME_EXTRA/TOTAL_EXTRA_INCOME
	label var frct_INC_EXTRA "Fraction of Total Non-reg Income attributed to row"

	// Generate Imputed Health Expenditures by Source: 
	gen double HLTH_EXP_REG = frct_INC_REG*TOT_HLTH_EXP_REG
	label var HLTH_EXP_REG "Imputed Reg. Budget Health Exp. by Income Source & Year"
	
	gen double HLTH_EXP_NONREG = frct_INC_EXTRA*TOT_HLTH_EXP_EXTRA
	label var HLTH_EXP_NONREG "Imputed Non-Reg. Budget Health Exp. by Income Source & Year"
	
	gen double EXPENDITURE_ALL = HLTH_EXP_REG + HLTH_EXP_NONREG
	label var EXPENDITURE "Imputed Total Health Expenditure by Income Source & Year"
	
	gen OUTFLOW = EXPENDITURE_ALL
	label var OUTFLOW "Imputed Total Health Expenditure by Income Source & Year"
	
	gen GHI = "UNICEF" 

	// Tag double counting 
	replace DONOR_NAME = upper(DONOR_NAME)
		
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, NEW YORK" 
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFP"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "IATF_TC_UNFPA"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA NEW YORK"
	replace DONOR_NAME = "UNFPA" if ///
		DONOR_NAME == "UNFPA PROGRAMME SUPPORT SERVICES"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, HEW YORK"
	replace DONOR_NAME = "UNFPA" if ///
		DONOR_NAME == "UNITED NATIONS POPULATION FUND (UNFPA)"
	replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNITED NATIONS POPULATION FUND"
	
	replace DONOR_NAME = "UNAIDS" if ///
		DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS"
	replace DONOR_NAME = "UNAIDS" if ///
		DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME"
	replace DONOR_NAME = "UNAIDS" if ///
		DONOR_NAME == "UNITED NATIONS JOINT PROGRAMME"
	replace DONOR_NAME = "UNAIDS" if ///
		DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS (UNAIDS)"
	replace DONOR_NAME = "UNAIDS" if ///
		DONOR_NAME == "UNITED NATIONS PROGRAMME ON HIV/AIDS"
	
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO. GENEVA"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO, GENEVA" 
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO.GENEVA"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (EXTRABUDGETARY FUNDS)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (REGULAR BUDGET)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WHO (ROLL BACK MALARIA)"
	replace DONOR_NAME = "WHO" if DONOR_NAME == "WORLD HEALTH ORGANIZATION"
	replace DONOR_NAME = "WHO" if ///
		DONOR_NAME == "WORLD HEALTH ORGANIZATION (WHO)"
	
	replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAHO, WASHINGTON"
	replace DONOR_NAME = "PAHO" if ///
		DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION (PAHO)"
	replace DONOR_NAME = "PAHO" if ///
		DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION"
	
	replace DONOR_NAME = "GFATM" if ///
		DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA"
	replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND"
	replace DONOR_NAME = "GFATM" if DONOR_NAME == ///
		"GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)"

	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINE"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINE"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINES"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINES"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
	replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI ALLIANCE"
	replace DONOR_NAME = "GAVI" if ///
		DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINE IMMUNIZATION (GAVI)"

	replace DONOR_NAME = "IDB" if DONOR_NAME == "INTER-AMERICAN DEVELOPMENT BANK"
	replace DONOR_NAME = "AfDB" if DONOR_NAME == "AFRICAN DEVELOPMENT BANK"
	replace DONOR_NAME = "AsDB" if regexm(DONOR_NAME, "ASIAN DEVELOPMENT BANK")	

	replace DONOR_NAME = "BMGF" if DONOR_NAME == "BILL & MELINDA GATES FOUNDATION"

	replace DONOR_NAME = "WB_IDA" if ///
		regexm(DONOR_NAME, "INTERNATIONAL DEVELOPMENT ASSOCIATION")
	replace DONOR_NAME = "WB" if regexm(DONOR_NAME, "WORLD BANK")		
	
	// Fix incorrect income_sector assignments
	replace INCOME_SECTOR="MULTI" if ///
		inlist(DONOR_NAME, "UNFPA", "UNAIDS", "WHO", "PAHO", "WB")
		
	// Tag double counting
	/* NOTES:
		(1) If UNDP gets added as a channel:
			replace DONOR_NAME = "UNDP" if DONOR_NAME == "UNDP, NEW YORK"
			replace DONOR_NAME = "UNDP" if DONOR_NAME == ///
				regexm(DONOR_NAME, "UNITED NATIONS DEVELOPMENT PROGRAMME")

		(2) No PAHO, GAVI, BMGF
		
		(3) No need to tag Unitaid, Wellcome, or CRS (European Commission) - these 
			are already handled
		
		(4) No need to tag AfDB, IDB, or WB, since these development banks data 
			did not include transfers in the first place; ie they do not report
			expenditure if that money is a transfer.
				replace ELIM_CH = 1 if DONOR_NAME == "AfDB"
				replace ELIM_CH = 1 if DONOR_NAME == "IDB"
				replace ELIM_CH = 1 if DONOR_NAME == "WB_IBRD" | DONOR_NAME=="WB"
				replace ELIM_CH = 1 if DONOR_NAME == "UNDP"
	*/

	gen ELIM_CH = 0
	replace ELIM_CH = 1 if DONOR_NAME == "UNFPA" 											
	replace ELIM_CH = 1 if DONOR_NAME == "UNAIDS"						
	replace ELIM_CH = 1 if DONOR_NAME == "WHO" 	
	replace ELIM_CH = 1 if DONOR_NAME == "PAHO"						
	replace ELIM_CH = 1 if DONOR_NAME == "GAVI" 	
	//replace ELIM_CH = 1 if DONOR_NAME == "GFATM" // this double counting is handled in GFATM code 
	//replace ELIM_CH = 1 if DONOR_NAME == "UNITAID" 
	//replace ELIM_CH = 1 if DONOR_NAME == "UNDP" 
	//replace ELIM_CH = 1 if DONOR_NAME == "AfDB" 
	replace ELIM_CH = 1 if DONOR_NAME == "AsDB" 
	//replace ELIM_CH = 1 if DONOR_NAME == "IDB" 
	//replace ELIM_CH = 1 if DONOR_NAME == "WB" | DONOR_NAME=="WB_IDA"
	gen SOURCE_CH = DONOR_NAME if ELIM_CH==1
	drop ELIM_CH
			
	save "`FIN'/UNICEF_INC_EXP_1990_`data_yr'.dta", replace
	save "`FIN'/archive/UNICEF_INC_EXP_1990_`data_yr'_`dateTag'.dta", replace

	// Get country codes to add ISO codes to Ebola data 
	preserve
		use "`country_codes'", clear
		rename iso3 ISO3_RC
		tempfile temp_rc
		save `temp_rc'
	restore

	// Ebola data is continually updated	
	import delimited using "`ebola_data'", clear case(preserve)
	keep if channel == "UNICEF" & contributionstatus == "paid"
	collapse (sum) amount, by (year source channel recipient_country HFA) fast
	drop if source=="UNICEF" // assuming this is an internal transfer
	rename year YEAR
	drop if YEAR == . // Some funding in 2018 doesn't have a decision date yet (Ebola outbreak in DRC in 2018)
	rename (source channel amount) (DONOR_NAME CHANNEL oid_ebz_DAH)
	gen REPORTING_AGENCY = "EBOLA"
	rename recipient_country country_lc

	replace country_lc = "Cote d'Ivoire" if strpos(country_lc, "Ivoire")
	replace country_lc = "Democratic Republic of the Congo" if country_lc == "Congo, The Democratic Republic of the"

	merge m:1 country_lc using `temp_rc', keepusing(ISO3_RC) keep(1 3) nogen // only QMA doesn't merge
	replace ISO3_RC = "QMA" if country_lc == "QMA" & ISO3_RC == ""
	 
	// Fill in source info
	 gen ISO_CODE = subinstr(DONOR_NAME, "BIL_", "", .)
	 gen INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME, "BIL") | DONOR_NAME == "EC"
	 replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "PRIVATE" | ///
	 	DONOR_NAME == "NGO" | DONOR_NAME == "US_FOUND" | DONOR_NAME == "INT_FOUND"
	 replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
	 replace INCOME_SECTOR = "MULTI" if inlist(DONOR_NAME, "WB")
	 replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR == ""
	 replace ISO_CODE = "NA" if INCOME_SECTOR=="OTHER"
	 replace DONOR_NAME="UNSPECIFIED" if INCOME_SECTOR=="OTHER"
	 
	// Format data
	gen WB_REGION = "Sub-Saharan Africa"
	gen WB_REGIONCODE = "SSA"
	gen LEVEL = "COUNTRY"
	replace LEVEL = "REGIONAL" if ISO3_RC == "QMA"
	gen EBOLA = 1
	collapse (sum) oid_ebz_DAH, by (YEAR DONOR_NAME CHANNEL REPORTING_AGENCY ///
		ISO_CODE INCOME_SECTOR EBOLA) fast
	*/
	replace DONOR_NAME="EUROPEAN COMMISSION" if DONOR_NAME == "EC"
	tempfile unicef_ebola
	save `unicef_ebola'

	sum oid_ebz_DAH
	local total_ebola = `r(sum)'

	// We're not able to merge on Ebola funding in the current year but we 
	// can use already-existing funding for predictions
	keep if YEAR == `report_yr'
	tempfile ebola_`report_YY'
	save `ebola_`report_YY''

	use "`FIN'/UNICEF_INC_EXP_1990_`data_yr'.dta", clear
	keep YEAR CHANNEL SOURCE_DOC INCOME_SECTOR INCOME_TYPE DONOR_NAME ///
		DONOR_COUNTRY ISO_CODE OUTFLOW SOURCE_CH
	collapse (sum) OUTFLOW, by(YEAR CHANNEL INCOME_SECTOR INCOME_TYPE ///
		DONOR_NAME DONOR_COUNTRY ISO_CODE SOURCE_CH)

	tempfile inc_exp
	save `inc_exp'

	preserve
		duplicates tag YEAR ISO_CODE OUTFLOW, gen(dup)
* LOOK INTO DUPLICATES HERE 
	restore

	use `inc_exp', clear
	merge m:1 YEAR ISO_CODE INCOME_SECTOR using `unicef_ebola'

* manual_inspections
//------------------------------------------------------------------------------
	// Look for bad merges
	li if EBOLA == 1
	
	// Deal with duplicates
	duplicates tag YEAR ISO_CODE INCOME_SECTOR oid_ebz_DAH, gen(dup)
	gen double OUTFLOW_pos = OUTFLOW if OUTFLOW > 0
	bysort YEAR ISO_CODE INCOME_SECTOR: egen double tot = total(OUTFLOW_pos)
	gen double ebz_frct = OUTFLOW_pos / tot
	replace ebz_frct = 0 if ebz_frct == .
	replace oid_ebz_DAH = oid_ebz_DAH * ebz_frct if _m == 3 & dup > 0
	drop dup OUTFLOW_pos tot ebz_frct
//------------------------------------------------------------------------------

	// Extract un-merged ebola data
	preserve
		keep if _m == 2
		drop INCOME_SECTOR INCOME_TYPE SOURCE_CH OUTFLOW _m
		// Rename variables to allow for second m:1 merge below
		rename (oid_ebz_DAH EBOLA) (oid_ebz_DAH2 EBOLA2)
		tempfile unicef_ebola2
		save `unicef_ebola2'
	restore

	// Drop unmerged from using (ie no corresponding observation in UNICEF data,
	// too generic, or poorly described)
	drop if _m == 2
	drop _m
		// Why drop _m == 2? Per Catherine Chen, 
		// "Yep this is intentional, I remember we don't want to add any UNOCHA 
		// income sources that aren't already in our DAH estimates. So if the 
		// UNICEF income sheet says they received money from UNAIDS but not PAHO 
		// in 2012, but the UNOCHA data says UNICEF received PAHO data, we would 
		// trust the UNICEF income sheet over UNOCHA.

	// Check for duplicates from m:1 merge
	duplicates tag YEAR ISO_CODE INCOME_SECTOR oid_ebz_DAH, gen(dup)
	count if dup != 0 & oid_ebz_DAH != 0 & oid_ebz_DAH != .
	if `r(N)' != 0 {
		di as error "There are duplicate ebola merges!"
		assert `r(N)' == 0
	}
	drop dup

	/* NOTE
		The merge below is specifically for World Bank and other relevant ebola
		funding missed from the previous merge. All other Ebola funding 
		is either too vague or isn't a donor in UNICEF data
	*/
	merge m:1 YEAR DONOR_NAME using `unicef_ebola2'
	replace EBOLA = 1 if _m == 3 & EBOLA == .
	replace REPORTING_AGENCY = "EBOLA" if _m == 3 & oid_ebz_DAH == .
	replace oid_ebz_DAH = oid_ebz_DAH2 if _m == 3 & oid_ebz_DAH == .

* manual_inspections
//------------------------------------------------------------------------------
	// Look for missed ebola disbursements
	li if _m == 2
		// The European Commission observations did not merge b/c the UNICEF
		// data has the donor name as "European Commission/ECHO"
	replace EBOLA = 1 if YEAR == 2014 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .
	replace REPORTING_AGENCY = "EBOLA" if YEAR == 2014 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .
	replace oid_ebz_DAH = 3926422 if YEAR == 2014 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .

	replace EBOLA = 1 if YEAR == 2015 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .
	replace REPORTING_AGENCY = "EBOLA" if YEAR == 2015 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .
	replace oid_ebz_DAH = 922000 if YEAR == 2015 & _m != 2 & ///
		regexm(DONOR_NAME, "EUROPEAN COMMISSION") & oid_ebz_DAH == .

	// No need to replace report year since there is no data there yet
//------------------------------------------------------------------------------

	// Drop too-generic ebola disbursements
	drop if _m == 2
	drop _m EBOLA2 oid_ebz_DAH2
	/* NOTE
		FGH 2018: While BMGF and the European Commission are donors to UNICEF 
		in the past, they aren't listed as donors in the annual report in the 
		years that they exist in the Ebola dataset, so these weren't merged on.

		Now make an Ebola file for merging onto the INC_EXP during predictions. 
		This contains the Ebola funding in observed years which we have matching 
		donor/years in the UNICEF dataset as well as 2018 data.
	*/

	preserve
		keep if oid_ebz_DAH != .
		append using `ebola_`report_YY'', force
		save "`INT'/UNICEF_EBOLA_FGH`report_yr'.dta", replace 
	restore

	// 2017 note: need to drop the entries that aren't merged or else 
	// OUTFLOW=negative ebz_other_DAH_15
	foreach var of varlist EBOLA oid_ebz_DAH OUTFLOW {
		replace `var' = 0 if `var' == .
	}

	collapse (sum) OUTFLOW oid_ebz_DAH, by (YEAR CHANNEL INCOME_SECTOR ///
		INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO_CODE ///
		REPORTING_AGENCY EBOLA SOURCE_CH)
	// for covid subtraction allocation (UNICEF_ADB_PDB_COVID_calc.R)
	save "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_prep.dta", replace

 	use "`FIN'/UNICEF_ADB_PDB_FGH_prepfix.dta", clear
	// Since we separated ebola from the rest of health spending be sure to 
	// adjust accordingly
	//replace OUTFLOW = OUTFLOW - oid_ebz_DAH
	replace OUTFLOW = OUTFLOW - (oid_ebz_DAH + oid_covid_OUTFLOW)
	
	// Allocate from Source to Channel to Health Focus area (merge ADB and PDB)

	rename OUTFLOW DAH
	gen ISO3_RC = "NA"
	
	tempfile compile
	save `compile'

	import excel using "`RAW'/UNICEF_EXP_BYYEAR_1990-`data_yr'.xlsx", ///
		sheet("Stata - HFA summary") firstrow clear
	drop check
	ren year YEAR

	ds YEAR, not
	local hfas "`r(varlist)'"

	merge 1:m YEAR using `compile'
	drop _m

	// Split out HFAs by percentage 
	foreach hfa in `hfas' {
		gen `hfa'_DAH = `hfa' * DAH
	}

	gen gov = 0

	// Subtract out negative disbursements from the year before
	// group to create id
	egen temp_id = group(DONOR_NAME DONOR_COUNTRY INCOME_SECTOR)
	xtset temp_id YEAR

	// replace with other minus other of the previous year, and set the 
	// negative other to 0
	foreach hfa in `hfas' {
		sort temp_id YEAR
		forval year = `data_yr'(-1)1991 {
			replace `hfa'_DAH = `hfa'_DAH + F.`hfa'_DAH if ///
				YEAR == `year'-1 & F.`hfa'_DAH < 0
			replace `hfa'_DAH = 0 if YEAR == `year' & `hfa'_DAH < 0
		}
		replace `hfa'_DAH = 0 if YEAR == 1990 & `hfa'_DAH < 0
	}

	// Check that there are no negative DAH values
	foreach var of varlist *_DAH {
		capture sum `var'
		local r_min_`var' `r(min)'
		if `r_min_`var'' < 0 {
			error_negative_dah
		}
	}
	// Save out a prepfile for hte second time, run the rest of the R code, and then pull it back in to stata
	// save "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_hfa.dta", replace

 	// use "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_hfafix.dta", clear
	// Calculate a new total as the sum of the HFAs since Ebola is added in 
	// addition to DAH pulled from UNICEF reports 
	drop DAH
    rename oid_covid_OUTFLOW oid_covid_DAH
	egen double DAH = rowtotal(*_DAH)
	
	// Add in-kind ratios 
	preserve
		import excel using "`RAW'/UNICEF_INKIND_RATIOS_1990_`report_yr'.xlsx", ///
			firstrow clear
		drop if YEAR == .
		tempfile ikratio 
		save `ikratio'
	restore

	preserve
		merge m:1 YEAR using `ikratio', keepusing(INKIND_RATIO) nogen
		// Add in-kind for 2012+ (previously, in-kind was included in total)
		foreach var of varlist DAH *_DAH {
			replace `var' = `var' * (1 - INKIND_RATIO) if YEAR <= 2011
		}
		tempfile real_tot 
		save `real_tot'
	restore

	preserve
		merge m:1 YEAR using `ikratio', keepusing(INKIND_RATIO) nogen
		foreach var of varlist DAH *_DAH { 
			replace `var' = `var' * INKIND_RATIO
		}
		gen INKIND = 1
		tempfile inkind
		save `inkind', replace
	restore
	
	use `real_tot', clear
	append using `inkind'
	replace INKIND = 0 if INKIND == .	

	keep YEAR INCOME_SECTOR DONOR_NAME DONOR_COUNTRY ISO_CODE ISO3_RC gov ///
		CHANNEL INKIND INKIND_RATIO SOURCE_CH *_DAH DAH

	drop if YEAR == `report_yr'

	save "`FIN'/UNICEF_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC.dta", replace
	save "`FIN'/archive/UNICEF_ADB_PDB_FGH`report_yr'_ebola_fixed_includesDC_`dateTag'.dta", replace

** END FILE **

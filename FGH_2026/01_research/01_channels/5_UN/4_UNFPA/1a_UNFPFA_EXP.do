** *****************************************************************************
** Project:	FGH
** Purpose:	Generate dataset with UNFPA expenditures imputed by sources of income
** Key updates: Updated some of the inputs
** *****************************************************************************

** *****************************************************************************
** INITIAL SETUP
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
** USER INPUTS
	local report_yr = 2024	// FGH report year
	local update_tag "20250522" // date of update

	// File paths
	local RAW 		"FILEPATH/RAW"
	local INT 		"FILEPATH/INT"

	//local INT       "FILEPATH"
	local FIN 		"FILEPATH/FGH_`report_yr'"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local CODES 	"FILEPATH/FGH_`report_yr'"

	// Files
	local country_codes "`CODES'/countrycodes_official.dta"
	local ebola_data "FILEPATH/ebola_all_collapsed.csv"
	local income_sector_type "FILEPATH/income_sector_and_type_assignments_2021.dta"
	
    capture mkdir "`INT'/FGH_`report_yr'", public

** *****************************************************************************
** DERIVED MACROS
	local data_yr = 2022	// Last year of data that needs to be processed by this script
	local short_data_yr = substr("`data_yr'", 3, 4)

** *****************************************************************************
** STEP 1: Compile all income and expenditure data
** *****************************************************************************

** *****************************************************************************
** Import datasets

	import delimited using ///
		"`RAW'/UNFPA_Audited_Financial_report_`data_yr'.csv", ///
		varnames(1) case(preserve) clear

	save "`INT'/unfpa_inc_exp_`short_data_yr'.dta", replace
	
	local years "9091 9293 9495 9697 9899 0001 0203 0405 06 07 08_updated20191028 09_updated20191028 1011 1112 1213 1314"
	
	forval YY=15/`short_data_yr' {
		local years "`years' `YY'"
	}

	foreach year in `years' {
		use "`INT'/unfpa_inc_exp_`year'.dta", clear
		gen id = _n
		
		if "`year'" == "08_updated20191028" {
			tempfile prelim_08
			save `prelim_08', replace
		}
		else if "`year'" == "09_updated20191028" {
			tempfile prelim_09
			save `prelim_09', replace
		}
		else {
			tempfile prelim_`year'
			save `prelim_`year'', replace
		}
	} 
			
** *****************************************************************************
** Clean datasets

	// 1990-2005
	foreach year in 9091 9293 9495 9697 9899 0001 0203 0405 { 
		use `prelim_`year'', clear
		di "`year'"
		
		// Not including Incomes & Expenditure Pertaining to Cost Sharing and Procurement:
		drop  INCOME_COST_SHRNG EXP_COST_SHRNG INCOME_PROC EXP_PROC

		// Clean .s
		foreach var in INCOME_REG EXP_REG INCOME_TRUST EXP_TRUST INCOME_JPOs ///
				EXP_JPOs INCOME_OTHER EXP_OTHER { 
			replace `var' = 0 if `var' == .
		} 

		// Split biannuals into annuals
		sort id 
		expand 2
		foreach var in   INCOME_REG EXP_REG INCOME_TRUST EXP_TRUST INCOME_JPOs ///
				EXP_JPOs INCOME_OTHER EXP_OTHER { 
			replace `var' = `var'/2
		} 
		sort id
		gen odd = mod(_n,2)

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
		drop odd
		
		tempfile temp_`year'
		save `temp_`year'', replace
	}
			
	// 2006-2009
	foreach year in 06 07 08 09 {
		use `prelim_`year'', clear
		di "`year'"
		
		tostring YEAR, replace
		capture rename ISO3 ISO_CODE
		drop  INCOME_COST_SHRNG EXP_COST_SHRNG INCOME_PROC EXP_PROC
		
		// Clean .s
		foreach var in  INCOME_REG EXP_REG INCOME_TRUST EXP_TRUST INCOME_JPOs ///
				EXP_JPOs INCOME_OTHER EXP_OTHER { 
			replace `var' = 0 if `var' == .
		} 
			
		tempfile temp_`year'
		save `temp_`year'', replace
	}

	// 2010
	use `prelim_1011', clear

	tostring YEAR, replace
	
	 // Drop year 2011 because new data has 2011 and 2012
	drop if YEAR == "2011"
	
	rename DONOR_COUNTRY country_lc
	replace country_lc = "Macedonia" if ///
		country_lc == "the former Yugoslav Republic of Macedonia"

	// Merge country codes
	merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
	rename countryname_ihme DONOR_COUNTRY
	drop if _m == 2
	//list DONOR_NAME _merge if _merge != 3
	//drop _m country_lc
	rename iso3 ISO_CODE
	
	// Clean .s
	foreach var in INCOME_REG EXP_REG INCOME_TRUST EXP_TRUST INCOME_JPOs ///
			EXP_JPOs INCOME_OTHER EXP_OTHER { 
		replace `var' = 0 if `var' == .
	}
	
	//tempfile temp_1011
	//save `temp_1011', replace
	tempfile temp_10
	save `temp_10'
		
	// 2011
	use `prelim_1112', clear
	
	tostring YEAR, replace

	// Drop year 2012 because new data has 2012 and 2013
	drop if YEAR == "2012" 
	
	rename DONOR_COUNTRY country_lc
	replace country_lc = "Macedonia" if ///
		country_lc == "the former Yugoslav Republic of Macedonia"

	// Country codes
	merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
	rename countryname_ihme DONOR_COUNTRY
	drop if _m == 2
	//list DONOR_NAME _merge if _merge != 3
	//drop _m country_lc
	rename iso3 ISO_CODE
	
	// Clean .s
	foreach var in INCOME_REG EXP_REG INCOME_TRUST EXP_TRUST INCOME_JPOs ///
			EXP_JPOs INCOME_OTHER EXP_OTHER { 
		replace `var' = 0 if `var' == .
	} 
	
	//tempfile temp_1112
	//save `temp_1112', replace
	tempfile temp_11
	save `temp_11'
		
	// 2012
	use `prelim_1213', clear
	
	tostring YEAR, replace

	// Drop year 2012 because new data has 2012 and 2013
	drop if YEAR == "2013" 
	
	rename DONOR_COUNTRY country_lc
	replace country_lc = "Bahamas" if country_lc == "Bahamas (the)"
	replace country_lc = "Dominican Republic" if country_lc == "Dominican Republic (the)"
	replace country_lc = "Occupied Palestinian Territory" if ///
		country_lc == "State of Palestine"
	
	// Merge country codes
	merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
	rename countryname_ihme DONOR_COUNTRY
	drop if _m == 2
	//list DONOR_NAME _merge if _merge != 3
	//drop _m country_lc
	rename iso3 ISO_CODE
	
	// Clean .s
	foreach var in "INCOME_REG" "EXP_REG" "INCOME_TRUST" "EXP_TRUST" "INCOME_JPOs" ///
			"EXP_JPOs" "INCOME_OTHER" "EXP_OTHER" {
		replace `var' = 0 if `var' == .
	}
	
	//tempfile temp_1213
	//save `temp_1213', replace
	tempfile temp_12
	save `temp_12'
		
	// 2013-2014
	use `prelim_1314', clear
	
	tostring YEAR, replace
	
	rename DONOR_COUNTRY country_lc
	replace country_lc = subinstr(country_lc, " (the)", "", 1)
	replace country_lc = "United Kingdom" if country_lc == "United Kingdom of Great Britain"
	replace country_lc = "Switzerland" if country_lc == "Switzerland "
	replace country_lc = "Occupied Palestinian Territory" if ///
		country_lc == "State of Palestine"
	replace country_lc = "Democratic People's Republic of Korea" if ///
		country_lc == "Democratic People's Rep. of Korea"
	replace country_lc = subinstr(country_lc, " (the Republic of)", "", 1) if ///
		regexm(country_lc, "Congo ") | regexm(country_lc, "Botswana")
	
	// Merge country codes
	merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
	rename countryname_ihme DONOR_COUNTRY
	drop if _m == 2
	//list DONOR_NAME _merge if _merge != 3
	//drop _m country_lc
	rename iso3 ISO_CODE
	
	// Clean .s
	foreach var in "INCOME_REG" "EXP_REG" "INCOME_TRUST" "EXP_TRUST" "INCOME_JPOs" ///
			"EXP_JPOs" "INCOME_OTHER" "EXP_OTHER" {
		replace `var' = 0 if `var' == .
	}
	
	tempfile temp_1314
	save `temp_1314', replace

	// Import 2015+
	forval YY=15/`short_data_yr' {
		use `prelim_`YY'', clear
        di "****** `YY'"

		capture drop if YEAR != "20`YY'"
		
		tostring YEAR, replace
        tostring INCOME_SECTOR, replace
        tostring INCOME_TYPE, replace
        tostring DONOR_COUNTRY, replace
        tostring DONOR_NAME, replace

        replace INCOME_SECTOR = trim(INCOME_SECTOR)
        replace INCOME_TYPE = trim(INCOME_TYPE)
        replace DONOR_COUNTRY = trim(DONOR_COUNTRY)
        replace DONOR_NAME = trim(DONOR_NAME)
		
		rename DONOR_COUNTRY country_lc

		// Country cleanup
		replace country_lc = subinstr(country_lc, " (the)", "", 1)
		replace country_lc = "United Kingdom" if ///
			country_lc == "United Kingdom of Great Britain"
		replace country_lc = "Democratic People's Republic of Korea" if ///
			country_lc == "Democratic People's Rep. of Korea"
		replace country_lc = "Afghanistan" if regexm(country_lc, "Afghanistan")
		replace country_lc = "Burkina Faso" if regexm(country_lc, "Burkina Faso")
		replace country_lc = "Chad" if regexm(country_lc, "Chad")
		replace country_lc = subinstr(country_lc, " (the Republic of)", "", 1) if ///
			regexm(country_lc, "Congo ") | ///
			regexm(country_lc, "Botswana") | ///
			regexm(country_lc, "Iran")
		replace country_lc = "Russian Federation" if ///
			DONOR_NAME == "Russian Federation (the)"
		replace country_lc = "Liberia" if country_lc == "Liberia "
		replace country_lc = "Nigeria" if country_lc == "Nigeria "
		replace country_lc = "Uruguay" if country_lc == "Uruguay Venezuela"
		replace country_lc = "Indonesia" if country_lc == "Indonesia (the Republic of)"
		replace country_lc = "Swaziland" if country_lc == "Eswatini"
		replace country_lc = "Occupied Palestinian Territories" if ///
			country_lc == "State of Palestine"
		replace country_lc = "Yugoslavia" if ///
			regexm(country_lc, "the former Yugoslav of Macedonia")
		replace country_lc = "Cote d'Ivoire" if regexm(country_lc, "D'Ivoire")
		replace country_lc = "Cote d'Ivoire" if regexm(country_lc, "Ivoire")
		replace country_lc = "Gambia" if country_lc == "Gambia (Republic of The)"
		replace country_lc = "Lao People's Democratic Republic" if ///
		    regexm(country_lc, "Lao People")
		replace country_lc = "Macedonia" if country_lc == "North Macedonia"
		replace country_lc = "Sao Tome and Principe" if ///
			ustrregexm(country_lc, "S.o Tom. and Pr.ncipe")

		// Merge country codes
		merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
		rename countryname_ihme DONOR_COUNTRY
		drop if _m == 2
		//list DONOR_NAME _merge if _merge != 3
		//drop _m country_lc
		rename iso3 ISO_CODE
		
		// Clean .s and ,s
	    foreach var in "INCOME_REG" "EXP_REG" "INCOME_TRUST" "EXP_TRUST" "INCOME_JPOs" ///
	    		"EXP_JPOs" "INCOME_OTHER" "EXP_OTHER" {
            destring `var', replace ignore(",")
	    	replace `var' = 0 if `var' == .
	    }
		
		tempfile temp_`YY'
		save `temp_`YY'', replace
	}

** *****************************************************************************
** APPEND 
	//local years "9293 9495 9697 9899 0001 0203 0405 06 07 08 09 1011 1112 1213 1314"
	local years "9293 9495 9697 9899 0001 0203 0405 06 07 08 09 10 11 12 1314"
	forval YY=15/`short_data_yr' {
		local years "`years' `YY'"
	}

	use	`temp_9091', clear

	foreach year in `years' {
		append using `temp_`year'', force
	}
	
	// Check that there are no countries that did not merge w/ countrycodes
	// If there are, go in and manually fix them
	// Note that there are non-countries that will be listed as _m == 1 or 
	// _m == 2 (eg "Other Income", "Unspecified", etc)

	// browse YEAR DONOR_NAME if _merge == 1 | _merge == 2
    list YEAR DONOR_NAME country_lc ISO_CODE if _merge == 1 | _merge == 2
	// checked and no countries

	drop _merge country_lc
	
	// Fix INCOME_SECTOR
	/* NOTE:
		These observations are true unallocable income sector because they are 
		adjustments we made so that total co-financing by country matches total
		co-financing reported in the financial review
	*/
	replace INCOME_SECTOR = "UNALL" if DONOR_NAME == "Unspecified" & INCOME_TRUST != 0
	replace INCOME_TYPE = "UNALL" if DONOR_NAME == "Unspecified" & INCOME_TRUST != 0

	// The rest don't seem to be actually unallocable income sector because they
	// seem to have once been items on an income statement (referring to the donor names)
	replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR == "UNSP" | INCOME_SECTOR == "NA"
	replace INCOME_SECTOR = "MULTI" if INCOME_SECTOR == "MUILTI"
	replace INCOME_TYPE = "CENTRAL" if INCOME_TYPE == "CENTRA:"
	replace INCOME_TYPE = "DEVBANK" if INCOME_TYPE == "DEVBANKS"
	replace INCOME_TYPE = "IND" if INCOME_TYPE == "INDIV"
	replace INCOME_SECTOR = "OTHER" if INCOME_TYPE == "EC" & INCOME_SECTOR == "PRIVATE"
	
	
** *****************************************************************************
** STEP 2: Cleaning/fixing the data
** *****************************************************************************		
	label var INCOME_REG "Regular Budget Income (by Year & Source)"
	rename EXP_REG TOT_EXP_REG
	label var TOT_EXP_REG "Total Regular Budget Expenditure (by Year)"
	label var INCOME_TRUST "Trust Fund Income (by Year & Source)"
	label var EXP_TRUST "Trust Fund Expenditure (by Year & Source)"
	label var INCOME_JPOs "JPO Income (by Year & Source)"
	rename EXP_JPOs TOT_EXP_JPOs
	label var TOT_EXP_JPOs "JPO Expenditure (By Year)"
	label var INCOME_OTHER "Other Income (by Year & Source)"
	label var EXP_OTHER "Other Expenditure (by Year & Source)"

	// Generate imputed health expenditures by source
	/* NOTE:
		In years 1990-2005 and 2007, trust fund 
		income and expenditure are split into individual funds in UNFPA's financial 
		statements. Therefore, no imputation of health expenditure by source is 
		necessary. However, there is interest income to trust funds with no 
		associated expenditure. These values are imputed by calculating what 
		percentage of total trust fund income these interest observations comprise 
		and assigning that percentage of total trust fund expenditure to those 
		observations.

		In 2006 and 2008+, we have a total trust fund expenditure and trust 
		fund Income by source; therefore, we need to impute expenditure by source.

		In 2006, the variable EXP_TRUST contains the total trust fund expenditure 
		for the year in every observation.
		
		In 2008 and on, the total trust fund expenditure is contained in a single 
		observation with DONOR_NAME == "Program expenditure"

		Once all imputation is complete, these observations are dropped.
	*/

	// Calculate annual totals
	bysort YEAR: egen double TOT_REG_INCOME = total(INCOME_REG)
	label var TOT_REG_INCOME "Total Reg. budget Income by Year"
	
	bysort YEAR: egen double TOT_TRUST_INCOME = total(INCOME_TRUST)
	label var TOT_TRUST_INCOME "Total Trust budget Income by Year"
	
	bysort YEAR: egen double TOT_EXP_TRUST = total(EXP_TRUST)
	
	bysort YEAR: egen double TOT_JPO_INCOME = total(INCOME_JPOs)
	label var TOT_JPO_INCOME "Total JPO Income by Year"
	
	// Calculate fractions
	gen frct_INCOME_REG = INCOME_REG / TOT_REG_INCOME
	gen frct_TRUST_INCOME = INCOME_TRUST / TOT_TRUST_INCOME
	gen frct_JPO_INCOME = INCOME_JPOs / TOT_JPO_INCOME
	
	// Impute expenditures
	gen double EXP_REG = frct_INCOME_REG * TOT_EXP_REG
	gen double EXP_TRUSTnoimp = frct_TRUST_INCOME * TOT_EXP_TRUST  if ///
		YEAR == "1990" | YEAR == "1991" | YEAR == "1992" | YEAR == "1993" | ///
		YEAR == "1994" | YEAR == "1995" | YEAR == "1996" | YEAR == "1997" | ///
		YEAR == "1998" | YEAR == "1999" | YEAR == "2000" | YEAR == "2001" | ///
		YEAR == "2002" | YEAR == "2003" | YEAR == "2004" | YEAR == "2005" | ///
		YEAR == "2007" 
	gen double EXP_TRUSTimp = frct_TRUST_INCOME * EXP_TRUST if YEAR == "2006"

	replace EXP_TRUSTimp = frct_TRUST_INCOME * TOT_EXP_TRUST if ///
		YEAR != "1990" & YEAR != "1991" & YEAR != "1992" & YEAR != "1993" & ///
		YEAR != "1994" & YEAR != "1995" & YEAR != "1996" & YEAR != "1997" & ///
		YEAR != "1998" & YEAR != "1999" & YEAR != "2000" & YEAR != "2001" & ///
		YEAR != "2002" & YEAR != "2003" & YEAR != "2004" & YEAR != "2005" & ///
		YEAR != "2006" & YEAR != "2007"

	replace EXP_TRUST = EXP_TRUSTnoimp if INCOME_SECTOR == "OTHER" & ///
		(YEAR == "1990" | YEAR == "1991" | YEAR == "1992" | YEAR == "1993" | ///
		YEAR == "1994" | YEAR == "1995" | YEAR == "1996" | YEAR == "1997" | ///
		YEAR == "1998" | YEAR == "1999" | YEAR == "2000" | YEAR == "2001" | ///
		YEAR == "2002" | YEAR == "2003" | YEAR == "2004" | YEAR == "2005" | ///
		YEAR == "2007")

	replace EXP_TRUST = EXP_TRUSTimp if ///
		YEAR != "1990" & YEAR != "1991" & YEAR != "1992" & YEAR != "1993" & ///
		YEAR != "1994" & YEAR != "1995" & YEAR != "1996" & YEAR != "1997" & ///
		YEAR != "1998" & YEAR != "1999" & YEAR != "2000" & YEAR != "2001" & ///
		YEAR != "2002" & YEAR != "2003" & YEAR != "2004" & YEAR != "2005" & ///
		YEAR != "2007"

	drop EXP_TRUSTnoimp EXP_TRUSTimp
	drop if DONOR_NAME == "Program expenditure"
	
	gen EXP_JPOs = frct_JPO_INCOME*TOT_EXP_JPOs
	
	// Clean .s
	foreach var in EXP_REG EXP_TRUST EXP_JPOs EXP_OTHER { 
		replace `var' = 0 if `var' == .
	} 

** *****************************************************************************
** MINOR FIXES
	gen double INCOME_ALL =  INCOME_REG + INCOME_TRUST + INCOME_JPOs + INCOME_OTHER
	gen double INCOME_NONREG = INCOME_TRUST + INCOME_JPOs + INCOME_OTHER
	gen double EXP_NONREG = EXP_TRUST + EXP_JPOs + EXP_OTHER
	gen double EXPENDITURE_ALL = EXP_REG + EXP_TRUST + EXP_JPOs + EXP_OTHER
	
	gen OUTFLOW = EXPENDITURE_ALL
	
	gen GHI = "UNFPA"
	destring YEAR, replace

	// Fix countries that no longer exist - not sure this is correct
	replace ISO_CODE = "CZE" if ISO_CODE=="CZE_FRMR" & YEAR > 1992
		// Assuming this is money from the Czech Republic and not Slovakia that 
		// had the wrong donor country name added
	replace ISO_CODE = "MKD" if ISO_CODE=="YUG_FRMR" & YEAR > 1992
	
	replace ISO_CODE = "PER" if ISO_CODE == "PRU"
	replace ISO_CODE = "PER" if DONOR_COUNTRY == "Peru" & ISO_CODE == "NA"
	replace ISO_CODE = "MAR" if ISO_CODE == "MOR"
	replace ISO_CODE = "BOL" if ISO_CODE == "BLO"
	replace ISO_CODE = "NA" if ISO_CODE == "UNSP" | ISO_CODE == ""


	// Save out
	drop if YEAR > `report_yr'
	// save "`FIN'/UNFPA_INC_EXP_1990_`data_yr'_`update_tag'.dta", replace
    save "`INT'/FGH_`report_yr'/INC_EXP_INITIAL.dta" , replace

** END OF FILE **

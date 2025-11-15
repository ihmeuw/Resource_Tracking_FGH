*** *************************************************
// Project:	FGH
// Purpose:	Cleaning, merging, and appending data together for 2013-2014	
*** *************************************************	

	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

	**// FGH report year
	local report_yr = 2024
	**// Two-digit year of update (if the year is biennium, name as both years, e.g. "10_11")				
	local update_yr = `report_yr' - 2001


	local RAW		"FILEPATH/RAW/FGH_`report_yr'"
	local INT		"FILEPATH/INT/FGH_`report_yr'"
	local INT_OLD	"FILEPATH/INT"
	local CODES 	"FILEPATH/FGH_`report_yr'"

**	/*insheet using "FILEPATH/fgh_custom_location_set.csv", comma clear
**	ren location_name countryname_ihme
**	ren ihme_loc_id iso3
**	tempfile locations
**	save `locations'*/

** ****
**// Step 1: Create tempfiles from the income and expenditure spreadsheets
** ****
	
**	// a.) Assessed contributions and expenditure 
		
		// import excel using "`RAW'/WHO_`update_yr'_Assessed_contributions.xlsx", first clear 
		import delimited using "`RAW'/WHO_`update_yr'_Assessed_contributions.csv", clear
        foreach var of varlist * {
            rename `var' `=strupper("`var'")'
        }
		replace DONOR_NAME = upper(DONOR_NAME)
		replace DONOR_COUNTRY = upper(DONOR_COUNTRY)
		destring REG*, replace
		recast long REG_COLL_PRIOR REG_NA_20`update_yr' REG_COLL_20`update_yr'
		replace DONOR_NAME = "AFGHANISTAN" if DONOR_NAME == "AFGHANISTANA"

		tempfile assessed
		save `assessed', replace
	
**	// b.)  Contributions to general fund (VFHP)
		import delimited using "`RAW'/WHO_`update_yr'_General_fund.csv", clear 
		rename *, upper 
		
		rename DONOR DONOR_NAME
		replace DONOR_NAME = upper(DONOR_NAME)
		replace DONOR_COUNTRY = upper(DONOR_COUNTRY)
		replace DONOR_COUNTRY = "CANADA" if DONOR_COUNTRY == "CANDADA"
		replace DONOR_COUNTRY = "GERMANY" if DONOR_COUNTRY == "GERMANT"
		replace DONOR_COUNTRY = "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND" if DONOR_COUNTRY == "UNITED KINGDOM"

		cap drop if DONOR_NAME == ""

		cap destring TOTAL*, replace


		tempfile generalfund
		save `generalfund', replace
		
	// c.) Contributions to the fiduciary fund
		// import excel using  "`RAW'/WHO_`update_yr'_VFHP_FIDUCIARY_FUND.xlsx", first clear 
		import delimited using  "`RAW'/WHO_`update_yr'_Fiduciary_fund.csv", clear 
        rename *, upper

		replace DONOR_NAME = upper(DONOR_NAME)
		replace DONOR_COUNTRY = upper(DONOR_COUNTRY)
		cap destring TOTAL*, replace


		rename TOTAL EB_VFHP

		// Fixes - for old files
		replace DONOR_NAME = "UNITED ARAB EMIRATES" if DONOR_NAME == "UNITEDARABEMIRATES"
		replace DONOR_NAME = "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND" if DONOR_NAME == "UNITEDKINGDOMOFGREATBRITAINANDNORTHERNIRELAND"
		replace DONOR_NAME = "UNITED REPUBLIC OF TANZANIA" if DONOR_NAME == "UNITEDREPUBLICOFTANZANIA"
		replace DONOR_NAME = "UNITED STATES OF AMERICA" if DONOR_NAME == "UNITEDSTATESOFAMERICA"
		replace DONOR_NAME = "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)"
		replace DONOR_NAME = "Children's Investment Fund Foundation (UK)" if DONOR_NAME=="Children’s Investment Fund Foundation (UK)"
		replace DONOR_NAME = "Other and miscellaneous receipts" if DONOR_NAME=="Other and miscellaneous receipts (note 3)"
		replace DONOR_NAME = "Refunds to donors (note 4)" if DONOR_NAME=="Refunds to donors (note 3)"

		// Merge in income sector, income type, and country from contributions to the general fund
		merge m:1 DONOR_NAME using `generalfund', keepusing(INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY)
		**// 321 observations with no contributions to the fiduciary fund; 282 in FGH17; 224 in FGH19
		keep if _m != 2		
		drop _m

		

**	// d.) Append general fund data to export and fill in the remaining donors that do not have income sector/type/country data
		append using `generalfund'
		drop if DONOR_NAME == ""
		rename TOTAL total
		replace EB_VFHP = total if EB_VFHP == .
		drop total
		
**		// Collapse donations by the same organization/government together.
		replace INCOME_TYPE = "NGO" if DONOR_NAME=="SIGHTSAVERS"
		replace DONOR_COUNTRY = "UNITED STATES" if DONOR_COUNTRY == "UNITED STATES OF AMERICA"
		replace DONOR_COUNTRY = "UNITED STATES" if DONOR_NAME == "TASK FORCE FOR GLOBAL HEALTH (TFGH)"



		collapse (sum) EB_VFHP, by(DONOR_NAME INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY)
		replace INCOME_SECTOR="UNSP" if INCOME_SECTOR=="" & INCOME_TYPE=="NA"
		
**		// Most observations now have non-missing income sector, type, and country.
**		// Outsheet the data as "TO_FILL_IN.xlsx" and save as final/edited version as "WHO_`update_yr'_VFHP_INT_FINAL.csv"
		export excel using "`RAW'/WHO_`update_yr'_VFHP_INT_TO_FILL_IN.xlsx", replace firstrow(variables)

//!!!

		import excel using "`RAW'/WHO_`update_yr'_VFHP_INT_FINAL.xlsx", first clear 
		
**		// Test if we need to correct ourselves
		foreach var of varlist DONOR_NAME INCOME* {
			display in red "we are onto `var'"
			quietly count if `var' == ""
			if `r(N)'>0 {
				display in red "need to correct quickly"
				local need_to_fix = 1
			}
			else {
				local need_to_fix = 0
			}
		}
        replace DONOR_COUNTRY = "QZA" if DONOR_COUNTRY == "" 
		if `need_to_fix' != 1 {
			export delimited "`RAW'/WHO_`update_yr'_VFHP_INT_FINAL.csv", replace
		}

** ****
**// Step 2: Append all the dataset together 
** ****	
		insheet using "`RAW'/WHO_`update_yr'_VFHP_INT_FINAL.csv", names comma clear case
		append using `assessed'

		// Fill in EXP_REG,  EXP_EB_VFHP and EXP_OTHER
			egen double EXP = mean(EXP_REG)
			drop EXP_REG
			rename EXP EXP_REG
			
			egen double EXPEB = mean(EXP_EB_VFHP)
			drop EXP_EB_VFHP
			rename EXPEB EXP_EB_VFHP
			
			egen double EXPOT = mean(EXP_OTHER)
			drop EXP_OTHER
			rename EXPOT EXP_OTHER		
			
**	// Collapse observations together that are the same; e.g., country governments contribute to general budget and to VFHP often.
		collapse (sum) EB_VFHP REG_NA_20`update_yr' REG_COLL_20`update_yr' REG_COLL_PRIOR (mean) EXP_REG EXP_EB_VFHP EXP_OTHER, by(DONOR_NAME INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY)
		sort DONOR_NAME

**	// Add donor country names and ISO codes
		replace DONOR_COUNTRY = "Saudi Arabia" if DONOR_COUNTRY == "Soudi Arabia"
		replace DONOR_COUNTRY = "Bolivia" if DONOR_COUNTRY == "BOLIVIA (PLURINATIONAL STATE OF)"
		replace DONOR_COUNTRY = "Ivory Coast" if regexm(DONOR_COUNTRY, "d'Ivoire") | regexm(DONOR_COUNTRY, "D'IVOIRE")
		replace DONOR_COUNTRY = "Democratic Republic of the Congo" if DONOR_COUNTRY == "DEMOCRATIC REPUBLIC OF THE CONGO"
		replace DONOR_COUNTRY = "Laos" if DONOR_COUNTRY == "LAO PEOPLEâ€™S DEMOCRATIC REPUBLIC"
		replace DONOR_COUNTRY = "Micronesia" if DONOR_COUNTRY == "MICRONESIA (FEDERATED STATES OF)"
		replace DONOR_COUNTRY = "Macedonia" if DONOR_COUNTRY == "THE FORMER YUGOSLAV REPUBLIC OF MACEDONIA" | DONOR_COUNTRY == "NORTH MACEDONIA"
		replace DONOR_COUNTRY = "United Kingdom" if DONOR_COUNTRY == "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"
		replace DONOR_COUNTRY = "Tanzania" if DONOR_COUNTRY == "UNITED REPUBLIC OF TANZANIA"
		replace DONOR_COUNTRY = "Venezuela" if DONOR_COUNTRY == "VENEZUELA (BOLIVARIAN REPUBLIC OF)"
		replace DONOR_COUNTRY = "Korea, Democratic People's Republic of" if DONOR_NAME == "DEMOCRATIC PEOPLEâ€™S REPUBLIC OF KOREA"
		replace DONOR_COUNTRY = "Bosnia and Herzegovina" if DONOR_COUNTRY == "BOSNIA AND HERZEGOVIA"
		replace DONOR_COUNTRY = "Cape Verde" if DONOR_COUNTRY == "CABO VERDE"
		replace DONOR_COUNTRY = "Democratic People's Republic of Korea" if regexm(DONOR_COUNTRY, "KOREA") & regexm(DONOR_COUNTRY, "DEMOCRATIC")
		replace DONOR_COUNTRY = "Syria" if DONOR_COUNTRY == "SYRIAN"
**		replace DONOR_COUNTRY = "United Kingdom" if DONOR_COUNTRY == "UNITED KINGDOM OF GREAT BRITAIN AND" | regexm(DONOR_COUNTRY, "United Kingdom of Great Bri")
**		replace DONOR_COUNTRY = "United States" if DONOR_COUNTRY == "UNITED STATE OF AMERICA" | DONOR_COUNTRY == "UNITED SATES OF AMERICA" | DONOR_COUNTRY=="United Staes" | DONOR_COUNTRY == "United States of America" | DONOR_COUNTRY == "United states" | DONOR_COUNTRY == "UNITED STATES OF AMERICA"
		replace DONOR_COUNTRY = "United Kingdom" if DONOR_COUNTRY == "UNITED KINGDOM OF GREAT BRITAIN AND" 
		replace DONOR_COUNTRY = "United States" if DONOR_COUNTRY == "UNITED STATE OF AMERICA" | DONOR_COUNTRY == "UNITED SATES OF AMERICA" | DONOR_COUNTRY=="United Staes"
		replace DONOR_COUNTRY = "Brunei Darussalam" if regexm(DONOR_COUNTRY, "BRUNEI")
		replace DONOR_COUNTRY = "Yugoslavia, Former" if regexm(DONOR_COUNTRY, "YUGOSLAV") & regexm(DONOR_COUNTRY, "FORMER")
		replace DONOR_COUNTRY = "India" if regexm(DONOR_NAME, "VECTOR CONTROL INNOVATIONS") | DONOR_NAME == "GHARDA CHEMICALS LIMITED" | DONOR_NAME == "Gharda Chemicals Limited"
		replace DONOR_COUNTRY = "CZECH REPUBLIC (THE)" if regexm(DONOR_NAME, "CZECHIA")
		replace DONOR_COUNTRY = "Lao" if regexm(DONOR_NAME, "LAO PEOPLE’S DEMOCRATIC REPUBLIC") | regexm(DONOR_NAME, "Lao People's Democratic Republic") 
		replace DONOR_COUNTRY = "Spain" if regexm(DONOR_NAME, "Barcelona")
		replace DONOR_COUNTRY = "Afghanistan" if DONOR_COUNTRY == "AFGHANISTANA"
		replace DONOR_COUNTRY = "Armenia" if DONOR_COUNTRY == "ARMENIAA"
		replace DONOR_COUNTRY = "Central African Republic" if DONOR_COUNTRY == "CENTRAL AFRICAN REPUBLICA"
		replace DONOR_COUNTRY = "Dominican Republic" if DONOR_COUNTRY == "DOMINICAN REPUBLICA"
		replace DONOR_COUNTRY = "Swaziland" if DONOR_COUNTRY == "ESWATINI"
		replace DONOR_COUNTRY = "Georgia" if DONOR_COUNTRY == "GEORGIAA"
		replace DONOR_COUNTRY = "Iraq" if DONOR_COUNTRY == "IRAQA"
		replace DONOR_COUNTRY = "Kyrgyzstan" if DONOR_COUNTRY == "KYRGYZSTANA"
		replace DONOR_COUNTRY = "Luxembourg" if DONOR_COUNTRY == "Luxembourg (note 5)"
		replace DONOR_COUNTRY = "Somalia" if DONOR_COUNTRY == "SOMALIAA"
		replace DONOR_COUNTRY = "PALESTINIAN TERRITORY" if DONOR_COUNTRY == "Palestine"
		replace DONOR_COUNTRY = "Republic of Moldova" if DONOR_COUNTRY == "REPUBLIC OF MOLDOVAA"
		replace DONOR_COUNTRY = "Tajikistan" if DONOR_COUNTRY == "TAJIKISTANA"
		replace DONOR_COUNTRY = "Ukraine" if DONOR_COUNTRY == "UKRAINEA"
		replace DONOR_COUNTRY = "Viet Nam" if DONOR_COUNTRY == "VIET NAM"
		replace DONOR_COUNTRY = "United Arab Emirates" if regexm(DONOR_NAME, "UNITED ARAF EM")
        replace DONOR_COUNTRY = "Czech Republic" if DONOR_COUNTRY == "CZECHIA"
        replace DONOR_COUNTRY = "Iran" if DONOR_COUNTRY == "IRAN (ISLAMIC REPUBLIC OF)A"
        replace DONOR_COUNTRY = "Netherlands" if DONOR_COUNTRY == "NETHERLANDS (KINGDOM OF THE)"
        replace DONOR_COUNTRY = "Occupied Palestinian Territory" if DONOR_COUNTRY == "PALESTINE"
        replace DONOR_COUNTRY = "Sudan" if DONOR_COUNTRY == "SUDANA"
        replace DONOR_COUNTRY = "Turkey" if DONOR_COUNTRY == "TÃ¼RKIYE"
        
**		replace DONOR_NAME = "LAO PEOPLE'S DEMOCRATIC REPUBLIC" if DONOR_NAME == "Lao People’s Democratic Republic"
**
**		replace DONOR_COUNTRY = upper(DONOR_COUNTRY)
**		replace DONOR_NAME = upper(DONOR_NAME)
**		collapse (sum) EB_VFHP REG_NA_20`update_yr' REG_COLL_20`update_yr' REG_COLL_PRIOR (mean) EXP_REG EXP_EB_VFHP EXP_OTHER, by(DONOR_NAME INCOME_SECTOR INCOME_TYPE DONOR_COUNTRY)
**		sort DONOR_NAME


		rename DONOR_COUNTRY country_lc
		merge m:1 country_lc using "`CODES'/countrycodes_official.dta", keepusing(countryname_ihme iso3)
		
		replace countryname_ihme = "South Sudan" if DONOR_NAME == "SOUTH SUDAN"
		replace iso3 = "SSD" if DONOR_NAME == "SOUTH SUDAN"
		replace _m = 3 if DONOR_NAME == "SOUTH SUDAN"

		replace countryname_ihme = "Eswatini" if DONOR_NAME == "Eswatini"
		replace iso3 = "SWZ" if DONOR_NAME == "Eswatini"
		replace _m = 3 if DONOR_NAME == "Eswatini"

		replace countryname_ihme = "Cote d'Ivoire" if regexm(DONOR_NAME, "IVOIRE")
		replace iso3 = "CIV" if regexm(DONOR_NAME, "IVOIRE")

		replace countryname_ihme = "Venezuela" if DONOR_NAME == "VENEZUELA (BOLIVARIAN REPUBLIC OF)A"
		replace iso3 = "VEN" if DONOR_NAME == "VENEZUELA (BOLIVARIAN REPUBLIC OF)A"

/* 	
		replace countryname_ihme = "Cape Verde" if DONOR_NAME == "CABO VERDE"
		replace iso3 = "CPV" if DONOR_NAME == "CABO VERDE"
		replace _m = 3 if DONOR_NAME == "CABO VERDE"
		replace DONOR_NAME = "CAPE VERDE" if DONOR_NAME == "CABO VERDE"

		replace countryname_ihme = "Croatia" if DONOR_NAME == "CROATIA"
		replace iso3 = "HRV" if DONOR_NAME == "CROATIA"
		replace _m = 3 if DONOR_NAME == "CROATIA"

		replace countryname_ihme = "Democratic Republic of the Congo" if DONOR_NAME == "DEMOCRATIC REPUBLIC OF THE CONGO"
		replace iso3 = "COD" if DONOR_NAME == "DEMOCRATIC REPUBLIC OF THE CONGO"
		replace _m = 3 if DONOR_NAME == "DEMOCRATIC REPUBLIC OF THE CONGO"

		replace countryname_ihme = "Guinea-Bissau" if DONOR_NAME == "GUINEABISSAU"
		replace iso3 = "GNB" if DONOR_NAME == "GUINEABISSAU"
		replace _m = 3 if DONOR_NAME == "GUINEABISSAU"
		replace DONOR_NAME = "GUINEA-BISSAU" if DONOR_NAME == "GUINEABISSAU"

		replace countryname_ihme = "Holy See (Vatican City)" if DONOR_NAME == "HOLY SEE, ROME"
		replace iso3 = "VAT" if DONOR_NAME == "HOLY SEE, ROME"
		replace _m = 3 if DONOR_NAME == "HOLY SEE, ROME"

		replace countryname_ihme = "LAOS" if DONOR_NAME == "LAO PEOPLE'S DEMOCRATIC REPUBLIC" 
		replace iso3 = "LAO" if DONOR_NAME == "LAO PEOPLE'S DEMOCRATIC REPUBLIC"
		replace _m = 3 if DONOR_NAME == "LAO PEOPLE'S DEMOCRATIC REPUBLIC"

		replace countryname_ihme = "LAOS" if DONOR_NAME == "LAO PEOPLE’S DEMOCRATIC REPUBLIC" 
		replace iso3 = "LAO" if DONOR_NAME == "LAO PEOPLE’S DEMOCRATIC REPUBLIC"
		replace _m = 3 if DONOR_NAME == "LAO PEOPLE’S DEMOCRATIC REPUBLIC"

		replace countryname_ihme = "Timor-Leste" if DONOR_NAME == "TIMORLESTE"
		replace iso3 = "TLS" if DONOR_NAME == "TIMORLESTE"
		replace _m = 3 if DONOR_NAME == "TIMORLESTE"

		replace countryname_ihme = "Tanzania" if DONOR_NAME == "UNITED REPUBLIC OF TANZANIA"
		replace iso3 = "TZA" if DONOR_NAME == "UNITED REPUBLIC OF TANZANIA"
		replace _m = 3 if DONOR_NAME == "UNITED REPUBLIC OF TANZANIA"

		replace countryname_ihme = "Cyprus" if DONOR_NAME == "CYPRUS" 
		replace iso3 = "CYP" if DONOR_NAME == "CYPRUS"
		replace _m = 3 if DONOR_NAME == "CYPRUS"

		replace countryname_ihme = "Cuba" if DONOR_NAME == "CUBA" 
		replace iso3 = "CUB" if DONOR_NAME == "CUBA"
		replace _m = 3 if DONOR_NAME == "CUBA"

		replace DONOR_NAME = "AFGHANISTAN" if DONOR_NAME == "AFGHANISTANA"
		replace DONOR_NAME = "CENTRAL AFRICAN REPUBLIC" if DONOR_NAME == "CENTRAL AFRICAN REPUBLICA"
		replace DONOR_NAME = "CZECH REPUBLIC" if DONOR_NAME == "CZECHIA"
		replace DONOR_NAME = "CôTE D'IVOIRE" if DONOR_NAME == "CôTE D’IVOIRE"
		replace DONOR_NAME = "IRAQ" if DONOR_NAME == "IRAQA"
		replace DONOR_NAME = "NORWAY" if DONOR_NAME == "NORWEGIAN INSTITUTE OF PUBLIC HEALTH (NIPH)"
		replace DONOR_NAME = "TAJIKISTAN" if DONOR_NAME == "TAJIKISTANA"
		replace DONOR_NAME = "UNITED KINGDOM" if DONOR_NAME == "UNITED KINGDOM OF GREAT BRITAIN AND NORTHEN IRELAND" | DONOR_NAME == "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"
		
 */
		drop country_lc
		rename countryname_ihme DONOR_COUNTRY



		drop if _m == 2
		drop _m
	
		rename iso3 ISO_CODE
	**// Fix if the data is biennium
		gen YEAR = 20`update_yr'	
		rename REG_NA_20`update_yr' REG_NA
		rename REG_COLL_20`update_yr' REG_COLL
	
	**// Collapse and save
		collapse (mean) EXP_REG EXP_EB_VFHP EXP_OTH (sum) EB_VFHP REG_NA REG_COLL REG_COLL_PRIOR, by(ISO_CODE YEAR DONOR_COUNTRY INCOME_TYPE INCOME_SECTOR DONOR_NAME)

		gen CHANNEL = "WHO"
		gen SOURCE_DOC = "Financial Statement WHO_A74_2021 -- Regular Contributions and Voluntary Contributions"
		// gen REG_COLL_PRIOR = .
		gen EXP_EB_OTHER = .
		gen EB_OTHER = .
		gen EB_TOTAL = .
		gen OTHER = .
		gen EXP_INT_AG = .
		gen INCOME_NOTES = ""

	save "`INT'/who_`update_yr'.dta", replace 


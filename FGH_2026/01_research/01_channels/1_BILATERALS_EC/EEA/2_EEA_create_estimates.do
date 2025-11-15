**************************************************************
// Project:		FGH
// Purpose: 	Importing, consolidating and cleaning raw files from the EEA
**************************************************************
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
	
	local report_yr = 2024					// FGH report year	
	local abrv_yr = 24						// Shortened FGH report year
	local update_mmyy = "1025"				// MMYY of data collection
	
	local RAW 		"FILEPATH/FGH_`report_yr'"
	local INT  		"FILEPATH/FGH_`report_yr'"
	local FIN 		"FILEPATH/FGH_`report_yr'"
	local INCOME 	"FILEPATH/FGH_`report_yr'/Income"
	local INKIND 	"FILEPATH/FGH_`report_yr'/Inkind"
	local COUNTRIES "FILEPATH/FGH_`report_yr'"
	local x_rates	"FILEPATH/FGH_`report_yr'"
	local UTILS 	"FILEPATH"
	local STATIC	"FILEPATH"

	set scheme s1color
	// Call on master keyword search file
	run "`UTILS'/Health_ADO_master.ado"
	run "`UTILS'/TT_smooth.ado"


	// Prep Euro to USD exchange rates
	insheet using "`x_rates'/OECD_XRATES_NattoUSD_1950_`report_yr'.csv", comma names case clear
	keep if LOCATION=="EA19"
	rename Value exchange_rate
	ren TIME YEAR
	tempfile xrates
	save `xrates', replace

	// Bring in ISO Codes
	use "`COUNTRIES'/fgh_location_set.dta", clear
	keep if level == 3
	keep ihme_loc_id location_name
	ren location_name country
	tempfile country_iso
	save `country_iso', replace	


	// Prep inkind data
	import delimited "`INKIND'/EEA_inkind_`update_mmyy'.csv", varnames(1) clear
	keep year inkind_ratio
	duplicates drop
	ren year YEAR
	tempfile inkind
	save `inkind', replace

	// Prep income data
	import delimited "`INCOME'/Income_data_`update_mmyy'.csv", varnames(1) clear
	keep if keep == 1 
	drop keep source
	ren year YEAR
	destring YEAR, replace
	tempfile income_data
	save `income_data', replace	


**************
// Compile project datasets

// Bring together the two datasets
	clear
	use "`INT'/data_2004_2009.dta"
	append using "`INT'/data_2009_2014.dta"
	gen CHANNEL = "EEA"
	gen INCOME_SECTOR = "PUBLIC"
	gen INCOME_TYPE = "CENTRAL"
	gen DONOR_NAME = upper(DONOR_COUNTRY)
	gen REPORTING_AGENCY = "EEA"
	tempfile historical
	save `historical'

	*** ***********************
	// TODO: When we get new data, make sure the columns roughly match what is being done below
	//			whether that be through writing new code, or through manually editing
	//			the column names
// New code formatting for 2019-beyond
	insheet using "`RAW'/Correspondence_Data/EEA_2023_grants_in_detail.csv", comma clear // no need to read in 2019 data as it is all in new dataset
	// renaming existing columns
	ren projectname projecttitle
	ren projectinitialdescription projectsummary
	ren projectsignaturedateyear YEAR
	ren projectgrant DAH_euro
	// generating new/missing columns
	gen CHANNEL = "EEA"
	gen REPORTING_AGENCY = "EEA"
	gen INCOME_SECTOR = "PUBLIC"
	gen INCOME_TYPE = "CENTRAL"
	gen INKIND = 0
	gen casenumber = ""					// No specific information from this document
	gen typeofinstitution = "" 			// No specific information from this document
	gen grant = "EEA & Norway" 			// No specific information from this document
	gen DONOR_NAME = ""					// No specific information from this document
	gen DONOR_COUNTRY = "" 				// No specific information from this document
	gen ISO_CODE = ""					// No specific information from this document

	// Merge on ISO Codes, Income Groups
	replace country = "Czechia" if country == "Czech Republic"
	merge m:1 country using `country_iso', keep(1 3) nogen
	rename ihme_loc_id ISO3_RC
	drop country

	// drop high-income recipients (in 2020 this was all observations)
	merge m:1 ISO3_RC YEAR using "`COUNTRIES'/wb_historical_incgrps.dta", keepusing(INC_GROUP) keep(1 3) nogen
	drop if INC_GROUP == "H"	

	expand 3
	bysort YEAR projectcode: gen n = _n
	gen donorname = "Norway" if mod(n, 3) == 0
	replace donorname = "Iceland" if mod(n+1, 3) == 0
	replace donorname = "Liechtenstein" if mod(n+2, 3) == 0
	merge m:1 YEAR donorname using `income_data', nogen keep(1 3)
	replace DAH_euro = DAH_euro * proportion_eea_grants / 100
	drop n proportion_eea_grants
	sort YEAR  casenumber donorname

	// Bring in donor isocodes
	ren donorname country
	merge m:1 country using `country_iso', keep(1 3) nogen
	drop ISO_CODE DONOR_COUNTRY
	rename ihme_loc_id ISO_CODE
	ren country DONOR_COUNTRY
	replace DONOR_NAME = upper(DONOR_COUNTRY)

	// Calculate INKIND
	preserve
		replace INKIND = 1
		merge m:1 YEAR using `inkind', keep(1 3) nogen
		replace DAH_euro = inkind_ratio * DAH_euro
		tempfile curr_inkind
		save `curr_inkind'
	restore

	append using `curr_inkind'
	destring DAH_euro, replace float

	// Append to historical data
	append using `historical'
	*** ***********************

	// Assign gov based on project promoter data
		replace typeofinstitution = upper(typeofinstitution)
		gen gov = 0 if regexm(typeofinstitution, "RESEARCH") | regexm(typeofinstitution, "OTHER") | regexm(typeofinstitution, "PRIVATE")
		replace gov = 1 if regexm(typeofinstitution, "GOVERNMENT MINISTRY") | regexm(typeofinstitution, "LOCAL AUTHORITY") | regexm(typeofinstitution, "NATIONAL A") | regexm(typeofinstitution, "REGIONAL AUTHORITY") | regexm(typeofinstitution, "PUBLIC OWNED")
		replace gov = 2 if regexm(typeofinstitution, "FOUNDATION") | regexm(typeofinstitution, "NON GOVERNMENTAL") | regexm(typeofinstitution, "NON-GOVERNMENTAL") | regexm(typeofinstitution, "NGO") | regexm(typeofinstitution, "PUBLIC BENEFIT ORG")
		drop typeofinstitution

	// Exchange values from nominal Euro to nominal USD
		merge m:1 YEAR using `xrates', keep(1 3) keepusing(exchange_rate) nogen
		gen double DAH = DAH_euro / exchange_rate
		drop DAH_euro exchange_rate

	// Run keyword search
		HFA_ado_master projecttitle projectsummary, language(english)

	// Assign funding by HFAs and PAs
		foreach healthfocus in swap_hss_hrh swap_hss_other swap_hss_pp rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other {
			gen double `healthfocus'_DAH = final_`healthfocus'_frct * DAH
			drop final_`healthfocus'_frct
		}	

		tempfile int
		save `int'

	*** ***********************
		use `int', clear

		save "`INT'\EEA_INTPDB_`update_mmyy'.dta", replace	


		collapse (sum) *DAH, by(YEAR ISO3_RC gov DONOR_COUNTRY ISO_CODE CHANNEL INCOME_SECTOR INCOME_TYPE DONOR_NAME REPORTING_AGENCY INKIND)
		save "`FIN'\EEA_ADB_PDB_`update_mmyy'.dta", replace	

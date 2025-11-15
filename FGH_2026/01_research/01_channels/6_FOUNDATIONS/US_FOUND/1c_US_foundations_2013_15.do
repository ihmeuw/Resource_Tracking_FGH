	** ****
******************************* 2013 data *******************************************************************
// Step 1: Import grant-level data from the Foundation Center for 2013-2015 data
** ***************************************************************************************************************

	set more off
	clear all
	set maxvar 32000
	cap log close
// Define drives for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "FILEPATH"
		global h "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
		global h "H:"
	}
	
	// FGH report year
	local report_yr = 2024			// FGH report year	
	local abrv_yr = 24				// abbreviated FGH report year
	local crs_mmyy = "1224"			// MMYY of current CRS update
	
	local working_dir 	            "FILEPATH"
	local RAW		                "FILEPATH"
	local INT 						"FILEPATH"
	local FIN 						"FILEPATH"
	local CRS 						"FILEPATH"
	local CODES						"FILEPATH"
	local KWS 						"FILEPATH"
	
** ***
// Step 1: Import raw data
** ***	

	import excel using "FILEPATH",  clear firstrow // Don't change this path - files not brought forward
	tempfile _2013
	save `_2013'

	import excel using "FILEPATH",  clear firstrow // Don't change this path - files not brought forward
	ren (msa_code msa_name) (recip_msa_code recip_msa_name)
	tempfile _2014
	save `_2014'

	import excel using "FILEPATH", clear firstrow // Don't change this path - files not brought forward
	tempfile _2015
	save `_2015'

	import delimited using "FILEPATH", clear varn(1)
	gen amount2=subinstr(amount, ",", "", .)
	destring(amount2), replace
	drop amount 
	ren amount2 amount 
	tempfile _2016
	save `_2016'


	import excel using "FILEPATH", clear firstrow
	tempfile _2017
	save `_2017'
	
	import excel using "FILEPATH", clear firstrow
	replace recip_country_tran="" if recip_country_tran=="United States"
	tempfile _2018
	save `_2018'

	import excel using "FILEPATH", clear firstrow
	replace recip_country_tran="" if recip_country_tran=="United States"
	tempfile _2019
	save `_2019'


	// FOR EACH new year of data, create a new tempfile and append to others
	import delimited "`INT'/FC_output_2022.csv", clear // This file also includes years 2020 and 2021

	append using `_2013', force
	append using `_2014', force
	append using `_2015', force
	append using `_2016', force
	append using `_2017', force
	append using `_2018', force
	append using `_2019', force


	
	tempfile data
	save `data'

	// keep only health projects or helth recipients
		keep if regexm(activity_override, "SE")==1 | regexm(recip_subject_code, "SE")==1
		
	// drop projects that are allied health	- (Water access, sanitation and hygiene, Sanitation, Environmental health, Clean water supply, Bioethics, Art and music therapy)
		drop if activity_override=="SE080200" | activity_override=="SE030100" | activity_override=="SE140100" | activity_override=="SE130701" | activity_override=="SE130200" | activity_override=="SE130702" | activity_override=="SE130700" 

	// drop BMGF (we track them separately) - overwhelms the prediction process
		drop if legacy_gm_key == "GATE023"        	
** ***
// Step 2: Splitting disbursements among the list recipient countries (often the country_tran variable will list two or more countries, so we will want to split the grant amount evenly between them).
** ***	
	// clean recipient country
		g countryname=intl_countries_tran
		
		// subset for only US agencies that recieved grants for international work
		replace countryname=intl_geotree_tran if countryname==""
		replace countryname=recip_country_tran if countryname==""
		split countryname, p(";")
	
		// split up grant amount among all recipient countries (some grants have up to 7 recipients listed)
		rename countryname recipient
		reshape long countryname, i(grant_key) j(recipient_country)
		drop if countryname=="" & recipient!=""
		
		bysort grant_key: gen n= _n
		bysort grant_key: gen N= _N
		gen double amount_split = amount/N
	
// merge in recipient country isocode 
	// clean data for merge
		replace countryname = trim(countryname) 
		replace countryname="Congo, Democratic Republic of" if countryname=="Congo, Democratic Republic of the"
		replace countryname="Gambia, The" if countryname=="Gambia, Republic of The"
		replace countryname="West Bank and Gaza" if regexm(countryname,"West Bank/Gaza")==1
		replace countryname="India" if countryname=="Delhi"
		replace countryname="Indonesia" if countryname=="Kalimantan"
		replace countryname="Tanzania" if countryname=="Rungwe"
		replace countryname="Congo, Democratic Republic of" if countryname=="Congo, Democratic Republic of the"


		rename countryname country_lc

		merge m:1 country_lc using "`CODES'/countrycodes_official.dta", keepusing(country_lc iso3) keep(1 3) nogen
		
		rename country_lc countryname
		
	// merge in income groups
		rename iso3 ISO3_RC
		rename yr_issued YEAR
	
	// Fill in ISO3_RC codes for regions and country names that didn't match
		replace ISO3_RC = "QMA" if countryname == "Africa" 
		replace ISO3_RC = "QRA" if countryname == "Asia" 
		replace ISO3_RC = "QNB" if countryname == "Caribbean" 
		replace ISO3_RC = "QME" if inlist(countryname, "West Africa", "Southern Africa", "Central Africa" , "Sub-Saharan Africa", "Western Africa")
		replace ISO3_RC = "QME" if inlist(countryname, "Eastern Africa", "Horn of Africa", "Africa-Great Lakes Region", "Sahel", "East Africa")
		replace ISO3_RC = "QNC" if inlist(countryname, "Central America", "North America")
		replace ISO3_RC = "QNB" if inlist(countryname, "Carribbean", "The Carribbean", "Carribbean, The")
		replace ISO3_RC = "QRS" if countryname == "Central Asia"
		replace ISO3_RC = "QSA" if inlist(countryname, "Eastern Europe", "Europe", "Western Europe", "Central Europe", "Moravia")
		replace ISO3_RC = "WLD" if inlist(countryname, "Global Programs", "Global programs", "World")
		replace ISO3_RC = "QNE" if inlist(countryname, "Latin America", "South America")
		replace ISO3_RC = "QMD" if inlist(countryname, "Mediterranean Basin", "Northeast Africa", "Northern Africa")
		replace ISO3_RC = "QTA" if inlist(countryname, "Oceania", "Pacific Ocean")
		replace ISO3_RC = "QZA" if inlist(countryname, "Pacific Rim", "Arctic Region", "Developing Countries", "Developing countries")
		replace ISO3_RC = "QRA" if inlist(countryname, "Southeast Asia", "Southeastern Asia", "Mekong River and Basin", "Eastern Asia")
		replace ISO3_RC = "QRC" if inlist(countryname, "Southern Asia", "Indian Subcontinent & Afghanistan", "South Asia")
		replace ISO3_RC = "QRE" if countryname=="Middle East"
	// Countries
		replace ISO3_RC = "COD" if countryname == "Zaire" 
		replace ISO3_RC = "GRB" if countryname == "Northern Ireland"
		replace ISO3_RC = "PSE" if countryname == "East Jerusalem"
		replace ISO3_RC = "PSE" if countryname == "West Bank/Gaza (Palestinian Territories)"
		replace ISO3_RC = "IDN" if inlist(countryname, "Kalimantan", "Sulawesi", "Java", "Bali", "Nusa Tenggara")
		replace ISO3_RC = "QNB" if countryname == "Greater Antilles" 
		replace ISO3_RC = "TZA" if countryname == "Rungwe"
		replace ISO3_RC = "EGY" if countryname == "Upper Egypt"
		replace ISO3_RC = "USA" if inlist(countryname, "Urban", "Great Plains of North America", "Northeastern")
		replace ISO3_RC = "IND" if countryname == "Patna"
		replace ISO3_RC = "CZE" if countryname == "Slezsko"
		replace ISO3_RC = "CHN" if countryname == "Western China"

		merge m:1 ISO3_RC YEAR using "`CODES'/wb_historical_incgrps.dta", keepusing(INC_GROUP)
		keep if _m==1 | _m==3
		drop _m

** ***
// Step 3: Tagging transfers from foundations to channels we already track (UN Agencies and NGOs).
** ***

		
	// Tagging transfers to other foundations
		preserve
		keep recip_name 
		duplicates drop
		keep if regexm(recip_name, "Foundation")
		rename recip_name gm_name
		gen namelen = length(gm_name)
		quietly summarize namelen
		local maxlen = r(max)
		recast str`maxlen' gm_name
		drop namelen
		tempfile grantrecipients
		save `grantrecipients', replace
		restore	
		
		gen ELIM_CH = 0
		gen RECIPIENT_AGENCY_SECTOR = "OTH" 
		gen CHANNEL = "US_FOUND"
		gen namelen = length(gm_name)
		quietly summarize namelen
		local maxlen = r(max)
		recast str`maxlen' gm_name
		drop namelen
		merge m:m gm_name using `grantrecipients'
		replace ELIM_CH = 1 if _m == 3
		drop if _m == 2 
		drop _m
		
	// Tagging transfers to UN Agencies/bilaterals
	replace ELIM_CH = 1 if regexm(lower(recip_name), "pan american health organization")
		replace CHANNEL = "PAHO" if regexm(lower(recip_name), "pan american health organization")
	// If UNDP is added as a channel:
		//replace ELIM_CH = 1 if regexm(recip_name, "United Nations Development Programme")
		//replace CHANNEL = "UNDP" if regexm(recip_name, "United Nations Development Programme") (change the caps in the following)
	replace ELIM_CH = 1 if regexm(lower(recip_name), "united nations population fund")
		replace CHANNEL = "UNFPA" if regexm(lower(recip_name), "united nations population fund")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "united nations programme on hiv/aids")
		replace CHANNEL = "UNAIDS" if regexm(lower(recip_name), "united nations programme on hiv/aids")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "world health organization")
		replace CHANNEL = "WHO" if regexm(lower(recip_name), "world health organization")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "global fund to fight aids, tuberculosis and malaria")
		replace CHANNEL = "GFATM" if regexm(lower(recip_name), "global fund to fight aids, tuberculosis and malaria")
		replace ELIM_CH = 1 if regexm(lower(recip_name), "global fund to fight aids tuberculosis & malaria")
		replace CHANNEL = "GFATM" if regexm(lower(recip_name), "global fund to fight aids tuberculosis & malaria")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "gavi")
		replace CHANNEL = "GAVI" if regexm(lower(recip_name), "gavi")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "inter-american development bank")
		replace CHANNEL = "IDB" if regexm(lower(recip_name), "inter-american development bank")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "world bank")
		replace CHANNEL = "WB" if regexm(lower(recip_name), "world bank")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "unicef")
		replace CHANNEL = "UNICEF" if regexm(lower(recip_name), "unicef")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "wellcome trust")
		replace CHANNEL = "WELLCOME" if regexm(lower(recip_name), "wellcome trust")
	replace ELIM_CH = 1 if regexm(lower(recip_name), "asian development bank")
		replace CHANNEL = "AsDB" if regexm(lower(recip_name), "asian development bank")


	// AfDB not in data 
	tempfile temp
	save `temp', replace

	// Tagging transfers from foundations to NGOs that we already track
	// Agencies
		// No need to update--we want the historical agency list as most of this dataset is historical.
		// also all domestic NGOs have already been dropped for the new data so it's not necessary to use new agency list
		use "FILEPATH", clear
		replace agency = strupper(agency)
		replace agency = trim(agency)
		replace agency = subinstr(agency, ".", "", .)
		replace agency = subinstr(agency, ",", "", .)
		replace agency = subinstr(agency, "'", "", .)
		replace agency = subinstr(agency, "(", "", .)
		replace agency = subinstr(agency, ")", "", .)	
		replace agency = subinstr(agency, "-", " ", .)
		replace agency = subinstr(agency, "THE", "", .)	
		replace agency = subinstr(agency, "&", "", .)
		replace agency = subinstr(agency, "AND", "", .)
		replace agency = subinstr(agency, "   ", " ", .)	
		replace agency = subinstr(agency, "  ", " ", .)	
		gen namelen = length(agency)
		quietly summarize namelen
		local maxlen = r(max)
		recast str`maxlen' agency
		drop namelen
		tempfile agency
		save `agency', replace

	// International agencies
		use "FILEPATH", clear // keep for now (new dataset not yet available)
		replace agency = strupper(agency)
		replace agency = trim(agency)
		replace agency = subinstr(agency, ".", "", .)
		replace agency = subinstr(agency, ",", "", .)
		replace agency = subinstr(agency, "'", "", .)
		replace agency = subinstr(agency, "(", "", .)
		replace agency = subinstr(agency, ")", "", .)	
		replace agency = subinstr(agency, "-", " ", .)
		replace agency = subinstr(agency, "THE", "", .)	
		replace agency = subinstr(agency, "&", "", .)
		replace agency = subinstr(agency, "AND", "", .)
		replace agency = subinstr(agency, "   ", " ", .)	
		replace agency = subinstr(agency, "  ", " ", .)	
		gen namelen = length(agency)
		quietly summarize namelen
		local maxlen = r(max)
		recast str`maxlen' agency
		drop namelen
		tempfile intlagency
		save `intlagency', replace

	// Foundations 	
		use `temp',clear
		rename recip_name agency
		replace agency = strupper(agency)
		replace agency = trim(agency)
		replace agency = subinstr(agency, ".", "", .)
		replace agency = subinstr(agency, ",", "", .)
		replace agency = subinstr(agency, "'", "", .)
		replace agency = subinstr(agency, "(", "", .)
		replace agency = subinstr(agency, ")", "", .)	
		replace agency = subinstr(agency, "-", " ", .)
		replace agency = subinstr(agency, "THE", "", .)	
		replace agency = subinstr(agency, "&", "", .)
		replace agency = subinstr(agency, "AND", "", .)
		replace agency = subinstr(agency, "   ", " ", .)	
		replace agency = subinstr(agency, "  ", " ", .)
		
	// merge the list of NGOs we track and mark to drop if it matches. Drop if merge = 2 because this is an NGO that we track but is not in the foundations data.
		gen namelen = length(agency)
		quietly summarize namelen
		local maxlen = r(max)
		recast str`maxlen' agency
		drop namelen
		merge m:m agency using `agency'
		replace ELIM_CH = 1 if _merge == 3
		replace RECIPIENT_AGENCY_SECTOR = "NGO" if _merge == 3
		replace CHANNEL = "NGO" if _merge == 3
		drop if _m == 2
		drop _merge
		merge m:m agency using `intlagency'
		replace ELIM_CH = 1 if _merge == 3
		replace RECIPIENT_AGENCY_SECTOR = "NGO" if _merge == 3
		replace CHANNEL = "NGO" if _merge == 3
		drop if _m == 2	
		drop _merge
		
	// These NGOs are tracked as part of FGH, but have a slight variation on the name
		replace ELIM_CH =1 if agency == "ACTIONAID" | agency == "ADVENTURES IN HEALTH EDUCATION AND AGRICULTURAL DEVELOPMENT" | agency == "AMERICARES" | agency == "CATHOLIC RELIEF SERVICES" | agency == "CHILDFUND" | agency == "DTREE INC" | agency == "DOCTORS OF THE WORLD USA" | agency == "DOCTORS WITHOUT BORDERS USA" | agency == "ENVIRONMENTAL DEFENSE FUND" | agency == "ENVIRONMENTAL LAW ALLIANCE WORLDWIDE ELAW" | agency == "FOUNDATION FOR A CIVIL SOCIETY" | agency == "FRIENDS OF THE WORLD FOOD PROGRAM" | agency == "HEALTHPARTNERS INSTITUTE FOR EDUCATION AND RESEARCH" | agency == "HEART TO HEART INTERNATIONAL CHILDRENS MEDICAL ALLIANCE" | agency == "INTERNATIONAL SERVICES OF HOPE/IMPACT MEDICAL DIVISION ISOH/IMPACT" | agency == "MANO A MANO MEDICAL RESOURCES" | agency == "MEDISEND" | agency == "MEDISEND" | agency == "MERCY AND TRUTH MEDICAL MISSIONS" | agency == "NATIONAL ASSOCIATION OF PEOPLE LIVING WITH HIV/AIDS" | agency == "OPEN DOOR MEDICAL MINISTRIES" | agency == "OPERATION SMILE INTERNATIONAL" | agency == "PARTNERS IN HEALTH" | agency == "PATH" | agency == "PROGRAM FOR APPROPRIATE TECHNOLOGY IN HEALTH PATH" | agency == "PROJECT HOPE PEOPLETOPEOPLE HEALTH FOUNDATION" | agency == "RARE CENTER FOR TROPICAL CONSERVATION" | agency == "SAVE THE CHILDREN" | agency == "SAVE THE CHILDREN FUND" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL" | agency == "VOLUNTEERS FOR INTERAMERICAN DEVELOPMENT ASSISTANCE VIDA" | agency == "WATERAID" | agency == "WORLD CONCERN" | agency == "WORLD VISION RELIEF AND DEVELOPMENT" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL ENDOWMENT TRUST"
		
		replace RECIPIENT_AGENCY_SECTOR = "NGO" if agency == "ACTIONAID" | agency == "ADVENTURES IN HEALTH EDUCATION AND AGRICULTURAL DEVELOPMENT" | agency == "AMERICARES" | agency == "CATHOLIC RELIEF SERVICES" | agency == "CHILDFUND" | agency == "DTREE INC" | agency == "DOCTORS OF THE WORLD USA" | agency == "DOCTORS WITHOUT BORDERS USA" | agency == "ENVIRONMENTAL DEFENSE FUND" | agency == "ENVIRONMENTAL LAW ALLIANCE WORLDWIDE ELAW" | agency == "FOUNDATION FOR A CIVIL SOCIETY" | agency == "FRIENDS OF THE WORLD FOOD PROGRAM" | agency == "HEALTHPARTNERS INSTITUTE FOR EDUCATION AND RESEARCH" | agency == "HEART TO HEART INTERNATIONAL CHILDRENS MEDICAL ALLIANCE" | agency == "INTERNATIONAL SERVICES OF HOPE/IMPACT MEDICAL DIVISION ISOH/IMPACT" | agency == "MANO A MANO MEDICAL RESOURCES" | agency == "MEDISEND" | agency == "MEDISEND" | agency == "MERCY AND TRUTH MEDICAL MISSIONS" | agency == "NATIONAL ASSOCIATION OF PEOPLE LIVING WITH HIV/AIDS" | agency == "OPEN DOOR MEDICAL MINISTRIES" | agency == "OPERATION SMILE INTERNATIONAL" | agency == "PARTNERS IN HEALTH" | agency == "PATH" | agency == "PROGRAM FOR APPROPRIATE TECHNOLOGY IN HEALTH PATH" | agency == "PROJECT HOPE PEOPLETOPEOPLE HEALTH FOUNDATION" | agency == "RARE CENTER FOR TROPICAL CONSERVATION" | agency == "SAVE THE CHILDREN" | agency == "SAVE THE CHILDREN FUND" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL" | agency == "VOLUNTEERS FOR INTERAMERICAN DEVELOPMENT ASSISTANCE VIDA" | agency == "WATERAID" | agency == "WORLD CONCERN" | agency == "WORLD VISION RELIEF AND DEVELOPMENT" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL ENDOWMENT TRUST"	
		
		replace CHANNEL = "NGO" if agency == "ACTIONAID" | agency == "ADVENTURES IN HEALTH EDUCATION AND AGRICULTURAL DEVELOPMENT" | agency == "AMERICARES" | agency == "CATHOLIC RELIEF SERVICES" | agency == "CHILDFUND" | agency == "DTREE INC" | agency == "DOCTORS OF THE WORLD USA" | agency == "DOCTORS WITHOUT BORDERS USA" | agency == "ENVIRONMENTAL DEFENSE FUND" | agency == "ENVIRONMENTAL LAW ALLIANCE WORLDWIDE ELAW" | agency == "FOUNDATION FOR A CIVIL SOCIETY" | agency == "FRIENDS OF THE WORLD FOOD PROGRAM" | agency == "HEALTHPARTNERS INSTITUTE FOR EDUCATION AND RESEARCH" | agency == "HEART TO HEART INTERNATIONAL CHILDRENS MEDICAL ALLIANCE" | agency == "INTERNATIONAL SERVICES OF HOPE/IMPACT MEDICAL DIVISION ISOH/IMPACT" | agency == "MANO A MANO MEDICAL RESOURCES" | agency == "MEDISEND" | agency == "MEDISEND" | agency == "MERCY AND TRUTH MEDICAL MISSIONS" | agency == "NATIONAL ASSOCIATION OF PEOPLE LIVING WITH HIV/AIDS" | agency == "OPEN DOOR MEDICAL MINISTRIES" | agency == "OPERATION SMILE INTERNATIONAL" | agency == "PARTNERS IN HEALTH" | agency == "PATH" | agency == "PROGRAM FOR APPROPRIATE TECHNOLOGY IN HEALTH PATH" | agency == "PROJECT HOPE PEOPLETOPEOPLE HEALTH FOUNDATION" | agency == "RARE CENTER FOR TROPICAL CONSERVATION" | agency == "SAVE THE CHILDREN" | agency == "SAVE THE CHILDREN FUND" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL" | agency == "VOLUNTEERS FOR INTERAMERICAN DEVELOPMENT ASSISTANCE VIDA" | agency == "WATERAID" | agency == "WORLD CONCERN" | agency == "WORLD VISION RELIEF AND DEVELOPMENT" | agency == "SURGICAL EYE EXPEDITIONS INTERNATIONAL ENDOWMENT TRUST"	

	// Drop high income countries
		tab countryname if INC_GROUP== "H"
		drop if INC_GROUP == "H"

** ***
// Step 5: Allocate to health focus areas 
** ***

// Doing a key word search on the detailed grant description, activity description, and grant type description
	
***********
	// a.) keyword searches
***********
	// new keywords		
		
		do "FILEPATH"
		
		HFA_ado_master description grant_subject_tran activity_override_tran grant_population_tran recip_subject_tran, language(english) channel(US_FOUNDATIONS)
				
	// ado. also does post keyword fixes and calculates weights by health focus area level 1 and level 2
	
	// allocate disbursements across all health focus areas using weights
		foreach var of varlist final*frct {
			if "`var'" != "final_total_frct"  { // we do not want to include these umbrella terms
				local healthfocus = subinstr("`var'", "final_", "", .)
				local healthfocus = subinstr("`healthfocus'", "_frct", "", .)
				gen double `healthfocus'_DAH = `var' * amount_split
				}
			}	
			

save "FILEPATH", replace




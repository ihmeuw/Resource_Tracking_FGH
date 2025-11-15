**************************************************************
// Project:		FGH 
// Purpose: 	Importing, consolidating and cleaning historical raw files from the EEA
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
	
	local INT  		"FILEPATH/FGH_`report_yr'"
	local INCOME 	"FILEPATH/FGH_`report_yr'/Income"
	local INKIND 	"FILEPATH/FGH_`report_yr'/Inkind"
	local COUNTRIES "FILEPATH/FGH_`report_yr'"
	local x_rates	"FILEPATH/FGH_`report_yr'"
	local HFA 		"FILEPATH"
	local STATIC	"FILEPATH"


	set scheme s1color
	// Call on master keyword search file
	run "`HFA'/Health_ADO_master.ado"

**************
// Prep supplemental datasets

		use "`COUNTRIES'/fgh_location_set.dta", clear
		keep if level == 3
		keep ihme_loc_id location_name
		ren location_name country
		tempfile country_iso
		save `country_iso', replace	

	// Prep income data
		import delimited "`INCOME'/Income_data_`update_mmyy'.csv", varnames(1) clear
		keep if keep ==1 
		drop keep source
		ren year YEAR
		destring YEAR, replace
		tempfile income_data
		save `income_data', replace	

	// Prep inkind data
		import delimited "`INKIND'/EEA_inkind_`update_mmyy'.csv", varnames(1) clear
		keep year grant inkind_ratio
		ren year YEAR
		tempfile inkind
		save `inkind', replace

	// Prep Euro to USD exchange rates
		insheet using "`x_rates'/OECD_XRATES_NattoUSD_1950_`report_yr'.csv", comma names case clear
		keep if LOCATION=="EA19"
		rename Value exchange_rate
		ren TIME YEAR
		tempfile xrates
		save `xrates', replace

	// Prep raw data received through correspondence containing a few more columns we wanted
		//2004-2009 data. New columns: project promoter, type of institution, project duration, project summary, programme area
		import delimited "`STATIC'/EEA_morecolumns_2004_2009.csv", varnames(1) clear
		keep casenumber typeofinstitution projectduration projectsummary
		tempfile append_2004_2009
		save `append_2004_2009', replace
		//2009-2014 data. New columns: project promoter institution type, summary of project results
		import delimited "`STATIC'/EEA_morecolumns_2009_2014.csv", varnames(1) clear
		keep projectcode projectpromotorinstitutiontype summaryofprojectresults
		ren projectpromotorinstitutiontype typeofinstitution
		ren summaryofprojectresults projectsummary
		ren projectcode casenumber
		tempfile append_2009_2014
		save `append_2009_2014', replace

**************
// Process downloaded project datasets

// First process 2009-2014 data
	import delimited "`STATIC'/EEAGrants_2009_2014.csv", varnames(1) clear

		// Bring start and end dates on to same line as rest of project info
			gen start_date = projectduration[_n+1] 
			gen end_date = projectduration[_n+2] 
			drop if country == ""
			replace start_date = subinstr(start_date, "From: ", "", .)
			replace end_date = subinstr(end_date, "To: ", "", .)

		// Check on project status
			tab status
			drop if status == "Terminated" // from the project website, it seems this project was withdrawn and no payment was made
			//br if status == "Non Completed" // from the project website, it seems this project was never completed but the beneficiary still received the payments listed, so we will treat it like all other projects
			drop status prioritysector partner projectpromoter targetgroup

			
			// Calculate total project duration in days by subtracting end date from start date, and calculate the average project spending per day
			// Note that the appropriate disbursement variable to use is grant rather than projectcost, since grants are the amounts given by EEA/Norway. Total project costs are higher because the projects are often sponsored in part by the recipient government or other agencies, and is not DAH given by the EEA. This is written out in the descriptions of many of the 2004-2009 project pages. 
			gen date_start = date(start_date, "YMD")
			gen date_end = date(end_date, "YMD")
			gen project_duration_days = date_end - date_start + 1 // The + 1 is a shift so that both date endpoints are counted rather than the number of days between the endpoints. Otherwise we end up with projects disbursing for zero days if the start and end days are equal, and non-leap years with only 364 days.
 			gen spend_per_day = grants / project_duration_days

			// Create one entry for each year the project spans across
			gen start_year = substr(start_date,1,4)
			gen end_year = substr(end_date,1,4)
			foreach var of varlist start_year end_year {
				destring `var', replace	
			}
			gen nr_yrs_duration = end_year - start_year
			expand nr_yrs_duration + 1
			bysort casenumber : gen n = _n - 1
			gen YEAR = start_year + n
			gen final_year = 1 if n[_n + 1] == 0 | _n == _N

			// Create start and end dates for that year, and calculate the number of days the project was in progress during that year 
			tostring YEAR, gen(str_yr)
			gen annual_start_date = date_start  if n == 0
			gen annual_start_date_string =  "1/1/" + str_yr
			replace annual_start_date = date(annual_start_date_string, "MDY") if annual_start_date == .
			gen annual_end_date = date_end if final_year == 1
			gen annual_end_date_string =  "12/31/" + str_yr
			replace annual_end_date = date(annual_end_date_string, "MDY") if annual_end_date == .
			gen days_diff = annual_end_date - annual_start_date + 1 				// The + 1 is a shift so that both date endpoints are counted rather than the number of days between the endpoints. Otherwise we end up with projects disbursing for zero days if the start and end days are equal, and non-leap years with only 364 days.

			// Calculate the total annual spending in that year by multiplying daily spending by number of days the project was in progress 
			gen double annual_spend = spend_per_day * days_diff
			drop start_date end_date date_start date_end project_duration_days spend_per_day start_year end_year nr_yrs_duration n str_yr final_year annual_start_date annual_start_date_string annual_end_date annual_end_date_string days_diff projectduration
			ren annual_spend DAH_euro
			drop projectcost grants

		// Merge on recipient isocode
			replace country = "Czechia" if country == "Czech Republic"
			merge m:1 country using `country_iso', keep(1 3) nogen	
			rename ihme_loc_id ISO3_RC
			drop country

		// Drop high-income recipients
			merge m:1 ISO3_RC YEAR using "`COUNTRIES'/wb_historical_incgrps.dta", keepusing(INC_GROUP) keep(1 3) nogen
			drop if INC_GROUP == "H"	// most recipients get dropped! Only BGR and ROU remain.

		// Calculate proportion of health projects funded by Norway vs EEA grants to be used for 2004-2009 data where this information is unavailable
			preserve
			collapse (sum) DAH_euro, by(programmearea)
			replace programmearea = "Norway" if programmearea == "Public health initiatives (Norway grant)"
			replace programmearea = "EEA" if programmearea == "Public health initiatives (EEA grants)"
			gen dummy = "dummy"
			reshape wide DAH_euro, i(dummy) j(programmearea) string
			gen double total_grants = DAH_euroEEA + DAH_euroNorway
			gen double prop_Norway = DAH_euroNorway / total_grants
			gen double prop_EEA = DAH_euroEEA / total_grants
			reshape long DAH_euro prop_, i(dummy) j(donor) string
			keep donor prop_
			tempfile grant_props
			save `grant_props', replace
			restore

		// Add in source information
			gen donorname = "Norway" if programmearea == "Public health initiatives (Norway grant)"
			expand 3 if programmearea == "Public health initiatives (EEA grants)"
			bysort programmearea casenumber YEAR : gen n = _n
			replace donorname = "Norway" if donorname == "" & inlist(n, 1) 
			replace donorname = "Iceland" if donorname == "" & inlist(n, 2)
			replace donorname = "Liechtenstein" if donorname == "" & inlist(n, 3)
			drop n
			merge m:1 YEAR donorname using `income_data', keep (1 3) nogen
			replace DAH_euro = DAH_euro * proportion_eea_grants / 100 if programmearea == "Public health initiatives (EEA grants)"
			drop proportion_eea_grants 

		// Bring in donor isocodes
			ren donorname country
			merge m:1 country using `country_iso', keep(1 3) nogen	
			rename ihme_loc_id ISO_CODE
			ren country DONOR_COUNTRY	

		// Add inkind
		ren programmearea grant
		replace grant = "Norway" if grant == "Public health initiatives (Norway grant)"
		replace grant = "EEA" if grant == "Public health initiatives (EEA grants)"
		expand 2
		merge m:m YEAR grant using `inkind', keep (1 3) nogen
		bysort YEAR casenumber DONOR_COUNTRY: gen INKIND = _n - 1
		replace DAH_euro = inkind_ratio * DAH_euro if INKIND == 1
		drop inkind_ratio

		// Merge on correspondence data
		merge m:1 casenumber using `append_2009_2014', keep(1 3) nogen

		save "`INT'/data_2009_2014.dta", replace


// Next process 2004-2009 data
		import delimited "`STATIC'/EEAGrants_2004_2009.csv", varnames(1) clear
		drop if beneficiarystate == ""

		// Check on project status
			tab status
			//br if status == "Ongoing" //based on the project site, it would seem this project should be completed since the project duration after grant date has passed. The directory for 2004-2009 projects also states "All the projects under the EEA and Norway Grants 2004-2009 have been completed by 30 of April 2012. The project status 'on going' refers to the administrative closure of the project by the Financial Mechanism Office.". We will treat this project as complete.
			drop status partner

		// Merge on correspondence data
			merge m:1 casenumber using `append_2004_2009', keep(1 3) nogen
			//fill in info on 3 projects from their websites which were missing from correspondence data
			replace typeofinstitution = "Regional Authority" if casenumber=="CZ0037"
			replace projectduration = "48 months" if casenumber=="CZ0037"
			replace projectsummary = "The purpose of the Programme is to support the development of children, youth and seniors, with focus on the renewal of historical cultural heritage, by providing organisations with reconstructed and modernised buildings to operate in, with the overall objective to provide an improved offer of educational programmes and services of medical prevention for children and youth, and of social and medical services for seniors. Reference is made to the application dated 12 August 2005 and subsequent correspondence with the Focal Point dated 2 August 2006, 11 September 2006, 29 January 2007 and 1 February 2007. The completed Programme shall include the following activities and results: - Programme management; - Sub-projects for the repair, reconstruction and modernisation of buildings used for social, educational and medical services. The Intermediary is the Hradec Králové Region. The Intermediary shall provide at least 15 percent of estimated eligible Programme cost." if casenumber=="CZ0037"
			replace typeofinstitution = "Other" if casenumber=="HU0029"
			replace projectduration = "20 months" if casenumber=="HU0029"
			replace projectsummary = "The purpose of the Block Grant is to improve the access of disabled people to buildings hosting social services and child protection institutions, with the overall objective of promoting equal opportunities and social integration of people with disabilities. Reference is made to the application, dated 31 March 2006 and to FP correspondence dated 22 June and 9 July 2007. The completed Block Grant shall include the following activities and results: - grants to sub-projects promoters; - management of the Block Grant. The Intermediary is the Fogyatékosok Esélye Közalapítvány (Public Foundation for the Equal Opportunities of People with Disabilities). The Focal Point shall ensure that the Intermediary provides at least 15 percent of the estimated eligible costs." if casenumber=="HU0029"
			replace typeofinstitution = "Non governmental organisation" if casenumber=="HU0050"
			replace projectduration = "20 months" if casenumber=="HU0050"
			replace projectsummary = "The purpose of the Fund is to improve disabled people's accessibility to buildings open for the public, support equal opportunities for participation in worklife and increase the general knowledge on challenges facing disabled people in the Duna-Mecsek region (consisting of 60 settlements), with the overall objective of complying with national and EU requirements on public buildings and accessibility for disabled people. Reference is made to the application dated 8 November 2007 and all subsequent correspondence with the Focal Point. The completed Fund shall include the following activities and results: - Coordination, management and publicity of the Fund; - Regranting of fund to selected sub-projects from open calls. The Intermediary is the Duna-Mecsek Regional Development Foundation. The Focal Point shall ensure that the Intermediary provides at least 15 percent of the estimated eligible costs of the Fund." if casenumber=="HU0050"
			//fill in info on 5 projects that had data online but they didn't give us valid data for
			replace typeofinstitution = "Non-governmental organisation" if casenumber=="2008/107476"
			replace projectduration = "24 months" if casenumber=="2008/107476"
			replace projectsummary = "Promoting mental health and improving the quality of care for vulnerable children and adolescents through (1) development of an operational framework, (2) development of training programmes. Expected results: The new Centre of excellence, run by Save the Children Romania, will provide children and their parents with a first of its kind community based service where they are available in a user friendly and non-stigmatizing way. The training of local staff and resource persons from institutions will also enable the project to have a long term impact on the design of these services in Romania. The centre will be multidisciplinary and constitute of three departments: 1/ service delivery for direct beneficiaries (children and parents); 2/ capacity building (training of personnel, supervision and technical assistance); 3/ resource development (e.g. studies, creation of materials, screening and diagnosis instruments etc.)." if casenumber=="2008/107476"
			replace typeofinstitution = "Public owned enterprise" if casenumber=="2008/112481"
			replace projectduration = "24.9666666666667 months" if casenumber=="2008/112481"
			replace projectsummary = "Improving capacity in the Romanian public health system to treat children with developmental disorders through setting up a day-care centre and transferring know-how on treatment methods. Expected results: Quantity/infrastructure: One 1500 sqm building constructed as per tender specifications, ready for operation and fully equipped with the items listed in the latest version of the detailed budget. Quality/institutional and human resources development:Day centre staff structure approved by the Ministry of Health and recruitment procedure for 46 staff initiated; At least 90 professionals (psychiatrists, educators, social workers etc) trained in therapy-related issues based on 7 toolkits/1000 pages of Norwegian training material; consultation meetings with Norwegian partners on exchanges of best practices relating to centre operation issues." if casenumber == "2008/112481"
			replace typeofinstitution = "Non-governmental organisation" if casenumber=="2008/114367"
			replace projectduration = "23.9666666666667 months" if casenumber=="2008/114367"
			replace projectsummary = "The project aims at providing basic/primary health care solutions and services to 8 rural areas in one of Romania’s poorest regions (North-East). As many as 22 292 rural patients are targeted by the project. The need for the project is explained by the lack of service capacities in the area (e.g. 1 general practitioner to an average of 2000 patients in remote areas where transportation can become difficult in bad weather)." if casenumber=="2008/114367"
			replace typeofinstitution = "Non-governmental organisation" if casenumber=="2008/115272"
			replace projectduration = "24 months" if casenumber=="2008/115272"
			replace projectsummary = "Romanian Pilot Centre for Network, Competence and Training for people affected by rare diseases, based on model from Frambu, Norwegian national centre for rare disorders and disabilities. The Norwegian PWS Association will follow the project through reporting from visits and meetings." if casenumber=="2008/115272"
			replace typeofinstitution = "National authority" if casenumber=="2008/108643"
			replace projectduration = "24 months" if casenumber=="2008/108643"
			replace projectsummary = "Public-health project aiming at preventing obesity and promoting healthy diet and lifestyle at national level in Romania. Expected results: To contribute to stop and reverse the trend of increasing overweight and obesity in children and adolescents in Romania and increase the percentage of children and adolescents that have a healthy lifestyle. Establishment of institutional platforms at national and local levels. The experiences/information gathered will be used as a base for a National Action Plan for Nutrition and Physical Activity." if casenumber=="2008/108643"

		// Generate project start and end dates based on approval date and project duration
			gen date_start = date(dateapproved, "MDY")
			split projectduration, parse("months")
			destring projectduration1, replace
			gen duration_days = projectduration1 * 31 
			gen date_end = date_start + duration_days

			gen project_duration_days = date_end - date_start + 1 // The + 1 is a shift so that both date endpoints are counted rather than the number of days between the endpoints. Otherwise we end up with projects disbursing for zero days if the start and end days are equal, and non-leap years with only 364 days.
 			gen spend_per_day = grantawarded / project_duration_days

			// Create one entry for each year the project spans across
			gen start_year = substr(dateapproved,-4,4)
			gen end_year = string(date_end, "%td")
			replace end_year = substr(end_year,-4,4)

			foreach var of varlist start_year end_year {
				destring `var', replace	
			}
			gen nr_yrs_duration = end_year - start_year
			expand nr_yrs_duration + 1
			bysort casenumber : gen n = _n - 1
			gen YEAR = start_year + n
			gen final_year = 1 if n[_n + 1] == 0 | _n == _N

			// Create start and end dates for that year, and calculate the number of days the project was in progress during that year 
			tostring YEAR, gen(str_yr)
			gen annual_start_date = date_start  if n == 0
			gen annual_start_date_string =  "1/1/" + str_yr
			replace annual_start_date = date(annual_start_date_string, "MDY") if annual_start_date == .
			gen annual_end_date = date_end if final_year == 1
			gen annual_end_date_string =  "12/31/" + str_yr
			replace annual_end_date = date(annual_end_date_string, "MDY") if annual_end_date == .
			gen days_diff = annual_end_date - annual_start_date + 1 				// The + 1 is a shift so that both date endpoints are counted rather than the number of days between the endpoints. Otherwise we end up with projects disbursing for zero days if the start and end days are equal, and non-leap years with only 364 days.

			// Calculate the total annual spending in that year by multiplying daily spending by number of days the project was in progress 
			gen double annual_spend = spend_per_day * days_diff
			drop  date_start date_end project_duration_days spend_per_day start_year end_year nr_yrs_duration n str_yr final_year annual_start_date annual_start_date_string annual_end_date annual_end_date_string days_diff projectduration duration_days
			ren annual_spend DAH_euro
			drop grantawarded projectduration2 projectduration1 dateapproved
			
			// Merge on recipient isocode
			ren beneficiarystate country
			replace country = "Czechia" if country == "Czech Republic"
			merge m:1 country using `country_iso', keep(1 3) nogen	
			rename ihme_loc_id ISO3_RC
			drop country

		// drop high-income recipients
			merge m:1 ISO3_RC YEAR using "`COUNTRIES'/wb_historical_incgrps.dta", keepusing(INC_GROUP) keep(1 3) nogen
			drop if INC_GROUP == "H"	// A few more recipients are present in this earlier data than the later data.
			

	// Drop childcare projects that were not related to health. healthproj variable was manually assigned based on project pages, but we also back this up by excluding childcare non-health projects based on a keywords list.
			//br if childcare == 1 // I manually checked all these project pages and set healthproj = 0 if the projects were not health related
		// Perform keyword search to identify childcare non-health projects
			local nonhealth_words NURSERY ORPHANAGE RECREATION FOSTER " CARE HOME " " CHILDREN S HOME " " CHILD DEVELOPMENT CENTRE " " YOUTH CENTER" " YOUTH CAMPS " " RESIDENTIAL CENTRE" " RESIDENTIAL CARE FACILITIES " "CHILDCARE" " INFORMAL EDUCATION FOR CHILDREN " " PREVENT INSTITUTIONALISATION " " SPORTS FIELD" " SPORT FIELD" " SPORTS FACILIT" " SPORTS ACTIVIT" " SPORTS INFRASTRUCTURE" " REINTEGRATION OF JUVENILE OFFENDERS " 
			gen health_kws = 1
			create_upper_vars projecttitle projectsummary
			foreach word of local nonhealth_words {
				replace health_kws = 0 if regexm(upper_projecttitle, "`word'") | regexm(upper_projectsummary, "`word'")
			}	
			gen diff = health_kws - healthproj
			// br diff upper_projecttitle upper_projectsummary if childcare==1 & healthproj != . & diff != 0 //1 means false positive for health, -1 means false negative. This should be an empty dataset if the assignment has gone well.
			// drop childcare non-health projects
			drop if childcare == 1 & health_kws == 0	
			drop upper* health_kws diff
			drop childcare healthproj prioritysector
	
		// Since this dataset lacks information on whether it is an EEA or Norway grant, we use the same proportion as for health projects in the 2009-2014 dataset
			expand 2
			bysort YEAR casenumber: gen n = _n
			gen donor = "EEA" if n == 1
			replace donor = "Norway" if n == 2
			merge m:m donor using `grant_props', nogen
			replace DAH_euro = DAH_euro * prop_
			drop n prop_

		// Add inkind
			ren donor grant
			expand 2
			merge m:m YEAR grant using `inkind', keep (1 3) nogen
			bysort YEAR casenumber grant: gen INKIND = _n - 1
			replace DAH_euro = inkind_ratio * DAH_euro if INKIND == 1
			drop  inkind_ratio	
			ren grant donor

		// Add in source information
			gen donorname = donor if donor == "Norway"
			expand 3 if donor == "EEA"
			bysort donor casenumber YEAR INKIND: gen n = _n
			replace donorname = "Norway" if donorname == "" & inlist(n, 1) 
			replace donorname = "Iceland" if donorname == "" & inlist(n, 2)
			replace donorname = "Liechtenstein" if donorname == "" & inlist(n, 3)
			drop n
			merge m:1 YEAR donorname using `income_data', keep (1 3) nogen
			replace DAH_euro = DAH_euro * proportion_eea_grants / 100 if donor == "EEA"
			drop proportion_eea_grants 
			ren donor grant
			sort YEAR  casenumber donorname

		// Bring in donor isocodes
			ren donorname country
			merge m:1 country using `country_iso', keep(1 3) nogen	
			rename ihme_loc_id ISO_CODE
			ren country DONOR_COUNTRY	


		save "`INT'/data_2004_2009.dta", replace



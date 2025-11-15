********************************************************
// Project:		FGH
// Purpose: 	Running regression on health exp fraction with observed data and predicting health exp fraction for unobserved data 
**********************************************************
	set more off
	clear all

	if c(os) == "Unix" | c(os) == "MacOSX" {
		global j "FILEPATH
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}

	// FGH report year	
	local report_yr = 2024	
	local data_yr = 2022
	
	local FIN  "FILEPATH"
	local GUIDESTAR  "FILEPATH"

	
// Bring in data
	// US PVOs with health expenditure data
		use "FILEPATH", clear
		gen USPVO_with_health_exp_data = 1
		tempfile final1
		save `final1'

	// US PVOs without health expenditure data
		use "FILEPATH", clear
		gen USPVO_without_health_exp_data = 1
		tempfile final2
		save `final2'

	// International PVOs
		use "FILEPATH", clear
		gen IPVO = 1
		sort agency
		drop if id == . 
		tempfile final3
		save `final3'

	// Append data files
		use `final1', clear
		append using `final2'
		append using `final3'
		replace id = id+10000 if IPVO==1			// IPVO id number started counting at 1, as did the US PVOs, so we differentiate the two by adding 10000 to the id of IPVOs.  
        ** FGH2024!!! Drop data from years with new data
        gen year2 = year + 1990
        drop if year2 > 2018
        drop year2

		gen PVO_type = "USPVO_with_he_data" if USPVO_with_health_exp_data == 1
		replace PVO_type = "USPVO_without_he_data" if USPVO_without_health_exp_data == 1
		replace PVO_type = "IPVO" if IPVO == 1
		ren year YEAR
		duplicates tag id YEAR , gen(dup)
		drop if dup > 0 & PVO_type == "USPVO_without_he_data"
		// FIX FOR FGH 2020/2021
		replace PVO_type = "IPVO" if id == 10109
		replace IPVO = 1 if id == 10109
		replace agency = "Joint Aid Management" if id == 10109
		drop if id == 10109 & health_expenditure_990 == . & YEAR == 27
		drop if id == 10109 & health_expenditure_990 == . & YEAR == 28
		// Fix Handicap international (look into this)
		drop if id == 10006 & YEAR == 28 & agency == "International Institute for Sustainable Development"

		drop dup
		drop if agency == "GLOBAL ALLIANCE FOR IMPROVED NUTRITION"
		xtset id YEAR

// Generate indicator for using health word in name and description
	// Generate variable to be searched
		gen agency_name = lower(agency)
		replace agency_name = subinstr(agency_name,".","",.)
		replace agency_name = subinstr(agency_name,"é","e",.)
		replace agency_name = subinstr(agency_name,"è","e",.)
		replace agency_name = subinstr(agency_name,"à","a",.)
		replace agency_name = subinstr(agency_name,"ñ","n",.)
		replace agency_name = subinstr(agency_name,"marie stopes","mariestopes",.)			// Marie Stopes is a health organization, but this is not indicated in the description
		replace agency_name = subinstr(agency_name,"humane society","humanesociety",.)		// The Humane Society is not a health organization, but includes "healthy" keywords, so needs to be tagged as "not healthy"
		
	// Fix some NGO names by hand (they should be tagged for health, but have slight name changes)	
		replace agency_name = "hesperian health guides" if agency_name == "hesperian foundation"
		replace agency_name = "mano a mano medical resources" if agency_name == "mano a mano international partners"
		replace agency_name = "doctors without borders usa, inc." if agency_name == "médecins sans frontières usa, inc"
		replace agency_name = "mercy international health services" if agency_name == "mercy-usa for aid and development, inc"
		replace agency_name = "population communications international" if agency_name == "pci-media impact, inc"
		replace agency_name = "project hope" if agency_name == "project hope-the people-to-people health foundation, inc" | agency_name == "project hope - the people-to-people health foundation, inc"
		replace agency_name = "buckner orphan care international, inc." if agency_name == "buckner adoption & maternity services, inc"
		
		// These organizations are either known health NGOs that do not get tagged based on their names, or non-health organizations that get tagged incorrectly

	// "Healthy" keywords: keywords that identify NGOs as health organizations. If any of these keywords appear in the agency name, the organization is tagged as a "healthy" NGO
		local healthy_word health hiv aids nutrition medical cancer gavi gfatm vaccine malaria bednet ncd doctor medicine medisend pathologist lung physician tuberculosis injuries noncommunicable paho syndrome retroviral tb dots polio tobacco smoking leprosy eye blind pediatric fistula population sante medecin pharmaciens pharmacy handicap prosthetics mariestopes medico      
		di "`healthy_word'"
 
		gen healthy_name = 0
		foreach word of local healthy_word {
			di "`word'"
			replace healthy_name = 1 if strmatch(agency_name, `"*`word'*"')		//"
			}
		
	// "Unhealthy" keywords: keywords that identify NGOs that might otherwise be seen as health organizations as non-health organizations. If any of these keywords appear in the agency name, the organization is tagged as an "unhealthy" NGO
		local unhealthy_word water sanitation agriculture climate environmental torture forest orphan fauna flora nature tree wildlife emergency energy soybean book earth green transportation road economic zoological humanitarian humanesociety food
		di "`unhealthy_word'"

		gen unhealthy_name = 0
		foreach word of local unhealthy_word {
			di "`word'"
			replace unhealthy_name = 1 if strmatch(agency_name, `"*`word'*"')		//"
			}

		
	// Add NGO descriptions for a keyword search of descriptions
		preserve
		import delimited "FILEPATH", clear 
		rename year YEAR
		rename agency agency_name
		keep id YEAR agency_name description
		drop if id == .							
		duplicates drop id YEAR, force
		tempfile temp
		save `temp'
		restore

		merge 1:1 id YEAR using `temp'
		drop if _m == 2
		drop agency_description _m
		
	// Generate variable to be searched
		gen agency_description = lower(description)
		replace agency_description = subinstr(agency_description,".","",.)
		replace agency_description = subinstr(agency_description,"- ","",.)
		replace agency_name = subinstr(agency_name,"é","e",.) 
		replace agency_name = subinstr(agency_name,"è","e",.)
		replace agency_name = subinstr(agency_name,"à","a",.)
		replace agency_name = subinstr(agency_name,"ñ","n",.)

	// "Healthy" keywords
		local healthy_word health hiv aids nutrition medical cancer vaccine malaria bednet ncd doctor medicine medisend pathologist lung physician tuberculosis injuries noncommunicable paho syndrome retroviral tb dots polio tobacco smoking leprosy eye blind pediatric fistula population sante medecin pharmaciens pharmacy handicap prosthetics mariestopes arv medico
		di "`healthy_word'"
 
		gen healthy_desc = 0
		foreach word of local healthy_word {
			di "`word'"
			replace healthy_desc = 1 if strmatch(agency_description, `"*`word'*"')		//"
			}
		egen max_temp = max(healthy_desc), by(id)
		replace healthy_desc = max_temp
		drop max_temp
		
	// "Unhealthy" keywords
		local unhealthy_word water sanitation agriculture climate environmental torture forest orphan fauna flora nature tree wildlife emergency energy soybean book earth green road economic zoological humanitarian humanesociety food
		di "`unhealthy_word'"

		gen unhealthy_desc = 0
		foreach word of local unhealthy_word {
			di "`word'"
			replace unhealthy_desc = 1 if strmatch(agency_description, `"*`word'*"')		//"
		}
		egen max_temp = max(unhealthy_desc), by(id)
		replace unhealthy_desc = max_temp
		drop max_temp

// Fix anomalies -- Handicap International and Handicap International Federation are listed as two separate organizations in 2010, but with most fields identical. Also need to correct tag so that Handicap International Federation 2011 is marked as the same organizations in other years
	duplicates tag id YEAR , gen(dup)
	//drop source
	//collapse (last) agency PVO_type agency_name agency_description description (mean) total_support_and_revenue total_expenses-USPVO_without_health_exp_data other_usg_support international_agencies host_gov foreign_gov international_agencies_frc foreign_gov_frc host_gov_frc (max) IPVO healthy_name unhealthy_name healthy_desc unhealthy_desc, by(id YEAR)
	collapse (last) agency PVO_type agency_name agency_description description (mean) total_support_and_revenue total_expenses-USPVO_without_health_exp_data other_usg_support international_agencies host_gov foreign_gov (max) IPVO healthy_name unhealthy_name healthy_desc unhealthy_desc, by(id YEAR)

// Run the random effect regression; save coefficient estimates, covariance matrix, and RE standard error; generate matrix with 1000 draws of coefficients
	// reml = fit model via restricted maximum likelihood

	set seed 61014
	
	xtmixed lnhlth tot_US_frc inkind_contributions_frc overseas_programs_frc YEAR healthy_name unhealthy_name healthy_desc unhealthy_desc || id:, reml nocons
	
	mat V = e(V)
	mat b = e(b)
	mat sd_re = exp(b[1,11])
	mat sd_e = exp(b[1,12])
	local sd_re = sd_re[1,1]
	local sd_e = sd_e[1,1]
	mat V = V[1..10, 1..10]
	mat b = b[1, 1..10]
	
	preserve
	clear
	set matsize 1000
	set obs 1000
	drawnorm private_contributions_frc tot_US_frc inkind_contributions_frc overseas_programs_frc YEAR healthy_name unhealthy_name healthy_desc unhealthy_desc _con, n(1000) means(b) cov(V) seed(082713) clear
	mkmat private_contributions_frc tot_US_frc inkind_contributions_frc overseas_programs_frc YEAR healthy_name unhealthy_name healthy_desc unhealthy_desc  _con, matrix(coef)	
	restore

// Predict values and take inverse logit transformation
	// US PVOs that had at least one year in which we had health expenditure data (so we have an actual estimated RE for this PVO)
		predict fit_lnhlth, fitted
		gen fit_hlth_frct = 1/(1+exp(-1*fit_lnhlth))

	// International PVOs or US PVOS that have no observations with health expenditure data - takes a bit of time to run
		forvalue i = 1(1)50 {
			qui gen fit_lnhlth_`i' = .
			qui local tot_obs = _N
			di "Starting sample #`i'"
			forvalue obs = 1(1)`tot_obs' {
				qui local draw_re = rnormal(0, `sd_re')				// Draw a different RE for each PVO
				qui local draw_e = rnormal(0, `sd_e')				// Draw a different error for each PVO
				qui replace fit_lnhlth_`i' = (private_contributions_frc*coef[`i',1]) + (tot_US_frc*coef[`i',2]) + (inkind_contributions_frc*coef[`i',3]) + (overseas_programs_frc*coef[`i',4]) + (YEAR*coef[`i',5]) + (healthy_name*coef[`i',6]) + (unhealthy_name*coef[`i',7]) + (healthy_desc*coef[`i',8]) + (unhealthy_desc*coef[`i',9]) + coef[`i',10] + `draw_re' + `draw_e' if _n==`obs'
				}
			gen fit_hlth_`i' = 1/(1+exp(-1*fit_lnhlth_`i'))
			}
		egen mean_fit_hlth = rowmean(fit_hlth_*)
		replace fit_hlth_frct = mean_fit_hlth if fit_hlth_frct==.
		drop fit_lnhlth_* mean_fit_hlth

		save "FILEPATH", replace
		use "FILEPATH", replace

// Format to match results from 6) NGO_Analyzing_Expenditure.do
	
		// Creating US public expenditure on health overseas variable:
			gen EXP_PUB_US = overseas_programs*hlth_frct*USpub_rev_frct	if hlth_frct != .
			replace EXP_PUB_US = overseas_programs*fit_hlth_frct*USpub_rev_frct if hlth_frct == .

		// Creating non-US public expenditure on health overseas variable:
			gen EXP_PUB_NONUS = overseas_programs*hlth_frct*nonUSpub_rev_frct if hlth_frct != .
			replace EXP_PUB_NONUS = overseas_programs*fit_hlth_frct*nonUSpub_rev_frct if hlth_frct == .
	
		// Creating BMGF expenditure on health overseas variable:
			gen EXP_BMGF = overseas_programs*hlth_frct*BMGF_frct if hlth_frct != .
			replace EXP_BMGF = overseas_programs*fit_hlth_frct*BMGF_frct if hlth_frct == .
		
		// Creating private in-kind expenditure on health overseas variable:	
			gen EXP_INK = overseas_programs*hlth_frct*ink_frct if hlth_frct != .
			replace EXP_INK = overseas_programs*fit_hlth_frct*ink_frct if hlth_frct == .
			
		// Creating private expenditure on health (non-BMGF) overseas variable:
			gen EXP_OTHER_PRIV = overseas_programs*hlth_frct*other_priv_frct if hlth_frct != .
			replace EXP_OTHER_PRIV = overseas_programs*fit_hlth_frct*other_priv_frct if hlth_frct == .
		
		replace IPVO = 0 if IPVO != 1
		replace sample = 0 if IPVO == 1


// Save:
	// 1.) US NGOs 1990-2010
		preserve
			drop if IPVO == 1 
			drop description
			replace YEAR = 1990 + YEAR
			save "FILEPATH", replace
		restore
		
	// 2.) INGOs 1998, 2000-2010
		preserve
			drop if IPVO != 1
			drop description
			replace YEAR = 1990 + YEAR
			save "FILEPATH", replace
		restore
		

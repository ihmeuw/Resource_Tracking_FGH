*** ********************************************************
// Project: 	FGH
// Purpose: 	Create a single DTA that has all the Guidestar data (2010-2013), the completed Volag data (1990-2011), and the one year of sampled NGO data that is not already combined with the Volag data.Estimate DAH for all years (1990-`report_yr')
*** ********************************************************
	set more off
	clear all

	if c(os) == "Unix" | c(os) == "MacOSX" {
		global j "/home/j"
        global h "/homes/`c(username)'"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

	// FGH report year	
	local report_yr = 2024	
	local data_yr = 2022
	local update_yr = "0522"
	local crs_update_yr = "0425"
	
	local GUIDESTAR_FIN  "FILEPATH"
	local PREDICTIONS  "FILEPATH"
	local FIN  		"FILEPATH"
	local DEFL		"FILEPATH"
	local COMMON	"FILEPATH"
	local FBO		"FILEPATH"
	local HFA  	   "FILEPATHS"
	local CRS  	   "FILEPATH"


	local RAW		"FILEPATH"
	local INT 		"FILEPATH"
	local OUT		"FILEPATH"


	// Call on master keyword search file
		run "FILEPATH"
		
	
** *******
// Step 3: Append previous years of completed Volag data
** *******	
	// a. Append data 
		use "FILEPATH", clear
		gen source = "VOLAG (includes sampled data up to 2018)"
		replace source = "990 efile Data Lake" if YEAR > 2018
		append using "FILEPATH"
		drop if id == 10285
		drop if id == 10235
		drop if id == 10282
		replace source = "International VOLAG" if source == ""
		replace source = "International usaspending.gov" if YEAR > 2018 & id >= 10000 & id <= 20000
        **** FGH2024 - initial estimates: estimate all years after 2018
        drop if YEAR > 2018
		gen overseas_frct = overseas_programs / new_tot_exp
		keep id agency YEAR overseas_programs total_expenses health_expenditure *_frct source total_support_and_revenue agency_description
		foreach var of varlist overseas_programs total_expenses fit_hlth_frct USpub_rev_frct nonUSpub_rev_frct BMGF_frct ink_frct other_priv_frct overseas_frct total_support_and_revenue {
			rename `var' vol_`var'
			}
		rename hlth_frct samp_hlth_frct	
		rename health_expenditure samp_health_expenditure	
		tempfile temp
		save `temp'
		use "FILEPATH", clear // seems to be old data. Look into more using stash 2018 code
		merge 1:1 id YEAR using `temp'
		drop _merge
		tempfile base_NGO_data
		save `base_NGO_data', replace
		use `base_NGO_data', clear


** *******
// Step 4: Gen placeholders for projects that have gaps in the Volag.  (Fill in gaps in between existing records and assume anything existing one fiscal year ago (`data_yr'-1) still exists through `report_yr'.)
** *******
//should add a fix to drop NGOs we know are no longer in existence
		keep id YEAR
		gen one = 1
		reshape wide one, i(id) j(YEAR)
        // ** FGH2024!!! change which years are imputed here - FGH2024, all years after 2019!
        gen one2019 = .
        gen one2020 = . 
        gen one2021 = .
        gen one2022 = .
		gen one2023 = .
		gen one2024 = .
		reshape long one, i(id) j(YEAR)
		xtset id YEAR
		gen year = YEAR if one == 1
		egen first_year = min(year), by(id)
		egen last_year = max(year), by(id)

		// Deal with international NGOs separately because Volag had more data on them then Guidestar so if we drop the ones that have two years of no obs we lose most of our international NGOs - this is a temporary fix for FGH 2019 and should be revisited
		preserve
		keep if id > 10000 & id <= 20000
		drop if YEAR < first_year
		// want to keep all placeholders between 2014-2018
		// want to scale placeholders for 2019-2020
		// drops placeholder data after last year if last year is 2013 or earlier
		drop if YEAR > last_year & last_year < 2015 // Needs to be the number of years since the volag was discontinued to not drop all the IPVO agencies (data_yr - 2 for FGH 2019)
		tempfile intl_ngos // 1848 obs for FGH 2019
		save `intl_ngos'
		restore
		// Drop US NGO agency-years that likely don't exist
		drop if id > 10000 & id <= 20000 // drop international NGOs (5970 obs in FGH2019)
		drop if YEAR < first_year
		drop if YEAR > last_year & last_year < (`data_yr'-1) 
		append using `intl_ngos' 
		
		drop one year
		merge 1:1 id YEAR using `base_NGO_data'
		replace source = "Placeholder for year where PVO not in Volag or Guidestar" if _merge == 1 & (id <= 10000 | id > 20000)
		replace source = "Placeholder for year where IPVO not in Volag" if _merge == 1 & (id >= 10000 & id <= 20000) // This is where more IPVOs are created for last year
		egen first_agency = first(agency), by(id)
		xfill first_agency, i(id)
		replace agency = first_agency if agency==""
		drop first_agency _merge first_year last_year
		sort id YEAR
		order _all, alpha
		order id YEAR agency source
		gen IPVO = source == "International VOLAG" | source == "Placeholder for year where IPVO not in Volag" | source == "International usaspending.gov" 
		
** *******	
// Step 5: Estimate usable fractions
** *******
	// Health fraction: prioritize data from sample, then modeled data using regression from code 5).
		gen usable_hlth_frct = samp_hlth_frct
		replace usable_hlth_frct = vol_fit_hlth_frct if samp_hlth_frct == .
		
	// For other fractions, only Volag data available, so fill in missingness by fitting linear regression to logit transformed data if more than one non-missing obs per id.  If one non-missing obs per id, then use that obs across all missing obs.  If all obs are missing then apply the median from the whole set.
		replace vol_USpub_rev_frct = 0 if vol_USpub_rev_frct == . & YEAR <= 2018	// Assume that if PVO isn't reporting in Volag when we have Volag then not receiving any US funds 
		replace vol_overseas_frct = 1 if vol_overseas_frct > 1 & vol_overseas_frct != .

	// Modeling
		levelsof id, local(id)
		replace vol_USpub_rev_frct = . if vol_USpub_rev_frct == 0 & YEAR > 2018
		foreach var in USpub_rev_frct ink_frct nonUSpub_rev_frct other_priv_frct overseas_frct hlth_frct {
			di "Looping through `var'"
			qui {
				cap gen usable_`var' = vol_`var'
				gen logit_`var' = log(usable_`var'/(1-usable_`var')) if usable_`var' != .
				sum logit_`var'
				replace logit_`var' = r(max) if usable_`var' == 1
				replace logit_`var' = r(min) if usable_`var' == 0
				gen fit_logit_`var' = logit_`var'
				gen fit_`var' = .
				sum fit_logit_`var', d
					local grand_med = r(p50)
				foreach i of local id {
					di "id is `i'"
					sum fit_logit_`var' if id == `i'
						local n = r(N)
						local mean = r(mean)
					if (`n'>1) {	
						reg logit_`var' YEAR if id == `i'
						predict temp if id==`i'
						replace fit_logit_`var' = temp if id == `i'
						drop temp
						}
					if (`n' == 1) {
						replace fit_logit_`var' = `mean' if id == `i'
						}
					if (`n' == 0) {
						replace fit_logit_`var' = `grand_med' if id == `i'
						}					
					replace fit_`var' = 1/(1+exp(-1*fit_logit_`var'))
					}
				}
			}			

	// Evaluate models
		tw scatter fit_overseas_frct vol_overseas_frct || line vol_overseas_frct vol_overseas_frct
		    gr export "FILEPATH",replace
		tw scatter fit_hlth_frct usable_hlth_frct || line usable_hlth_frct usable_hlth_frct
		    gr export "FILEPATH",replace
		tw scatter fit_USpub_rev_frct vol_USpub_rev_frct || line vol_USpub_rev_frct vol_USpub_rev_frct
		    gr export "FILEPATH",replace
		tw scatter fit_ink_frct vol_ink_frct || line vol_ink_frct vol_ink_frct
		    gr export "FILEPATH",replace
		tw scatter fit_nonUSpub_rev_frct vol_nonUSpub_rev_frct || line vol_nonUSpub_rev_frct vol_nonUSpub_rev_frct
		    gr export "FILEPATH",replace
		tw scatter fit_other_priv_frct vol_other_priv_frct || line vol_other_priv_frct vol_other_priv_frct
		    gr export "FILEPATH",replace

	// Fill in blanks with the modeled fractions	
		foreach var in USpub_rev_frct ink_frct nonUSpub_rev_frct other_priv_frct overseas_frct hlth_frct {
			replace usable_`var' = fit_`var' if usable_`var'==.
		}

		gen usable_BMGF_frct = vol_BMGF_frct
		replace usable_BMGF_frct = 0 if usable_BMGF_frct == .

	// correct for lack of nonUSpub_rev_frct in 2019-2020
	replace usable_nonUSpub_rev_frct = fit_nonUSpub_rev_frct if YEAR > 2018 & fit_nonUSpub_rev_frct > usable_nonUSpub_rev_frct
			
	// Correct for the fact that BMGF didn't exist before 1999
		count if usable_BMGF_frct > 0 & YEAR < 1999	// BMGF didn't exist before 1999; assume PVO got aid from other private source
		replace usable_other_priv_frct = usable_BMGF_frct + usable_other_priv_frct if YEAR < 1999 & usable_BMGF_frct > 0
		replace usable_BMGF_frct = 0 if YEAR < 1999 & usable_BMGF_frct > 0

	// Correct for the fact that model fractions don't add up to exactly one
		gen temp = usable_USpub_rev_frct + usable_nonUSpub_rev_frct + usable_BMGF_frct + usable_ink_frct + usable_other_priv_frct

		foreach var of varlist usable_USpub_rev_frct usable_nonUSpub_rev_frct usable_BMGF_frct usable_ink_frct usable_other_priv_frct {
			replace `var' = `var'/temp
		}
			
		drop logit* fit* temp
		
		tempfile modeled
		save `modeled'
		use `modeled', clear
	// Estimate DAH and revenue (Prioritize Guidestar data, then Volag data, then modeled data)
		// For Guidestar NGO-years that doesn't also have Volag or sampled NGO data for that save NGO-year (essentially just 2012 for FGH `report_yr') 	
		gen dah = usable_hlth_frct * usable_overseas_frct * gs_total_exp 
		gen rev = gs_total_rev	
			
		// For Volag and sampled NGO-years
		replace dah = samp_health_expenditure if YEAR > 2019 & YEAR < 2023
		replace dah = usable_hlth_frct * vol_overseas_programs if dah == .
		replace rev = vol_total_support_and_revenue if rev == .
	
		// For placeholders (These NGO-years have no health expenditure or total expenditure data because these NGO-years don't have Volag, Guidestar, or sampled data.  So this fills in the blanks for NGOs that have observations that pop in and out of different years of the Volag and is for the preliminary estimates, ie. 2013 and `report_yr' for FGH `report_yr')
		
		// Pull in covariate data
			// GDP data
				preserve
				insheet using "`COMMON'/USgdppc_from_IMF_WEO_2025.csv", comma names clear // update this file using most recent downloaded deflators dataset and "Calculating US GDP_PC for NGO Channel.do" in the DEFLATORS/CODE file
				rename *,lower
				rename gdppc_curusd gdppc_curUSD
				gen ln_gdppc_curUSD = log(gdppc_curUSD)
				keep year ln_gdppc_curUSD gdppc_curUSD
				rename year YEAR
				tempfile usgdp
				save `usgdp', replace
				restore
				merge m:1 YEAR using `usgdp'
				drop if _m==2
				drop _merge 

				//output a dataset of agency-years to pull into CRS to search for agencies and tag for elimination bilateral transfers to NGOs
				preserve
				rename agency RECIPIENT_AGENCY
				drop if RECIPIENT_AGENCY == ""
				replace RECIPIENT_AGENCY = upper(RECIPIENT_AGENCY)

				create_upper_vars RECIPIENT_AGENCY
				
				sort RECIPIENT_AGENCY
				// drop NGOs with vague names. Deleted "GOAL" from this in FGH 2019 because was able to handle it separately
				drop if  RECIPIENT_AGENCY == " RARE " | RECIPIENT_AGENCY == " CARE " | RECIPIENT_AGENCY == " HEALTH PROM " | RECIPIENT_AGENCY == " MAPS " |  RECIPIENT_AGENCY == "RARE" | RECIPIENT_AGENCY == "CARE" | RECIPIENT_AGENCY == "HEALTH PROM" | RECIPIENT_AGENCY == "MAPS"
				gen obs = YEAR
				gen intl = IPVO
				keep RECIPIENT_AGENCY YEAR obs intl
				bysort RECIPIENT_AGENCY YEAR: gen n=_n
				drop if n!=1
				drop n	
				reshape wide obs, i(RECIPIENT_AGENCY intl) j(YEAR)
				bysort RECIPIENT_AGENCY: gen n=_n
				drop if n>1
				drop n
				save "`INT'/FGH_`report_yr'/NGO_agencyyears_`report_yr'_`update_yr'.dta", replace		
				restore	
	save "`INT'/FGH_`report_yr'/NGOS_INTERMEDIATE_`report_yr'_`update_yr'.dta", replace

	//Stop on NGOs here and run through the rest of CRS to the end. Run the rest of this code after finishing CRS and running 7_BILATERAL_PREDICTIONS.do, which outputs the necessary file below.

** *******	
// Step 6: Keyword search and NGO preliminary estimates
** *******
		// Bring in US bilateral aid per capita to predict out NGO DAH for the most recent three years	
		// This file is created in CRS/7_BILATERAL_PREDICTIONS.do. After the whole CRS procedure is completed, return to this point and re-run from here on to create NGO predictions. A placeholder file can be used in the meantime.
		// If US bilateral estimates are rerun/changed, NGO prediction code needs to be rerun to reflect these changes		
				insheet using "FILEPATH", comma clear
				tempfile bilat_per_cap
				save `bilat_per_cap', replace

				use "FILEPATH", clear
				rename YEAR year
				merge m:1 year using `bilat_per_cap'		
				drop _m
				// change OUTFLOW to current USD instead of constant   
				rename year YEAR

				local deflate_yr = 24
				merge m:1 YEAR using "FILEPATH", keepusing(GDP_deflator_20`deflate_yr')
				keep if _m == 1 | _m == 3
				drop _m

				gen OUTFLOW = outflow_`deflate_yr' * 20`deflate_yr'
				gen US_OUTFLOW_PC = OUTFLOW/ total_pop
				gen ln_USdahpc = log(US_OUTFLOW_PC)
				rename US_OUTFLOW_PC USdahpc
                gen ln_dah = log(dah)
				rename YEAR year
                
                save "FILEPATH", replace
			
	// Regression
		// Regress log DAH on log GDP, log USAID and PVO RE. Add year and year squared variables, each with a random parameter. 
			xtset id year
			egen total_dah_observed = sum(dah), by(id)
			gen placeholderonly = 1 if year > 2014 & year < 2019
			xtmixed dah gdppc_curUSD USdahpc || id: gdppc_curUSD USdahpc, cov(un)
			predict fit_dah, fitted
			replace fit_dah = 0 if fit_dah<0 
			replace fit_dah = 0 if dah == 0 | total_dah_observed == 0
			
			// Inspect results
			scatter fit_dah dah if IPVO == 0 || line dah dah if IPVO == 0
				gr export "FILEPATH", replace	
			scatter fit_dah dah if IPVO == 1 || line dah dah if IPVO == 1
				gr export "FILEPATH", replace	

		// Calculate DAH estimate
			replace dah = fit_dah if dah == . // 3668 changes for 2019

		// Calculate revenue estimates
			egen total_rev_observed = sum(rev), by(id)
			xtmixed rev gdppc_curUSD USdahpc || id: gdppc_curUSD USdahpc, cov(un)
			predict fit_rev, xb
			replace fit_rev = 0 if fit_rev < 0 
			replace fit_rev = 0 if rev == 0 | total_rev_observed == 0
			scatter fit_rev rev || line rev rev	
			replace rev = fit_rev if rev == .

			drop USdahpc ln_USdahpc total_dah_observed gdppc_curUSD ln_gdppc_curUSD fit_rev ln_dah
		
	// Quick visualization of DAH estimates
		preserve
		collapse (sum) dah fit_dah rev, by(year IPVO)
		tw line dah year if IPVO == 0 || line fit_dah year if IPVO == 0, lpattern(dash)
			gr export "FILEPATH",replace
		tw line dah year if IPVO == 1 || line fit_dah year if IPVO == 1, lpattern(dash)
			gr export "FILEPATH",replace
		collapse (sum) dah rev, by(year)
		tw line dah year 
			gr export "FILEPATH",replace
		tw line dah year || line rev year, lpattern(dash)
			gr export "FILEPATH",replace
		restore

	
** *******	
// Step 7: Fill in IPVO aggregates for prior to 1998
** *******
	// Set up	
		cap restore
		preserve
		keep if IPVO == 1
		gen weight = round(dah,1)
		collapse (rawsum) dah rev (mean) usable_USpub_rev_frct usable_nonUSpub_rev_frct usable_BMGF_frct usable_ink_frct usable_other_priv_frct [fweight=weight], by(year)

		//create blank observations from 1990-1997 to be predicted
		//local fillinyears=`report_yr'- 1990 + 1
		local fillinyears=`report_yr'- 1990 + 2
		set obs `fillinyears'
		gen dummy = 0 if year==.
		sort dummy year
		drop dummy
		replace year = (1990+_n-1) if year == .
		tab year 		//check that this part is working as expected (1990-report_yr are all showing up)
			//drop if year==1998 & dah==.
	// DAH
		sort year
		gen ln_dah = log(dah)
		reg ln_dah year
		predict fit_ln_dah, xb
		gen fit_dah = exp(fit_ln_dah)
		
		// Inspect model
			scatter fit_dah dah || line dah dah
			line fit_dah year || scatter dah year
			gr export "FILEPATH",replace
		
		// Replace
			replace dah = fit_dah if dah == .
	
	// Revenue
		gen ln_rev = log(rev)			
		reg ln_rev year
		predict fit_ln_rev, xb
		gen fit_rev = exp(fit_ln_rev)
		
		// Inspect model
			scatter fit_rev rev || line rev rev
			line fit_rev year || scatter rev year
			gr export "FILEPATH",replace

		// Replace
			replace rev = fit_rev if rev==.

	// Fractions
		foreach var of varlist usable* {
			sum `var',d
			replace `var' = r(p50) if `var' == .
		}
		replace usable_BMGF_frct = 0	
	
	// Save
		drop ln_dah ln_rev fit_ln_dah fit_dah fit_ln_rev fit_rev
		drop if year > 1997
		gen IPVO = 1
		gen source = "Aggregate IPVO, modeled pre-1998 using time trend"
		tempfile temp
		save `temp'
		restore
	
	// Appending
		append using `temp'		
		save "FILEPATH", replace


** *******		
// Step 8: Keyword searches
** *******
		use "FILEPATH", replace
		drop if id == .
		tempfile FBO
		save `FBO', replace

		import delimited using "FILEPATH",  clear
		drop agency
		tempfile programdesc
		save `programdesc', replace



	use "FILEPATH", clear

	// Parse into faith-based NGOs and non-faith-based NGOS
		merge m:1 id using `FBO'
		// br id agency NGOName FBO _merge
			drop if _m == 2
			drop _merge

		merge 1:1 id year using `programdesc'
		drop if _m == 2
		drop _merge
// fill in description data, prioritizing scraped + program description followed by VOLAG/GUIDESTAR
	replace agency_description = agency_description + " " + program_concatenated
	replace agency_description = "" if agency_description == " "
	sort id year
	by id: replace agency_description = agency_description[_n-1] if missing(agency_description)
	by id: replace agency_description = agency_description[_n-1] if year > 2018 & missing(program_concatenated)



	// Prepare for keyword search	
		foreach var of varlist agency agency_description {
			gen upper_`var' = upper(`var') 
			replace upper_`var' = subinstr(upper_`var', "ÿ", "Y", .)
			replace upper_`var' = subinstr(upper_`var', "Ÿ", "Y", .)
			replace upper_`var' = subinstr(upper_`var', "æ", "AE", .)
			replace upper_`var' = subinstr(upper_`var', "Æ", "AE", .)
			replace upper_`var' = subinstr(upper_`var', "œ", "OE", .)
			replace upper_`var' = subinstr(upper_`var', "Œ", "OE", .) 
			replace upper_`var' = subinstr(upper_`var', "ç", "C", .)
			replace upper_`var' = subinstr(upper_`var', "Ç", "C", .)
			replace upper_`var' = subinstr(upper_`var', "ñ", "N", .)
			replace upper_`var' = subinstr(upper_`var', "Ñ", "N", .)
			replace upper_`var' = subinstr(upper_`var', "ß", "SS", .)
			replace upper_`var' = subinstr(upper_`var', "/", " ", .)
			replace upper_`var' = subinstr(upper_`var', "\", " ", .)
			replace upper_`var' = subinstr(upper_`var', ":", " ", .)
			replace upper_`var' = subinstr(upper_`var', ",", " ", .) 
			replace upper_`var' = subinstr(upper_`var', ";", " ", .)
			replace upper_`var' = subinstr(upper_`var', ".", " ", .)
			replace upper_`var' = subinstr(upper_`var', "-", " ", .)
			replace upper_`var' = subinstr(upper_`var', "(", " ", .)
			replace upper_`var' = subinstr(upper_`var', ")", " ", .)
			replace upper_`var' = subinstr(upper_`var', `"'"', " ", .)
			replace upper_`var' = subinstr(upper_`var', `"""', " ", .)
			replace upper_`var' = subinstr(upper_`var', "«", " ", .) 
			replace upper_`var' = subinstr(upper_`var', "»", " ", .) 
			replace upper_`var' = subinstr(upper_`var', "·", " ", .) 
			replace upper_`var' = stritrim(upper_`var')
			// Fix spacing since descriptions ending in special characters get an extra space on the end which should be taken out	
			replace upper_`var' = strtrim(upper_`var')
			replace upper_`var' = " " + upper_`var' + " "	
		}
		
		foreach var of varlist agency agency_description {
			foreach letter in á Á à À ã Ã â Â å Å ä Ä {
				replace upper_`var' = subinstr(upper_`var', "`letter'", "A", .)
			}
			foreach letter in é É ê Ê è È ë Ë {
				replace upper_`var' = subinstr(upper_`var', "`letter'", "E", .)
			}
			foreach letter in í Í ì Ì î Î ï Ï {
				replace upper_`var' = subinstr(upper_`var', "`letter'", "I", .)
			}
			foreach letter in ó Ó ò Ò õ Õ ô Ô ø Ø ö Ö {
				replace upper_`var' = subinstr(upper_`var', "`letter'", "O", .)
			}
			foreach letter in ú Ú ù Ù û Û ü Ü {
				replace upper_`var' = subinstr(upper_`var', "`letter'", "U", .)
			}
		}

	// Call on health focus area keywords master list (for keyword search)
		// Running function within Health_ADO_master to perform search on descriptive variables in english
		run_keyword_search agency agency_description, language(english) channel(NGO)
	
	save "FILEPATH", replace


	use "FILEPATH", clear		
	// Post-search keyword fixes
		// NGOs cannot give to SWAps, so all observations for SWAps are set to zero
		// swap_hss: Keywords " SWAP ", " DATA SYSTEM ", " SECTOR WIDE APPROACH" (swap_hss_level words 2,5,6) refer to SWAps, while all other keywords refer to Health Systems Strengthening (HSS)
			egen swap_total = rowtotal(swap_hss_level_2_* swap_hss_level_5_* swap_hss_level_6_*)		
			replace swap_hss_level = swap_hss_level - swap_total
			drop swap_total
			drop swap_hss_level_1*_*  swap_hss_level_2*_*  swap_hss_level_3*_*  swap_hss_level_4*_*  swap_hss_level_5*_*  swap_hss_level_6*_*  swap_hss_level_7*_*  swap_hss_level_8*_*  swap_hss_level_9*_* 

		// fix for keyword "communicable" in oid picking up "non communicable"
			egen ncd_level_78_total = rowtotal(ncd_level_78_*)
			replace oid_level = oid_level - ncd_level_78_total
			drop ncd_level_1*_*  ncd_level_2*_*  ncd_level_3*_*  ncd_level_4*_*  ncd_level_5*_*  ncd_level_6*_*  ncd_level_7*_*  ncd_level_8*_*  ncd_level_9*_* 

		// Categorize HIV prevention- PMTCT as HIV PMTCT only 
			foreach var of varlist hiv_treat hiv_prev rmh_level nch_level rmh_fp rmh_mh {
				replace `var' = max(`var' - hiv_pmtct,0) if hiv_pmtct >= 1
			}
				
		// Projects tagged as child vaccines and oid should be isolated under child vaccines
			replace oid_level = 0 if oid_level >= 1 & nch_level >= 1  & nch_cnv >= 1
			
		// Projects tagged as one of the three major diseases (HIV, TB, malaria) and containing the word "infectious" (tagged as oid) in their name/description should be allocated completely to the major disease
			egen oid_level_2_total = rowtotal(oid_level*_2_*)
			foreach var of varlist hiv_level tb_level mal_level {
				replace oid_level = 0 if `var' >= 1 & oid_level_2_total >=1
			}
			drop oid_level_2_total

		// Projects tagged as one of the three major diseases (HIV, TB, malaria) and containing the word vaccine or vaccination (tagged as child vaccines) in their name/description should be allocated completely to the major disease
			egen vaccine_total = rowtotal(nch_level*_23_* nch_level*_24_* nch_level*_33_*)
			foreach var of varlist hiv_level tb_level mal_level {
				replace nch_level = 0 if `var' >= 1 & vaccine_total >=1
			}
			drop vaccine_total

		// Incorrectly tagged projects
			replace rmh_level = 0 if inlist(id,83,1437,10033) // "ORT" "STI" "ORS" are false positives
			replace nch_level = 0 if inlist(id,83,1437,10033)
			replace rmh_mh = 0 if id == 1437
			replace hiv_level = 0 if inlist(id,1,41,113,181,793,1266,1331) // "ART" is a false positive
			replace hiv_treat = 0 if inlist(id,1,41,113,181,793,1266,1331)
			replace oid_level = 0 if inlist(id,601,784) // picks up spread of communicable diseases among animals
			foreach healthfocus of local hfa {
				replace `healthfocus' = 0 if inlist(id,112,10018,10073,10131,10154)		// Veterinary programs
			}

		// Fix if " SUPPLY " in swap_hss_level is being tagged by " WATER SUPPLY "
			gen water_supply = 0
			foreach var of varlist agency agency_description {
				egen water_`var' = noccur(upper_`var'), string(" WATER SUPPLY ")
				replace water_supply = water_supply + water_`var'
				drop water_`var'
			}
			replace swap_hss_level = swap_hss_level - water_supply
			drop water_supply*	

		drop nch_level_1* nch_level_2* nch_level_3* nch_level_4* nch_level_5* nch_level_6* nch_level_7* nch_level_8* nch_level_9*
		drop nch_cnv_1* nch_cnv_2* nch_cnv_3* nch_cnv_4* nch_cnv_5* nch_cnv_6* nch_cnv_7* nch_cnv_8* nch_cnv_9*
		drop oid_level_1*_* oid_level_2*_* oid_level_3*_* oid_level_4*_* oid_level_5*_* oid_level_6*_* oid_level_7*_* oid_level_8*_* oid_level_9*_* 
			
		tempfile data_after_fixes
		save `data_after_fixes', replace

	// Running function within Health_ADO_master to create HFA and PA proportions based on keyword tags
		create_keyword_props agency agency_description
	
	// Save data so that we do not have to rerun keyword search:
save "FILEPATH", replace


** *******	
// Step 9: Gen output variables using health fractions
** *******
// okay so the commented out line here lacks report_yr.  so it's possible i ran it before and it was the wrong dataset?
// not possible, there's nothing there.  but if i somehow ran the archive one here
		use "FILEPATH", clear

	// US public overseas health expenditure and revenue
		gen EXP_PUB_US = dah * usable_USpub_rev_frct
		gen REV_PUB_US = rev * usable_USpub_rev_frct

	// Non-US public overseas health expenditure and revenue
		gen EXP_PUB_NONUS = dah * usable_nonUSpub_rev_frct
		gen REV_PUB_NONUS = rev * usable_nonUSpub_rev_frct

	// BMGF overseas health expenditure and revenue
		gen EXP_BMGF = dah * usable_BMGF_frct
		gen REV_BMGF = rev * usable_BMGF_frct

	// In-kind overseas health expenditure and revenue
		// Must down adjust expenditure estimates, as agencies typically overreport value of in kind donations
		// In-kind fraction: 0.183 (Note: do not adjust international NGOs!!! because different tax code)
		gen EXP_INK = 0.183 * dah * usable_ink_frct if IPVO==0
		replace EXP_INK = dah * usable_ink_frct if IPVO==1
		gen REV_INK = rev * usable_ink_frct

	// Private (non-BMGF) overseas health expenditure and revenue
		gen EXP_OTHER_PRIV = dah * usable_other_priv_frct
		gen REV_OTHER_PRIV = rev * usable_other_priv_frct

	// Totals (Note: EXP_HEALTH is different from dah because in-kind is discounted to reflect real market value in EXP_HEALTH.)
		gen EXP_HEALTH = EXP_PUB_US + EXP_PUB_NONUS + EXP_BMGF + EXP_INK + EXP_OTHER_PRIV   
		gen REV_HEALTH = REV_PUB_US + REV_PUB_NONUS + REV_BMGF + REV_INK + REV_OTHER_PRIV   

	// Faith-based org expenditure
		foreach e in EXP_PUB_US EXP_PUB_NONUS EXP_BMGF EXP_INK EXP_OTHER_PRIV EXP_HEALTH {
			gen faith_`e' = `e'*FBO
		}	

	//preserve
	//collapse (sum) EXP_PUB_US EXP_PUB_NONUS EXP_BMGF EXP_INK EXP_OTHER_PRIV EXP_HEALTH, by (year)

	// Health focus areas expenditure disbursements
		foreach healthfocus in swap_hss_hrh swap_hss_me swap_hss_other swap_hss_pp rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_hss_me nch_hss_me hiv_hss_me mal_hss_me tb_hss_me oid_hss_me ncd_hss_me rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other {
			foreach e in EXP_PUB_US EXP_PUB_NONUS EXP_BMGF EXP_INK EXP_OTHER_PRIV EXP_HEALTH {
				gen `healthfocus'_`e' = final_`healthfocus'_frct * `e'
			}
		}

		drop *_total 
		
		tempfile complete_database
		save `complete_database', replace	
	

** *******		
// Step 10: Saving data for PDB
** *******
	// Reshape the data
		use `complete_database', clear

		drop *EXP_HEALTH REV_HEALTH 
		//this is where income_sector gets created
		
		reshape long EXP_ REV_ tb_hss_other_EXP_ oid_hss_other_EXP_ ncd_hss_other_EXP_ mal_hss_other_EXP_ hiv_hss_other_EXP_ rmh_hss_other_EXP_ nch_hss_other_EXP_  ///
		tb_hss_hrh_EXP_ oid_hss_hrh_EXP_ ncd_hss_hrh_EXP_ mal_hss_hrh_EXP_ hiv_hss_hrh_EXP_ rmh_hss_hrh_EXP_ nch_hss_hrh_EXP_ ///
		tb_hss_me_EXP_ oid_hss_me_EXP_ ncd_hss_me_EXP_ mal_hss_me_EXP_ hiv_hss_me_EXP_ rmh_hss_me_EXP_ nch_hss_me_EXP_ ///
		rmh_fp_EXP_ rmh_mh_EXP_ nch_cnn_EXP_ nch_cnv_EXP_ rmh_other_EXP_ nch_other_EXP_ tb_treat_EXP_ tb_diag_EXP_ tb_other_EXP_ tb_amr_EXP_ ///
		hiv_treat_EXP_ hiv_prev_EXP_ hiv_pmtct_EXP_ hiv_other_EXP_ hiv_ct_EXP_ hiv_ovc_EXP_ hiv_care_EXP_ hiv_amr_EXP_ ///
		mal_diag_EXP_ mal_con_oth_EXP_ mal_treat_EXP_ mal_comm_con_EXP_ mal_con_nets_EXP_  mal_con_irs_EXP_ mal_other_EXP_ mal_amr_EXP_  ///
		ncd_tobac_EXP_ ncd_mental_EXP_ ncd_other_EXP_ oid_other_EXP_ oid_ebz_EXP_ oid_zika_EXP_ oid_amr_EXP_ swap_hss_other_EXP_ swap_hss_hrh_EXP_ swap_hss_pp_EXP_ other_EXP_, i(year IPVO id) j(INCOME_SECTOR) string
	
		rename year YEAR
						
	// Create variables for PDB database
		gen FUNDING_TYPE = ""
		gen DATA_LEVEL = "Project"
		gen PROJECT_ID = "NA"
				
		rename source DATA_SOURCE
		rename agency_description PROJECT_DESCRIPTION

		// Funding agency
		gen FUNDING_AGENCY = "USA_MISC_USNGO" if INCOME_SECTOR == "PUB_US" & IPVO == 0
		replace FUNDING_AGENCY = "USA_MISC_INTLNGO" if INCOME_SECTOR == "PUB_US"  & IPVO == 1
		
		replace FUNDING_AGENCY = "OTHER_PUB_USNGO" if INCOME_SECTOR == "PUB_NONUS"  & IPVO == 0
		replace FUNDING_AGENCY = "OTHER_PUB_INTLNGO" if INCOME_SECTOR == "PUB_NONUS"  & IPVO == 1
		
		replace FUNDING_AGENCY = "OTHER_PRIV_USNGO" if INCOME_SECTOR == "OTHER_PRIV" & IPVO == 0
		replace FUNDING_AGENCY = "OTHER_PRIV_INTLNGO" if INCOME_SECTOR == "OTHER_PRIV"  & IPVO== 1
		
		replace FUNDING_AGENCY = "BMGF_USNGO" if INCOME_SECTOR == "BMGF" & IPVO == 0
		replace FUNDING_AGENCY = "BMGF_INTLNGO" if INCOME_SECTOR == "BMGF" & IPVO == 1
		
		gen FUNDING_AGENCY_SECTOR = "GOV" if FUNDING_AGENCY == "OTHER_PUB" | FUNDING_AGENCY == "USA_MISC"
		replace FUNDING_AGENCY_SECTOR = "CSO" if FUNDING_AGENCY == "BMGF"
		
		gen FUNDING_AGENCY_TYPE = "OTHER" if FUNDING_AGENCY_SECTOR == "GOV"
		replace FUNDING_AGENCY_TYPE = "FOUND" if FUNDING_AGENCY == "BMGF"

		gen ISO3_FC = "NA" if FUNDING_AGENCY == "OTHER_PUB"
		replace ISO3_FC = "USA" if FUNDING_AGENCY == "BMGF" | FUNDING_AGENCY == "USA_MISC"		
		
		gen ISO3_RC = "QZA"
		replace ISO3_RC = "USA" if IPVO == 0
		gen RECIPIENT_COUNTRY = "UNSPECIFIED" 
		replace RECIPIENT_COUNTRY = "United States of America" if ISO3_RC == "USA"
		
		rename agency RECIPIENT_AGENCY
		
		gen RECIPIENT_AGENCY_SECTOR = "CSO"
		gen RECIPIENT_AGENCY_TYPE = "NGO" if IPVO == 0
		replace RECIPIENT_AGENCY_TYPE = "INTLNGO" if IPVO == 1
		
		rename EXP_ DISBURSEMENT
		
		preserve

		
		drop if INCOME_SECTOR == "INK" 	// Inkind not included in PDB
			
		keep YEAR RECIPIENT_AGENCY DATA_SOURCE PROJECT_DESCRIPTION swap_hss_hrh swap_hss_me swap_hss_other swap_hss_pp rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_fp rmh_hss_me nch_hss_me hiv_hss_me mal_hss_me tb_hss_me oid_hss_me ncd_hss_me rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other FUNDING_TYPE DATA_LEVEL PROJECT_ID FUNDING_AGENCY FUNDING_AGENCY_SECTOR FUNDING_AGENCY_TYPE ISO3_FC ISO3_RC RECIPIENT_COUNTRY RECIPIENT_AGENCY_SECTOR RECIPIENT_AGENCY_TYPE DISBURSEMENT

save "FILEPATH", replace


** ******* 
// Step 11: Allocating from Source to Channel to Health Focus Area (merging ADB and PDB):
** *******
	restore

	ren iso3 ISO3
	keep PROJECT_DESCRIPTION YEAR IPVO id INCOME_SECTOR RECIPIENT_AGENCY DATA_SOURCE DISBURSEMENT REV_ rmh_* nch_* hiv_* tb_* mal_* ncd_* oid_* swap_hss_* other other_* final_* FUNDING_AGENCY ISO3 ISO3_RC RECIPIENT_COUNTRY RECIPIENT_AGENCY_SECTOR RECIPIENT_AGENCY_TYPE upper_agency upper_agency_description
	drop *_EXP_ 
	
	// Generate DAH by HFA using HFA expenditure fractions
		rename DISBURSEMENT DAH
		
		foreach healthfocus in swap_hss_hrh swap_hss_other swap_hss_pp swap_hss_me rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_fp rmh_hss_me nch_hss_me hiv_hss_me mal_hss_me tb_hss_me oid_hss_me ncd_hss_me rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other {
			gen double `healthfocus'_DAH = final_`healthfocus'_frct * DAH
		}
		//drop *_frct
	tempfile NGO_before_crs_frct
	save `NGO_before_crs_frct', replace

// Bringing in and collapsing CRS HFA fractions to NGOs that have CRS projects to apply these NGOs

		use `complete_database', clear // use this one because it has all the info needed to make the CRS fractions and the NGO_before_crs_frct dataset has many duplicates because of the reshaping in step 10 
		
		// check for duplicates in NGOs
		//drop if id == 1439
		replace upper_agency = "INTERNATIONAL PLANNED PARENTHOOD FEDERATION WESTERN HEMPISHERE" if id == 527
		//replace id = 438 if id == 1526 & year >= 2013 & year <= 2018
		//drop if id == 1526
		drop if id == 10007 & (year == 2019 | year == 2020)
		replace id = 10007 if id == 1313 & year >= 2019 & year <= 2020
		drop if id == 1313
		//drop if id == 1313
		drop if id == 10202
		replace agency = "HANDICAP INTERNATIONAL USA" if id == 1275
		replace upper_agency = "HANDICAP INTERNATIONAL USA" if id == 1275

		// remove duplicates that should be just international NGOs but have both
		drop if upper_agency == " CONCERN UNIVERSAL " & IPVO == 0

		// Clean names to help match more CRS NGOS to Volag/Guidestar NGOS
		replace upper_agency = subinstr(upper_agency, "&", "AND", .)
		replace upper_agency = strtrim(upper_agency)
		replace upper_agency = "WORLD VISION INC" if upper_agency=="WORLD VISION" 
		replace upper_agency = "THE MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES" if upper_agency == "MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES"
		drop if upper_agency == "INTERNATIONAL SENIOR LAWYERS CORPORATION" & id == 1452
		drop if id == 10228  //just 2023 because scraped data looks worse than real for action contre la faim...
		duplicates list upper_agency year
		sort upper_agency year
		quietly by upper_agency year:  gen dup = cond(_N==1,0,_n)
		drop if dup > 1
		tempfile unique_ngos
		save `unique_ngos', replace

		// Get HFA fractions from CRS NGOs
		preserve

		insheet using "`CRS'/B_CRS_`crs_update_mmyy'_INTNEW.csv", comma clear
		keep if ngo_eliminations == 1 | ingo_eliminations == 1 //Keep NGOs that are tagged in 4_CRS_keywordsearch_fix
		keep *_frct ngos ngo_name ngo ngo_eliminations ingo_eliminations year donor_name upper_* crs_id ia_all* //keep variables we want
		drop pepfar_* final_total_frct
		egen sum=rowtotal(*_frct)
		rename final_* CRS_final_*
		rename ngo_name upper_agency 

		// Clean names to help match more CRS NGOS to Volag/Guidestar NGOS
		replace upper_agency = subinstr(upper_agency, "&", "AND", .)
		replace upper_agency = strtrim(upper_agency)
		replace upper_agency = "WORLD VISION INC" if upper_agency=="WORLD VISION" 
		replace upper_agency = "THE MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES" if upper_agency == "MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES"
		
		tempfile CRS_NGO_frcts
		save `CRS_NGO_frcts'
		
		restore

		// Merge with NGO health expenditure totals by agency and year 

		merge 1:m year upper_agency using `CRS_NGO_frcts' //1896 tagged NGOs from CRS did not match 
		gen CRS_match = 1 if _m == 3
		keep if _merge==3 | _merge==1 //
		drop _m

		tempfile crs_ngo_matched
		save `crs_ngo_matched'

	// There are one to multiple CRS projects for one NGO in a given year but we want to end with just one row for each NGO for each year. 
	//To accomplish this we use a weighted average based on how much of the disbursement a CRS project contributes to. And take the weighted average of the HFA fractions.
		
		// Make file of just NGOs with CRS projects
		use `crs_ngo_matched', clear
		keep if CRS_match == 1 
		
		tempfile crs_ngo_match3
		save `crs_ngo_match3', replace

		preserve 
		collapse(sum) ia_all*, by(upper_agency year) //sum amount of NGO disbursement in CRS
		tempfile sum_crs
		save `sum_crs', replace
		restore

		collapse(mean) EXP_HEALTH dah, by(upper_agency year) //Use mean because each year of an NGO should be the same (duplicates are created from more CRS projects per an NGO per yer)
		merge 1:1 year upper_agency using `sum_crs'
		drop _m
		
		ren ia_all* sum_ia_all*
		ren EXP_HEALTH mean_EXP_HEALTH

		merge 1:m year upper_agency using `crs_ngo_match3' 
		drop _m 
		
		preserve
		//Create a weight for each CRS project for each NGO year
		gen double crs_weight = ia_all_disbcurr / sum_ia_all_disbcurr
		replace crs_weight = 0 if crs_weight < 0 //remove negative projects from weight (drop or just make 0?)
		
		//Create weighted average of CRS 
		collapse(mean) CRS_final_*_frct [w=crs_weight], by(year upper_agency)
		egen double total_frct=rowtotal(CRS_*frct) //should add to 1
		ren CRS_final_*_frct CRS_w_*_frct

		tempfile crs_weighted_hfa
		save `crs_weighted_hfa', replace

		restore
	

	//Replace NGO HFA Assignments for NGOs with matches across years 

		//merge CRS weighted fractions (one set per each NGO and year) back into unique NGOs dataset
		use `unique_ngos', clear
		merge 1:1 year upper_agency using `crs_weighted_hfa'
		gen crs_frct = 1 if _m==3
		drop _m

		// Create weighted fraction to loop over years and fill in missing gaps 
		//Mark the NGOs that have CRS 
		preserve
		keep if crs_frct==1
		levelsof upper_agency, local(agency_list)
		restore

		foreach l of local agency_list {
			replace crs_frct = 2 if upper_agency== "`l'" & crs_frct== .
		}
	
		// Find mean for each NGO that has some CRS data (alternative to weighted mean)
		preserve 
			keep if crs_frct==1 
			collapse(mean) CRS_w_*, by(upper_agency)
			ren CRS_w_* ave_CRS_*
		
			//scale to 1 as some are less
			egen double total_frct = rowtotal(ave_CRS_*)
			gen scale_props = 1 / total_frct
			foreach var of varlist ave_CRS_* {
				gen double new_`var' = `var' * scale_props
			}
			egen double sum=rowtotal(new_*)

			tempfile crs_hfa_mean_by_ngo
			save `crs_hfa_mean_by_ngo'
		restore

	
	merge m:1 upper_agency using `crs_hfa_mean_by_ngo'
	drop _m

	//Replace CRS fractions for thos NGO years for NGOs that have CRS projects in other years
	foreach var in swap_hss_hrh swap_hss_other swap_hss_pp rmh_fp rmh_mh rmh_other rmh_hss_other rmh_hss_hrh nch_cnn nch_cnv nch_other nch_hss_other hiv_treat hiv_prev hiv_pmtct hiv_ovc hiv_care hiv_ct hiv_amr hiv_other hiv_hss_other hiv_hss_hrh mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other mal_hss_other mal_hss_hrh tb_treat tb_diag tb_amr tb_other tb_hss_other tb_hss_hrh oid_ebz oid_zika oid_amr oid_other oid_hss_other oid_hss_hrh ncd_tobac ncd_mental ncd_other ncd_hss_other ncd_hss_hrh other   {
			replace CRS_w_`var' = new_ave_CRS_`var' if crs_frct==2
	}

	//Keep what vars we want and clean up 
	keep year upper_agency CRS_w_*
	ren year YEAR
	egen double total_frct=rowtotal(CRS_*frct)
	keep if total_frct > 0

			//scale to 1 as some are less
			drop total_frct
			egen double total_frct = rowtotal(CRS_w_*)
			gen scale_props = 1 / total_frct
			foreach var of varlist CRS_w_* {
				gen double new_`var' = `var' * scale_props
			}
			egen double sum=rowtotal(new_*)
			drop CRS_w_*
			ren new_* *
			drop total_frct scale_props

	tempfile final_crs_frcts
	save `final_crs_frcts'

	//merge back into dataset prior to this and replace NGO fractions with CRS fractions
	use `NGO_before_crs_frct', clear
	replace upper_agency = subinstr(upper_agency, "&", "AND", .)
	replace upper_agency = strtrim(upper_agency)
	replace upper_agency = "WORLD VISION INC" if upper_agency=="WORLD VISION" 
	replace upper_agency = "THE MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES" if upper_agency == "MENNONITE ECONOMIC DEVELOPMENT ASSOCIATES"
		
	merge m:1 YEAR upper_agency using `final_crs_frcts'
	//replace health fractions for matched obs
	foreach var in swap_hss_hrh swap_hss_other swap_hss_pp rmh_fp rmh_mh rmh_other rmh_hss_other rmh_hss_hrh nch_cnn nch_cnv nch_other nch_hss_other nch_hss_hrh hiv_treat hiv_prev hiv_pmtct hiv_ovc hiv_care hiv_ct hiv_amr hiv_other hiv_hss_other hiv_hss_hrh mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other mal_hss_other mal_hss_hrh tb_treat tb_diag tb_amr tb_other tb_hss_other tb_hss_hrh oid_ebz oid_zika oid_amr oid_other oid_hss_other oid_hss_hrh ncd_tobac ncd_mental ncd_other ncd_hss_other ncd_hss_hrh other   {
			replace final_`var'_frct = CRS_w_`var'_frct if _m==3
	}

	// reapply health fractions that have changed 
	foreach healthfocus in swap_hss_hrh swap_hss_other swap_hss_pp rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other {
			gen double `healthfocus'_DAH_new = final_`healthfocus'_frct * DAH if _m==3
		}

	foreach healthfocus in swap_hss_hrh swap_hss_other swap_hss_pp rmh_hss_other nch_hss_other hiv_hss_other mal_hss_other tb_hss_other oid_hss_other ncd_hss_other rmh_hss_hrh nch_hss_hrh hiv_hss_hrh mal_hss_hrh tb_hss_hrh oid_hss_hrh ncd_hss_hrh rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_ct hiv_ovc hiv_care hiv_amr hiv_other mal_diag mal_con_nets mal_con_irs mal_con_oth mal_treat mal_comm_con mal_amr mal_other tb_treat tb_diag tb_amr tb_other oid_ebz oid_zika oid_amr oid_other ncd_tobac ncd_mental ncd_other other {
			replace `healthfocus'_DAH = `healthfocus'_DAH_new if _m==3
		}

	drop _m CRS_w_* *_DAH_new
	save "FILEPATH", replace
	drop *_frct

// Format data and save:
		rename RECIPIENT_AGENCY CHANNEL_NAME
		rename DATA_SOURCE SOURCE_DOC
		rename ISO3 ISO_CODE
		rename FUNDING_AGENCY DONOR_NAME
		rename RECIPIENT_AGENCY_TYPE CHANNEL
		
		gen DONOR_COUNTRY = "United States" if (INCOME_SECTOR != "PUB_NONUS" & IPVO == 0) | INCOME_SECTOR == "PUB_US"
		replace DONOR_COUNTRY = "UNSP" if INCOME_SECTOR == "PUB_NONUS" & IPVO == 0 | (INCOME_SECTOR != "PUB_US" & IPVO == 1)
		replace ISO_CODE = "UNSP" if INCOME_SECTOR == "PUB_NONUS" & IPVO == 0 | (INCOME_SECTOR != "PUB_US" & IPVO == 1)
		gen INKIND = 1 if INCOME_SECTOR == "INK"
		replace INKIND = 0 if INKIND != 1

save "FILEPATH", replace



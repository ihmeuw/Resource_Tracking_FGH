** *****************************************************************************
// Project:		FGH
// Purpose: 	Merge new IDB data with old and calculate disbursements
** *****************************************************************************

** *****************************************************************************
// SETUP
** *****************************************************************************

	// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global j "FILEPATH"
        global h "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
        global h "H:"
	}

	*sysdir set PLUS "FILEPATH"
	
** *****************************************************************************
// USER INPUTS
** *****************************************************************************
// FGH report year
	local report_yr = 2024		
	local previous_yr = `report_yr' - 1		
// YYYYMMDD data was downloaded
	local data_date "2025-03-05" 		
// YYYYMMDD correspondence data arrived
// 	local corr_date ""
// _round_date		

	// Derived macros
	// Previous FGH report year
	local report_yr_short = substr("`report_yr'", 3, 4)
	local previous_yr_short = substr("`previous_yr'", 3, 4)

	// Filepaths
	local WD 			"FILEPATH"
	local RAW			"FILEPATH"
	local FIN 			"FILEPATH"
	local OUT			"FILEPATH"
	local INT_DB		"`FILEPATH"
    local HFA            "FILEPATH"
	local country_feat	"FILEPATH"

	// Files
	local pd_2017 "`WD'FILEPATH/M_IDB_PD_0917_UPDATED.xlsx" 
	local country_codes "FILEPATH/countrycodes_official.dta"
	local incgrps "FILEPATH/wb_historical_incgrps.dta"
	local fso_cont_1990_2016 "`WD'FILEPATH/FSO_Contribution_Percentage_1990_2016.xlsx"
	local idb_cont_quota_1990_2016 "FILEPATH""
    local data_prev "`WD'/FILEPATH/FGH_`previous_yr'/all_idb_clean.xlsx"

	// Runs
	run "`HFA'/Health_ADO_master.ado"
	run "FILEPATH/covid_intercept.ado"

	local update_tag "2025-03"		
	
** *****************************************************************************
// STEP 1: LOAD IDB DATA
** *****************************************************************************

/* -----------------------------------------------------------------------------
	NOTE:
	As of FGH 2018, the web-scraping tool no longer works. Instead, we download
	project-level data found on IDB's website.

	In FGH 2019, the downloaded project-level dataset appears to be a collapsed
	version compared to FGH 2018's. Specifically,:
		- the operation numbers are identical to the project numbers. Last year 
		  there were multiple unique operation numbers per project number.
		- all statuses are APP (presumably "approved") or Preparation. Last year
		  there was also "Approved/Pending", "Closed", "Completed", and 
		  "Implementation".
		- many of the sums of ApprovalAmountUSM from last year's dataset
		  match this year's ApprovalAmountUSM (implying it is indeed a
		  collapsed dataset).

	With the collapsed format, we need to collapse the old datasets we
	use when calculating completion dates (the dataset from 2017 which is the
	last one we have that was web-scraped. Assumptions along the way include:
		- Old data completion dates are the latest listed for each project/operation combo
		- Old data approval dates are the oldest listed for each project/operation combo
		- Changed statuses to those from the old dataset where they exist.

	Further, the data no longer includes disbursed amounts (only approval
	amounts) and so we use a local adjustment (completed project
	disbursed amount / approved	amount) from FGH 2017.

	NOTE:
	For FGH 2021, the correspondence data came in a different format and we manually 
	extracted project description, operation type, country name, sector, approval date, 
	and status. The overall formatting changed and so we changed most of the original data
	in excel to try and keep the code the same. The raw data was "._Desembolsos-31 enero 2022"
	which became "Desembolsos-31 enero_20223101_regular_updated" which was then copied over to
	"IDB_2015_2021_disbursements.csv""
------------------------------------------------------------------------------*/


	// import excel using "`RAW'/All_IDB_Health_projects_`data_date'.xlsx", cellrange(A2) firstrow clear
	import excel using "`RAW'/all_idb_clean.xlsx", firstrow clear

	// Check for duplicate project numbers or operation IDs
	isid Projectnumber
	//isid Operationnumber ##This column was dropped by IDB in 2020

	tab Status
 	
	ren (ApprovalAmountUSM Projectnumber Status Projecttitle) (new_amt proj_id new_status new_proj_name)
	
	// Convert amounts to numbers
	destring new_amt, replace

	//replace new_amt = new_amt * 1e6
    replace new_status = "Implementation" if new_status == "ACTIVE"
    replace new_status = "Closed" if new_status == "EXITED"
	replace new_status = "Missing" if new_status == ""

	gen new_approval_date = date(ApprovalDate, "YMD", 2050)
	format new_approval_date %td

	drop ApprovalDate //Operationnumber dropped in 2022
	tostring ESGClassification, replace

	ren (Projectdescription ProjectOperationtype ProjectCountry ProjectSector) ///
	    (new_proj_description new_proj_type new_proj_country new_proj_sector)

/* -----------------------------------------------------------------------------
	NOTE:
		FGH2019: Drop Operation number because it is no longer unique - it is the same as
		proj_id
------------------------------------------------------------------------------*/
	
	tempfile data_`report_yr'
	save `data_`report_yr''
	
** *****************************************************************************
** LOAD LAST YEAR'S DATASET
	import excel using "`data_prev'", firstrow clear
 	
	// get rid of strings where necessary - the amount variables - long, not string
	ren ApprovalAmountUSM old_amt
	ren Projectnumber proj_id
	ren ProjectOperationtype old_proj_type
			
	replace Status = "missing" if Status == "" 
	rename Status old_status
    replace old_status = "Implementation" if old_status == "ACTIVE"
    replace old_status = "Closed" if old_status == "EXITED"

	gen old_approval_date = date(ApprovalDate, "DMY", 2050)
	format old_approval_date %td

    gen old_oper_id = ""

	ren (Projecttitle Projectdescription ProjectCountry ProjectSector) ///
	    (old_proj_name old_proj_description old_proj_country old_proj_sector)
	
	tempfile data_old
	save `data_old'

** *****************************************************************************
** INTEGRATE/UPDATE LAST YEAR'S DATA
	use `data_old', replace
	merge m:1 proj_id using `data_`report_yr''

** -----------------------------------------------------------------------------
// EXPLORE 
	preserve
		drop if _merge == 2
		drop _merge
		duplicates tag proj_id, gen(dup_id)
		bysort proj_id: egen tot_old_amt = total(old_amt)
		collapse(mean) tot_old_amt new_amt, by(proj_id)
		collapse (sum) tot_old_amt new_amt
		gen perc_diff = (new_amt - tot_old_amt) / tot_old_amt * 100
		quietly sum perc_diff
		di in red "The difference in matched project approval amounts (in real USD)" ///
		    "between this year and last year datasets is `r(mean)'%"
	restore
** -----------------------------------------------------------------------------
	
	drop if _merge == 1
	drop _merge

	ren old_oper_id oper_id

	// proj_name
	gen proj_name = new_proj_name if new_proj_name != old_proj_name
	replace proj_name = new_proj_name if proj_name == ""
	replace proj_name = old_proj_name if proj_name == ""
	drop old_proj_name new_proj_name

	// proj_description
	gen proj_description = new_proj_description if new_proj_description != old_proj_description
	replace proj_description = new_proj_description if proj_description == ""
	replace proj_description = old_proj_description if proj_description == ""
	drop old_proj_description new_proj_description

	// proj_type
	gen proj_type = new_proj_type if new_proj_type != old_proj_type
	replace proj_type = new_proj_type if proj_type == ""
	replace proj_type = old_proj_type if proj_type == ""
	drop old_proj_type new_proj_type

	// proj_country
	gen proj_country = new_proj_country if new_proj_country != old_proj_country
	replace proj_country = new_proj_country if proj_country == ""
	replace proj_country = old_proj_country if proj_country == ""
	drop old_proj_country new_proj_country

	// proj_sector
	gen proj_sector = new_proj_sector if new_proj_sector != old_proj_sector
	replace proj_sector = new_proj_sector if proj_sector == ""
	replace proj_sector = old_proj_sector if proj_sector == ""
	drop old_proj_sector new_proj_sector

	// status
/* -----------------------------------------------------------------------------
NOTE:
	The assumed order of status a project can go is
	"Approved" -> "Preparation" -> "Implementation" -> "Completed"

	A project can presumably become "Cancelled" at any time.

	It is unclear what "Closed" means but it's treated like "Completed".

	Note that this analysis assumes that "Cancelled" projects disbursed no
	money - even though they very well might have prior to become cancelled.
----------------------------------------------------------------------------- */

	tab old_status
	gen status = old_status if inlist(old_status, "Cancelled", "Closed", "Completed")
	
/* -----------------------------------------------------------------------------
NOTE:
	At this point we've assigned statuses if they were already considered 
	"Cancelled", "Closed", or "Completed". Further, new_status (while mostly
	filled with the not-so-useful "APP") can have a value of "Preparation".
	All that is left to assign is "Approved" or "Implementation".

	Any leftover status with "APP" (ie it's not "Cancelled", 
	"Closed", "Completed", or "Preparation") must be either "Approved" or
	"Implementation". We then assume that any old_status == "Preparation" &
	new_status != "Preparation" must be "Implementation"
----------------------------------------------------------------------------- */
	
	replace status = new_status if status == ""
	
	replace status = "Implementation" if old_status == "Preparation" & new_status != "Preparation"

	drop new_status

/* -----------------------------------------------------------------------------
NOTE:
	For approval amounts:
		1. Use the new amount instead of the old for unique proj_id
		2. Use old amounts for unique proj_id where there are no new amounts
		3. For non-unique proj_ids (ie where the old dataset had two or more
			operation ids for a given proj_id), scale the old amount up or down
			to match their total envelope to that of the new amount.
----------------------------------------------------------------------------- */
	duplicates tag proj_id, gen(dup_id)
	gen approval_amt = new_amt if dup_id == 0 & new_amt != old_amt
	replace approval_amt = new_amt if dup_id == 0 & approval_amt == .
	replace approval_amt = old_amt if dup_id == 0 & approval_amt == .

	bysort proj_id: egen tot_old_amt = total(old_amt)
	gen old_amt_frac = old_amt / tot_old_amt
	replace approval_amt = old_amt_frac * new_amt if approval_amt == .
	drop old_amt new_amt old_amt_frac tot_old_amt 

	// approval_date
	gen approval_date = new_approval_date if ///
	    new_approval_date == old_approval_date
	replace approval_date = new_approval_date if ///
	    old_approval_date == . & approval_date == .
	list proj_id oper_id status old_approval_date new_approval_date approval_date approval_amt if ///
	    approval_date == . & (old_approval_date != . & new_approval_date != .)
	replace approval_date = new_approval_date if ///
	    approval_date == . & new_approval_date != old_approval_date
/* -----------------------------------------------------------------------------
NOTE:
	Most changes to approval data are no more than 6-7 months apart. 
	proj_id HA0045, however, went from an approval data of 
	29sep2010 to 12aug1998 (approval amount $830,000).
----------------------------------------------------------------------------- */
	format approval_date %td
	drop old_approval_date new_approval_date

	// Clean up
	rename (approval_amt status) (new_amt new_status)
	drop dup_id

	tempfile data
	save `data'

** *****************************************************************************
// STEP 2. MERGE FGH 2017 PD AND NEW DOWNLOADED DATASET
** *****************************************************************************
/* -----------------------------------------------------------------------------
NOTE:
	We load FGH 2017 PD dataset because that was the last set of data that was 
	web-scraped where we have completion dates and other variables no longer 
	available.
------------------------------------------------------------------------------*/

	import excel using "`pd_2017'", firstrow clear
	keep proj_id proj_type oper_id status completion_date approval_amt disbursed_amt
	foreach var of varlist status completion_date approval_amt disbursed_amt proj_type {
		rename `var' `var'_2017
	}

// since new data does not contain oper_id, sum different project rows together
// and default to the latest completion date within the project.

	bysort proj_id: egen completion_date_2 = max(completion_date_2017)
	format completion_date_2 %tdd_m_y
	drop completion_date_2017
	rename completion_date_2 completion_date_2017
	collapse(sum) approval_amt_2017 disbursed_amt_2017, by (proj_id completion_date_2017)
** *****************************************************************************
// Merge new data onto FGH 2017 datset
	merge 1:1 proj_id using `data'
	drop if _merge == 1
		// FGH 2020 - 1 observation deleted - if large drops occur, do not ignore.


	// Percent differences between old and new
	gen diff_appr = new_amt - approval_amt_2017
	gen percdiff_appr = diff_appr / approval_amt_2017 * 100 
	gen diff_disb = new_amt - disbursed_amt_2017
	gen percdiff_disb = diff_disb / disbursed_amt_2017 * 100 
	count
	count if _m == 3 & (percdiff_appr > 1 | percdiff_appr < -1)
	count if _m == 3 & (percdiff_disb > 1 | percdiff_disb < -1)
	count if _m == 3 & ((percdiff_appr > 1 | percdiff_appr < -1) & (percdiff_disb > 1 | percdiff_disb < -1))

	drop _merge diff_* percdiff_*

	tab old_status new_status if old_status != new_status
	drop if new_status == "Cancelled" | new_status == "Preparation"
	replace completion_date_2017 = . if new_status != "Completed" & new_status != "Closed" & new_status != "EXITED"

** *****************************************************************************
// Fix project types
/*
	// Check for differences
	list proj_type_2017 proj_type if proj_type_2017 != proj_type & proj_type_2017 != "" 
	drop proj_type_2017
*/
** *****************************************************************************
// Cleanup
	keep completion_date_2017 proj_id oper_id proj_name proj_description ///
	    proj_type proj_country proj_sector new_amt old_status new_status approval_date 

	ren new_status status
	ren new_amt approval_amt
	ren proj_country country_lc

	order proj_id oper_id old_status status country_lc approval_date ///
	    completion_date_2017 approval_amt proj_type proj_sector proj_name proj_description

** *****************************************************************************
// Merge on iso codes
	merge m:1 country_lc using "`country_codes'", keepusing(iso3)
	//merge m:1 location_name using "`country_codes'", keepusing(ihme_loc_id)
	drop if _m == 2

	// Clean up ISO codes
	replace iso3 = "QNE" if country_lc == "Regional"
    // TODO: these projects are regional in the sense that they are multicountry,
    // but we could divide them up since the country names are given
    replace iso3 = "QNE" if iso3 == ""
	quietly count if iso3 == ""
	if `r(N)' != 0 {
		di in red ("Fix ISO codes!")
		empty_iso3_codes
	}
	drop _merge
	
	rename (country_lc iso3) (RECIPIENT_COUNTRY ISO3_RC)
	order ISO3_RC proj_id oper_id old_status status approval_date completion_date_2017 ///
	    approval_amt proj_type proj_sector proj_name proj_description RECIPIENT_COUNTRY

	tempfile alldata_check1
	save `alldata_check1'

** *****************************************************************************
// STEP 3. CALCULATE PROJECT LENGTH
** *****************************************************************************
	// Get completion dates and statuses from last years' data
	preserve
		use "`WD'/CHANNELS/3_DEV_BANKS/2_IDB/DATA/FIN/FGH_`previous_yr'/M_IDB_INTPDB_`previous_yr'_update.dta", clear
		keep proj_id PROJECT_ID completion_day
        rename PROJECT_ID oper_id
		rename completion_day completion_date_`previous_yr'

		// Drop duplicates
		duplicates drop proj_id completion_date_`previous_yr', force
        drop if completion_date_`previous_yr' == .

		tempfile old_dates
		save `old_dates'
	restore

	merge 1:1 proj_id oper_id using `old_dates'
	drop if _merge == 2
	drop _merge

	format completion_date_2017 %td
	format completion_date_`previous_yr' %td


	// Use newer completion dates when different from old ones
	generate completion_day = completion_date_`previous_yr'
	replace completion_day = completion_date_2017 if completion_day == .
	format completion_day %td
	list proj_id oper_id status approval_amt completion_date_2017 completion_date_`previous_yr' completion_day if ///
	    completion_date_2017 != completion_date_`previous_yr' & completion_date_2017 != . & completion_date_`previous_yr' != .
		// There are 30 observations here with differences in completion dates
		// between the fgh 2017 PDB and last year's INTPDB dataset; some of these
		// differences are quite large (eg 7 years for proj_id ME-L1066)

	gen completion_year = year(completion_day)
	rename approval_date approval_day
	gen approval_year = year(approval_day)
	format completion_day %td

	// Fix new project statuses
/* -----------------------------------------------------------------------------
	NOTE:
	We make the following assumptions regarding new projects that haven't yet
	been assigned a status (ie those with status == "APP"):
		1. If there is a completion day then the status is "Implementation" 
			(ie it's farther along then simply approved but it's not yet closed)
		2. Replace the status with the old status unless the old status is empty
		3. Replace the status with "Approved" if the old status is empty
----------------------------------------------------------------------------- */
	replace status = "Implementation" if status == "APP" & completion_day != .
	replace status = old_status if status == "APP" & old_status != ""
	replace status = "Approved" if status == "APP"
	quietly count if status == ""
	if `r(N)' != 0 {
		di in red "Still have empty statuses"
		check_empty_status
	}
	quietly count if status == "APP"
	if `r(N)' != 0 {
		di in red "Still have unassigned (APP) statuses"
		check_APP_status
	}

** *****************************************************************************
// 1. Closed and completed projects with completion and approval days
	gen PROJ_LENGTH_cl = (completion_day - approval_day) if ///
	  status == "Completed" | status == "Closed" | status == "EXITED"

	// Fix negative project lengths
	replace completion_day = . if ///
	    (status == "Completed" | status == "Closed" | status == "EXITED") & PROJ_LENGTH_cl < 0 
	replace PROJ_LENGTH_cl = . if ///
	    PROJ_LENGTH_cl < 0 & (status == "Completed" | status == "Closed" | status == "EXITED") 

	tempfile alldata_check2
	save `alldata_check2'

** *****************************************************************************
// 2.) Closed projects with no completion day (fill in average by project type)
	
	preserve
		keep if status == "Completed" | status == "Closed" | status == "EXITED"
		collapse (mean) PROJ_LENGTH_cl, by(proj_type)
		rename PROJ_LENGTH_cl average_length_for_closed_type
		tempfile avg_clo
		save `avg_clo'
	restore
	merge m:1 proj_type using `avg_clo'
	
	replace PROJ_LENGTH_cl = average_length_for_closed_type if ///
	    (PROJ_LENGTH_cl == .) & (status == "Completed" | status == "Closed" | status == "EXITED") 						
	replace completion_day = approval_day + PROJ_LENGTH_cl if ///
	    completion_day == . & (status == "Completed" | status == "Closed" | status == "EXITED")

	tempfile alldata_check3
	save `alldata_check3'
		
** *****************************************************************************
// 3.) Open projects with disbursement data but no completed or closed date
	local ref_date = date("`data_date'", "YMD", 2050)

	// a.) calculate number of days the project has been open (any disbursement 
	// amount for these projects will be allocated throughout the number of days 
	// it has been open)
	gen PROJ_LENGTH_ac = (`ref_date' - approval_day) if status == "Implementation" | status == "ACTIVE"
	
	// b.) calculate number of additional days the project will be open (based on average length)
	gen PROJ_LENGTH_extra = average_length_for_closed_type - PROJ_LENGTH_ac if ///
	    (PROJ_LENGTH_ac < average_length_for_closed_type) 
	
	// c.) fill in completion day 
	
	// i.)  if the average project length is less than the difference between 
	// the open day and ref_date -- fill in the completion date as ref_date 
	replace completion_day = `ref_date' if ///
	    PROJ_LENGTH_ac != . & PROJ_LENGTH_ac > average_length_for_closed_type
		
	// i.)  if the average project length is greater than the difference between 
	// the open day and ref_date -- fill in the completion date as the 
	// open day + average project length 
	// the average project length = PROJ_LENGTH_ac + PROJ_LENGTH_extra
	replace completion_day = approval_day + PROJ_LENGTH_ac + PROJ_LENGTH_extra if ///
	    PROJ_LENGTH_ac < average_length_for_closed_type

	// 4.) Odd projects
	replace PROJ_LENGTH_cl = 1 if PROJ_LENGTH_cl == 0
	replace PROJ_LENGTH_cl = 0 if status == "Approved"
		
	// 5.) Calculate % of commitments that were disbursed for closed projects by 
	// type for the last 5 years (Used to adjust commitments for open projects)
	/* COMMENTED CHUNK:
			preserve
				keep if status == "Completed"
				local cutoff = `report_yr' - 5 
				keep if approval_year >= `cutoff'
				
				
				gen comm_disb_ratio = disbursed_amt / approval_amt
				collapse (mean) comm_disb_ratio
				local adjustment = comm_disb_ratio 

				/* NOTE:
					FGH 2014: was 0.879 for FGH 2014, possible more now as 
						approval_amt has become idb_financing with web page changes, 
						and IDB financing includes spending estimates including 
						cancelled amounts, which may be a bit different than initial 
						spending estimates. 
					FGH 2015: Now it is 0.919. 
					FGH 2016: it is 0.879
					FGH 2017, it is .891
				*/
			restore 
	*/
/* -----------------------------------------------------------------------------
NOTE:
	The new downloaded dataset no longer has disbursed amounts. We use the
	ratio calculated from FGH 2017.
----------------------------------------------------------------------------- */

	local adjustment .891 
		//In the FGH 2017 webscraping, there are only 38 projects for which 
		// disbursed amount != approval_amount (aka the IDB Financing variable)

	tempfile alldata_check4
	save `alldata_check4'
			
** *****************************************************************************
// STEP 4: Generate yearly disbursed amounts
** *****************************************************************************
		
** *****************************************************************************
// 1.) Closed projects	
	preserve
		keep if status == "Completed" | status == "Closed"
		
		// Gen disbursements per day
		gen IDB_DISB_PER_DAY = approval_amt / PROJ_LENGTH_cl

		// Gen number of days that the project was open per year
		forvalues i = 1984(1)2030 {
			gen firstday_`i' = date("01/01/`i'", "MDY", 2050)
			gen lastday_`i' = date("12/31/`i'", "MDY", 2050)
			gen yearlength_`i' = lastday_`i' - firstday_`i' + 1
			gen fundeddays_`i' = 0
			replace fundeddays_`i' = max(0, min(completion_day, lastday_`i') - max(approval_day, firstday_`i'))
			drop firstday_`i' lastday_`i' yearlength_`i'
			replace fundeddays_`i' = 365 if fundeddays_`i' == 364
		}

		// Gen amount disbursed per year
		forvalues i = 1984(1)2030 {
			gen IDB_DISB_`i' = IDB_DISB_PER_DAY * fundeddays_`i'
		}

		drop fundeddays_*

		// Drop disbursements after report_yr
		local next_yr = `report_yr' + 1
		forvalues i = `next_yr'/2030 { 
			drop IDB_DISB_`i'
		} 
			
		tempfile disb_closed
		save `disb_closed', replace
	restore
	
** *****************************************************************************
// 2.) Open projects
	preserve
		keep if status == "Implementation" | status == "ACTIVE"

		// calculate disbursements from when the project opened until ref_date
		// Gen disbursements per day
	    gen IDB_DISB_PER_DAY = approval_amt / PROJ_LENGTH_ac

		// Generate proxy var for ref_date
		gen completion_day_today = `ref_date'
		
		// Gen number of days that the project was open per year
		forvalues i = 1984(1)2030 {
			gen firstday_`i' = date("01/01/`i'", "MDY", 2050)
			gen lastday_`i' = date("12/31/`i'", "MDY", 2050)
			gen yearlength_`i' = lastday_`i' - firstday_`i'+1
			gen fundeddays_`i' = 0
			replace fundeddays_`i' = ///
				max(0, min(completion_day_today, lastday_`i') - max(approval_day, firstday_`i'))
			drop firstday_`i' lastday_`i' yearlength_`i'
		}
			
		// Gen amount disbursed per year
		forvalues i = 1984(1)2030 {
			gen IDB_DISB_`i' = IDB_DISB_PER_DAY * fundeddays_`i'
		}
		drop fundeddays_* 

		// Drop disbursements after reportyr
		forvalues i = `next_yr'/2030 { 
			drop IDB_DISB_`i'
		} 
			
		// calculate adjusted commitments
		gen comm_disb_ratio = `adjustment'
		gen adjusted_comm = approval_amt * comm_disb_ratio
			
		// calculate the difference between total disbursements and the adjusted committed amount
		egen total_disb = rowtotal(IDB_DISB_*)
		gen future_disb = adjusted_comm - total_disb
		replace future_disb = 0 if future_disb < 0
			
		// calculate disbursements for the remainder of the year
		gen IDB_DISB_PER_DAY_EXTRA = future_disb / PROJ_LENGTH_extra
			
		// Gen number of days that the project will be open per year
		forvalues i = 1984(1)2030 {
			gen firstday_`i' = date("01/01/`i'", "MDY", 2050)
			gen lastday_`i' = date("12/31/`i'", "MDY", 2050)
			gen yearlength_`i' = lastday_`i' - firstday_`i' + 1
			gen fundeddays_`i' = 0
			replace fundeddays_`i' = ///
				max(0, min(completion_day, lastday_`i') - max(completion_day_today, firstday_`i'))
			drop firstday_`i' lastday_`i' yearlength_`i'
		}

		// Gen amount disbursed per year
		forvalues i = 1984(1)2030 {
			gen IDB_DISB_`i'_EXTRA = IDB_DISB_PER_DAY_EXTRA * fundeddays_`i'
		}
		drop fundeddays_* 

		// Drop disbursements after this year
		forvalues i = `next_yr'/2030 {
			drop IDB_DISB_`i'_EXTRA
		} 
				
		// Sum actual disbursements in report year with future disbursements in report year
		replace IDB_DISB_`report_yr' = ///
			IDB_DISB_`report_yr' + IDB_DISB_`report_yr'_EXTRA if ///
			IDB_DISB_`report_yr'_EXTRA != .
			
		drop total_disb future_disb *_EXTRA _m
		
		tempfile disb_open
		save `disb_open', replace
	restore
	
	drop _merge

	// Append both datasets
	drop if status == "Implementation" | status == "Completed" | status=="Closed"
	
	append using `disb_closed' 
	drop _merge
	append using `disb_open'

	// Reshaping database: 
	reshape long IDB_DISB_, i(proj_id oper_id) j(YEAR)

----------------------------------------------------------------------------- */

	tempfile alldata_check5
	save `alldata_check5'

** *****************************************************************************
// STEP 5: Preparing Version of Database that can be merged into the PDB
** *****************************************************************************
	gen FUNDING_COUNTRY = "NA"
	gen ISO3_FC = "NA"
	gen FUNDING_AGENCY = "IDB"
	gen FUNDING_AGENCY_SECTOR = "IGO"
	gen FUNDING_AGENCY_TYPE = "IFI"
	gen RECIPIENT_AGENCY_SECTOR = "GOV"
	gen	RECIPIENT_AGENCY_TYPE = "UNSP"
	gen DATA_LEVEL = "Project"
	gen DATA_SOURCE = "IDB Correspondence and Online Projects Database"

	// Dropping projects funded by the Multilateral Investment Fund (Focus is on Private Sector)
	drop if proj_type == "Multilateral Investment Fund Operation"
				
	gen FUNDING_TYPE = "LOAN" if proj_type == "Loan Operation" 
	replace FUNDING_TYPE = "GRANT" if proj_type == "Investment Grants"
	replace FUNDING_TYPE = "OTHER" if proj_type == "Technical Cooperation" 
	
	rename oper_id PROJECT_ID
	rename proj_name PROJECT_NAME
	rename proj_description PROJECT_DESCRIPTION
	rename proj_sector SECTOR
	rename approval_amt COMMITMENT
	rename IDB_DISB_ DISBURSEMENT
		
	// Changing names of regional projects: 
	replace RECIPIENT_COUNTRY = "South America, regional/multi-country" if ///
		RECIPIENT_COUNTRY == "Regional"
		
	// Changing Commitments so that they are not double-counted: 
	sort PROJECT_ID YEAR
	by PROJECT_ID: gen n = 1 if YEAR == approval_year
	replace COMMITMENT = 0 if n != 1

/* NOTE
	Drop old (<1990) data as well as 2015+ data. We drop 2015+ data because
	Project-level data was web-scraped from 2015-2017 and correspondence data is 
	used for 2018. These newer years are appended below.
*/
	drop if YEAR < 1990 | YEAR > 2014

	tempfile data_1990_2014
	save `data_1990_2014'

	// get report year numbers from correspondence
	preserve
		import delimited using "`RAW'/IDB_2015_`report_yr'_disbursements_covidadjust.csv", varn(1) clear
		rename proj_name new_name
		ren year YEAR
		ren iso3_rc ISO3_RC 
		ren recipient_country RECIPIENT_COUNTRY
		collapse (sum) disbursement, by(YEAR proj_id new_name ISO3_RC RECIPIENT_COUNTRY is_covid)
		gen SECTOR = "HEALTH" 
		ren new_name PROJECT_NAME
		gen currency = "USD - United States Dollar" 
		ren disbursement DISBURSEMENT
		tempfile corr_`report_yr'
		save `corr_`report_yr''
	restore

	append using `corr_`report_yr'', force 

	// Dropping High Income Countries: 
	merge m:1 ISO3_RC YEAR using "`incgrps'", keepusing(INC_GROUP)
	drop if _merge == 2
	drop _merge
	
	tab RECIPIENT_COUNTRY if INC_GROUP == "H"
	drop if INC_GROUP == "H"

	tempfile alldata_check6
	save `alldata_check6'

** *****************************************************************************
// Inflate this year's disbursements
	/*
	replace DISBURSEMENT = DISBURSEMENT * 365 / ///
		(365 - (date("`report_yr'-12-31", "YMD", 2050) - date("`corr_date'", "YMD", 2050))) ///
		if YEAR == `report_yr'
	*/
	
** *****************************************************************************
// STEP 6: Tag health focus areas
** *****************************************************************************
	// run the .ado file
	HFA_ado_master PROJECT_NAME PROJECT_DESCRIPTION, language(english)
	tempfile posthfa
	save `posthfa'

    replace is_covid = "FALSE" if is_covid == ""
    table is_covid
    
	
	// allocate commitments and disbursements across all health focus areas using weights
	foreach var of varlist final*frct {
		if "`var'" != "final_total_frct" {
			local healthfocus = subinstr("`var'", "final_", "", .)
			local healthfocus = subinstr("`healthfocus'", "_frct", "", .)
			gen `healthfocus'_DAH = `var' * DISBURSEMENT if is_covid == "FALSE"
		}
	}			
    gen oid_covid_DAH = DISBURSEMENT if is_covid == "TRUE"

	keep if YEAR >= 1990

	
* ------------------------------------------------------------------------------
	// HARD CODE FIX 
//ManualFixesHere
	// There aren't enough negative numbers to really need to write a comprehensive fix
	// Move negative disbursements into previous year. Repeat until none are negative.
	
	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	// Peru - other DAH, need to subtract from other Peru projects 
	replace other_DAH = 0 if proj_id == "PE-L1100" & YEAR == 2015 
	replace DISBURSEMENT = 0 if proj_id == "PE-L1100" & YEAR == 2015 
	replace other_DAH = (other_DAH - 13180) if proj_id == "PE-L1005" & YEAR == 2015 
	replace DISBURSEMENT = (DISBURSEMENT - 13180) if proj_id == "PE-L1005" & YEAR == 2015 
		// No other lines for this project outside of 2015. Removed from PE-L1005

	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	// Argentina - other DAH
	replace other_DAH = 0 if proj_id == "AR-L1118" & YEAR == 2016
	replace DISBURSEMENT = 0 if proj_id == "AR-L1118" & YEAR == 2016 
	replace other_DAH = (other_DAH - 91986) if proj_id == "AR-L1118" & YEAR == 2015 
	replace DISBURSEMENT = (DISBURSEMENT - 91986) if proj_id == "AR-L1118" & YEAR == 2015 

	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	// Peru - RMH and NCH other
	replace other_DAH = 0 if proj_id == "PE-L1005" & YEAR == 2016
	replace DISBURSEMENT = 0 if proj_id == "PE-L1005" & YEAR == 2016
	replace other_DAH = (other_DAH - 1473) if proj_id == "PE-L1005" & YEAR == 2015 
	replace DISBURSEMENT = (DISBURSEMENT - 1473) if proj_id == "PE-L1005" & YEAR == 2015 

	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	//El Salvador other DAH
	replace other_DAH = 0 if proj_id == "ES-L1027" & YEAR == 2017
	replace DISBURSEMENT = 0 if proj_id == "ES-L1027" & YEAR == 2017
	replace other_DAH= (other_DAH - 77517.61) if proj_id == "ES-L1027" & YEAR == 2015
	replace DISBURSEMENT = (DISBURSEMENT - 77517.61) if proj_id == "ES-L1027" & YEAR == 2015
		// No 2016 data - used 2015 instead

	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	// Haiti - nch_hss_other
	replace nch_other_DAH = 0 if proj_id == "HA-L1042" & YEAR == 2015
	replace DISBURSEMENT = 0 if proj_id == "HA-L1042" & YEAR == 2015
	replace nch_hss_other_DAH = (nch_hss_other_DAH - 551) if proj_id == "HA-L1042" & YEAR == 2014 
	replace DISBURSEMENT = (DISBURSEMENT - 551) if proj_id == "HA-L1042" & YEAR == 2014 
		// No nch_other_DAH in 2014 to subtract from - used nch_hss_other_DAH instead

	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}
	
		// Dominican Republic - other_DAH
	replace other_DAH = 0 if proj_id == "DR-L1067" & YEAR == 2021
	replace DISBURSEMENT = 0 if proj_id == "DR-L1067" & YEAR == 2021
	replace other_DAH = (other_DAH - 298341.2) if proj_id == "DR-L1067" & YEAR == 2020 
	replace DISBURSEMENT = (DISBURSEMENT - 298341.2) if proj_id == "DR-L1067" & YEAR == 2020 
	
	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

    // 2024 negative projects
    replace swap_hss_other_DAH = 0 if proj_id == "BR-L1415" & YEAR == 2024
    replace DISBURSEMENT = 0 if proj_id == "BR-L1415" & YEAR == 2024
    replace swap_hss_other_DAH = (swap_hss_other_DAH - 68887) if proj_id == "BR-L1415" & YEAR == 2023
    replace DISBURSEMENT = (DISBURSEMENT - 68887) if proj_id == "BR-L1415" & YEAR == 2023

    replace other_DAH = 0 if proj_id == "BO-L1082" & YEAR == 2024
    replace DISBURSEMENT = 0 if proj_id == "BO-L1082" & YEAR == 2024
    replace other_DAH = (other_DAH - 243130) if proj_id == "BO-L1082" & YEAR == 2021
    replace DISBURSEMENT = (DISBURSEMENT - 243130) if proj_id == "BO-L1082" & YEAR == 2021

    replace other_DAH = 0 if proj_id == "CO-J0011" & YEAR == 2024
    replace DISBURSEMENT = 0 if proj_id == "CO-J0011" & YEAR == 2024
    replace other_DAH = (other_DAH - 62589) if proj_id == "CO-J0011" & YEAR == 2023
    replace DISBURSEMENT = (DISBURSEMENT - 62589) if proj_id == "CO-J0011" & YEAR == 2023

    replace oid_covid_DAH = 0 if proj_id == "EC-L1270" & YEAR == 2024 & is_covid == "TRUE"
    replace DISBURSEMENT = 0 if proj_id == "EC-L1270" & YEAR == 2024 & is_covid == "TRUE"
    replace oid_covid_DAH = (oid_covid_DAH - 4705970) if proj_id == "EC-L1270" & YEAR == 2023 & is_covid == "TRUE"
    replace DISBURSEMENT = (DISBURSEMENT - 4705970) if proj_id == "EC-L1270" & YEAR == 2023 & is_covid == "TRUE"

    replace oid_covid_DAH = 0 if proj_id == "ES-L1144" & YEAR == 2024 & is_covid == "TRUE"
    replace DISBURSEMENT = 0 if proj_id == "ES-L1144" & YEAR == 2024 & is_covid == "TRUE"
    replace oid_covid_DAH = (oid_covid_DAH - 1655) if proj_id == "ES-L1144" & YEAR == 2023 & is_covid == "TRUE"
    replace DISBURSEMENT = (DISBURSEMENT - 1655) if proj_id == "ES-L1144" & YEAR == 2023 & is_covid == "TRUE"


    // should be nothiing left listed
	foreach var of varlist *_DAH {
		list proj_id YEAR ISO3_RC DISBURSEMENT `var' if `var' < 0
	}

	// test that it is good
	egen double total_DAH = rowtotal(*_DAH)
	gen tester = total_DAH - DISBURSEMENT
	quietly count if abs(tester) > 50 & tester != . 
	if `r(N)' > 0 {
		display in red "we are broke"
		WeAreBroken
	}
	
    drop total_DAH tester
* ------------------------------------------------------------------------------

	tempfile disbursements
	save `disbursements', replace

	// Add in-kind
	preserve
		import excel using "`RAW'/IDB_INKIND_RATIOS_1990_`report_yr'.xlsx", firstrow clear
		keep YEAR INKIND_RATIO
		merge 1:m YEAR using `disbursements'
		rename DISBURSEMENT DAH
		foreach var of varlist DAH *_DAH {
			replace `var' = `var' * INKIND_RATIO
		}
		gen INKIND = 1
		drop _merge
		tempfile inkind
		save `inkind',  replace
	restore
		
	rename DISBURSEMENT DAH 
	append using `inkind'
	replace INKIND = 0 if INKIND == .
	//// ERROR with DAH total column fpor pre-2015 ////
	drop if DAH == . | DAH == 0
	gen CHANNEL = "IDB"
    replace ISO3_RC = "QNE" if RECIPIENT_COUNTRY == "Regional"
		
	// save
	save "`FIN'/M_IDB_INTPDB_`report_yr'_`update_tag'.dta", replace

** *****************************************************************************
// STEP 6: Save Version of Database that can be merged into the ADB
** *****************************************************************************
	// add donor information 
	use `disbursements', clear
	collapse (sum) DISBURSEMENT, by(YEAR)
	tempfile disb_tot 
	save `disb_tot'

	import excel using "`fso_cont_1990_2016'", firstrow clear
	keep YEAR pct_FSO 
	tempfile pct_fso 
	save `pct_fso'

	import excel using "`idb_cont_quota_1990_2016'", firstrow clear
	replace COUNTRY = trim(COUNTRY)

	// Numbers were entered in different units across different reports
	replace CONTRIBUTION = CONTRIBUTION * Multiple
	sort COUNTRY YEAR
	drop Multiple

	// Add country ISO Codes 
	ren COUNTRY country_lc
	merge m:1 country_lc using "`country_codes'", keepusing(iso3 countryname_ihme)
	drop if _m == 2 
	tab country_lc if _merge == 1
	drop _merge country_lc

	ren iso3 ISO_CODE
	ren countryname_ihme DONOR_COUNTRY

	merge m:1 YEAR using `disb_tot', nogen
	merge m:1 YEAR using `pct_fso', keep(1 3) nogen

	// Need to adjust contribution amounts so they are scaled down to equal total disbursements
	bysort YEAR: egen tot_cont = total(CONTRIBUTION)
	gen cont_pct = DISBURSEMENT / tot_cont
	gen country_pct = cont_pct * pct_FSO * (CONTRIBUTION / tot_cont)
	replace CONTRIBUTION = CONTRIBUTION * country_pct
	*replace CONTRIBUTION = DISBURSEMENT if inlist(YEAR, 2017, 2018)
	replace CONTRIBUTION = DISBURSEMENT if YEAR > 2016

/* NOTE:
	This is the money from ordinary capital, aka the money made on 
	investments rather than country donors
*/
	preserve 
		keep YEAR DISBURSEMENT pct_FSO 
		duplicates drop 
		*drop if YEAR >= `previous_yr'
		drop if YEAR > 2016
		gen CONTRIBUTION = DISBURSEMENT * (1 - pct_FSO)
		tempfile oc_90_16 
		save `oc_90_16'
	restore 

	append using `oc_90_16'
	drop tot_cont country_pct DISBURSEMENT pct_FSO cont_pct
	ren CONTRIBUTION DISBURSEMENT

	// The last replenishment round ended in 2016 and the fund for special 
	// operations was merged into ordinary capital. Hence, no more countries are 
	// contributing for 2017 and 2018 and instead projects are paid from interest
	replace ISO_CODE = "QZA" if DONOR_COUNTRY == ""
	replace DONOR_COUNTRY = "Unallocable" if DONOR_COUNTRY == ""

	collapse (sum) DISBURSEMENT, by(YEAR DONOR_COUNTRY ISO_CODE) 
		// combine multiple types of "other", the "other" from the fund for 
		// special operations and the "other" that is the ordinary capital money
	tempfile cont_total
	save `cont_total'

	// add in-kind
	preserve
		import excel using "`RAW'/IDB_INKIND_RATIOS_1990_`report_yr'.xlsx", firstrow clear
		keep YEAR INKIND_RATIO
		merge 1:m YEAR using `cont_total'
		rename DISBURSEMENT DAH
		replace DAH = DAH * INKIND_RATIO
		gen INKIND = 1
		drop _m
		tempfile inkind
		save `inkind',  replace
	restore

	rename DISBURSEMENT DAH 
	append using `inkind'
	replace INKIND = 0 if INKIND == .
	drop if DAH == . | DAH == 0

	gen CHANNEL = "IDB" 

    // assign income_sector and income_type
	gen INCOME_SECTOR = "OTHER" if ISO_CODE == "QZA"
	replace INCOME_SECTOR = "PUBLIC" if ISO_CODE != "QZA"
	
	gen INCOME_TYPE = "OTHER" if ISO_CODE == "QZA"
	replace INCOME_TYPE = "CENTRAL" if ISO_CODE != "QZA"

    // merge on debt fraction to disaggregate "OTHER" into DEBT vs OTHER.
    preserve
        insheet using "`RAW'/IDB_DEBT.csv", comma clear
        keep year debt_frac
        rename year YEAR
        replace debt_frac = 1 if debt_frac > 1 // bad year for investments in 2008, so losses on investments cause total income to be much lower than income on loans

        rename debt_frac val1
        gen val2 = 1 - val1
        reshape long val, i(YEAR) j(don)

        // gen INCOME_SECTOR = "DEBT" if don == 1
        // replace INCOME_SECTOR = "OTHER" if don == 2
        // gen INCOME_TYPE = "OTHER"
        // drop don
        rename val donor_prop
        
        gen key = "other_debt"
        tempfile donor_props
        save `donor_props'
    restore

    // merge donor props
    gen key = "donor"
    replace key = "other_debt" if ISO_CODE == "QZA"
    joinby YEAR key using `donor_props', unmatched(master)
    drop _merge

    // update INCOME_SECTOR
    replace INCOME_SECTOR = "DEBT" if don == 1
    replace INCOME_SECTOR = "OTHER" if don == 2
    replace INCOME_TYPE = "OTHER" if don == 1 | don == 1
    drop don key

    // Multiply DAH by donor proportions where available
    replace DAH = DAH * donor_prop if donor_prop != .

	gen SOURCE = "IDB Online Projects Database and Annual Reports"
	rename DAH OUTFLOW
	gen GHI = "IDB"
	tab YEAR
	
	save "`FIN'/M_IDB_INC_DISB_FINAL_`report_yr'_`update_tag'.dta", replace
	
** *****************************************************************************
// STEP 7: Save ADB/PDB
** *****************************************************************************	
	use  "`FIN'/M_IDB_INTPDB_`report_yr'_`update_tag'.dta", clear
	drop if INKIND == 1
	
	// fill in all ISO3_RC codes
	count if ISO3_RC == ""
	if `r(N)' != 0 {
		di in red "Fix ISO3_RC codes!"
		fix_iso3_codes
	}
			
	// collapse yearly disbursements
	collapse (sum) DAH *_DAH, by(YEAR ISO3_RC CHANNEL)

	rename DAH DAH_TOTAL
	rename *DAH *DAH_TOTAL
		
	// get list for reshaping long
	local command
	foreach var of varlist *DAH_TOTAL {
		local command `command' `var'
	}
			
	reshape wide *DAH_TOTAL, i(YEAR CHANNEL) j(ISO3_RC)string
	
	// Merge with income data & computing shares of total income from each income source
	merge 1:m CHANNEL YEAR using ///
		"`FIN'/M_IDB_INC_DISB_FINAL_`report_yr'_`update_tag'.dta"
	drop if INKIND == 1
	drop _merge
	
	bysort YEAR: egen tot_out = total(OUTFLOW)
	gen don_pct = OUTFLOW / tot_out

	foreach var of varlist *_DAH_TOTAL* DAH_TOTAL* {
		replace `var' = `var' * don_pct
	}
		
	drop don_pct tot_out 
		
	// Reshape to year-donor-recipient  level
	reshape long `command', ///
		i(YEAR CHANNEL INCOME_SECTOR INCOME_TYPE ISO_CODE DONOR_COUNTRY INKIND) ///
		j(ISO3_RC)string
			
	tempfile adb_pdb
	save `adb_pdb'

	// add in-kind
	preserve
		import excel using "`RAW'/IDB_INKIND_RATIOS_1990_`report_yr'.xlsx", firstrow clear
		keep YEAR INKIND_RATIO
		merge 1:m YEAR using `adb_pdb'
		foreach var of varlist DAH_TOTAL *_DAH_TOTAL {
			replace `var'=`var' * INKIND_RATIO
		}
		replace INKIND = 1
		drop _merge
		tempfile inkind
		save `inkind', replace
	restore

	append using `inkind'
		
	rename *_DAH_TOTAL *_DAH
	rename DAH_TOTAL DAH
	label var DAH "Imputed Disbursement by Income Source and Year"
	
	// Drop observations with no DAH
	drop if DAH == .
	
	// Variables for DAHG-DAHNG
	// Determine whether it is a country level project /regional or world project			
	// According to DAHG_integration code; all IDB is assigned gov
	gen gov = 1
		
	// Make sure HFAs add up to envelope
	egen double dah_check = rowtotal(*_DAH)
	gen double diff = dah_check - DAH
	sum diff 
	if `r(max)' > 2 | `r(min)' < -2 {
		di in red "HFAs do not add up to DAH!"
		HFAsDontAddUpToDAH
	}
	else {
		drop dah_check diff 
	}
		
	// saving database
	save "`FIN'/IDB_ADB_PDB_FGH`report_yr'_`update_tag'.dta", replace
	save "`FIN'/IDB_ADB_PDB_FGH`report_yr'.dta", replace
						
** END OF FILE **

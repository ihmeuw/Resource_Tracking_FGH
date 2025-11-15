*** *************************************************
// Project:	FGH
// Purpose:	Adapting python code to stata from FGH 2016: 
//			Merging and creating HFA fractions, based off previous FGH 2016 & FGH 2014
//
****************************************************	

	set more off
	clear all
	if c(os) == "Unix" {
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		global j "J:"
	}

	local report_year = 2024 // FGH report year
	
	local RAW		"FILEPATH/RAW/HFA"
	local INT		"FILEPATH/INT/FGH_`report_year'"
	local INT_OLD	"FILEPATHA/INT"
	local CODES 	"'FILEPATH/FGH_`report_year'"
	
	**insheeting and appending 2000-2019 HFA data, pulled from Programme Budget Documents and mapped to HFA I & HFA II (program areas)
	
	foreach y in 2000_2001 2002_2003 2004_2005 2006_2007 2008_2009 2010_2011 {
		import excel using "`RAW'/who_hfa_`y'.xlsx", clear
		rename A program_area
		rename B hfa
		rename C exp
		gen year = "`y'"
		
		if "`y'" =="2000_2001" {
		    dis "`y'"
			tempfile hfa
			save `hfa'
		}
		else {
			append using `hfa'
			drop if exp==.
			save `hfa', replace
		}
	}
		
	foreach y in 2012_2013 2014_2015 2016_2017_new 2018_2019 {
		import delimited using "`RAW'/Utilization_by_entity_`y'.csv", clear
		rename v1 program_area
		rename v2 hfa
		rename v3 exp
		gen year = "`y'"
		
		append using `hfa'
		capture drop D
		save `hfa', replace
	}

replace year = "2016_2017" if year == "2016_2017_new"

split year, gen(year) parse("_")
destring year1 year2, replace force
	
**want to get all years in the same denominator
replace exp = exp/1000 if year1<2008
replace exp = exp/1000000 if year1==2012 | year1==2013

sort year hfa		

**fixing a few hfas, names have changed over time
replace hfa="hiv_other" if hfa=="hiv" | hfa=="hiv_unid"
replace hfa="ncd_tobac" if hfa=="ncd_tab"
replace hfa="tb_other" if hfa=="tb"
replace hfa="oid_other" if hfa=="oid"
replace hfa="ncd_other" if hfa=="ncd_unid"
replace hfa="rmh_mh" if hfa=="mnch_mh"
replace hfa="rmh_fp" if hfa=="mnch_fp"
replace hfa="nch_cnv" if hfa=="mnch_cnv"
replace hfa="nch_cnn" if hfa=="mnch_cnn"
replace hfa="nch_other" if hfa=="mnch_cnu"
replace hfa="mal_other" if hfa=="mal_unid"

replace hfa="swap_hss_hrh" if regexm(program_area, "Management and administration")
replace hfa="swap_hss_hrh" if regexm(program_area, "Leadership and governance")
replace hfa="swap_hss_hrh" if regexm(program_area, "Human resources")
replace hfa="swap_hss_other" if hfa=="swap_hss"

replace hfa="other" if program_area=="Violence and injuries"

replace hfa="rmh_other" if program_area=="Reproductive health"

**splits mnch_unid into rmh_ther and nch_other, w/ half of expense in each
preserve
keep if hfa=="mnch_unid"
replace exp=exp/2
replace hfa="rmh_other"
tempfile rmh 
save `rmh'
restore


append using `rmh'
replace exp=exp/2 if hfa=="mnch_unid"
replace hfa="nch_other" if hfa=="mnch_unid"

replace hfa="comm" if hfa==""

collapse (sum) exp, by(hfa year1 year2)
rename exp exp_
reshape wide exp_, i(year1 year2) j(hfa) string


**generate total health expenditure for a given year
egen total_exp = rowtotal(exp_*)

**In 2000 and 2001, HIV, TB, malaria, and OID is housed within "communicable disease" category
**however, currently it is double counted in the excel file
replace exp_oid_other=0 if exp_oid_other==. & year1<2008 & year1>2011
replace exp_comm=0 if exp_comm==.
replace exp_oid_other = exp_oid + (exp_comm - exp_hiv_other - exp_mal_other - exp_tb_other) if year1== 2000
replace total_exp = total_exp - exp_hiv_other - exp_mal_other - exp_tb_other if year1== 2000

**for the years 2002-2007 and 2012-2017 communicable disease category is listed and should be allocated to oid
replace exp_oid_other = exp_oid_other + exp_comm if (year1>=2002 & year1<=2007) | (year1>=2012 & year1<=2017)

**since communicable disease is missing or unaacounted for from 2012+, we will replace this with the sum of hiv, tb, mal, and oid
replace exp_comm = exp_oid_other + exp_hiv_other + exp_mal_other + exp_tb_other if year1>=2012 | year1<=2001 // communicable diseases may be lower because of oid change

**calculating fraction of health expenditure to each programme area (HFA)
foreach y in comm hiv_other mal_other ncd_mental ncd_other ncd_tobac nch_cnn nch_cnv nch_other oid_other other rmh_fp rmh_mh rmh_other swap_hss_hrh swap_hss_other swap_hss_pp tb_other {
	replace exp_`y'=0 if exp_`y'==. & (year1<2008 | year1>2011)
	gen `y'_frct= exp_`y'/total_exp
}

**for the years 2008-2011 we do not have the amount of hiv, tb, malaria, and OID funding
**we will predict these, using a center-log ratio transformation of the fractions (which will make sure they sum to 100%)
	
foreach y in hiv_other mal_other oid_other tb_other {
	*lemon squeeze
	summ exp_`y'
	local N = r(N)
	di `N'
	gen `y'_lemon = (((`y')*(`N'-1))+0.5)/`N'
}

foreach y in hiv_other mal_other oid_other tb_other {
		gen clr_`y' = ln((`y'_lemon)/((hiv_other_lemon*mal_other_lemon*oid_other_lemon*tb_other_lemon)^(1/4)))
}

gen ln_tot_exp = ln(total_exp)
gen ln_comm_exp = ln(exp_comm)
egen new_comm = rowtotal(exp_oid_other exp_tb_other exp_hiv_other exp_mal_other) if year1<2008 | year1>2011
replace new_comm = exp_comm if year1>=2008 & year1<=2011
gen ln_new_comm_exp = ln(new_comm)
tsset year1

**want to regress then predict the fraction of the sum of communicable diseases hiv_frct, tb_frct, oid_frct, and mal_frct	
foreach y in hiv_other mal_other oid_other tb_other {
	reg clr_`y' ln_new_comm_exp ln_comm_exp year1
	predict pred_clr_`y', xb
}

**re-transformation from CLR (center-log ratio) and lemon-squeezing
foreach y in hiv_other mal_other oid_other tb_other {
	summ exp_`y'
	local N = r(N)
	di `N'
	gen pred_`y' = (exp(pred_clr_`y')/(exp(pred_clr_hiv_other)+exp(pred_clr_mal_other)+exp(pred_clr_oid_other)+exp(pred_clr_tb_other)))
}

egen check = rowtotal(pred_hiv_other pred_mal_other pred_oid_other pred_tb_other) // check that this column is equal to 1

replace hiv_other_frct = comm_frct*pred_hiv_other if hiv_other_frct==.
replace mal_other_frct = comm_frct*pred_mal_other if mal_other_frct==.
replace tb_other_frct = comm_frct*pred_tb_other if tb_other_frct==.
replace oid_other_frct = comm_frct*pred_oid_other if oid_other_frct==.

egen check_2 = rowtotal(*_frct)
replace check_2 = check_2-comm_frct

**filling in missings with zero
foreach y in comm hiv_other mal_other ncd_mental ncd_other ncd_tobac nch_cnn nch_cnv nch_other oid_other other rmh_fp rmh_mh rmh_other swap_hss_hrh swap_hss_other swap_hss_pp tb_other {
	replace `y'_frct= 0 if `y'_frct==.
}

**we now need to split years
keep year* *_frct
rename year2 year
drop *comm*

tempfile hfa
save `hfa', replace

drop year
rename year1 year
append using `hfa'

sort year
drop year1
rename year YEAR

tempfile hfa_old
save `hfa_old', replace

** load new values, post-2019
import delimited using "`INT'/who_iati_hfa_frcts.csv", clear case(preserve)
append using `hfa_old'
sort YEAR

unab frct_cols: *_frct
foreach col of local frct_cols {
    replace `col' = 0 if `col' == .
}
save `hfa', replace

**for 1990-1999 we want to use the previously collected data
use "`INT_OLD'/WHO_EXP_BY_HFA_FGH2014", clear
	ren mnch_fp_frct rmh_fp_frct
	ren mnch_mh_frct rmh_mh_frct
	ren mnch_cnn_frct nch_cnn_frct
	ren mnch_cnv_frct nch_cnv_frct
	ren mnch_cnu_frct nch_other_frct
	ren mnch_unid_frct rmh_other_frct
	ren hiv_unid_frct hiv_other_frct
	ren tb_frct tb_other_frct
	ren mal_unid_frct mal_other_frct
	ren ncd_unid_frct ncd_other_frct
	ren oid_frct oid_other_frct
	ren swap_hss_frct swap_hss_other_frct
    ren mal_nets_frct mal_con_nets_frct

**want to update old data with the new data using merge
merge 1:1 YEAR using `hfa', update replace

**filling in missing info
replace CHANNEL="WHO" if CHANNEL==""
replace SOURCE="WHO PROGRAMMATIC AND FINANCIAL REPORT FOR 2014-2015; A69/45" if YEAR==2014 | YEAR==2015
replace SOURCE="Proposed programme budget 2016-2017" if YEAR==2016 | YEAR==2017
replace SOURCE="Proposed programme budget 2018-2019" if YEAR==2018 | YEAR==2019
replace SOURCE="Proposed programme budget 2020-2021" if YEAR==2020 | YEAR==2021
replace SOURCE="Proposed programme budget 2022-2023" if YEAR==2022 | YEAR==2023
drop _merge

**we want to have allocations for NCD Tobacco consistently
**for 2000 and 2001, we simply want to use 0.006, as this is constant prior to 2000
**for 2008+, we will use a weighted average estimated based on the previous 3 years
**then subtract this amount from NCD unidentifed to make sure fractions still add to 1

replace ncd_tobac_frct = 0.006 if YEAR==2000 | YEAR==2001
replace ncd_other_frct = ncd_other_frct-ncd_tobac_frct if YEAR==2000 | YEAR==2001

gen lag1 = ncd_tobac_frct[_n-1]
gen lag2 = ncd_tobac_frct[_n-2]
gen lag3 = ncd_tobac_frct[_n-3]

forvalues y = 2008(1)2019 {
	replace ncd_tobac_frct = (lag1/2)+(lag2/3)+(lag3/6) if YEAR==`y'
	replace ncd_other_frct = ncd_other_frct-ncd_tobac_frct if YEAR==`y'
	replace lag1 = ncd_tobac_frct[_n-1]
	replace lag2 = ncd_tobac_frct[_n-2]
	replace lag3 = ncd_tobac_frct[_n-3]
}

drop lag*

**years less than 2002, we have no information about SWAP/HSS PP, but assume it has always been a major function and goal of WHO
**to estimate this fraction we use the weighted average of the observed ratio of SWAP_HSS to SWAP_HSS_PP from 2002, 2003, and 2004
gen ratio_hss_pp = swap_hss_pp_frct/(swap_hss_other_frct+swap_hss_hrh_frct+swap_hss_pp_frct)
gen lag1 = ratio_hss_pp[_n+1]
gen lag2 = ratio_hss_pp[_n+2]
gen lag3 = ratio_hss_pp[_n+3]

forvalues y = 2001(1)2001 {
	gen ratio_hss_pp_avg = (lag1/2)+(lag2/3)+(lag3/6) if YEAR==`y'
}
summ ratio_hss_pp_avg
replace ratio_hss_pp_avg = `r(mean)' if YEAR<2002

*gen swap_hss_pp_frct_est = swap_hss_pp_frct
replace swap_hss_pp_frct = ratio_hss_pp_avg*swap_hss_other_frct if YEAR<2002
replace swap_hss_other_frct = swap_hss_other_frct - swap_hss_pp_frct if YEAR<2002 // subtracting out the swap_pp from swap_hss to not double count

drop lag* ratio_hss*

unab frct_cols: *_frct
foreach v of local frct_cols {
	replace `v'=0 if `v'==. 
}

//replace mnch_frct = mnch_fp_frct + mnch_mh_frct + mnch_cnn_frct + mnch_cnv_frct + mnch_cnu_frct + mnch_unid_frct
//replace hiv_frct = hiv_treat_frct + hiv_prev_frct + hiv_pmtct_frct + hiv_unid_frct
//replace mal_frct = mal_nets_frct + mal_unid_frct
//replace ncd_frct = ncd_tobac_frct + ncd_mental_frct + ncd_unid_frct

drop mnch_frct hiv_frct mal_frct ncd_frct 
egen check=rowtotal(*_frct)

foreach var of varlist *_frct {
    replace `var' = `var' / check
}

egen check2=rowtotal(*_frct)

cap drop check* lag*

order YEAR CHANNEL rmh* nch* hiv* tb* mal* ncd* oid* swap* other*

// make sure the INT dir has been created or this won't work
save "`INT'/WHO_EXP_BY_HFA_FGH`report_year'", replace


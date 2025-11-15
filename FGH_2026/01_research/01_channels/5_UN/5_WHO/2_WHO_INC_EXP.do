*** *********************************************************
// Project:	FGH
// Purpose: Generating dataset with WHO expenditures imputed by sources of income
//   
//
// !!!NOTE!!!
//    At present, R script 2b_LABELS_FIX_UPDATED.R has to be run during the middle of this script,
//    so make sure to do so - and make sure to rerun this script interactively, along with 2b, if
//    any of this script's input data changes.
*** ***********************************************************
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
	**// Year of update (if the year is biennium, name as both years, e.g. "10_11")		
	local update_yr = 2023			
	

	local RAW		"FILEPATH/RAW/FGH_`report_yr'"
	local INT		"FILEPATH/INT/FGH_`report_yr'"
	local INT_OLD	"FILEPATH/INT"
	local CODES 	"FILEPATH/FGH_`report_yr'"
	local FIN		"FILEPATH/FIN/FGH_`report_yr'"
	local FIN_OLD	"FILEPATH/FIN"
	local OUT 		"FILEPATH/FGH_`report_yr'"
	local PAHO		"FILEPATH/FGH_`report_yr'"

** ****
**// Step 1: Clean each dataset and add transfers from WHO to PAHO that are removed for double counting
** ****
	local years 9091 9293 9495 9697 9899 0001 0203 0405 0607 0809 10 11 12 13 14 15 16 17 18 19 20 21 22 23
			
	foreach yr of local years { 
		
		di as red "Cleaning data for year `yr'"
        if (`yr' < 23 | `yr' > 200) {
		    use "`INT_OLD'/who_`yr'.dta", clear
        }
        else {
		    use "`INT'/who_`yr'.dta", clear
        }
		
		drop if INCOME_SECTOR == "ELIMINATIONS" 
		
		** // Dropping supply services trust funds: 
		drop if DONOR_NAME == "IATF_SS_OTHER_UN"

		tostring YEAR, replace
		
		if (YEAR == "1996_97") { 
			foreach var in REG_NA REG_COLL REG_COLL_PRIOR EB_VFHP EB_OTHER EB_TOTAL { 
				capture split `var' 
				capture egen `var'_cnct = concat(`var'1 `var'2 `var'3)
				capture destring `var'_cnct, replace
				capture drop `var'1 `var'2 `var'3 `var'
				capture rename `var'_cnct `var'
				rename `var' INC_`var'
			} 
			rename OTHER INC_OTHER
		}

		if (YEAR != "1996_97" & YEAR != "2006_07" & YEAR != "2008_09" & YEAR != "2010" & YEAR != "2011" & YEAR!= "2012" & YEAR != "2013" & YEAR != "2014" & YEAR != "2015" & YEAR != "2016" & YEAR != "2017" & YEAR != "2018" & YEAR != "2019" & YEAR != "2020" & YEAR != "2021" & YEAR != "2022" & YEAR != "2023") {
			foreach var in REG_NA REG_COLL REG_COLL_PRIOR { 
				capture split `var' 
				capture egen `var'_cnct = concat(`var'1 `var'2 `var'3)
				capture destring `var'_cnct, replace
				capture drop `var'1 `var'2 `var'3 `var'
				capture rename `var'_cnct `var'
				rename `var' INC_`var'
				} 
			foreach var in EB_VFHP EB_OTHER EB_TOTAL OTHER { 
				rename `var' INC_`var'
			}		
		} 

		if (YEAR == "2006_07" | YEAR=="2008_09" | YEAR == "2010" | YEAR == "2011" | YEAR == "2012" | YEAR == "2013" | YEAR == "2014" | YEAR == "2015" | YEAR == "2016" | YEAR == "2017" | YEAR == "2018" | YEAR == "2019" | YEAR == "2020" | YEAR == "2021" | YEAR == "2022" | YEAR == "2023") {
			foreach var in REG_NA REG_COLL REG_COLL_PRIOR EB_VFHP EB_OTHER EB_TOTAL OTHER { 
				rename `var' INC_`var'
			} 
        }

		foreach var in INC_REG_NA INC_REG_COLL INC_REG_COLL_PRIOR INC_EB_VFHP INC_EB_OTHER INC_EB_TOTAL INC_OTHER EXP_REG EXP_EB_VFHP EXP_EB_OTHER EXP_OTHER EXP_INT_AG { 
			destring `var', replace
			replace `var' = 0 if `var' == .
		}

	**// Renaming Expenditure Variables to denote that they represent totals (rather than expenditures specific to each row)

		foreach var in EXP_REG EXP_EB_VFHP EXP_EB_OTHER EXP_OTHER { 
            rename `var' TOT_`var'
		}

		if YEAR=="2008_09" {
			foreach var in INC_REG_NA INC_REG_COLL INC_REG_COLL_PRIOR INC_EB_VFHP INC_EB_OTHER INC_EB_TOTAL INC_OTHER TOT_EXP_REG TOT_EXP_EB_VFHP TOT_EXP_EB_OTHER TOT_EXP_OTHER EXP_INT_AG { 
				replace `var' = 0 if `var' == .
			}
		}

		** in 2021 Angela noted that only 76% of the assessed contributions go to DAH
		foreach var in INC_REG_NA INC_REG_COLL INC_REG_COLL_PRIOR { 
		    replace `var' = `var' * .76
		}
			
		egen double INC_REG = 	rowtotal(INC_REG_COLL INC_REG_COLL_PRIOR)
		egen double INC_NONREG =  rowtotal(INC_EB_VFHP INC_EB_OTHER INC_OTHER)
	
		gen INCOME_ALL = 0
		replace INCOME_ALL = INC_REG + INC_NONREG
		label var INCOME_ALL "Total Income"
			
**	// Computing Income Shares: 

		egen double INC_TOTAL_REG = total(INC_REG)
		egen double INC_TOTAL_VFHP = total(INC_EB_VFHP)
		
		replace EXP_INT_AG = 0 if EXP_INT_AG == . 
		replace INC_EB_OTHER = 0 if (EXP_INT_AG != 0)
		
		egen double INC_TOTAL_EB_OTHER = total(INC_EB_OTHER) 
		egen double INC_TOTAL_OTHER = total(INC_OTHER)
		
		gen double INC_REG_SHARE = INC_REG/INC_TOTAL_REG
		gen double INC_VFHP_SHARE = INC_EB_VFHP/INC_TOTAL_VFHP
		gen double INC_EB_OTHER_SHARE = INC_EB_OTHER/INC_TOTAL_EB_OTHER if (EXP_INT_AG != 0 | EXP_INT_AG != .)
		gen double INC_OTHER_SHARE = INC_OTHER/INC_TOTAL_OTHER
		recode INC_OTHER_SHARE (.=0)

	
**	// Imputing Expenditures by Income Source: 
		gen double EXP_REG = INC_REG_SHARE*TOT_EXP_REG
		gen double EXP_VFHP = INC_VFHP_SHARE*TOT_EXP_EB_VFHP
		
		gen double EXP_EB_OTHER = INC_EB_OTHER_SHARE*TOT_EXP_EB_OTHER
		replace EXP_EB_OTHER = EXP_INT_AG if EXP_INT_AG != 0
		replace EXP_EB_OTHER = 0 if EXP_EB_OTHER == .
		
		gen double EXP_OTHER = INC_OTHER_SHARE*TOT_EXP_OTHER
		
		gen double EXP_NONREG = EXP_VFHP + EXP_EB_OTHER + EXP_OTHER
		replace EXP_NONREG = EXP_INT_AG if EXP_INT_AG > 0
		 
		gen double EXPENDITURE_ALL = EXP_REG + EXP_NONREG	
		gen OUTFLOW = EXPENDITURE_ALL
		
		drop INCOME_NOTES
			
		capture keep YEAR CHANNEL INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO_CODE SOURCE_DOC ALT_DONOR_NAME ALT_INCOME_SECTOR ALT_INCOME_TYPE ALT_DONOR_COUNTRY ALT_ISO_CODE INC_REG INC_EB_VFHP INC_EB_OTHER INC_OTHER INC_NONREG INCOME_ALL EXP_REG EXP_VFHP EXP_EB_OTHER EXP_OTHER EXP_NONREG EXPENDITURE_ALL OUTFLOW id

**// Reshaping database so rows represent single years rather than biennia: 

	if YEAR == "1990_91" | YEAR == "1992_93"  | YEAR == "1994_95" | YEAR == "1996_97" | YEAR == "1998_99" | YEAR == "2000_01" | YEAR == "2002_03" | YEAR == "2004_05" | YEAR == "2006_07" | YEAR == "2008_09" { 
			
		sort id
		expand 2
		foreach var in  INC_EB_VFHP INC_EB_OTHER INC_OTHER INC_REG INC_NONREG INCOME_ALL EXP_REG EXP_VFHP EXP_EB_OTHER EXP_OTHER EXP_NONREG EXPENDITURE_ALL OUTFLOW { 
			replace `var' = `var'/2
		}
		sort id
		gen odd = mod(_n,2) 
		
		split(YEAR), p("_") gen(yr)
		replace yr2 = "19" + yr2 if regexm(yr2, "9")==1
		replace yr2 = "20" + yr2 if regexm(yr2, "9")==0 
		replace yr2 = "2009" if yr2 == "1909"
		
		replace YEAR = yr1 if odd == 0 
		replace YEAR = yr2 if odd == 1
		
		drop odd yr1 yr2
		tab YEAR
		
		tempfile who_`yr'_clean
		save `who_`yr'_clean'
			
	} 
		
	if YEAR == "2010" | YEAR == "2011" | YEAR == "2012" | YEAR == "2013" | YEAR == "2014" | YEAR == "2015" | YEAR == "2016" | YEAR == "2017" | YEAR == "2018" | YEAR == "2019" | YEAR == "2020" | YEAR == "2021" | YEAR == "2022" | YEAR == "2023" {
		tempfile who_`yr'_clean
		save `who_`yr'_clean'
	}
	
}
** ****
**// Step 2: Appending all years together
** ****
	use `who_9091_clean', clear
					
	foreach yr in 9293 9495 9697 9899 0001 0203 0405 0607 0809 10 11 12 13 14 15 16 17 18 19 20 21 22 23 { 
		di as red "appending data for year `yr'"
		append using  `who_`yr'_clean', force
	}

	destring YEAR, replace
	save "`INT'/who_1990_`update_yr'_appended.dta", replace
	**// Missing will be filled in based on donor below	
	tab INCOME_SECTOR, missing 
	tab INCOME_TYPE, missing

* saving to check against Hayley's INCOME SECTOR/TYPE file in R
export delimited "`INT'/WHO_INCOME_SECTOR_TYPE_FIX_`report_yr'.csv", replace

// !!!
// run script 2b_LABELS_FIX_UPDATED.R
// !!!

* importing back the data with corrected INCOME SECTOR and TYPE
insheet using "`INT'/WHO_INCOME_SECTOR_TYPE_FIXED_`report_yr'.csv", names comma clear case

* fixing INCOME_SECTOR Multi
replace INCOME_SECTOR = "PRIVATE" if regexm(DONOR_NAME, "^SCLAVO VACCINE")
replace INCOME_SECTOR = "OTHER" if regexm(DONOR_NAME, "^ARAB GULF PROGRAMME FOR UNITED N")
replace INCOME_SECTOR = "OTHER" if regexm(DONOR_NAME, "^VOLUNTARY ASSESSED CONTRIBUTIONS")

* fixing INCOME_TYPE SOC
replace INCOME_TYPE = "PPP" if regexm(DONOR_NAME,"^ALLIANCE FOR HEALTH POLICY AND SY") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^ASOCIACION PROTECTORA DE CAR") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^ASSOCIAZIONE SCIENTIFICA INTERDISCIP") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UNIV" if regexm(DONOR_NAME,"^CAIRO DENTAL RES") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^CANADIAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^CENTRO STUDI ") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^CHRISTOFFEL BLINDENMI") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^COM") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^DAMLEN") & INCOME_TYPE=="SOC"
* DANSK SYGEHUS, DASMAN Center
replace INCOME_TYPE = "UNIV" if regexm(DONOR_NAME,"^DA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^DRUGS FOR NE") & INCOME_TYPE=="SOC"
* education international, European committe, european respiratory
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^E") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^FAMILY") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^FEDERATION INTERNA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^GERMAN PHARMA") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME,"^HEALTH PROTECTION") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "CENTRAL" if regexm(DONOR_NAME,"^HEALTH PROTECTION") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^HEARING CONSER") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^HUMAN ETL") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INSTITUT ZA") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME,"^INSTITUTE OF PUBLIC") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INSTITUTE OF PUBLIC") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INSTITUTO NAZION") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^INTERCHURCH") & INCOME_TYPE=="SOC"
* all international agencies, associations, federations, LEAGUES left are OTHER
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL A") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL FED") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL LEA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL PHARMA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^INTERNATIONAL PLANNED") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^INTERNATIONAL RESCUE") & INCOME_TYPE=="SOC"
* all internaltional society left are OTHER
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL SOCIETY") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UN" if regexm(DONOR_NAME,"^INTERNATIONAL TASK") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL TUBERCULOSIS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL UNION AGAINST TUBERCULOSIS AND") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UN" if regexm(DONOR_NAME,"^INTERNATIONAL VACCINE") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERNATIONAL WATER") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^INTERVIDA") & INCOME_TYPE=="SOC"
* all ISTITUTO and ITALIAN are OTHER
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^ISTITUTO") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^ITALIAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^JAPAN ANTI") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^JAPAN COMMITTEE") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^JAPAN HOSPITAL") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^JAPANESE RED ") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^JOHANN JACO") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME,"^JOHN F FOGAR") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "CENTRAL" if regexm(DONOR_NAME,"^JOHN F FOGAR") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "INK" if regexm(DONOR_NAME,"^KAYO WERK") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "CORP" if regexm(DONOR_NAME,"^KAYO WERK") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^KOREA ELECTRO") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^KOREAN FOUND") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^MARIUS NASTA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^MECTIZAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^MEDECINS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^MEDICAL RESE") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME,"^MILLENNIUM") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "CENTRAL" if regexm(DONOR_NAME,"^MILLENNIUM") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^NATIONAL PHILAN") & INCOME_TYPE=="SOC"
* NETSPEAR, NETWORK ASSISTENZA, NEw VENTURE, NORSK INSTITUT are other
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^NE") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^NORSK INST") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^NORWEGIAN SAVE THE") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^OPEN SOCIETY") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "UNALL" if regexm(DONOR_NAME,"^OTHER AND MIS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UNALL" if regexm(DONOR_NAME,"^OTHER AND MIS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UN" if regexm(DONOR_NAME,"^PAN AMERICAN") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "UNALL" if regexm(DONOR_NAME,"^PASS THROUGH") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "UNALL" if regexm(DONOR_NAME,"^PASS THROUGH") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^PHYSIC") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^PILIPINAS SH") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^PNG") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^POPULATION SER") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^PORTUGUESE SOC") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^PROGRAM FOR APP") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^RESULTS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "FOUND" if regexm(DONOR_NAME,"^RIVER BLINDNESS") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^ROYAL TROPICAL") & INCOME_TYPE=="SOC"
* SABIN, SAITAMA, SCLAVO are OTHER
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^SA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^SCLAVO") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^SIGHT") & INCOME_TYPE=="SOC"
* SOCIETE, SOUTHEAST ASIA are OTHER
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^SO") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^SWEDISH") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^SWISS PARA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^SYNDICAT") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^TASK FORCE SIGHT") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^THE TASK FORCE FOR GLOBAL") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^UNITED STATES PHARMA") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^UNITING CHURCH") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^WORLD SELF") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^WORLD VISION AUS") & INCOME_TYPE=="SOC"

	* Fixing INCOME_TYPE UN, DEVBANK, and PPP to INCOME_SECTOR OTHER
	* INCOME_TYPE CORP to INCOME_SECTOR INK
	replace INCOME_SECTOR = "OTHER" if (INCOME_TYPE=="UN" | INCOME_TYPE=="PPP" | INCOME_TYPE=="DEVBANK")
	replace INCOME_SECTOR = "INK" if INCOME_TYPE=="CORP"

replace INCOME_TYPE = "NGO" if regexm(DONOR_NAME,"^CHRISTOFFEL") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_NAME,"^HUMAN-ETLSK") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME,"^JOHN F") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "CENTRAL" if regexm(DONOR_NAME,"^JOHN F") & INCOME_TYPE=="SOC"
replace INCOME_SECTOR = "OTHER" if regexm(ALT_DONOR_NAME,"^COMMISSION OF THE EUROPEAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "EC" if regexm(ALT_DONOR_NAME,"^COMMISSION OF THE EUROPEAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if regexm(DONOR_COUNTRY,"^JAPAN") & INCOME_TYPE=="SOC"
replace INCOME_TYPE = "OTHER" if  INCOME_TYPE=="SOC"

	drop DONOR_NAME_upper
	ren DONOR_NAME DONOR_NAME_upper
	ren original_DONOR_NAME DONOR_NAME
	
	replace DONOR_NAME = trim(DONOR_NAME)
	replace DONOR_NAME = upper(DONOR_NAME)
	
	replace DONOR_NAME = "AGEO CHUO GENERAL HOSPITAL" if DONOR_NAME == "AGED CHUO GENERAL HOSPITAL"
	
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "INSTITUT DE PROTECTION ET DE SURETE NUCLEAIRE"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "AGEO CHUO GENERAL HOSPITAL"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "JAPAN SHIPBUILDING INDUSTRY FOUNDATION (SASAKAWA HEALTH FUND)"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "LABORATOIRES D'ETUDES ET DE RECHERCHES SYNTHELABO"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "OSSERVATORIO ECOLOGICO"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "DELAGRANGE INTERNATIONAL"
	replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "THE ASPEN INSTITUTE"
	replace INCOME_SECTOR = "OTHER" if DONOR_NAME == "OTHER AND MISCELLANEOUS RECEIPTS"
	replace INCOME_SECTOR = "PRIVATE" if INCOME_TYPE == "FOUND" & INCOME_SECTOR != "BMGF"
	replace INCOME_SECTOR = "BMGF" if regexm(DONOR_NAME,"GATES")
	
	replace ISO_CODE = "USA" if DONOR_NAME == "AMERICAN CYANAMID USA"
	replace DONOR_COUNTRY = "United States" if DONOR_NAME == "AMERICAN CYANAMID USA"
	replace ISO_CODE = "NOR" if DONOR_NAME == "BRUNDTLAND, DR GRO HARLEM (DISCOVERY INSPIRATION AWARD)"
	replace DONOR_COUNTRY = "Norway" if DONOR_NAME == "BRUNDTLAND, DR GRO HARLEM (DISCOVERY INSPIRATION AWARD)"
	replace ISO_CODE = "DEU" if DONOR_NAME == "CHRISTOFFEL-BLINDENMISSION"
	replace DONOR_COUNTRY = "Germany" if DONOR_NAME == "CHRISTOFFEL-BLINDENMISSION"
	replace ISO_CODE = "BEL" if DONOR_NAME == "PROCTOR & GAMBLE"
	replace DONOR_COUNTRY = "Belgium" if DONOR_NAME == "PROCTOR & GAMBLE"
	replace ISO_CODE = "USA" if DONOR_NAME == "ROTARY INTERNATIONAL"
	replace DONOR_COUNTRY = "United States" if DONOR_NAME == "ROTARY INTERNATIONAL"
	replace ISO_CODE = "DEU" if DONOR_NAME == "ALLGEMEINE ORTSKRANKENKASSE FUR NIEDERSACHSEN (AOK)"
	replace DONOR_COUNTRY = "Germany" if DONOR_NAME == "ALLGEMEINE ORTSKRANKENKASSE FUR NIEDERSACHSEN (AOK)"
	replace ISO_CODE = "CHE" if DONOR_NAME == "CANTONE TICINO"
	replace DONOR_COUNTRY = "Switzerland" if DONOR_NAME == "CANTONE TICINO"
	replace ISO_CODE = "NOR" if DONOR_NAME == "NORSK TJENESTEMANNSLAG"
	replace DONOR_COUNTRY = "Norway" if DONOR_NAME == "NORSK TJENESTEMANNSLAG"
	replace ISO_CODE = "GBR" if DONOR_NAME == "UNIVERSITY OF WALES COLLEGE OF MEDICINE"
	replace DONOR_COUNTRY = "United Kingdom" if DONOR_NAME == "UNIVERSITY OF WALES COLLEGE OF MEDICINE"
	replace ISO_CODE = "USA" if DONOR_NAME == "WORLD VISION INTERNATIONAL" & ISO_CODE == ""
	replace DONOR_COUNTRY = "United States" if DONOR_NAME == "WORLD VISION INTERNATIONAL" & DONOR_COUNTRY == ""
	replace ISO_CODE = "USA" if DONOR_NAME == "AMERICAN PSYCHIATRIC FOUNDATION"
	replace DONOR_COUNTRY = "United States" if DONOR_NAME == "AMERICAN PSYCHIATRIC FOUNDATION"
	replace ISO_CODE = "AUS" if regexm(DONOR_NAME, "BRIEN HOLDEN")
	replace DONOR_COUNTRY="Australia" if regexm(DONOR_NAME, "BRIEN HOLDEN")
	replace ISO_CODE = "CHE" if regexm(DONOR_NAME, "BRIGHTON COLLABORATION")
	replace DONOR_COUNTRY="Switzerland" if regexm(DONOR_NAME, "BRIGHTON COLLABORATION")
	replace ISO_CODE = "USA" if regexm(DONOR_NAME, "BRISTOL-MYERS SQUIBB")
	replace DONOR_COUNTRY="United States" if regexm(DONOR_NAME, "BRISTOL-MYERS SQUIBB")
	replace ISO_CODE = "GBR" if regexm(DONOR_NAME, "IMPERIAL COLLEGE OF SCIENCE")
	replace DONOR_COUNTRY="United Kingdom" if regexm(DONOR_NAME, "IMPERIAL COLLEGE OF SCIENCE")
	
	replace ISO_CODE = "NA" if ISO_CODE == "" | ISO_CODE == " NA"

	replace INCOME_TYPE = "CENTRAL" if DONOR_NAME== "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"
	replace INCOME_SECTOR = "PUBLIC" if DONOR_NAME== "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND"
	replace INCOME_TYPE = "CENTRAL" if DONOR_NAME== "UNITED STATES OF AMERICA"
	replace INCOME_SECTOR = "PUBLIC" if DONOR_NAME== "UNITED STATES OF AMERICA"
	
	tab INCOME_SECTOR, missing
	tab INCOME_TYPE, missing
		
	**// Replacing DONOR_NAME in WHO cases where the source donor was listed
	replace DONOR_NAME = ALT_DONOR_NAME if ALT_DONOR_NAME != "" & ALT_DONOR_NAME != "NA" & ALT_DONOR_NAME != "."
	replace INCOME_SECTOR = ALT_INCOME_SECTOR if ALT_INCOME_SECTOR != "" & ALT_INCOME_SECTOR != "NA" & ALT_INCOME_SECTOR != "."
	replace INCOME_TYPE = ALT_INCOME_TYPE if ALT_INCOME_TYPE != "" & ALT_INCOME_TYPE != "NA" & ALT_INCOME_TYPE != "."
	replace DONOR_COUNTRY = ALT_DONOR_COUNTRY if ALT_DONOR_COUNTRY != "" & ALT_DONOR_COUNTRY != "NA" & ALT_DONOR_COUNTRY != "."
	replace ISO_CODE = ALT_ISO_CODE if ALT_ISO_CODE != ""  & ALT_ISO_CODE != "NA" & ALT_ISO_CODE != "."

	**// Donor name fixes
	replace DONOR_NAME = subinstr(DONOR_NAME,"  "," ",.)
	replace DONOR_NAME = subinstr(DONOR_NAME,"PROGRAMME","PROGRAM",.)
	replace DONOR_NAME = "3M CORPORATION" if DONOR_NAME == "3M UNITED STATES"
	replace DONOR_NAME = "ABT ASSOCIATES" if DONOR_NAME == "ABT ASSOCIATES INC."
	replace DONOR_NAME = "ALBERT B. SABIN VACCINE INSTITUTE" if DONOR_NAME == "ALBERT B SABIN VACCINE INSTITUTE"
	replace DONOR_NAME = "AMERICAN ACADEMY OF ALLERGY, ASTHMA AND IMMUNOLOGY" if DONOR_NAME == "AMERICAN ACADEMY OF ALLERGY ASTHMA & IMMUNOLOGY"
	replace DONOR_NAME = "AMERICAN ACADEMY OF ALLERGY, ASTHMA AND IMMUNOLOGY" if DONOR_NAME == "AMERICAN ACACDEMY OF ALLERGY, ASTHMA AND IMMUNOLOGY" 
	replace DONOR_NAME = "AMERICAN ACADEMY OF OTOLARYNGOLOGY – HEAD AND NECK SURGERY (AAO-HNS)" if DONOR_NAME == "AMERICAN ACADEMY OF OTOLARYNGOLOGY – HEAD AND NECK SURGERY"
	replace DONOR_NAME = "AMERICAN CYANAMID" if regexm(DONOR_NAME, "AMERICAN CYANAMID") 
	replace DONOR_COUNTRY = "UNITED STATES" if DONOR_NAME == "AMERICAN CYANAMID"
	replace ISO_CODE = "USA" if DONOR_COUNTRY == "UNITED STATES"
	replace DONOR_NAME = "AFRICAN PROGRAM FOR ONCHOCERCIASIS CONTROL (APOC)" if DONOR_NAME == "AFRICAN PROGRAMME FOR ONCHOCERCIASIS CONTROL (APOC)"
	replace INCOME_SECTOR = "OTHER" if DONOR_NAME == "AFRICAN PROGRAM FOR ONCHOCERCIASIS CONTROL (APOC)"
	replace INCOME_TYPE = "UN" if DONOR_NAME == "AFRICAN PROGRAM FOR ONCHOCERCIASIS CONTROL (APOC)"
	replace DONOR_NAME = "ARAB GULF PROGRAM FOR THE UNITED NATIONS DEVELOPMENT ORGANIZATIONS (AGFUND)" if DONOR_NAME == "ARAB GULF PROGRAM FOR UNITED NATIONS DEVELOPMENT ORGANIZATIONS (AGFUND)"
	replace DONOR_NAME = "ASSOCIATION FOR THE PULMONARY DISABLED" if DONOR_NAME == "ASSOCIATION FOR THE PULMONARY DISABLE"
	replace DONOR_NAME = "ASSOCIATION JAPONAISE POUR L'AFRIQUE" if DONOR_NAME == "ASSOCIATION JAPONAISE POUR L'AFRIQUE  JAPAN"
	replace DONOR_NAME = "ASSOCIAZIONE SCIENTIFICA INTERDISCIPLINAIRE PER LE STUDIO DELLE MALATTIE RESPIRATORIE (AIMAR)" if DONOR_NAME == "ASSOCIAZIONE SCIENTIFICA INTERDISCIPLINARE PER LE STUDIO DELLE MALATTIE RESPIRATOIRE (AIMER)"
	replace DONOR_NAME = "WORLD SELF-MEDICATION INDUSTRY (WSMI)" if DONOR_NAME == "WORLD SELF-SELF-MEDICATION INDUSTRY (WSMI)"
	replace DONOR_NAME = "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND" if DONOR_NAME == "UNITED KINGDOM OF GREAT BRITAIN AND"
	replace DONOR_COUNTRY = "Netherlands" if DONOR_NAME == "ROYAL TROPICAL INSTITUTE (KIT), NETHERLANDS"
	replace ISO_CODE = "NLD" if DONOR_NAME == "ROYAL TROPICAL INSTITUTE (KIT), NETHERLANDS"
	replace DONOR_NAME = "ROYAL TROPICAL INSTITUTE (KIT), NETHERLANDS" if DONOR_NAME == "ROYAL TROPICAL INSTITUTE (KIT)"

	replace ISO_CODE = "GRC" if DONOR_COUNTRY == "GREECE"
	replace INCOME_TYPE = "CORP" if DONOR_NAME == "NOVARTIS"
	replace ISO_CODE = "NA" if DONOR_NAME== "PASS THROUGH FUNDING" & YEAR==2015
	replace INCOME_TYPE = "UNALL" if DONOR_NAME== "PASS THROUGH FUNDING" & YEAR==2015
	replace INCOME_SECTOR = "UNALL" if DONOR_NAME== "PASS THROUGH FUNDING" & YEAR==2015
	replace DONOR_COUNTRY = "" if DONOR_NAME== "PASS THROUGH FUNDING" & YEAR==2015
		
	* Fixing INCOME_TYPE UN, DEVBANK, and PPP to INCOME_SECTOR OTHER
	* INCOME_TYPE CORP to INCOME_SECTOR INK
	replace INCOME_SECTOR = "OTHER" if (INCOME_TYPE=="UN" | INCOME_TYPE=="PPP" | INCOME_TYPE=="DEVBANK")
	replace INCOME_SECTOR = "INK" if INCOME_TYPE=="CORP"

	gen GHI = "WHO"

	**// Countries that no longer exist
	**// Assuming this is money from the Czech Republic and not Slovakia that had the wrong donor country name added
		replace ISO_CODE = "CZE" if ISO_CODE=="CZE_FRMR" & YEAR > 1992 
		replace ISO_CODE = "MKD" if (ISO_CODE=="YUG_FRMR" | ISO_CODE=="YUG") & YEAR > 1992
			
	
	**we would like to reallocate any negative DAH either back in time to the appropriate donor 
	**or if not donor specific, reallocate to all donors in that year based off their fraction of total DAH
	preserve
	keep if OUTFLOW<0 & DONOR_COUNTRY!=""
	**Ukraine takes care of itself in later collapsing
	drop if DONOR_NAME=="UKRAINE"

	**CONGO has 4 years (2000-2004) which are negative
	**we will evenly reallocate these to the year 1999 & 1998 since it was a biennium report
	**the Royal Tropical Instit. also had negative DAH in 2016, but gave in 2015
	collapse (sum) OUTFLOW, by(DONOR_COUNTRY DONOR_NAME)
	expand 2 if DONOR_NAME=="CONGO", gen(id)
	replace OUTFLOW = OUTFLOW/2 if DONOR_NAME=="CONGO"
	gen YEAR = 1999 if DONOR_NAME=="CONGO"
	replace YEAR = 1998 if id==1
	replace YEAR = 2012 if regexm(DONOR_NAME,"^ROYAL TROPICAL INS")
	rename OUTFLOW neg_dah
	
	tempfile neg_dah
	save `neg_dah', replace
	restore
	
	merge m:1 YEAR DONOR_COUNTRY DONOR_NAME using `neg_dah', force
	replace neg_dah = 0 if neg_dah==.
	replace OUTFLOW = OUTFLOW + neg_dah
	
	drop neg_dah _merge id
	
	**negative DAH that are not donor specific
	preserve
	keep if OUTFLOW<0 & DONOR_COUNTRY==""
	collapse (sum) OUTFLOW, by(YEAR)
	rename OUTFLOW neg_dah
	
	tempfile neg_dah
	save `neg_dah', replace
	restore
	
	**getting rid of negative disbursements that are now accounted for
	**Ukraine again gets collapsed later and is not lost
	drop if OUTFLOW<0 & DONOR_COUNTRY!="UKRAINE"
	merge m:1 YEAR using `neg_dah'
	bysort YEAR: egen total_dah = total(OUTFLOW)
	gen fract_tot_dah = OUTFLOW/total_dah
	bysort YEAR: egen check = total(fract_tot_dah)
	replace neg_dah = fract_tot_dah*neg_dah
	replace neg_dah = 0 if neg_dah==.
	replace OUTFLOW = OUTFLOW+neg_dah
	drop neg_dah check total_dah fract_tot_dah _merge

	**// Tag double counting 
	replace DONOR_NAME = upper(DONOR_NAME)
		
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, NEW YORK" 
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFP"
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "IATF_TC_UNFPA"
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA NEW YORK"
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA PROGRAMME SUPPORT SERVICES"
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNFPA, HEW YORK"
		replace DONOR_NAME = "UNFPA" if DONOR_NAME == "UNITED NATIONS POPULATION FUND (UNFPA)"
		replace DONOR_NAME = "UNFPA" if regexm(DONOR_NAME_upper , "UNFPA")
		
		replace DONOR_NAME = "UNICEF" if DONOR_NAME == "UNITED NATIONS CHILDREN'S FUND"
 		replace DONOR_NAME = "UNICEF" if DONOR_NAME == "IATF_TC_UNICEF"
 		replace DONOR_NAME = "UNICEF" if DONOR_NAME == "IATF TC UNICEF"
		replace DONOR_NAME = "UNICEF" if DONOR_NAME == "UNITED NATIONS CHILDREN’S FUND (UNICEF)"
		replace DONOR_NAME = "UNICEF" if regexm(DONOR_NAME_upper , "^UNICEF")
		replace DONOR_NAME = "UNICEF" if regexm(DONOR_NAME_upper , "^UNITED NATIONS CHILDREN")

		replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME ON HIV/AIDS"
		replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAMME"
		replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "UNITED NATIONS JOINT PROGRAMME"
		replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "JOINT UNITED NATIONS PROGRAM ON HIV/AIDS (UNAIDS)"
		replace DONOR_NAME = "UNAIDS" if DONOR_NAME == "THE JOINT UNITED NATIONS PROGRAM ON HIV/AIDS (UNAIDS)"
		
		replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAHO, WASHINGTON"
		replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION (PAHO)"
		replace DONOR_NAME = "PAHO" if DONOR_NAME == "PAN AMERICAN HEALTH ORGANIZATION"
		replace DONOR_NAME = "PAHO" if regexm(DONOR_NAME_upper , "PAHO") | regexm(DONOR_NAME_upper , "PAN AMERICAN H")
		
		replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA"
		replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND"
		replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM)"
		replace DONOR_NAME = "GFATM" if DONOR_NAME == "GLOBAL FUND TO FIGHT AIDS, TUBERCULOSIS AND MALARIA (GFATM) (NOTE 6)"

		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINE"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINE"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN'S VACCINES"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI GLOBAL FUND FOR CHILDREN’S VACCINES"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINES AND IMMUNIZATION"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "GAVI ALLIANCE"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "THE GAVI ALLIANCE"
		replace DONOR_NAME = "GAVI" if DONOR_NAME == "THE GLOBAL ALLIANCE FOR VACCINE IMMUNIZATION (GAVI)"

		replace DONOR_NAME = "BMGF" if DONOR_NAME == "BILL & MELINDA GATES FOUNDATION"
		replace DONOR_NAME = "BMGF" if DONOR_NAME == "BILL AND MELINDA GATES FOUNDATION"
		replace DONOR_NAME = "BMGF" if DONOR_NAME == "GATES FOUNDATION"
		replace DONOR_NAME = "BMGF" if DONOR_NAME == "THE GATES FOUNDATION"

		replace DONOR_NAME = "AfDB" if regexm(DONOR_NAME, "AFRICAN DEVELOPMENT BANK")
		replace DONOR_NAME = "WB" if DONOR_NAME == "WORLD BANK"
		replace DONOR_NAME = "WB" if regexm(DONOR_NAME_upper , "WORLD BANK")
		replace DONOR_NAME = "WB_IBRD" if DONOR_NAME == "INTERNATIONAL BANK FOR RECONSTRUCTION AND DEVELOPMENT"
		replace DONOR_NAME = "AsDB" if DONOR_NAME == "ASIAN DEVELOPMENT BANK"
		
		**// Couldn't find EEA
		**// Don't need to tag Unitaid, Wellcome Trust, or European Commission because they already tag WHO
		
		**// Tag double counting
		gen ELIM_CH = 0
		replace ELIM_CH = 1 if DONOR_NAME == "UNFPA" 						
		replace ELIM_CH = 1 if DONOR_NAME == "UNICEF" 						
		replace ELIM_CH = 1 if DONOR_NAME == "UNAIDS"						
		replace ELIM_CH = 1 if DONOR_NAME == "WHO"  & CHANNEL != "PAHO"		
		replace ELIM_CH = 1 if DONOR_NAME == "PAHO"						
		replace ELIM_CH = 1 if DONOR_NAME == "GAVI" & CHANNEL != "GAVI"		
		replace ELIM_CH = 1 if DONOR_NAME == "GFATM" & CHANNEL != "GFATM"
		replace ELIM_CH = 1 if DONOR_NAME == "AfDB"	
		replace ELIM_CH = 1 if DONOR_NAME == "AsDB"
		replace ELIM_CH = 1 if DONOR_NAME == "WB" | DONOR_NAME=="WB_IBRD"
		
		gen SOURCE_CH=DONOR_NAME if ELIM_CH==1
		drop ELIM_CH
		
	tempfile adb
	save `adb', replace

	save "`FIN'/WHO_INC_EXP_1990_`update_yr'.dta", replace

	**//Get ebola DAH from UNOCHA 
		use  "FILEPATH/countrycodes_official.dta", clear
		rename iso3 ISO3_RC
		bysort ISO3_RC: gen N=_N
		drop if N>1
		drop N
		tempfile temp_rc
		save `temp_rc'

		insheet using "FILEPATH/FGH_`report_yr'/ebola_all_collapsed.csv", names comma clear case
		keep if channel =="WHO" &  contributionstatus =="paid"
		collapse (sum) amount, by(year source channel recipient_country HFA) fast
		rename year YEAR
		rename source DONOR_NAME
		rename channel CHANNEL
		rename amount oid_ebz_DAH
		rename recipient_country country_lc
		* Merge in iso3_rc info by matching on country_lc
		merge m:1 country_lc using `temp_rc', keepusing(ISO3_RC)
		keep if _m == 1 | _m == 3
		drop _m
		replace ISO3_RC = "QMA" if country_lc == "QMA" | country_lc == ""
		 
	**	// fill in source info
		gen ISO_CODE = subinstr(DONOR_NAME, "BIL_", "",.)
		gen INCOME_SECTOR = "PUBLIC" if regexm(DONOR_NAME, "BIL")
		gen INCOME_TYPE = "CENTRAL" if regexm(DONOR_NAME, "BIL")
		replace INCOME_TYPE = "DEVBANK" if DONOR_NAME == "WB" | DONOR_NAME == "ADB"
		replace INCOME_TYPE = "EC" if DONOR_NAME == "EC"
		replace INCOME_SECTOR = "OTHER" if DONOR_NAME == "EC" | DONOR_NAME == "WB"
		replace INCOME_SECTOR = "FOUND" if DONOR_NAME == "US_FOUND"
		replace INCOME_SECTOR = "PRIVATE" if DONOR_NAME == "PRIVATE" | DONOR_NAME == "NGO" | DONOR_NAME == "US_FOUND" | DONOR_NAME == "INT_FOUND"
		replace INCOME_SECTOR = "BMGF" if DONOR_NAME == "BMGF"
		replace INCOME_SECTOR = "OTHER" if INCOME_SECTOR == ""
		replace INCOME_TYPE = "OTHER" if INCOME_TYPE == ""
		 
	**	 // format data
		replace ISO3_RC = "COD" if country_lc == "Congo, The Democratic Republic of the" & ISO3_RC == ""
		gen INKIND = 0
		gen WB_REGION = "Sub-Saharan Africa"
		gen WB_REGIONCODE = "SSA"
		gen LEVEL = "COUNTRY"
		replace LEVEL = "REGIONAL" if ISO3_RC == "QMA"
		gen EBOLA = 1
		collapse (sum) oid_ebz_DAH, by (YEAR DONOR_NAME CHANNEL ISO_CODE INCOME_SECTOR INCOME_TYPE EBOLA) fast
		tempfile who_ebola
		save `who_ebola'

		use "`FIN'/WHO_INC_EXP_1990_`update_yr'.dta", clear 
		replace DONOR_NAME =  upper(DONOR_NAME)
		keep YEAR CHANNEL SOURCE_DOC INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_NAME DONOR_COUNTRY ISO_CODE OUTFLOW SOURCE_CH
		collapse (sum) OUTFLOW, by(YEAR CHANNEL SOURCE_DOC INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO_CODE SOURCE_CH)
		* appends ebola as merge was causing a duplication error
*		merge m:1 YEAR CHANNEL ISO_CODE INCOME_SECTOR INCOME_TYPE using `who_ebola'
*		replace ISO_CODE = "USA" if _merge == 2 & !inlist(ISO_CODE, "CAN", "NOR")
		append using `who_ebola'
		replace EBOLA = 0 if EBOLA == .
		replace oid_ebz_DAH = 0 if oid_ebz_DAH ==.
		replace OUTFLOW = 0 if  OUTFLOW ==.
*		drop _merge
		collapse (sum) OUTFLOW oid_ebz_DAH, by (YEAR CHANNEL INCOME_SECTOR INCOME_TYPE DONOR_NAME DONOR_COUNTRY ISO_CODE EBOLA SOURCE_CH)

* importing back the data with corrected INCOME SECTOR and TYPE
tempfile who_ebola_temp
save `who_ebola_temp'
insheet using "`FIN'/COVID_DONOR_AGGREGATE_EBOLA_MERGE.csv", names comma clear case
replace DONOR_NAME = upper(DONOR_NAME)
merge 1:m YEAR DONOR_NAME ISO_CODE DONOR_COUNTRY INCOME_SECTOR INCOME_TYPE CHANNEL using `who_ebola_temp'
replace EBOLA = 0 if EBOLA == .
replace oid_ebz_DAH = 0 if oid_ebz_DAH ==.
replace oid_covid_DAH = 0 if oid_covid_DAH ==.
replace OUTFLOW = 0 if  OUTFLOW ==.
drop _merge
	**// Add in-kind
		preserve
		import excel using "`RAW'/WHO_INKIND_RATIOS_1990_`update_yr'.xlsx", firstrow clear
		keep YEAR INKIND_RATIO
		tempfile ik_ratio 
		save `ik_ratio'
		restore

		merge m:1 YEAR using `ik_ratio', nogen keep(1 3)
		preserve
		replace OUTFLOW = OUTFLOW*INKIND_RATIO 
		replace oid_ebz_DAH = oid_ebz_DAH*INKIND_RATIO 
		replace oid_covid_DAH = oid_covid_DAH*INKIND_RATIO 
		gen INKIND=1
		tempfile with_inkind
		save `with_inkind'
		restore

		replace OUTFLOW = OUTFLOW*(1-INKIND_RATIO)
		replace oid_ebz_DAH = oid_ebz_DAH*(1-INKIND_RATIO)
		replace oid_covid_DAH = oid_covid_DAH*(1-INKIND_RATIO)
		append using `with_inkind'
		replace INKIND=0 if INKIND==.
		gen COVID = 1 if oid_covid_DAH > 0
		replace COVID = 0 if COVID == .		
	save "`FIN'/WHO_INC_EXP_1990_`update_yr'_ebola_fixed.dta", replace
	
**// Allocating from Source to Channel to Health Focus Area (merging ADB and PDB):

	use "`INT'/WHO_EXP_BY_HFA_FGH`report_yr'.dta", clear
		
**	// Merge with DAH data and calculate expenditure by HFA:
		merge 1:m YEAR using "`FIN'/WHO_INC_EXP_1990_`update_yr'_ebola_fixed.dta"
		**//will get added back in at the end
		** added the oid_covid part in 2021
			replace OUTFLOW = OUTFLOW - oid_ebz_DAH - oid_covid_DAH
			**// sometimes Ebola (and covid) amount is more than total DAH
			replace OUTFLOW = 0 if OUTFLOW < 0  
			drop if _m != 3
			drop _m
			rename OUTFLOW DAH
        // note - if HFAs change, make sure they are accounted for here
		// foreach healthfocus in rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_other tb_other mal_con_nets mal_con_oth mal_other ncd_tobac ncd_mental ncd_other oid_other swap_hss_other swap_hss_hrh swap_hss_pp other {					

gen tot = 0
        foreach healthfocus in hiv_amr hiv_care hiv_ct hiv_hss_hrh hiv_hss_me hiv_hss_other hiv_other hiv_ovc hiv_pmtct hiv_prev hiv_treat mal_amr mal_comm_con mal_con_irs mal_con_nets mal_con_oth mal_diag mal_hss_hrh mal_hss_me mal_hss_other mal_other mal_treat ncd_hss_hrh ncd_hss_me ncd_hss_other ncd_mental ncd_other ncd_tobac nch_cnn nch_cnv nch_hss_hrh nch_hss_me nch_hss_other nch_other oid_amr oid_hss_hrh oid_hss_me oid_hss_other oid_other oid_zika other rmh_fp rmh_hss_hrh rmh_hss_me rmh_hss_other rmh_mh rmh_other swap_hss_hrh swap_hss_me swap_hss_other swap_hss_pp tb_amr tb_diag tb_hss_hrh tb_hss_me tb_hss_other tb_other tb_treat unalloc {
			quietly lookfor `healthfocus'
			if "`r(varlist)'" == "" {
			    gen `healthfocus'_frct = 0 
			}
			gen double `healthfocus'_DAH = `healthfocus'_frct * DAH
            replace tot = tot + `healthfocus'_frct // for checking sum of fractions
		}
    
		gen ISO3_RC = "NA"
		sort YEAR
		drop check

	gen gov = 0
** added the oid_covid part in 2021
	replace DAH = DAH + oid_ebz_DAH + oid_covid_DAH
	drop if YEAR >= `report_yr'


    // check no major differences after hfa allocation
    egen pa_tot = rowtotal(*_DAH)
    gen diff = abs(pa_tot - DAH)
    egen max_diff = max(diff)
    di "max_diff:"
    levelsof max_diff
    if max_diff > 150 {
        // double-check HFA fractions above sum to 1!
        _error_allocating_HFAs_
    }
    drop pa_tot diff max_diff


	save "`FIN'/WHO_ADB_PDB_FGH`report_yr'_ebola_fixed.dta", replace
	
**// Saving PDB:
	use "`FIN'/WHO_ADB_PDB_FGH`report_yr'_ebola_fixed.dta", clear
	rename DAH OUTFLOW
	gen DISBURSEMENT = OUTFLOW
	collapse (sum) OUTFLOW DISBURSEMENT (first) *_frct, by(YEAR CHANNEL)
	format %12.3g OUTFLOW
	gen ISO3_FC = "NA"
	gen FUNDING_TYPE = "GRANT"
	gen FUNDING_AGENCY = "WHO"
	gen FUNDING_AGENCY_SECTOR = "MULTI"
	gen RECIPIENT_AGENCY_SECTOR = "UNSP"
	gen RECIPIENT_AGENCY_TYPE = "UNSP"

	// foreach healthfocus in rmh_fp rmh_mh rmh_other nch_cnn nch_cnv nch_other hiv_treat hiv_prev hiv_pmtct hiv_other tb_other mal_con_nets mal_other mal_con_oth ncd_tobac ncd_mental ncd_other oid_other swap_hss_other swap_hss_hrh swap_hss_pp other {
        foreach healthfocus in hiv_amr hiv_care hiv_ct hiv_hss_hrh hiv_hss_me hiv_hss_other hiv_other hiv_ovc hiv_pmtct hiv_prev hiv_treat mal_amr mal_comm_con mal_con_irs mal_con_nets mal_con_oth mal_diag mal_hss_hrh mal_hss_me mal_hss_other mal_other mal_treat ncd_hss_hrh ncd_hss_me ncd_hss_other ncd_mental ncd_other ncd_tobac nch_cnn nch_cnv nch_hss_hrh nch_hss_me nch_hss_other nch_other oid_amr oid_hss_hrh oid_hss_me oid_hss_other oid_other oid_zika other rmh_fp rmh_hss_hrh rmh_hss_me rmh_hss_other rmh_mh rmh_other swap_hss_hrh swap_hss_me swap_hss_other swap_hss_pp tb_amr tb_diag tb_hss_hrh tb_hss_me tb_hss_other tb_other tb_treat unalloc {
	    rename `healthfocus'_frct `healthfocus'
	}
	
	save "`FIN'/WHO_INTPBD_1990_`update_yr'.dta", replace


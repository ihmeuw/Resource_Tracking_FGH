# Compiling DAH Estimates


The goal of "compiling" is to take all of our channel-level DAH estimates and combine them into one final, standardize database.
This is a complicated process, partially because the channel-level data is still really messy (something to improve over time!).
The main final outputs are:  

- The "DAH ADB PDB" - the final DAH estimates, used internally by the DAH team. This file is still somewhat messy/not user friendly, and thus
  is generally only used outside the DAH team.  
- The "FGH EZ" data - this is a slightly more user friendly version of the DAH ADB PDB.  
- "dah_by_donor_year" and "dah_by_recipient_year", created by script `5_THE_retro_data.R`, are files passed off to the EI sub-team for DAH forecasting purposes.  
- New in FGH 2024: "fgh_figure_data" - a more standardized version of the final estimates, ideally something we can share with other teams.  





## 0_un_doublecounting_fix.R
- **Purpose:**	Compiles the **"includesDC"** dataset from relevant channels and adjusts for transfers between channels to avoid double-counting.
	
## FGH 2024 temporary: 0b_UN_recipient:  
- **Purpose:** Scripts used to process data on country recipients of UN DAH, since up until this year we did not estimate any recipient-level funding for UN agencies (it was all 'unallocable').
For the sake of Goalkeepers in summer 2025, we patched in unallocable-recipient updates for UN agencies and NGOs for 2000-2023.


## 1_compiling.R	
- **Purpose:** Bring together all channel data (an UN data adjusted for double-counting).

## 1b_backcasting.R	
- **Purpose:** Adjusts data for countries which no longer exist, distributing to the new countries that they split into. 

## 2_region_split_launch.R	
- **Purpose:**	Launches the code in **2a_region_split.R** to be ran.	
- **!!!   NOTE   !!!**
	- Make sure any needed changes have been made to the code in 2a_region_split.R before running this file.
- **2a_region_split.R**  
    - Re-distributes DAH to regional-level recipients to the member countries of the region.

## 4a_finalization.R	
- **Purpose:** Brings together the regional data. Cleans up all the attributes, standardizing INCOME_SECTOR, INCOME_TYPE, ISO3_RC, etc.

## 4b_source_disagg.R  
- **Purpose:** Disaggregates donors that should not appear as sources of funding in our framework (e.g., World Bank, UN agencies) by splitting those disbursements out amongst the donors of those channels.

## 4c_save_adbpdb.R  
- **Purpose:** Saves the final ADB PDB data set.  

## 5_THE_retro_data.R	
- **Purpose:**	Create DAH datasets used by the EI sub-team for forecasting

## 6_graphs.R  
- **Purpose:** Create a bunch of diagnostic graphs to compare new estimates with previous years.  


## shiny  
- **Purpose:** interactive dashboard to visualize the DAH estimates at various points in the compiling pipeline, to help track bugs or changes.


# NEW FOR FGH 2024:

In FGH 2024/2025, we introduced a new level of standardization. This was done partially because we had to extend the estimates an additional year, past where we would have typically estimated (normally the estimates would have stopped at 2024, but we also predicted 2025).
These scripts standardize the ADB PDB data further based on what is needed for our typical analyses. In the future, this standardization would still be super useful.
However, we will probably not need the 'predict2025' portion anymore or the unallocable recipient updates, so some code will need to be updated.

## 7a_standardize.R 
- Begins the standardization process by identifying source, channel, and recipient info and aggregating  

## 7b_fgh2024_predict2025/.R  
- Applies our 2025 prediction methodology to the standardized data to produce our 2025 estimates by source and channel.  

## 7c_finalize_fgh_data.R  
- Combines the standardized retrospective data (from 7a) with the 2025 estimates (from 7b) to produce a final standardized FGH 2024/2025 data set for EI that contains retrospective and preliminary estimates.

## 8a_result_graphs.R  
- Creates a bunch of nice-ish plots to visualize the final data.

## 8b_recipient_graphs.R
- More final result graphs, specifically focused on recipient-level data.  


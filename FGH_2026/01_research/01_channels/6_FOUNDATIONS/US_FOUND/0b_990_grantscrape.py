#$ -S /bin/sh
#!/bin/bash

"""
Purpose: Scraping 990s
"""

# Download Packages
import sys
from bs4 import BeautifulSoup
from bs4.element import Comment
import lxml
import pandas as pd
import numpy as np
import requests
import time
from unidecode import unidecode

# Set Paths and variables
report_yr = 2024
data_yr = report_yr - 2
raw_root = "FILEPATH/"
raw = raw_root + "FGH_" + str(report_yr) + "/"



def geturl(url, year = 2021):
    """
    Gets xml link given main page link

    Args:
        url (str): main page of organization to scrape
        year (str): year of return to scrape

    Returns:
        str: url of xml page
    """
    root = "https://projects.propublica.org"
    if url == "":
        return ""
    time.sleep(1) # Scraping time lag
    html_data = requests.get(url).content
    soup = BeautifulSoup(html_data, "lxml")
    NotFound = BeautifulSoup("<html><body><p>Not Found</p></body></html>", "lxml")
    if soup == NotFound:
        return ""
    year = "filing" + str(year)
    if soup.find("section", {"id" : year}):
        filing = soup.find("section", {"id" : year})
        rightbar = filing.find("section", {"class": "right-content"})
        buttons = rightbar.findAll("a", {"class": "btn"})
        for button in buttons:
            if(button.string == "XML"):
                xmlurl = button["href"]
                xmlurl = root + xmlurl
                return xmlurl
                break
    # Return empty string if no URL found
    xmlurl = ""
    return xmlurl  



def scrapeinkind (soup):
    """
    gets variables for total expense and inkind

    Args:
        soup (soup): soup object to scrape
    Returns:
        soupdict (dict): dictionary of scraped strings
    """
    soupdict = {}
    soupdict["Admin"] = 0
    if soup.find("TaxYr"):
        soupdict["YEAR"] = soup.TaxYr.string
    if soup.find("ReturnTypeCd"):
        soupdict["Form Type"] = soup.ReturnTypeCd.string
    if soup.find("TotalExpensesDsbrsChrtblAmt"):
        soupdict["Expense"] = soup.TotalExpensesDsbrsChrtblAmt.string
        soupdict["Expense Source"] = "990 form Line 26 column D, page 1"
    if soup.find("TotOprExpensesDsbrsChrtblAmt"):
        soupdict["Admin"] = soup.TotOprExpensesDsbrsChrtblAmt.string
        soupdict["Admin source"] = "990 form Line 24 column D"
    if soup.find("CYTotalExpensesAmt"):
        soupdict["Expense"] = soup.CYTotalExpensesAmt.string
        soupdict["Expense Source"] = "990 Form line 18 Col B"
    if soup.find("CYBenefitsPaidToMembersAmt"):
        soupdict["Admin"] += int(soup.CYBenefitsPaidToMembersAmt.string)
        soupdict["Admin source"] = "990 Form sum Lines 14-17 Col B"
    if soup.find("CYSalariesCompEmpBnftPaidAmt"):
        soupdict["Admin"] += int(soup.CYSalariesCompEmpBnftPaidAmt.string)
        soupdict["Admin source"] = "990 Form sum Lines 14-17 Col B"
    if soup.find("CYTotalFundraisingExpenseAmt"):
        soupdict["Admin"] += int(soup.CYTotalFundraisingExpenseAmt.string)
        soupdict["Admin source"] = "990 Form sum Lines 14-17 Col B"
    return soupdict


def scrapeurl(url, year):
    """
    Scrapes single link
    Args:
        url (str): url to scrape
        year (int): year to scrape
    Returns:
        [inkinddict, grants, foreignexpense] (list):
            inkinddict is dict, grants is list of dict, foreignexpense is list of dict
    """
    inkinddict = {}
    grants = []
    foreignexpense = []
    xmlurl = geturl(url, year)
    print(xmlurl)
    # if xml url was not found, try next year to see if it is available
    if xmlurl == "":
        print("Checking Next Year")
        xmlurl = geturl(url, year+1)
        if xmlurl == "":
            print("Year Not Found")
            return [inkinddict, grants, foreignexpense]
    print("XML Was Found")
    xml_data = requests.get(xmlurl).content
    soup = BeautifulSoup(xml_data, "xml")
    # Check that year is correct
    if soup.find("TaxYr"):
        # Scrape if year is correct
        if soup.TaxYr.string == str(year):
            print("Correct year on first try!)")
            inkinddict = scrapeinkind(soup)
            grants = scrapegrants(soup, year)
            foreignexpense = scrapeforeign(soup, year)
            return [inkinddict, grants, foreignexpense]
        else:
            # Try next year if year was incorrect
            xmlurl = geturl(url, year + 1)
            if xmlurl == "":
                print("Correct year not found")
                return [inkinddict, grants, foreignexpense]
            xml_data = requests.get(xmlurl).content
            soup = BeautifulSoup(xml_data, "xml")
            if soup.TaxYr.string == str(year):
                print("Correct year on second try!")
                inkinddict = scrapeinkind(soup)
                grants = scrapegrants(soup, year)
                foreignexpense = scrapeforeign(soup, year)
                return [inkinddict, grants, foreignexpense]
            return[inkinddict, grants, foreignexpense]
    else:
        return [inkinddict, grants, foreignexpense]



def scrapegrants(soup, taxyr):
    """
    Scrapes soup for grants
    Args:
        soup (soup): beautiful soup xml object
        taxyr (str): correct tax year
    Returns:
        grants (list): list of dictionaries of scraped grants
    """
    grants = []
    if soup.find("GrantOrContributionPdDurYrGrp"):
        for grant in soup.find_all("GrantOrContributionPdDurYrGrp"):
            if grant.find("RecipientPersonNm"):
                pass
            else:
                grantinfo = {}
                grantinfo["Year"] = taxyr
                if soup.find("BusinessNameLine1Txt"):
                    grantinfo["Recipient"] = grant.BusinessNameLine1Txt.string
                else:
                    grantinfo["Recipient"] = grant.BusinessNameLine1.string
                grantinfo["Amount"] = grant.Amt.string
                if grant.find("RecipientFoundationStatusTxt"):
                    grantinfo["Foundation Status"] = grant.RecipientFoundationStatusTxt.string
                grantinfo["Purpose"] = grant.GrantOrContributionPurposeTxt.string
                if grant.find("RecipientUSAddress"):
                    address = ""
                    for line in grant.RecipientUSAddress.children:
                        if line == "\n":
                            pass
                        else:
                            address += line.string + ", "
                    grantinfo["Address"] = address[:-2]
                    grantinfo["Country"] = "US"
                if grant.find("RecipientForeignAddress"):
                    address = ""
                    for line in grant.RecipientForeignAddress.children:
                        if line == "\n":
                            pass
                        else:
                            if line.name == "CountryCd":
                                grantinfo["Country"] = line.string
                            else:
                                address += line.string + ", "
                    grantinfo["Address"] = address[:-2]
                grants.append(grantinfo)
    if soup.find("RecipientTable"):
        for grant in soup.find_all("RecipientTable"):
            if grant.find("RecipientPersonNm"):
                pass
            else:
                grantinfo = {}
                if soup.find("BusinessNameLine1Txt"):
                    grantinfo["Recipient"] = grant.BusinessNameLine1Txt.string
                else:
                    grantinfo["Recipient"] = grant.BusinessNameLine1.string    
                grantinfo["Amount"] = grant.CashGrantAmt.string
                if soup.find("Foundation Status"):
                    grantinfo["Foundation Status"] = grant.IRCSectionDesc.string
                if grant.find("PurposeOfGrantTxt"):
                    grantinfo["Purpose"] = grant.PurposeOfGrantTxt.string
                if grant.find("USAddress"):
                    address = ""
                    for line in grant.USAddress.children:
                        if line == "\n":
                            pass
                        else:
                            address += line.string + ", "
                    grantinfo["Address"] = address[:-2]
                    grantinfo["Country"] = "US"
                if grant.find("ForeignAddress"):
                    address = ""
                    for line in grant.ForeignAddress.children:
                        if line == "\n":
                            pass
                        else:
                            if line.name == "CountryCd":
                                grantinfo["Country"] = line.string
                            else:
                                address += line.string + ", "
                    grantinfo["Address"] = address[:-2]
                grants.append(grantinfo)
    return grants

def scrapeforeign(soup, taxyr):
    """
    Scrapes soup for foreign expense info
    Args:
        soup (soup): beautiful soup xml object
        taxyr (str): correct tax year
    Returns:
        foreigndict (list): list of dictionaries of scraped foreign activities
    """
    foreigndict = []
    if soup.find("AccountActivitiesOutsideUSGrp"):
        for activity in soup.find_all("AccountActivitiesOutsideUSGrp"):
            actinfo = {}
            actinfo["Year"] = taxyr
            if activity.find("RegionTxt"):
                actinfo["Region"] = activity.RegionTxt.string
            if activity.find("TypeOfActivitiesConductedTxt"):
                actinfo["Activity_Type"] = activity.TypeOfActivitiesConductedTxt.string
            if activity.find("RegionTotalExpendituresAmt"):
                actinfo["Expenditure"] = activity.RegionTotalExpendituresAmt.string
            if soup.find("ActivityOrMissionDesc"):
                actinfo["Activity_Desc"] = soup.ActivityOrMissionDesc.string
            if soup.find("MissionDesc"):
                actinfo["Mission_Desc"] = soup.MissionDesc.string
            foreigndict.append(actinfo)
    return foreigndict
        





def scrapecsv (start, end, year, foundations):
    """
    Scrapes a csv of foundations

    Args:
        start (int): index to begin
        end (int): index to end
        year (int): year to scrape
        foundations (pandas): foundations csv

    Returns:
        _type_: _description_
    """
    df = pd.DataFrame(columns= ["Foundation", "Recipient", "Address", "Country", "Amount", "Purpose", "Foundation Status"])
    dfmain = pd.DataFrame(columns = ["DONOR_NAME", "YEAR", "Form Type", "INKIND_RATIO", "Expense", "Expense Source", "Admin", "Admin source"])
    dfforeign = pd.DataFrame(columns = ["Foundation", "Year", "Region", "Activity_Type", "Expenditure", "Activity_Desc", "Mission_Desc"])
    for index, row in foundations.iterrows():
        # Allows setting of what links to scrape
        if index < start:
            pass
        elif index > end:
            break
        else:
            newgrant = {"Foundation": row["gm_name"]}
            newact = {"Foundation": row["gm_name"]}
            print(row)
            link = "https://projects.propublica.org/nonprofits/organizations/" + str(row["gm_ein"])
            inkinddict, grants, foreignexpense = scrapeurl(link, year)
            if not grants and not inkinddict and not foreignexpense:
                print("not scrapable")
            else:
                inkinddict["DONOR_NAME"] = row["gm_name"]
                if inkinddict["Expense"] != "0":
                    inkinddict["INKIND_RATIO"] = int(inkinddict["Admin"])/int(inkinddict["Expense"])
                dfmain.loc[len(dfmain)] = inkinddict
                for grant in grants:
                    newgrant.update(grant)
                    df.loc[len(df)] = newgrant
                for activity in foreignexpense:
                    newact.update(activity)
                    dfforeign.loc[len(dfforeign)] = newact
    return df, dfmain, dfforeign

foundations = pd.read_csv(raw + "propublica_scraping_target.csv")
df, dfmain, dfforeign= scrapecsv(0, foundations.shape[0], data_yr, foundations)

df.to_csv(raw + "propublica_foundations_grants.csv", index = False)
dfmain.to_csv(raw + "propublica_foundations_inkind.csv", index = False)
dfforeign.to_csv(raw + "propublica_foundations_foreignact.csv", index = False)

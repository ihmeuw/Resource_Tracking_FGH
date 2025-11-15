"""
Purpose: Scraping 990s
"""
print("I'm running!")

# Download Packages
import sys
from bs4 import BeautifulSoup
from bs4.element import Comment
import pandas as pd
import numpy as np
import requests
import time


def scrape_main_form (soup):
    """
    gets variables from main body of tax form

    Args:
        soup (soup): soup object to scrape
    Returns:
        soupdict (dict): dictionary of scraped strings
    """
    soupdict = {}
    soupdict["Admin"] = 0
    if soup.find("TaxYr"):
        soupdict["YEAR"] = soup.TaxYr.string
    if soup.find("TotalExpensesDsbrsChrtblAmt"):
        soupdict["Expense"] = soup.TotalExpensesDsbrsChrtblAmt.string
        soupdict["Expense source"] = "990 form Line 26 column D, page 1"
    if soup.find("TotOprExpensesDsbrsChrtblAmt"):
        soupdict["Admin"] = soup.TotOprExpensesDsbrsChrtblAmt.string
        soupdict["Admin source"] = "990 form Line 24 column D"
    return soupdict


def scrapeurl(url, year):
    """
    Scrapes single link
    Args:
        url (str): url to scrape
        year (int): year to scrape
    Returns:
        [maindict, grants] (list): maindict is dict, grants is list of dict
    """
    maindict = {}
    grants = []
    xml_data = requests.get(url).content
    soup = BeautifulSoup(xml_data, "xml")
    if soup.TaxYr.string == str(year) and soup.ReturnTypeCd.string == "990PF":
        maindict = scrape_main_form(soup)
        grants = scrapegrants(soup)
        return [maindict, grants]
    else:
        return [maindict, grants]



def scrapegrants(soup):
    """
    Scrapes xml link for grants
    Args:
        xmlurl (str): url of xml page
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
                if grant.find("BusinessNameLine1Txt"):
                    grantinfo["Recipient"] = grant.BusinessNameLine1Txt.string
                if grant.find("Amt"):
                    grantinfo["Amount"] = grant.Amt.string
                if grant.find("RecipientFoundationStatusTxt"):
                    grantinfo["Foundation Status"] = grant.RecipientFoundationStatusTxt.string
                if grant.find("GrantOrContributionPurposeTxt"):
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
    return grants


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
    df = pd.DataFrame(columns= ["Foundation", "EIN", "Recipient", "Address", "Country", "Amount", "Purpose", "Foundation Status"])
    dfmain = pd.DataFrame(columns = ["DONOR_NAME", "EIN", "YEAR", "INKIND_RATIO", "Expense", "Expense source", "Admin", "Admin source"])
    for index, row in foundations.iterrows():
        # Allows setting of what links to scrape
        sys.stdout.flush()
        if index < start:
            pass
        elif index > end:
            break
        else:
            newdata = {"Foundation": row["OrganizationName"], "EIN": row["EIN"]}
            print(index, row["OrganizationName"])
            print(row["URL"])
            maindict, grants = scrapeurl(row["URL"], year)
            if grants == []:
                continue
            maindict["DONOR_NAME"] = row["OrganizationName"]
            maindict["EIN"] = row["EIN"]
            if maindict["Expense"] != "0":
                maindict["INKIND_RATIO"] = int(maindict["Admin"])/int(maindict["Expense"])
            dfmain.loc[len(dfmain)] = maindict
            for grant in grants:
                newdata.update(grant)
                df.loc[len(df)] = newdata
    return df, dfmain


def main(start, end, year):
    start = int(start)
    end = int(end)
    year = int(year)
    foundations = pd.read_csv("FILEPATH" + 
    "990_pf_index_" + str(year) + ".csv")
    start_time = time.time()
    df, dfmain = scrapecsv(start, end, year, foundations)
    end_time = time.time()
    elapsed_time = round((end_time - start_time)/60, 2)
    print(f"Elapsed time: {elapsed_time} minutes")
    channel_loc = "FILEPATH"
    dfmain.to_csv(channel_loc + "INKIND_RATIOS_FGH2024_" + str(year) + str(start) + ".csv", index = False)
    df.to_csv(channel_loc + "scraped_990pf_" + str(year) + str(start) + ".csv", index = False)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Please provide three arguments.")
        sys.exit(1)
    print(sys.argv[1], sys.argv[2], sys.argv[3])
    main(sys.argv[1], sys.argv[2], sys.argv[3])

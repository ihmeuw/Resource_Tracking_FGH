"""
Use the iati-aggregator lib to compile transaction level data for the specified
organizations from the IATI data dump.
"""
from pathlib import Path
import pandas as pd
import json
import os
import sys
### note: this is bad, but package under development still
sys.path.append("FILEPATH")
from iati_agg import activities_to_transaction_data
###

REPORT_YEAR = 2024
# TODO: import utilities for relative paths
INT_DIR = f"FILEPATH"

# needed for iati-aggregator
DATADUMP_DIR = "FILEPATH"
CODELIST_DIR = "FILEPATH"


# dstore publisher code: your publisher name
PUBLISHER_MAP = {
    "iadb": "Inter-American Development Bank"
}




def org_scrape_datadump(datadump_org_dir, codelist_dir):
    """
    Scrape all activity documents in the given organization directory within the
    IATI datadump.

    Parameters
    ----------
    datadump_dir : str
        Path to the directory containing the datadump files.
    codelist_dir : str
        Path to the directory containing the codelists.
    
    Returns
    -------
    data : pd.DataFrame
        The transaction data set for the organization.
    meta : dict
        The metadata for the transaction data set.
    fail_set : set
        The set of documents that have failed to be scraped.
    """
    documents = list(Path(datadump_org_dir).glob("*.xml"))
    ndocs = len(documents)
    
    out = []
    fail_set = set()
    for i, doc in enumerate(documents):
        print(f"  {i+1}/{ndocs}: Processing {doc}...")
        data = activities_to_transaction_data(doc, codelist_dir, verbose=True)
        if data is None:
            print("    This document is not a valid iati-activities document. Skipping.",
                  end="\n\n")
            fail_set.add(doc)
            continue
        out.append(data)
        print("")

    if len(out) > 0:
        meta = out[0]["meta"]
        data = pd.concat([x["data"] for x in out], ignore_index=True)
    else:
        meta = None
        data = None
    return data, meta, fail_set


def save_meta(meta, fail_set, outdir):
    if meta is not None:
        with open(outdir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)
    if fail_set:
        with open(outdir / "iati_failed.txt", "w") as f:
            f.write("\n".join(str(x) for x in fail_set))


if __name__ == "__main__":
    #
    # Setup output directory
    #
    os.umask(0)
    outdir = Path(INT_DIR)
    outdir.mkdir(exist_ok=True, mode=0o777, parents=True)
    print(f"Processed data will be saved to {outdir}")
    print("")
    #
    # Setup tracking of failed documents
    #
    fail_path = outdir / "iati_failed.txt"
    fail_set = set()
    
    #
    # Scrape projects from the IATI data dump
    #
    meta = None
    try:
        for (pub_code, name) in PUBLISHER_MAP.items():
            print(f"\n*** {name}: {pub_code} ***")
            # Scrape the data
            xml_dir = Path(DATADUMP_DIR, f"FILEPATH/{pub_code}")
            data, meta, fail = org_scrape_datadump(xml_dir, CODELIST_DIR)
            fail_set.update(fail)
            # Save
            if data is None:
                print(f"  No data for {pub_code}.")
            else:
                print(f"  Saving {pub_code}.csv")
                data["pub_code"] = pub_code
                data.to_csv(outdir / f"{pub_code}_iati_scrape.csv", index=False)
            
    except (KeyboardInterrupt, Exception) as e:
        print(f"\nAn exception was encountered. Saving metadata and reporting.")
        save_meta(meta, fail_set, outdir)
        raise e
    
    save_meta(meta, fail_set, outdir)
    print("\nDone.")

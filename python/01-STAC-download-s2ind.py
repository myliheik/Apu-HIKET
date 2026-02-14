"""
2026-02-13 MY 
Search STAC and download s2ind data.

Date range set from 15 May to 31 July.

Edit years and asset list.

RUN:

python ../python/01-STAC-download-s2ind.py -o /scratch/project_2008047/Apu-HIKET/s2ind



"""

import pystac_client
from dask.distributed import Client, Lock
import os
import re
import urllib.request
import time


import os.path
from pathlib import Path
import argparse
import textwrap


# EDIT THIS:
years = range(2016, 2025)
years = range(2024, 2025)

# EDIT asset list (ndvi, ndti, ndmi, ndbi, ndsi, meta):
assetList = ['ndvi', 'meta']

def searchSTAC(years, assetList, sentinel_folder):
    # STAC:
    no_of_workers = len(os.sched_getaffinity(0))
    client = Client(n_workers=no_of_workers)
    URL = "https://paituli.csc.fi/geoserver/ogc/stac/v1"
    catalog = pystac_client.Client.open(URL)

    print("Paituli STAC catalog has these Sentinel collections:")
    for collection in catalog.get_collections():
        #print(collection.id)
        if re.findall(r"(sentinel)", collection.id): # "(sentinel)[\._]"
            print(collection.id)


    collectionName = 'sentinel_2_monthly_index_mosaics_at_fmi'
    print(f'We take {collectionName}')

    max_attempts = 2
    attempts = 0
    sleeptime = 10 #in seconds, no reason to continuously try if network is down



    collection = catalog.get_collection(collectionName)
    #print(f"ID: {collection.id}")
    print(f"\nTitle: {collection.title or 'N/A'}")
    print(f"Description: {collection.description or 'N/A'}")


    for year in years:

        print(f'\n##########################')
        print(f'\nYear {year}')

        params = {
            "collections": collectionName,
            "datetime": f"{year}-06-15/{year}-07-01",
        }
        items = list(catalog.search(**params).items_as_dicts())
        
        for asset in assetList:
            for i in range(len(items)):
                #items[0]['assets']['meta']['href']
                url = items[i]['assets'][asset]['href']

                # Define local path for downloaded file,
                filename = os.path.join(sentinel_folder, os.path.basename(url))

                # Download the file:
                while attempts < max_attempts:
                    time.sleep(sleeptime)
                    try:
                        response = urllib.request.urlopen(url, timeout = 5)
                        content = response.read()
                        f = open(filename, 'w')
                        f.write(content)
                        f.close()
                        break
                    except urllib.request.URLError as e:
                        attempts += 1
                        print(type(e))

        
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.outputshppath:
            raise Exception('Missing output filepath argument. Try --help .')

        print(f'\n01-STAC-download-s2ind.py')
        
        # directory for output:
        out_dir_path = args.outputshppath
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        
        
        searchSTAC(years, assetList, out_dir_path)
        
        print(f'\nDone.')

    except Exception as e:
        print('\n\nUnable to download data. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-o', '--outputshppath',
                        help='Directory to save parcel geometries.',
                        type=str)   
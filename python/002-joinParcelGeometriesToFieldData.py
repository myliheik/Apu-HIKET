"""
@author: MY

Join IACS parcel data (kasvulohkodata) to field data by X,Y.


***

After this run: etc.


RUN:

python 002-joinParcelGeometriesToFieldData.py -i /Users/myliheik/Documents/GISdata/Kasvulohkot2023/Kasvulohkot2023.gpkg \
-f /Users/myliheik/Documents/myPython/Apu-HIKET/data/Verrokkipellot.csv \
-o /Users/myliheik/Documents/myPython/Apu-HIKET/shapefiles \
-d /Users/myliheik/Documents/myCROPMAPPING/data/kasviDict.pkl 



"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os.path
from pathlib import Path
import argparse
import textwrap
#import math
import pickle
#import warnings
#warnings.filterwarnings("ignore")


def readLPIS(fpkasvu):
    kasvulohko = gpd.read_file(fpkasvu)
    #print(kasvulohko.columns)
    kasvulohko.rename(columns={"PLVUOSI_PERUSLOHKOTUNNUS": "PLOHKO", "KVI_KASVIKOODI": "KASVIKOODI", "KVI_KASVIK": "KASVIKOODI", "MAATILA_TUNNUS": "MAATILA_TU", "KLILM_TUNN": "KLILM_TUNNUS", "PLVUOSI_PE": "PLOHKO", "PINTAALA": "P_ALA_HA"
                              }, inplace=True)

    year = str(kasvulohko['VUOSI'][0])
    projection = kasvulohko.crs
    
    print(f'The total number of parcels: {len(kasvulohko)}')
    
    kasvulohko00 = kasvulohko[~kasvulohko.geometry.isna()].copy()
    kasvulohko0 = kasvulohko00[~kasvulohko00['KASVIKOODI'].isna()]
    
    if len(kasvulohko) - len(kasvulohko00) > 0:
        print(f'There were {len(kasvulohko) - len(kasvulohko00)} nas in parcel geometries! Excluded now.')
        
    kasvulohko0['P_ALA_HA'] = round(kasvulohko0.area/10000, 2)
 
    print(f'The mean area of parcels: {round(kasvulohko0["P_ALA_HA"].mean(), 1)}')

        
    # drop broken typologies:
    row_mask2 = kasvulohko0.geometry.is_valid
    filtered3 = kasvulohko0[row_mask2]

    filtered3 = filtered3.rename(columns={'MAATILA_TU': 'farm_ID', 'KASVIKOODI': 'plant_ID'}).copy()
    print(f'Broken typologies were checked. {len(filtered3)} parcels remains.')
    
    filtered3['perimeter'] = filtered3.length.round(0)
    
    filtered3['parcelID'] = filtered3['KLILM_TUNNUS'].apply(lambda x: "{}{}{}".format(year,'_', x)) + '_' + filtered3['PLOHKO'].astype(str) + '_' + filtered3['plant_ID'].astype(str)
    
    print('\n--------')
    #print(f"The share of crop types in the data, by area and by number: \n{pd.concat([tmpala, tmpnr, alatotos, tmpnrkaikki, alatiacs], axis = 1)}")
    
    return filtered3, year, projection

def mergeData(kasvulohkot, fieldDataPath):
    df = pd.read_csv(fieldDataPath)
    df.rename(columns = {'X': 'x', 'Y': 'y'}, inplace = True)

    gdfVerrokki = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x'], df['y']))

    gdfVerrokki2 = gdfVerrokki.set_crs(kasvulohkot.crs)
    gdfVerrokki3 = kasvulohkot.sjoin(gdfVerrokki2, predicate = 'contains', how = 'right')
    gdfVerrokki4 = gdfVerrokki3.drop(columns = ['index_left'])
    
    return gdfVerrokki4


def savingParcels(kasvulohkot, out_dir_path, year, kasviDict):
    
    outputfile = os.path.join(out_dir_path, 'fieldData-' + str(year) + '.shp')  
    outputfile2 = os.path.join(out_dir_path, 'fieldData-' + str(year) + '.csv')  

    print(f'Saving geometries to {outputfile}')           
    kasvulohkot[['parcelID', 'geometry']].to_file(driver = 'ESRI Shapefile', filename = outputfile)
    
    print(f'Saving metafiles to {outputfile2}') 
    # append plant name:    
    kasvulohkot['plant_name'] = kasvulohkot['plant_ID'].map(kasviDict)    
    kasvulohkot.drop(columns = ['geometry']).to_csv(outputfile2, index = False)

    
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputpath:
            raise Exception('Missing input filepath argument. Try --help .')

        print(f'\n002-joinParcelGeometriesToFieldData.py')
        print(f'\nLPIS data in: {args.inputpath}')
        print(f'Field data in: {args.data}')
        
        # directory for output:
        out_dir_path = args.outputshppath
        Path(out_dir_path).mkdir(parents=True, exist_ok=True)
        
        # READ LPIS, filter out too small parcels and select only relevant crop types:
        kasvulohkot, year, projection = readLPIS(args.inputpath)
        print(kasvulohkot.head())
        
        

        with open(args.kasviDict, 'rb') as fp:
            kasviDict = pickle.load(fp)

        gdfVerrokki = mergeData(kasvulohkot, args.data)
        
        savingParcels(gdfVerrokki, out_dir_path, year, kasviDict)
        
        
        print(f'\nDone.')

    except Exception as e:
        print('\n\nUnable to read input or write out. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        help='Parcel geometries (LPIS)',
                        type=str)  
    parser.add_argument('-o', '--outputshppath',
                        help='Directory to save parcel geometries.',
                        type=str)  

    parser.add_argument('-f', '--data', 
                        help='Filepath to field data. Must include X and Y columns for spatial information (X,Y).',
                        type=str)  
    
    parser.add_argument('-d', '--kasviDict',
                        help='Filepath to Python dictionary, where plant IDs and names.',
                        type=str)  

    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)

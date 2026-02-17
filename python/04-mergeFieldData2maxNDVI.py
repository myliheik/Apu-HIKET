"""
2026-02-16 MY

Join maxNDVI data to field data (mittalohkot).

RUN:

python 04-mergeFieldData2maxNDVI.py

"""

import pandas as pd
import geopandas as gpd
import os 
import glob

# EDIT paths if needed:

fp = '/scratch/project_2008047/Apu-HIKET/results'
parcelpath = '/scratch/project_2008047/Apu-HIKET/shpfiles'

maxNDVIpaths = glob.glob(fp + '/maxNDVI*.csv')
fieldDataPaths = glob.glob(parcelpath + '/fieldData*.shp')
fieldDataPaths2 = glob.glob(parcelpath + '/fieldData*.csv')

for year in range(2018, 2026):
    print(year)
    dataPath = [filename for filename in maxNDVIpaths if str(year) in filename]
    fieldDataPath = [filename for filename in fieldDataPaths if str(year) in filename]
    fieldDataPath2 = [filename for filename in fieldDataPaths2 if str(year) in filename]
    gdf = gpd.read_file(fieldDataPath[0])
    df = pd.read_csv(dataPath[0])
    df2 = pd.read_csv(fieldDataPath2[0])
    merged = gdf.merge(df, how = 'left', on = 'parcelID')
    merged2 = merged.merge(df2, how = 'left', on = 'parcelID')
    
    print(f'Saving file to: {os.path.join(fp, os.path.basename(fieldDataPath[0]).replace('field', 'maxNDVI-field').replace('shp', 'gpkg'))}')
    merged2.to_file(os.path.join(fp, os.path.basename(fieldDataPath[0]).replace('field', 'maxNDVI-field').replace('shp', 'gpkg')))

print('Done')

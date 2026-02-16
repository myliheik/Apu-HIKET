"""
2026-02-16 MY

Read intermediate pkl files and join data by years.

python 03-maxFromDailyNDVI.py -i /scratch/project_2008047/Apu-HIKET/results_temp

"""

import pandas as pd
import os
from pathlib import Path
import pickle
import glob
from datetime import datetime
import argparse
import textwrap


def load_intensities(filename):
    with open(filename, "rb") as f:
        data = pickle.load(f)
    return data


def readTempResults(fpin, outputpath):
    metafiles = glob.glob(os.path.join(fpin, 'meta*'))

    for year in range(2018, 2026):
        mymetafiles = [metafile for metafile in metafiles if str(year) in metafile]

        if mymetafiles:
            outputfilename = os.path.join(outputpath, 'maxNDVI-' + str(year) + '.csv')
            
            mydata = []
            for filepath in mymetafiles:
        
                # Read in meta data:
                data = load_intensities(filepath)
                df = pd.DataFrame(data, columns = ['parcelID', 'Value', 'featureName', 'timeID'])
        
                #ndvi file:
                filepathndvi = filepath.replace('meta', 'ndvi')
                data2 = load_intensities(filepathndvi)
                df2 = pd.DataFrame(data2, columns = ['parcelID', 'Value', 'featureName', 'timeID'])        
        
                mydata.append(pd.concat([df, df2], axis = 0))
    
    
            _calculateMaxNDVI(mydata, str(year), outputfilename)

def _calculateMaxNDVI(listofdfs, year, outputfilename):
    df = pd.concat(listofdfs, ignore_index=True)
    dfmeta = df[df['featureName'] == 'meta']
    dfndvi = df[df['featureName'] == 'ndvi']
    print(f'NDVI: {len(dfndvi)}, DOY: {len(dfmeta)}')    
    ndvimax = dfndvi.groupby(['parcelID']).max().reset_index()
    print(f'NDVI max: {len(ndvimax)}')
    merge1 = ndvimax.merge(dfmeta, how = "left", on = ['parcelID', 'timeID'])
    merge1.rename(columns = {'Value_x': 'maxNDVI', 'Value_y': 'DOY'}, inplace = True)
    print(f'Merged: {len(merge1)}')
    merge1['Date'] = pd.to_datetime(merge1['DOY'], unit='D', origin = pd.Timestamp(year)).dt.strftime("%Y-%m-%d")
    merge1['cropID'] = merge1['parcelID'].str.split('_', expand  = True)[3]
    
    merge1[['parcelID', 'cropID', 'maxNDVI', 'DOY', 'Date']].to_csv(outputfilename, index = None)

            
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputpath:
            raise Exception('Missing input filepath argument. Try --help .')

        print(f'\n03-maxFromDailyNDVI.py')
        print(f'\nIntermediate results in: {args.inputpath}')
        
        # directory for output:
        outputpath = os.path.join(os.path.dirname(args.inputpath), 'results')
        Path(outputpath).mkdir(parents=True, exist_ok=True)

        readTempResults(args.inputpath, outputpath)

        print(f'Results save into {outputpath}')
        print('Done.')
    except Exception as e:
        print('\n\nUnable to read input. Check prerequisites and see exception output below.')
        parser.print_help()
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent(__doc__))
    parser.add_argument('-i', '--inputpath',
                        help='Directory path intermediate results (.pkl).',
                        type=str)  
    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)


"""
2026-02-16 MY

Read s2ind images and save to pkl files.

python 02-readS2ind.py -i /scratch/project_2008047/Apu-HIKET/s2ind -p /scratch/project_2008047/Apu-HIKET/shpfiles

"""

import geopandas as gpd
import os
from pathlib import Path
from rasterstats import zonal_stats
import pickle

import argparse
import textwrap


def save_intensities(filename, arrayvalues):
    with open(filename, 'wb+') as outputfile:
        pickle.dump(arrayvalues, outputfile)

def readS2ind(fpin, parcelpath, outputpath):
    filepaths = os.listdir(fpin)
    
    for filepath in filepaths:
        myarrays = []
        filepath2 = fpin + '/' + filepath
        year = filepath[-12:-8]
        timestamp = '_'.join(filepath.split('_')[4:6]).replace('.tif', '')
        parcelshp = os.path.join(parcelpath, 'parcels-' + year + '.shp')
        #print(parcelshp)
        # Check that shpfile exists:
        if os.path.isfile(parcelshp):
            feature = filepath.split('_')[3]
            outputfilename = os.path.join(outputpath, feature + '-' + year + '-' + timestamp + '.pkl')
            
            if 'meta' in filepath:  
                statistics = 'majority'
            else:
                statistics = 'mean'
    
            results = zonal_stats(parcelshp, filepath2, stats = statistics, geojson_out=True, all_touched = False, 
                              raster_out = False)            
    
            for x in results:
                # if not None:
                if x['properties'][statistics]:
                    featureValue = int(x['properties'][statistics])
                    myid = [x['properties']['parcelID'], featureValue, feature, timestamp]
                    myarrays.append(myid)
                    #print(myarrays)
                else:
                    continue
                
                
    
        else:
            continue
            
        save_intensities(outputfilename, myarrays)            
            
# HERE STARTS MAIN:

def main(args):
    try:
        if not args.inputpath:
            raise Exception('Missing input filepath argument. Try --help .')

        print(f'\n02-readS2ind.py')
        print(f'\nLPIS data in: {args.inputpath}')
        print(f'Parcel data in: {args.parcelpath}')
        
        # directory for output:
        outputpath = os.path.join(os.path.dirname(args.parcelpath), 'results_temp')
        Path(outputpath).mkdir(parents=True, exist_ok=True)

        readS2ind(args.inputpath, args.parcelpath, outputpath)

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
                        help='Directory path s2ind images.',
                        type=str)  
    parser.add_argument('-p', '--parcelpath',
                        help='Directory path to parcel data.',
                        type=str)  
    parser.add_argument('--debug',
                        help='Verbose output for debugging.',
                        action='store_true')

    args = parser.parse_args()
    main(args)






        
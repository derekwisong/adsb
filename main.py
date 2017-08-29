import geo
import logging
import argparse
import zipfile
import pandas as pd
import json
import io


# create a logging instance
log = logging.getLogger(__name__)


def command_line_args(description):
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('file', type=str, help="file to load")
    parser.add_argument('--starttime', type=str, dest='starttime',
                        default='19:00',
                        help="time to start the search in UTC")
    parser.add_argument('--endtime', type=str, dest='endtime', default='20:00',
                        help="time to end the search in UTC")
    parser.add_argument('--lat', type=float, dest='lat', default=41.6798157,
                        help="latitude in degrees to center the search")
    parser.add_argument('--lon', type=float, dest='lon', default=-74.1504231,
                        help="longitude in degrees to center to search")
    parser.add_argument('--radius', type=float, dest='radius', default=5.0,
                        help="radius in miles to look for records")
    parser.add_argument('--outfile', type=str, dest='outfile',
                        default="aircraft.pickle",
                        help="aircraft output file")
    args = parser.parse_args()
    return args


def extract_data(zipped_data, filename):
    f = zipped_data.read(filename).decode('utf-8')
    return f


def calculate_distance(row, args):
    distance = geo.haversine(args.lat, args.lon, row['Lat'], row['Long'])
    return distance

def find_aircraft(t, zipped_data, index, args):
    filename = t.filename[index]
    data = json.loads(extract_data(zipped_data, filename))
    aircraft = pd.DataFrame(data['acList'])
    aircraft['distance'] = aircraft.apply(calculate_distance, args=(args,),
                                          axis=1)
    cond = (aircraft['distance'] > 0) & (aircraft['distance'] <= args.radius)
    aircraft = aircraft[cond]
    timestamp = t['timestamp'][index]
    aircraft['timestamp'] = timestamp
    log.info("Found {} aircraft at {}".format(len(aircraft), timestamp))
    return aircraft

if __name__ == '__main__':
    args = command_line_args("ADSB Data Search Tool")
    logging.basicConfig(level=logging.INFO)

    z = zipfile.ZipFile(args.file)

    t = pd.DataFrame(pd.Series(z.namelist(), name='filename'))
    t['timestamp'] =  t.filename.apply(lambda filename: pd.Timestamp(filename.split('.')[0]).time())

    # filter to the time period
    start = pd.Timestamp(args.starttime).time()
    end = pd.Timestamp(args.endtime).time()

    t = t[(t.timestamp >= start) & (t.timestamp <= end)]

    results = []
    aircraft = pd.concat([find_aircraft(t,z,index,args) for index in t.filename.index])

    if args.outfile is not None:
        log.info("Saving pickled aircraft dataframe to {}".format(args.outfile))
        aircraft.to_pickle(args.outfile)

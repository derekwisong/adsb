import geo
import adsbexchange

import logging
import argparse
import pandas as pd
import multiprocessing
import functools

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
    parser.add_argument('--lat', type=float, dest='lat', default=41.6667600,
                        help="latitude in degrees to center the search")
    parser.add_argument('--lon', type=float, dest='lon', default=-74.1495897,
                        help="longitude in degrees to center to search")
    parser.add_argument('--radius', type=float, dest='radius', default=5.0,
                        help="radius in miles to look for records")
    parser.add_argument('--pool', type=int, dest='pool', default=10,
                        help="Pool size for multiprocessing")
    parser.add_argument('--outfile', type=str, dest='outfile',
                        default="aircraft.pickle",
                        help="aircraft output file")
    args = parser.parse_args()
    return args




def calculate_distance(row, args):
    distance = geo.haversine(args.lat, args.lon, row['Lat'], row['Long'])
    return distance



def build_aircraft_table(data):
    aircraft = pd.DataFrame(data['acList'])
    return aircraft


def find_aircraft(t, zip_filepath, args, index):
    """
    Given 
    """
    filename = t.filename[index]
    data = adsbexchange.parse_data(zip_filepath, filename)
    aircraft = build_aircraft_table(data)

    # calculate distance from the target
    aircraft['distance'] = aircraft.apply(calculate_distance, args=(args,),
                                          axis=1)
    timestamp = t['timestamp'][index]
    aircraft['timestamp'] = timestamp

    if args.radius > 0:
        cond = (aircraft['distance'].abs() >=0) & (aircraft['distance'].abs() <= args.radius)
        aircraft = aircraft[cond]

    log.info("Found {} aircraft at {}".format(len(aircraft), timestamp))
    return aircraft


if __name__ == '__main__':
    args = command_line_args("ADSB Data Search Tool")
    logging.basicConfig(level=logging.INFO)

    file_list = adsbexchange.get_file_list(args.file)

    t = pd.DataFrame(pd.Series(file_list, name='filename'))
    t['timestamp'] =  t.filename.apply(lambda filename: pd.Timestamp(filename.split('.')[0]).time())

    # filter to the time period
    start = pd.Timestamp(args.starttime).time()
    end = pd.Timestamp(args.endtime).time()
    t = t[(t.timestamp >= start) & (t.timestamp <= end)]

    # create a projection of the find aircraft function
    find_func = functools.partial(find_aircraft, t, args.file, args)

    # find aircraft matching args filters in the data files
    pool = multiprocessing.Pool(args.pool)
    results = pool.map(find_func, t.filename.index)
    aircraft = pd.concat(results)

    # if desired, save results to file
    if args.outfile is not None:
        log.info("Saving pickled aircraft dataframe to {}".format(args.outfile))
        aircraft.to_pickle(args.outfile)

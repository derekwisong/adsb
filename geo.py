#!/usr/bin/env python

import math

def haversine_distance(lat1_deg, lon1_deg, lat2_deg, lon2_deg, conversion=3959):
    """
    Calculate haversine distance using great-circle approximation.
    lat1, lon2, lat2, lon2 are all expected to be given in degrees
    """
    # convert degrees to radians
    lat1_rad = math.radians(lat1_deg)
    lon1_rad = math.radians(lon1_deg)
    lat2_rad = math.radians(lat2_deg)
    lon2_rad = math.radians(lon2_deg)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    haversine = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
    haversine = 2 * math.asin(math.sqrt(haversine)) * conversion

    return haversine


if __name__ == '__main__':
    print "Test distance between New York, NY and Los Angeles, CA"

    ny_lat = 40.7142700
    ny_lon = -74.0059700

    la_lat = 34.0522300
    la_lon = -118.2436800

    distance = haversine_distance(ny_lat, ny_lon, la_lat, la_lon)

    print "Distance between New York, NY and Los Angeles, CA is {} miles".format(distance)

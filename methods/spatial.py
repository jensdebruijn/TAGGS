from geopy.distance import great_circle


def distance_coords(coord1, coord2):
    return great_circle(coord1[::-1], coord2[::-1]).meters

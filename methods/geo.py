import shapely.wkt
import shapely.geometry as sgeom
import geojson
import json
import numpy as np
import pdb
import sys
from matplotlib.path import Path
from matplotlib.collections import PathCollection


def PolygonCodes(shape):
    def coding(ob):
        n = len(getattr(ob, 'coords', None) or ob)
        vals = np.ones(n, dtype=Path.code_type) * Path.LINETO
        vals[0] = Path.MOVETO
        return vals

    vertices = np.concatenate(
                            [np.asarray(shape.exterior)] +
                            [np.asarray(r) for r in shape.interiors] + [np.zeros((1, 2))])
    codes = np.concatenate(
                    [coding(shape.exterior)] +
                    [coding(r) for r in shape.interiors] + [np.array([Path.CLOSEPOLY])])
    return codes, vertices


def PolygonPath(shape):
    """Constructs a compound matplotlib path from a Shapely or GeoJSON-like
    geometric object"""

    if isinstance(shape, (sgeom.LineString, sgeom.Point)):
        return Path(np.vstack(shape.xy).T)

    elif isinstance(shape, sgeom.Polygon):
        codes, vertices = PolygonCodes(shape)
        return Path(vertices, codes)

    elif isinstance(shape, (sgeom.MultiPolygon,
                            sgeom.MultiLineString, sgeom.MultiPoint)):
        codes, vertices = [], []
        for poly in shape:
            sub_codes, sub_vertices = PolygonCodes(poly)
            codes.append(sub_codes)
            vertices.append(sub_vertices)
        codes = np.concatenate(codes)
        vertices = np.concatenate(vertices)
        return Path(vertices, codes)

    elif isinstance(shape, sgeom.GeometryCollection):
        return PathCollection([PolygonPath(geom) for geom in shape.geoms])

    else:
        raise ValueError('Unsupported shape type {}.'.format(type(shape)))


def wkt_to_geom(wkt):
    g = shapely.wkt.loads(wkt)
    return sgeom.asShape(g)


def geojson_to_geom(geojson):
    return sgeom.shape(json.loads(geojson))


def relation_to_geom(ID):
    folder = 'output/osm'
    try:
        os.makedirs(folder)
    except OSError:
        pass
    file_name = os.path.join(folder, f'relation_{ID}.wkt')
    if not os.path.exists(file_name):
        url = f'http://polygons.openstreetmap.fr/get_wkt.py?id={ID}&params=0'
        wkt = requests.get(url).content.decode("utf-8")
        with open(file_name, 'w') as f:
            f.write(wkt)
    else:
        with open(file_name, 'r') as f:
            wkt = f.read()
    wkt = wkt.split(';')[1]
    geom = geo.wkt_to_geom(wkt)

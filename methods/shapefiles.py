#!/usr/bin/python3
from osgeo import ogr
import os
import numpy as np
import matplotlib.path as mpath
from collections import defaultdict as dd
import shapefile as pyshp
import matplotlib.path
import matplotlib.patches as mpatches

from methods import function


def open_shape(shapefile):
    return pyshp.Reader(shapefile)


def open_shp(shapefile):
    return pyshp.Reader(shapefile)


def get_records(shapefile, index=False):
    if not isinstance(shapefile, pyshp.Reader):
        shapefile = open_shape(shapefile)

    if not index:
        index = 0
    else:
        index = get_field_names(shapefile).index(index)

    records = {
        r.record[index]: r.shape
        for r in shapefile.shapeRecords()
    }
    return records


def get_bboxes(records):
    return {
        key: shape.bbox
        for key, shape in records.items()
    }


def create_paths(region):
    return [matplotlib.path.Path(path) for path in [region.points[i:j] for i, j in zip(list(region.parts), list(region.parts)[1:]+[None])]]


def get_field_names(shapefile):
    fields = shapefile.fields[1:]
    field_names = [field[0] for field in fields]
    return field_names


def create_paths_shapefile_dict(shapefile, index):
    paths = {}
    ds = ogr.Open(shapefile)
    lyr = ds.GetLayer()
    for feat in lyr:
        key = feat.GetField(index)
        if key is None:
            continue
        key = key.strip()
        geom = feat.geometry()
        codes = []
        all_x = []
        all_y = []
        for i in range(geom.GetGeometryCount()):
            geometry = geom.GetGeometryRef(i)
            if geometry.GetGeometryName() == 'POLYGON':
                for j in range(geometry.GetGeometryCount()):
                    small_geom = geometry.GetGeometryRef(j)
                    x = [small_geom.GetX(j) for j in range(small_geom.GetPointCount())]
                    y = [small_geom.GetY(j) for j in range(small_geom.GetPointCount())]
                    codes += [mpath.Path.MOVETO] + (len(x)-1)*[mpath.Path.LINETO]
                    all_x += x
                    all_y += y
            else:
                x = [geometry.GetX(j) for j in range(geometry.GetPointCount())]
                y = [geometry.GetY(j) for j in range(geometry.GetPointCount())]
                codes += [mpath.Path.MOVETO] + (len(x)-1)*[mpath.Path.LINETO]
                all_x += x
                all_y += y
        path = mpath.Path(np.column_stack((all_x, all_y)), codes)
        paths[key] = path
    return paths


def create_paths_shapefile(shapefile):
    paths = []
    for r in shapefile.shapeRecords():
        paths.extend(create_paths(r.shape))
    return paths


def create_patches(shapefile, facecolor, alpha):
    patches = []
    for path in create_paths_shapefile(shapefile):
        patch = mpatches.PathPatch(path, facecolor=facecolor, alpha=alpha, lw=2)
        patches.append(patch)
    return patches


def write_WGS84(name):
    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
    out_prj = name + '.prj'
    with open(out_prj, 'w') as prj:
        prj.write(epsg)


def points_to_shapefile(points, name, fields):
    w = pyshp.Writer(pyshp.POINT)
    for field in fields:
        w.field(*field)

    field_names = [field[0] for field in fields]
    for point in points:
        w.point(point['lon'], point['lat'])
        w.record(*(point[field] for field in field_names))

    w.save(name)
    write_WGS84(name)


def shapefile_to_wkt(shapefile, index, force_multipolygon=False):
    shapefile = ogr.Open(shapefile)
    layer = shapefile.GetLayer(0)
    wkt = {}
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        field = feature.GetField(index)
        geometry = feature.GetGeometryRef()
        if force_multipolygon and geometry.GetGeometryType() == ogr.wkbPolygon:
            geometry = ogr.ForceToMultiPolygon(geometry)
        wkt[field] = geometry.ExportToWkt()
    return wkt


def merge_shapefile_by_id(infile, outfile, layer_name, ID):
    dirname = os.path.dirname(outfile)
    try:
        os.makedirs(dirname)
    except OSError:
        pass
    inshp = ogr.Open(infile)
    inLayer = inshp.GetLayer()

    driver = ogr.GetDriverByName("ESRI Shapefile")

    outshp = driver.CreateDataSource(outfile)
    outLayer = outshp.CreateLayer(layer_name, geom_type=ogr.wkbPolygon)

    idField = ogr.FieldDefn(ID, ogr.OFTInteger)
    outLayer.CreateField(idField)

    gn_ids = [abs(feature.GetField(ID)) for feature in inLayer]
    duplicate_ids = function.find_duplicates(gn_ids)

    duplicate_ids_features = dd(list)

    outLayerDefn = outLayer.GetLayerDefn()

    for i in range(inLayer.GetFeatureCount()):
        inFeature = inLayer.GetFeature(i)
        outFeature = ogr.Feature(outLayerDefn)

        gn_id = abs(inFeature.GetField(ID))
        if gn_id not in duplicate_ids:

            outFeature.SetField(ID, gn_id)

            geom = inFeature.GetGeometryRef()
            outFeature.SetGeometry(geom)

            outLayer.CreateFeature(outFeature)
            outFeature = None

        else:
            duplicate_ids_features[gn_id].append(i)

    for gn_id, features in duplicate_ids_features.items():
        geom = ogr.Geometry(ogr.wkbPolygon)
        for i in features:
            inFeature = inLayer.GetFeature(i)
            geom = geom.Union(inFeature.GetGeometryRef())

        outFeature = ogr.Feature(outLayerDefn)
        outFeature.SetField(ID, gn_id)
        outFeature.SetGeometry(geom)
        outLayer.CreateFeature(outFeature)
        outFeature = None

    inshp = None
    outshp = None


def get_unique_records(shp, ID):
    inShp = ogr.Open(shp)
    inLayer = inShp.GetLayer()
    return set(feature.GetField(ID) for feature in inLayer)

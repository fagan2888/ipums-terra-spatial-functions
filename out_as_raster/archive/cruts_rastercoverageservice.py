# -*- coding: utf-8 -*-
"""
Created on Fri Dec 02 15:02:14 2016
This script is used for transforming the netcdf files into tiffs
@author: dahaynes
"""

import os
import numpy as np
from osgeo import gdal, ogr, osr
import requests, psycopg2
from psycopg2 import extras


def GetGeometryExtent(cur, sgl_id):
    query = '''With terrapop_geography as
          (
          SELECT sgl.id as sample_geog_level_id, ST_Extent(bound.geom)::text as extent_text
          FROM sample_geog_levels sgl
          inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
          inner join boundaries bound on bound.geog_instance_id = gi.id
          WHERE sgl.id = %s
          GROUP BY sgl.id
          ), sample_extent AS
          (
          SELECT sample_geog_level_id, replace(replace(replace(replace(extent_text, 'BOX', ''), '(', ','), ' ', ','), ')', '') as extent
          FROM terrapop_geography
          )
          SELECT split_part(extent, ',', 2) as min_x, split_part(extent, ',', 3) as min_y, split_part(extent, ',', 4) as max_x, split_part(extent, ',', 5) as max_y
          from sample_extent''' % (sgl_id)
          
    cur.execute(query)
    results = cur.fetchone()

    min_x, min_y, max_x, max_y = results

    return float(min_x), float(min_y), float(max_x), float(max_y)
    
def GetGeometry(cur, sgl_id):
    query = ''' SELECT b.id as id, b.description, ST_AsText(b.geom)::text as geometry
          FROM sample_geog_levels sgl
          inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
          inner join boundaries b on b.geog_instance_id = gi.id
          WHERE sgl.id = %s ''' % (sgl_id)
          
    cur.execute(query)
    results = cur.fetchall()

    return results
    

def postgis_layer_to_shapefile(results, geoproj, shape_location):
    '''This function returns a geometry layer using ogr'''

    driver = ogr.GetDriverByName('ESRI Shapefile')
    
    if os.path.exists(shape_location): 
        driver.DeleteDataSource(shape_location)
        postGISGeometry = driver.CreateDataSource(shape_location)
    else:
        postGISGeometry = driver.CreateDataSource(shape_location)
    
        
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(geoproj)

    layer = postGISGeometry.CreateLayer(shape_location, srs, geom_type=ogr.wkbMultiPolygon)

    fields = ["fid", "geom_id"]
    for field in fields:
        newfield = ogr.FieldDefn(field, ogr.OFTInteger)
        layer.CreateField(newfield)

    #Create string field for name
    newfield = ogr.FieldDefn("name", ogr.OFTString)
    layer.CreateField(newfield)

    for r, rec in enumerate(results):
        feature = ogr.Feature(layer.GetLayerDefn())
        polygon = ogr.CreateGeometryFromWkt(rec['geometry'])
        feature.SetGeometry(polygon)
        feature.SetField("fid", r)
        feature.SetField("geom_id", rec['id'])
        feature.SetField("name", rec['description'])
        layer.CreateFeature(feature)
        feature.Destroy()

    postGISGeometry.Destroy()

def DescribeCoverage(coverage_name):
    coverage_url = 'http://geoserver3.pop.umn.edu:8080/geoserver/wcs?service=WCS&request=DescribeCoverage&version=1.0.0&coverage=%s' % ('Haynes:tmn')
    headers = {'Content-type':'text/xml'}
    resp = requests.get(coverage_url, headers=headers)
    
    return resp
    
def GetCoverage(coverageName, maxX, maxY, minX, minY, width, height,imageType,userDate, outTiff):
    '''This function forms the correct url for accessing the geoserver WebCoverage Service '''    
    ###Keep as reference
    #coverage_url = 'http://geoserver3.pop.umn.edu:8080/geoserver/wcs?service=WCS&request=GetCoverage&version=1.0.0&coverage=Haynes:tmn&BBOX=-82.0,-4.0,-67.0,13.0&CRS=EPSG:4326&Width=30&height=34&format=geotiff'
    ######                     
    
    coverage_url = 'http://geoserver3.pop.umn.edu:8080/geoserver/wcs?service=WCS&request=GetCoverage&version=1.0.0&coverage=%s&BBOX=%s,%s,%s,%s&Width=%s&Height=%s&CRS=EPSG:4326&TIME=%s&format=%s' % (coverageName,maxX,maxY,minX,minY,width,height,userDate,imageType)    
    #print coverage_url
    resp = requests.get(coverage_url)
    if resp.status_code == 200:
        with open(outTiff, 'wb') as image:
            for block in resp.iter_content(1024):
                image.write(block)       
    return resp

def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    
    return (pixel, line)
  
def Pixel2world(geoMatrix, row, col):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the x,y location of a pixel location
    """
    
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    x_coord = (ulX + (row * xDist))
    y_coord = (ulY - (col * xDist))
    
    return (x_coord, y_coord)
    
def RasterizePolygon(inRasterPath, vector_path, outRasterPath):
    '''This function takes the postgis geometry and rasterizes using the reference raster resolution, clipped the vector extent '''
    #The array size, sets the raster size 
    inRaster = gdal.Open(inRasterPath)
    
    #Open the vector dataset
    vector_dataset = ogr.Open(vector_path)
    layer = vector_dataset.GetLayer()

    #Masked Raster of the WebCoverageService
    tiffDriver = gdal.GetDriverByName('GTiff')
    theRast = tiffDriver.Create(outRasterPath, inRaster.RasterXSize, inRaster.RasterYSize, 1, gdal.GDT_Float64)

    os.chmod(outRasterPath, 0777)

    theRast.SetProjection(inRaster.GetProjection())
    theRast.SetGeoTransform(inRaster.GetGeoTransform())
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)

    #Rasterize
    gdal.RasterizeLayer(theRast, [1], layer, burn_values=[1])


def WriteMaskedWCS(maskedImagePath, wcsImagePath, outImagePath):
    '''This function reads in the boundary mask and the wcs service data and outputs the new array as tiff '''
    maskedRaster = gdal.Open(maskedImagePath)
    maskedArray = maskedRaster.ReadAsArray()
    wcsRaster = gdal.Open(wcsImagePath)
    wcsArray = wcsRaster.ReadAsArray()
    
    
    height, width = maskedArray.shape
    maskedWCSArray = np.empty((height, width), dtype=np.float64)
    
    np_it = np.nditer([wcsArray, maskedArray], flags=['multi_index'], op_flags =['readonly'])

    for v in np_it:
        x, y = np_it.multi_index
        if v[1] == 1:
            
            maskedWCSArray[x,y] = v[0]
        else:
            maskedWCSArray[x,y] = -999
    
    tiffDriver = gdal.GetDriverByName('GTiff')
    theRast = tiffDriver.Create(outImagePath, wcsRaster.RasterXSize, wcsRaster.RasterYSize, 1, gdal.GDT_Float64)
    os.chmod(outImagePath, 0777)

    theRast.SetProjection(wcsRaster.GetProjection())
    theRast.SetGeoTransform(wcsRaster.GetGeoTransform())
    
    band = theRast.GetRasterBand(1)
    band.SetNoDataValue(-999)
    
    band.WriteArray(maskedWCSArray)
    
    del theRast
#t = DescribeCoverage('something')
  
cruts_metadata = [-180, .5,-179.75, 90, 89.75, -.5 ]

host = 'terrapop-internal-db.pop.umn.edu'
db = 'terrapop_v1_94_11282016_demo_green'
port = '5433'
user = 'dahaynes'
sgl_id = 616
aPath = r"c:\work\columbia_june_2010"
aDate = '2010-6-16'

tmpWCSPath = r"%s\%s" % (aPath, "tmpWCS.tiff")
outCRUTSBoundary = r"%s\%s" % (aPath, "rasterizeBoundary.tiff")
outMaskedWCS = r"%s\%s"  % (aPath, "crutsWCS.tiff")

outShapeFilePath = r"%s\%s" % (aPath, "sampleBoundary.shp")
rasterCoverageName = 'Haynes:tmn'
outImageType = 'geotiff'

if not os.path.exists(aPath):
    os.makedirs(aPath)

connection = psycopg2.connect(host=host, database=db, port=port, user=user)
cursor = connection.cursor(cursor_factory=extras.DictCursor)

geomMax_X, geomMin_Y, geomMin_X, geomMax_Y = GetGeometryExtent(cursor, sgl_id)
#print(geomMin_X, geomMin_Y, geomMax_X, geomMax_Y)

ulX, ulY = world2Pixel(cruts_metadata, geomMax_X, geomMax_Y )
lrX, lrY = world2Pixel(cruts_metadata, geomMin_X, geomMin_Y )

newlrX = lrX +1
newlrY = lrY +1 

imageWidth = abs(int(newlrX - ulX))
imageHeight = abs(int(ulY - newlrY))

coordBottomRight = Pixel2world(cruts_metadata, ulX, ulY)
coordTopLeft = Pixel2world(cruts_metadata, newlrX, newlrY)

#print (coordTopLeft, coordBottomRight)

GetCoverage(rasterCoverageName, coordBottomRight[0], coordTopLeft[1], coordTopLeft[0], coordBottomRight[1], imageWidth, imageHeight,outImageType, aDate, tmpWCSPath)
postGISGeom = GetGeometry(cursor, sgl_id)
postgis_layer_to_shapefile(postGISGeom, 4326, outShapeFilePath)
#
RasterizePolygon(tmpWCSPath, outShapeFilePath, outCRUTSBoundary)
#
WriteMaskedWCS(outCRUTSBoundary, tmpWCSPath, outMaskedWCS)

connection.close()
cursor.close()
print "Done"
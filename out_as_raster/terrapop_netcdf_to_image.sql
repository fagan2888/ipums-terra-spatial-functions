/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrapop_netcdf_to_image(bigint, bigint, text, text);

CREATE OR REPLACE FUNCTION terrapop_netcdf_to_image( sample_geog_level_id bigint, raster_variable_id bigint, raster_date text, datapath text)
RETURNS table (nodata_value numeric, srid integer, final_image boolean, image_path text, data_type text) AS


    $BODY$

    import numpy as np
    from osgeo import ogr, osr, gdal
    import os, requests

    def GetGeometryExtent(sgl_id):
        '''This function returns the extent (minx, miny, maxx, maxy) for the sample_geog_level '''

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
        FROM sample_extent''' % (sgl_id)

        results = plpy.execute(query)

        min_x = results[0]['min_x']
        min_y = results[0]['min_y']
        max_x = results[0]['max_x']
        max_y = results[0]['max_y']

        plpy.notice(results[0])

        return float(min_x), float(min_y), float(max_x), float(max_y)

    def GetGeometry(sgl_id):
        ''' Returns the geometry as well known text'''
        query = ''' SELECT b.id as id, b.description, ST_AsText(b.geom)::text as geometry
        FROM sample_geog_levels sgl
        inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
        inner join boundaries b on b.geog_instance_id = gi.id
        WHERE sgl.id = %s ''' % (sgl_id)

        results = plpy.execute(query)

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

    def GetCoverage(coverageName, maxX, maxY, minX, minY, width, height,imageType,userDate, outTiff):
        '''This function forms the correct url for accessing the geoserver WebCoverage Service '''
        ### Keep the url as reference for what is should look like
        #coverage_url = 'http://geoserver3.pop.umn.edu:8080/geoserver/wcs?service=WCS&request=GetCoverage&version=1.0.0&coverage=Haynes:tmn&BBOX=-82.0,-4.0,-67.0,13.0&CRS=EPSG:4326&Width=30&height=34&format=geotiff'
        ######
        coverage_url = 'http://geoserver3.pop.umn.edu:8080/geoserver/wcs?service=WCS&request=GetCoverage&version=1.0.0&coverage=%s&BBOX=%s,%s,%s,%s&Width=%s&Height=%s&CRS=EPSG:4326&TIME=%s&format=%s' % (coverageName,maxX,maxY,minX,minY,width,height,userDate,imageType)
        plpy.notice(coverage_url) # print coverage_url
        resp = requests.get(coverage_url)
        if resp.status_code == 200:
            with open(outTiff, 'wb') as image:
                for block in resp.iter_content(1024):
                    image.write(block)

            os.chmod(outTiff, 0777)
        
        return resp

    def GetCoverageName(raster_variable_id):
        query = 'Select id, mnemonic from raster_variables where id = %s' % (raster_variable_id)
        results = plpy.execute(query)

        return results[0]['mnemonic']


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

    def CreateDataPackage(boundaryPath,maskedWCSPath ):
        package = []
        package.append([ -999, 4326, True, boundaryPath, 'boundary'])
        package.append([ -999, 4326, True, maskedWCSPath, 'data'])

        return package



    #Step 0 define output files
    tmpWCSPath = r"%s/%s" % (datapath, "tmpWCS.tiff")
    outCRUTSBoundary = r"%s/%s" % (datapath, "rasterizeBoundary.tiff")
    outShapeFilePath = r"%s/%s" % (datapath, "sampleBoundary.shp")
    outImageType = 'geotiff'

    rasterCoverageName = GetCoverageName(raster_variable_id)
    outMaskedWCS = r"%s/%s_%s.tiff"  % (datapath, rasterCoverageName, raster_date)
    #'2010-6-16' example
    plpy.notice(rasterCoverageName, raster_date)


    #Step 1 define CRU_TS metadata..  min_x, resolution_x, center_x, max_y, center_y, resolution_y
    cruts_metadata = [-180, .5,-179.75, 90, 89.75, -.5 ]

    geomMax_X, geomMin_Y, geomMin_X, geomMax_Y = GetGeometryExtent(sample_geog_level_id)

    ulX, ulY = world2Pixel(cruts_metadata, geomMax_X, geomMax_Y )
    lrX, lrY = world2Pixel(cruts_metadata, geomMin_X, geomMin_Y )

    #Step 2b this expands the bounding box. To adjust it remove the plus 1
    newlrX = lrX +1
    newlrY = lrY +1

    imageWidth = abs(int(newlrX - ulX))
    imageHeight = abs(int(ulY - newlrY))

    coordBottomRight = Pixel2world(cruts_metadata, ulX, ulY)
    coordTopLeft = Pixel2world(cruts_metadata, newlrX, newlrY)
    #print (coordTopLeft, coordBottomRight)

    #Step 3 Get the Coverage and Geometry and save as local files
    GetCoverage(rasterCoverageName, coordBottomRight[0], coordTopLeft[1], coordTopLeft[0], coordBottomRight[1], imageWidth, imageHeight, outImageType, raster_date, tmpWCSPath)
    postGISGeom = GetGeometry(sample_geog_level_id)
    postgis_layer_to_shapefile(postGISGeom, 4326, outShapeFilePath)

    #Step 4 Use the shapefile to create a mask of the country
    RasterizePolygon(tmpWCSPath, outShapeFilePath, outCRUTSBoundary)

    #Step 5 Write new tiff
    WriteMaskedWCS(outCRUTSBoundary, tmpWCSPath, outMaskedWCS)

    #Step 6, pack it up
    apackage = CreateDataPackage(outCRUTSBoundary,outMaskedWCS )
    #plpy.notice(apackage)

    return apackage

    $BODY$

LANGUAGE plpythonu VOLATILE;

-- SELECT * FROM terrapop_netcdf_to_image(30,1,'2001-6-16','/tmp')

--

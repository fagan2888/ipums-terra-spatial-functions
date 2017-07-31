/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_create_dense_cruts( sample_geog_level_id bigint, the_template_table text, out_cruts_table_name text, temp_path text) 
RETURNS TEXT AS

$$
import numpy as np
from osgeo import gdal, osr, ogr
import os


def get_sgl_geometry(sgl_id, shape_location):
    '''Get the Sample_geogl_level from postgis and create a shapefile '''
    query  = ''' SELECT bound.id as bound_id, bound.description, ST_AsText(bound.geog::geometry) as geometry
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id  = %s ''' % (sgl_id)

    results = plpy.execute(query)

    create_ogr_layer(results, shape_location, 6, 'postgis_layer', 'geog_id', 'bound_id')


def create_CRUTS_geometry(sgl_id, cruts_template_table, shape_location):
    query  = '''WITH country_boundary as
    (
    SELECT sgl.id as sample_geog_level_id, ST_Buffer(ST_Collect(ST_ConvexHull(bound.geog::geometry)),.5) as geom
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id  = %s
    GROUP BY sgl.id
    )
    SELECT c.pixel_id, ST_asText(ST_Centroid(c.geom)) as geometry
    FROM %s c inner join country_boundary b on ST_Intersects(c.geom, b.geom) ''' % (sgl_id, cruts_template_table)

    results = plpy.execute(query)   
    #Make a gdal_layer
    create_ogr_layer(results, shape_location, 1, 'cruts_centroids', 'cruts_id', 'pixel_id')

    return results
   

def CRUTSMetadata(sgl_id, cruts_template_table):
    '''This function will get the bounding box for CRUTS data set based on the sgl.id '''

    query  = '''WITH country_boundary as
    (
    SELECT sgl.id as sample_geog_level_id, ST_Buffer(ST_Collect(ST_ConvexHull(bound.geog::geometry)),.5) as geom
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id  = %s
    GROUP BY sgl.id
    )
    SELECT ST_AsText(ST_Envelope(ST_Collect(c.geom))) as boundingbox
    FROM %s c inner join country_boundary b on ST_Within(c.geom, b.geom) ''' % (sgl_id, cruts_template_table)

    results = plpy.execute(query)

    data = results[0]['boundingbox']
    bbox = data[data.find('((')+2:-2].split(',')

    #Get the polygon extents
    extent_w, extent_n = bbox[0].split(' ')
    extent_e, extent_s = bbox[2].split(' ')

    #Modified the return extents so that it would be 1 CRUTS pixel size larger.
    return [float(extent_w)-.5, float(extent_n)-.5, float(extent_e)+.5, float(extent_s)+.5]


def create_ogr_layer(results, shape_location, ogr_geomtry_type, layername, dataset_id_name, sql_dataset_id):
    '''This function returns a geometry layer using ogr'''
    #Create Shapefile   
    driver = ogr.GetDriverByName('ESRI Shapefile')
    postGISGeometry = driver.CreateDataSource(shape_location)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    layer = postGISGeometry.CreateLayer(layername, srs, geom_type=ogr_geomtry_type)

    fields = ["fid", dataset_id_name]

    for field in fields:
        newfield = ogr.FieldDefn(field, ogr.OFTInteger)
        layer.CreateField(newfield)       

    for r, rec in enumerate(results):
        feature = ogr.Feature(layer.GetLayerDefn())
        polygon = ogr.CreateGeometryFromWkt(rec['geometry'])
        feature.SetGeometry(polygon)
        feature.SetField("fid", r)
        feature.SetField(dataset_id_name, rec[sql_dataset_id])
        layer.CreateFeature(feature)
        feature.Destroy()
            
    postGISGeometry.Destroy()


def world2Pixel(x, y, geoTrans):
      """ Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate the pixel location of a geospatial coordinate  """
  
      ulX = geoTrans[0]
      ulY = geoTrans[3]
      xDist = geoTrans[1]
      yDist = geoTrans[5]
      rtnX = geoTrans[2]
      rtnY = geoTrans[4]
      pixel = int((x - ulX) / xDist)
      line = int((ulY - y) / xDist)
      return (pixel, line)
  
def DetermineProjection(bbox, cruts_transform ):

    minX, minY, maxX, maxY = bbox
    ulX, ulY = world2Pixel(minX, maxY, cruts_transform)
    lrX, lrY = world2Pixel(maxX, minY, cruts_transform)

    # Calculate the pixel size of the new image
    pxWidth = int(lrX - ulX)
    pxHeight = int(lrY - ulY)

    return pxWidth, pxHeight


def vector_cleanup(shapefile_location):
    '''This function will delete a shapefile '''

    if os.path.exists(shapefile_location):
        driver = ogr.GetDriverByName('ESRI Shapefile')
        driver.DeleteDataSource(shapefile_location)

def rasterize_polygon(template_array,transformation, vector_path, proj, attribute_column, img_name):
    '''This function takes the postgis geometry and rasterizes using the reference raster resolution, clipped the vector extent '''

    #The array size, sets the raster size        
    height, width = template_array.shape

    #Open the vector dataset
    vector_dataset = ogr.Open(vector_path)
    layer = vector_dataset.GetLayer()

    #Create Raster with given raster projection information
    imagepath = r'%s/%s' % ('/tmp', img_name)
    rDriver = gdal.GetDriverByName('GTiff')
    rRast = rDriver.Create(imagepath, width, height, 1, 6)

    os.chmod(imagepath, 0777)
    rRast.SetProjection(proj.ExportToWkt())

    #Transformation should be original or densified            
    rRast.SetGeoTransform(transformation)
    band = rRast.GetRasterBand(1)
    band.SetNoDataValue(-999)

    #Rasterize
    vector_attribute = "ATTRIBUTE=%s" % (attribute_column)
    gdal.RasterizeLayer(rRast, [1], layer, options = [vector_attribute])

    #Get Data as Numpy Array
    vector_as_array = rRast.GetRasterBand(1).ReadAsArray()

    return vector_as_array


def raster_densification(oldarray,densifier):
    '''This function will takes integer and a numpy array and returns a densified array'''    

    denseGrid = []
    #read the array by the axis 1
    for row in oldarray:           
    #Repeat the array
        denseArray = np.repeat(row, densifier).tolist()            
        #denseArray = np.repeat(oldarray, densificationValue).tolist()            
        for i in range(densifier):
            denseGrid.append(denseArray)    
            denseRaster = np.array(denseGrid)
        
    return denseRaster

def raster_resolution(geoTransform, densification):
    '''Adjusts the spatial resoltuion (Pixel X & Y size)'''
    
    NewTransform = list(geoTransform)
    NewTransform[1] = NewTransform[1]/ densification
    NewTransform[5] = NewTransform[5]/ densification

    return NewTransform


def cruts_raster_densification(shapefilepath, cruts_array, geoTransform, geoproj, feature_count ):
    '''This function is a loop that determines how much the raster has to be densified to for small geographies '''
    boundary_lag_count = -1        
    num_boundary_ids = 0
    densifier = 1
    densifier_list = []
    #BlankRaster = generate_blank_raster(raster_metadata) 
    #geoTransform = raster_metadata["geoTransform"]

    cruts_geoids = np.delete(np.unique(cruts_array), 0)
    found_geometries = len(cruts_geoids)

    while found_geometries < feature_count and boundary_lag_count != num_boundary_ids:
    
        #plpy.notice(num_boundary_ids, feature_count, boundary_lag_count, num_boundary_ids)

        DenseValuesArray = raster_densification(cruts_array,densifier)
        NewTransform = raster_resolution(geoTransform, densifier)
        densifier_name = '%s.tiff' % (densifier ,)
    
        DenseVectorArray = rasterize_polygon(DenseValuesArray, NewTransform, shapefilepath, geoproj, 'geog_id', densifier_name)         
        unique_boundary_ids = np.delete(np.unique(DenseVectorArray),0)
        boundary_lag_count = num_boundary_ids
        num_boundary_ids = unique_boundary_ids.shape[0]
    
        found_geometries = len(unique_boundary_ids)    
        plpy.notice('%s geographic units of %s found in raster ' % (found_geometries, feature_count ))
        #print ('%s geographic units of %s found in raster ' % (len(unique_boundary_ids), feature_count ))
    
        if len(unique_boundary_ids) < feature_count and boundary_lag_count != num_boundary_ids: 
            #Now increment                
            densifier *= 2
            densifier_list.append(densifier)

    return densifier, densifier_list

def densifiction_grid_resolution(Transform):

    #### This is a function #####
    x = Transform[0]
    y = Transform[3]
    x_res = Transform[1]
    y_res = Transform[5]
    x_start =  x + x_res
    y_start =  y + y_res

    return x_start, y_start, x_res, y_res

def create_cruts_data_table(temp_table_name):
    try:    
        plpy.execute("Create TABLE %s (pixel_id bigint, x double precision, y double precision);" % (temp_table_name) )
    except plpy.SPIError, e:
        plpy.execute("DROP TABLE %s;" % (temp_table_name))
        plpy.execute("Create TABLE %s (pixel_id bigint, x double precision, y double precision);" % (temp_table_name) )

def create_cruts_dense_table(temp_table_name, cruts_table_name):
    plpy.execute("select pixel_id, ST_GeomFromText('POINT('||x||' '||y||')',4326) as geom, x, y into %s from %s;" % (cruts_table_name, temp_table_name))

    index_name = cruts_table_name.replace(".", "_")
    plpy.execute("CREATE INDEX %s_geom_gist ON %s USING gist(geom);" % (index_name.strip(), cruts_table_name))
    plpy.execute("CREATE INDEX %s_id ON %s USING btree(pixel_id);" % (index_name.strip(), cruts_table_name))      


def create_tiff(raster_file_path, transformation, width, height):

    tiffDriver = gdal.GetDriverByName('GTIFF')
    rDataset = tiffDriver.Create(rasterpath, width, abs(height), 1, gdal.GDT_Float32)
    rDataset.SetGeoTransform(transformation)

    #This sets the spatial reference, and we return it for all subsequent datasets.    
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    rDataset.SetProjection(srs.ExportToWkt())

    array = rDataset.ReadAsArray()

    return array, srs

def cleanup(temp_table_name, shp1, shp2):
  plpy.execute("DROP TABLE %s;" % (temp_table_name))
  vector_cleanup(shp1)
  vector_cleanup(shp2)

def share_shapefile(shapefiles):
    shapefile_extensions =['prj', 'shp', 'dbf','shx']
    for s in shapefiles:
        basepath = s.split('.')[0]
        for ext in shapefile_extensions:
            shapepath = r'%s.%s' % (basepath, ext)
            os.chmod(shapepath, 0777)

def populate_CRUTS_table(outfilepath, pixel_data_array, x_start, y_start, x_res, y_res, cruts_table_name):
    outfilepath = r'/tmp/cruts_dense2.csv'
    it = np.nditer(pixel_data_array, flags=['multi_index'])
    with open(outfilepath, 'w') as f:
        while not it.finished:
            pixel_id = it.value.tolist()
            h, w = it.multi_index
            centroid_x = start_x + (x_resolution*w)
            centroid_y = start_y + (y_resolution*h)
            outtext =  '%s,%s,%s\n' % (int(pixel_id), centroid_x, centroid_y )

            f.write(outtext)
            it.iternext()
        
    copy_statement = "COPY %s FROM '%s' DELIMITER ',' " % (cruts_table_name, outfilepath)
    plpy.execute(copy_statement)


#Set up variables      
cruts_centroids_shape = r'%s/%s' % (temp_path, 'cruts_centroids.shp')
layer_shapepath = r'%s/%s' % (temp_path, 'cruts_boundary.shp')
rasterpath = r'%s/%s' % (temp_path, 'cruts_test1.tif')
temp_txt_file_path = r'%s/%s' % (temp_path, 'CRUTS.csv')
temp_table = 'climate.cruts_dense_xx'

vector_cleanup(cruts_centroids_shape)
vector_cleanup(layer_shapepath)

#Generating CRUTS Metadata
CRUTS_BBox = CRUTSMetadata(sample_geog_level_id, the_template_table)
CRUTS_TRANS = [CRUTS_BBox[0], .5, 0, CRUTS_BBox[3], 0, -.5]
rWidth, rHeight = DetermineProjection(CRUTS_BBox, CRUTS_TRANS )

#Create CRUTS ARRAY of initial cell size
rArray, srs, = create_tiff(rasterpath, CRUTS_TRANS, rWidth, rHeight)

create_CRUTS_geometry(sample_geog_level_id, the_template_table, cruts_centroids_shape)
get_sgl_geometry(sample_geog_level_id, layer_shapepath)

#Unnecessary
share_shapefile([cruts_centroids_shape, layer_shapepath])

###Rasterize Boundaries, get unique features/ geographies
CRUTS_Boundary_Array = rasterize_polygon(rArray,CRUTS_TRANS, layer_shapepath, srs, 'geog_id', 'CRUTS_Boundary_IDS.tif')
CRUTS_PixelID_Array = rasterize_polygon(rArray,CRUTS_TRANS, cruts_centroids_shape, srs, 'cruts_id', 'CRUTS_Pixel_IDS.tif')
CRUTS_GeoIDS = np.delete(np.unique(CRUTS_Boundary_Array), 0)

vDataset = ogr.Open(layer_shapepath)
vLayer = vDataset.GetLayer()
num_features = vLayer.GetFeatureCount()
if len(CRUTS_GeoIDS) != num_features:
    #Densify
    plpy.notice('DENSIFY')
    densifier_value, dense_list = cruts_raster_densification(layer_shapepath, CRUTS_Boundary_Array, CRUTS_TRANS, srs, num_features )
    plpy.notice('Densifyication complete, CRUTS was densified %s times and needed a maximum densification of %s' % (len(dense_list), densifier_value))
    CRUTS_DENSE_PixelArray = raster_densification(CRUTS_PixelID_Array,densifier_value)
    CRUTS_DENSE_Transform = raster_resolution(CRUTS_TRANS, densifier_value)

    start_x, start_y, x_resolution, y_resolution = densifiction_grid_resolution(CRUTS_DENSE_Transform)
    create_cruts_data_table(temp_table)
    populate_CRUTS_table(temp_txt_file_path, CRUTS_DENSE_PixelArray, start_x, start_y, x_resolution, y_resolution, temp_table)
    plpy.notice('Created Densified CRUTS Table')
else:
  start_x, start_y, x_resolution, y_resolution = densifiction_grid_resolution(CRUTS_TRANS)
  create_cruts_data_table(temp_table)
  populate_CRUTS_table(temp_txt_file_path, CRUTS_PixelID_Array, start_x, start_y, x_resolution, y_resolution, temp_table)

create_cruts_dense_table(temp_table, out_cruts_table_name)
plpy.notice('Generating new CRUTS Template data table %s' % (out_cruts_table_name))

vDataset.Destroy()

cleanup(temp_table, cruts_centroids_shape, layer_shapepath)

return out_cruts_table_name

$$
LANGUAGE plpythonu VOLATILE;
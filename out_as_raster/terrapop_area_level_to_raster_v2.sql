/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP Function terrapop_area_level_to_raster(bigint, bigint, bigint, bigint, text);

CREATE OR REPLACE FUNCTION terrapop_area_level_to_raster_v2( sample_geog_level_id bigint, area_data_id bigint, raster_variable_id bigint, raster_band bigint, datapath text) 
RETURNS table (nodata_value numeric, densifier integer, srid integer, final_image boolean, image_path text, data_type text) AS
	$BODY$   

	import numpy as np
	from osgeo import ogr, osr
	from osgeo import gdal
	import os


	def raster_metadata(raster_variable_id):
		'''Returns raster data tables, which contains values_table, area_reference_table, second_area_reference_table '''

		query  = "select id, mnemonic, area_reference_id, second_area_reference_id, variable_type_description as mnemonic_type from rasters_metadata_view where id = %s" % (raster_variable_id)

		results = plpy.execute(query)

		plpy.notice(query)

		raster_data_type = results[0]['mnemonic_type'].lower()
		raster_mnemonic = results[0]['mnemonic']
		#print 'Generating Raster dataset for %s' % (raster_data_type)

		## This creates a dictionary of all potential Raster Data Tables        
		RasterVariables = [raster_variable_id, results[0]['area_reference_id'], results[0]['second_area_reference_id']]          
		raster_tables = ['values_table', 'area_reference_table', 'second_area_reference_table']
		raster_data_tables = {}

		for count, r in enumerate(RasterVariables):            
			if r:
				query = '''SELECT schema || '.' || tablename as tablename FROM rasters_metadata_view WHERE id = %s;''' % (r)
				results = plpy.execute(query)

			#Build the dictionary, with the key coming from raster_tables, and value being a list [raster_table name, rasterband]
			raster_data_tables[raster_tables[count]] = [results[0]['tablename'],raster_band]

		#Manipulating raster_data_tables if the type is binary
		if raster_data_type == 'binary':            
			raster_data_tables['values_table'] = raster_data_tables['second_area_reference_table']
			raster_data_tables.pop('second_area_reference_table')

		return raster_data_tables

	def raster_array_metadata(sample_geog_level_id, raster_data_tables, raster_band ):
		'''This function will get the initial array metadata '''    

		### This will fail for big geographies need a work around, need upper leftx,y and number of rows/columns

		query = ''' WITH boundaries as
		(
		SELECT sgl.id as sample_geog_level_id, bound.geog::geometry as geometry
		FROM sample_geog_levels sgl inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
		inner join boundaries bound on bound.geog_instance_id = gi.id
		WHERE sgl.id = %s
		), projection as
		(
		SELECT ST_SRID(rast) as srid
		FROM %s
		Limit 1
		), clip as
		(
		SELECT boundaries.sample_geog_level_id, ST_Union(ST_Clip(r.rast, %s, ST_Transform(boundaries.geometry, prj.srid), -999.0, True)) as rast
		FROM projection prj, boundaries inner join %s r on ST_Intersects(r.rast,boundaries.geometry)
		GROUP by sample_geog_level_id
		)
		SELECT (ST_Metadata(rast)).*
		from clip''' % (sample_geog_level_id, raster_data_tables['values_table'][0], raster_band, raster_data_tables['values_table'][0])


		results = plpy.execute(query)

		#Create a dictionary to store all of the metadata
		raster_metadata = {}
		raster_metadata["scalex"] = results[0]["scalex"]
		raster_metadata["scaley"] = results[0]["scaley"]
		raster_metadata["srid"] = results[0]["srid"]
		raster_metadata["width"] = results[0]["width"]        
		raster_metadata["height"] = results[0]["height"]
		raster_metadata["geoTransform"] = [ results[0]["upperleftx"], results[0]["scalex"], 0.0, results[0]["upperlefty"], 0.0, results[0]["scaley"] ]

		query  = "SELECT srtext, proj4text from spatial_ref_sys where srid = %s;" % (raster_metadata['srid'])
		results = plpy.execute(query)

		raster_metadata["geoProj"] = results[0]["srtext"]

		return raster_metadata

	def world2Pixel(geoTransform, x, y):
		""" Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate the pixel location of a geospatial coordinate """

		ulX = geoTransform[0]
		ulY = geoTransform[3]
		xDist = geoTransform[1]
		yDist = geoTransform[5]
		rtnX = geoTransform[2]
		rtnY = geoTransform[4]
		pixel = int((x - ulX) / xDist)
		line = int((ulY - y) / xDist)

		return (pixel, line)

	def Pixel2world(geoMatrix, row, col):
		"""  Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate the x,y location of a pixel location """

		ulX = geoMatrix[0]
		ulY = geoMatrix[3]
		xDist = geoMatrix[1]
		yDist = geoMatrix[5]
		rtnX = geoMatrix[2]
		rtnY = geoMatrix[4]
		x_coord = (ulX + (row * xDist))
		y_coord = (ulY - (col * xDist))

		return (x_coord, y_coord)


	def new_raster_array_metadata(raster_data_tables):
		''' New function to replace the old, does not use the ST_Union'''

		query = ''' 
		With raster_metadata as
		(
		select (ST_metadata(rast)).*
		from %s
		), raster_extent as
		(
		SELECT min(upperleftx) as minx, max(upperlefty) as maxy
		FROM raster_metadata
		)
		SELECT r.scalex, r.scaley, r.srid, re.minx, re.maxy
		FROM raster_metadata r, raster_extent re
		limit 1 ''' % (raster_data_tables['values_table'][0])

		results = plpy.execute(query)

		#Create a dictionary to store all of the metadata
		raster_metadata = {}
		raster_metadata["scalex"] = results[0]["scalex"]
		raster_metadata["scaley"] = results[0]["scaley"]
		raster_metadata["srid"] = results[0]["srid"]

		raster_metadata["geoTransform"] = [ results[0]["minx"], results[0]["scalex"], 0.0, results[0]["maxy"], 0.0, results[0]["scaley"] ]

		query  = "SELECT srtext, proj4text from spatial_ref_sys where srid = %s;" % (raster_metadata['srid'])
		results = plpy.execute(query)

		raster_metadata["geoProj"] = results[0]["srtext"]

		return raster_metadata

	def get_raster_extent(raster_md, shapeFilePath):
		'''Use alternative functions to define the extent of the raster '''

		vector = ogr.Open(shapeFilePath)
		theLayer = vector.GetLayer()
		geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()

		transform = raster_md["geoTransform"]

		ulX, ulY = world2Pixel(transform, geomMin_X, geomMax_Y )
		lrX, lrY = world2Pixel(transform, geomMax_X, geomMin_Y )

		imageWidth = abs(int(lrX - ulX))
		imageHeight = abs(int(ulY - lrY))


		raster_md["width"] = imageWidth
		raster_md["height"] = imageHeight

		coordTopLeft = Pixel2world(transform, ulX, ulY)
		#coordBottomRight = Pixel2world(transform, lrX, lrY)
		transform[0] = coordTopLeft[0]
		transform[3] = coordTopLeft[1]

		return imageWidth, imageHeight, transform

	def get_area_data_values(sample_geog_level_id, area_data_id, raster_data_tables):
		''' This function return the postgis geometry and attribute values to be burned'''

		query = '''SELECT adv.measurement_type_id 
			FROM area_data_values area_data inner join area_data_variables adv on area_data.area_data_variable_id = adv.id
			WHERE adv.id = %s
			Limit 1''' % (area_data_id)
		
		results = plpy.execute(query)

		variable_type = results[0]['measurement_type_id']

		query = '''SELECT mt.label FROM measurement_types AS mt WHERE id = %s''' % variable_type
		
		results = plpy.execute(query)
		
		variable_type = results[0]['label']

		if variable_type == 'Count':
			raster_data_tables
			query = '''
			WITH projection as
			(
			SELECT ST_SRID(rast) as srid
			FROM %s
			Limit 1
			), boundary as
			(
			SELECT bound.code::bigint as id, bound.description as name, ST_Transform(bound.geog::geometry, p.srid) as geometry
			FROM projection p, sample_geog_levels sgl inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
			inner join boundaries bound on bound.geog_instance_id = gi.id
			WHERE sgl.id = %s
			),clip as
			( 
			SELECT b.id, b.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, 1, b.geometry, True), 1, True, 1)).count as pixel_count
			FROM projection prj, boundary b inner join %s r on ST_Intersects(r.rast,b.geometry)
			GROUP by id, name
			), data_values as
			(
			SELECT bound.code::bigint as id, bound.description, adv.mnemonic, area_data.value, ST_AsText(bound.geog::geometry) as geometry
			,adv.measurement_type_id ,adv.label
			FROM sample_geog_levels sgl
			inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
			inner join boundaries bound on bound.geog_instance_id = gi.id
			inner join area_data_values area_data on area_data.geog_instance_id = gi.id
			inner join area_data_variables adv on area_data.area_data_variable_id = adv.id
			WHERE sgl.id = %s and adv.id = %s
			) 
			SELECT dv.id, dv.description, dv.mnemonic, c.pixel_count as cell_count, dv.value as original_value, dv.value/c.pixel_count::double precision as value, dv.geometry
			FROM clip c inner join data_values dv on c.id=dv.id''' % (raster_data_tables['values_table'][0], sample_geog_level_id, raster_data_tables['values_table'][0], sample_geog_level_id, area_data_id )
		else:
			query  = '''SELECT bound.code::bigint as id, bound.description, adv.mnemonic, area_data.value:: double precision as value, ST_AsText(bound.geog::geometry) as geometry
			FROM sample_geog_levels sgl
			inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
			inner join boundaries bound on bound.geog_instance_id = gi.id
			inner join area_data_values area_data on area_data.geog_instance_id = gi.id
			inner join area_data_variables adv on area_data.area_data_variable_id = adv.id
			WHERE sgl.id = %s and adv.id = %s ''' % (sample_geog_level_id, area_data_id )

		plpy.notice(query)
		results = plpy.execute(query)

		return results, variable_type

	def get_feature_count(sgl_id, area_id):

		query = '''SELECT bound.code::bigint as id, bound.description, adv.mnemonic, area_data.value::double precision as value, ST_AsText(bound.geog::geometry) as geometry
		FROM sample_geog_levels sgl
		inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
		inner join boundaries bound on bound.geog_instance_id = gi.id
		inner join area_data_values area_data on area_data.geog_instance_id = gi.id
		inner join area_data_variables adv on area_data.area_data_variable_id = adv.id
		WHERE sgl.id = %s and adv.id = %s '''  % (sgl_id, area_id )

		results = plpy.execute(query)    
		feature_count = len(results)
		#plpy.notice(feature_count)

		return feature_count, results

	def postgis_layer_to_shapefile(results, geoproj, shape_location):
		'''This function returns a geometry layer using ogr'''

		#This function will query the database and return the appropriate area data values
		#results = get_area_data_values(sample_geog_level_id, area_data_id, raster_data_tables)

		#Create Memory File
		driver = ogr.GetDriverByName('ESRI Shapefile')

		postGISGeometry = driver.CreateDataSource(shape_location)
		srs = osr.SpatialReference()
		srs.ImportFromWkt(geoproj)

		layer = postGISGeometry.CreateLayer('postgis_boundaries', srs, geom_type=ogr.wkbMultiPolygon)

		fields = ["fid", "geom_id"]
		for field in fields:
			newfield = ogr.FieldDefn(field, ogr.OFTInteger)
			layer.CreateField(newfield)

		#Create string field for name
		newfield = ogr.FieldDefn("name", ogr.OFTString)
		layer.CreateField(newfield)

		newfield = ogr.FieldDefn("value", ogr.OFTReal)
		layer.CreateField(newfield)

		for r, rec in enumerate(results):
			feature = ogr.Feature(layer.GetLayerDefn())
			polygon = ogr.CreateGeometryFromWkt(rec['geometry'])
			feature.SetGeometry(polygon)
			feature.SetField("fid", r)
			feature.SetField("geom_id", rec['id'])
			feature.SetField("name", rec['description'])
			feature.SetField("value", rec['value'])
			layer.CreateFeature(feature)
			feature.Destroy()

		reponse = 'PostGIS finds %s features that contain at least 1 raster pixel'  % (layer.GetFeatureCount())
		plpy.notice(reponse)

		postGISGeometry.Destroy()

	def raster_resolution(geoTransform, densification):
		'''Adjusts the spatial resoltuion (Pixel X & Y size)'''

		NewTransform = list(geoTransform)
		NewTransform[1] = NewTransform[1]/ densification
		NewTransform[5] = NewTransform[5]/ densification

		return NewTransform

	def raster_densification(oldarray,densifier):
		'''This function takesa numpy array of values and denseifier value and returns a densified array'''    

		denseGrid = []
		#read the array by the axis 1
		for row in oldarray:           
			#Repeat the array
			denseArray = np.repeat(row, densifier).tolist()
			for i in range(densifier):
				denseGrid.append(denseArray)    
		denseRaster = np.array(denseGrid)

		return denseRaster

	def generate_blank_raster(RasterMD):
		'''This function will generate a blank raster using the defined raster_geometric extent '''
		height = RasterMD["height"]
		width = RasterMD["width"]
		blank_raster = np.empty( (height, width), dtype=float)

		return blank_raster

	def get_boundary_ids(vector_path, theArray):
		'''This function determines the number of boundaries in the raster '''
		num_ids = 0
		vector_dataset = ogr.Open(vector_path)
		layer = vector_dataset.GetLayer()
		plpy.notice(layer.GetFeatureCount())
		for feature in layer:
			geom_id = int(feature.GetField("geom_id"))
			if geom_id in theArray: 
				num_ids +=1

		return num_ids


	def determine_raster_densification(shapefilepaths, raster_metadata, geoproj, feature_count, area_data_type ):
		'''This function is a loop that determines if the raster has to be densified to for small geographies '''
		boundary_lag_count = -1        
		num_boundary_ids = 0
		densifier = 1
		densifier_list = []
		densifier_list.append(densifier)
		BlankRaster = generate_blank_raster(raster_metadata)

		geoTransform = raster_metadata["geoTransform"]

		#Modified while statement so that is a heurestic.
		while num_boundary_ids < feature_count and boundary_lag_count != num_boundary_ids:

			plpy.notice(num_boundary_ids, feature_count, boundary_lag_count, num_boundary_ids)
			DenseValuesArray = raster_densification(BlankRaster,densifier)
			NewTransform = raster_resolution(geoTransform, densifier)

			if densifier == 1:
				DenseVectorArray = rasterize_polygon(DenseValuesArray, NewTransform, shapefilepaths[0], geoproj, 'geom_id', densifier)
				#the_shapepath = shapefilepaths[0]
			else:
				DenseVectorArray = rasterize_polygon(DenseValuesArray, NewTransform, shapefilepaths[1], geoproj, 'geom_id', densifier)
				#the_shapepath = shapefilepaths[1]
			
			#found_ids = get_boundary_ids(the_shapepath, DenseVectorArray)

			#plpy.notice(found_ids)

			unique_boundary_ids = np.delete(np.unique(DenseVectorArray),0)
			boundary_lag_count = num_boundary_ids
			num_boundary_ids = unique_boundary_ids.shape[0]

			plpy.notice('Densification at: %s finds %s geographic units of %s found in raster ' % (densifier, unique_boundary_ids.shape[0], feature_count ))

			if num_boundary_ids < feature_count and boundary_lag_count != num_boundary_ids: 
				#Now increment                
				densifier *= 2
				densifier_list.append(densifier)

		#Rasterize Vector Attribute
		#boundary_ids, boundary_cellcount = np.unique(DenseVectorArray, return_counts=True) #can't do this yet without numpy 1.9
		boundary_ids = np.unique(DenseVectorArray)
		boundary_ids = np.delete(boundary_ids, 0)

		if area_data_type == 'Count':
			#recalculate the population value. 
			boundary_cellcount = {}
			for b in boundary_ids:
				masked_values = np.ma.masked_where(DenseVectorArray != b, DenseVectorArray)
				boundary_cellcount[str(int(b))] = masked_values.count()

			#plpy.notice(boundary_cellcount)
			vector_dataset = ogr.Open(shapefilepaths[1], 1)
			layer = vector_dataset.GetLayer()
			for feature in layer:
				feature_geom_id = feature.GetField('geom_id')

				if boundary_cellcount.has_key(str(feature_geom_id)):
					orig_pop_value = feature.GetField('value')
					calculated_pop_value = feature.GetField('value') / boundary_cellcount[str(feature_geom_id)]
					feature.SetField('value', calculated_pop_value)                
					cell_count = boundary_cellcount[str(feature_geom_id)]
				else:
					#Because we have a heuristic and we won't densify until we run out of memory
					orig_pop_value = 'None'
					feature.SetField('value', -999)
					calculated_pop_value = -999
					cell_count = 0

				layer.SetFeature(feature)
				#plpy.notice(orig_pop_value, cell_count, calculated_pop_value, feature.GetField('value'))

			#Update changes
			vector_dataset.Destroy()

		rasterize_polygon(DenseValuesArray, NewTransform, shapefilepaths[1], geoproj, 'value', 'attribute')

		if densifier == 1:
			return [1]
		else:
			return densifier_list

	def rasterize_polygon(template_array,transformation, vector_path, proj, attribute_column, img_number):
		'''This function takes the postgis geometry and rasterizes using the reference raster resolution, clipped the vector extent '''
		#The array size, sets the raster size        
		width, height = template_array.shape

		#Open the vector dataset
		vector_dataset = ogr.Open(vector_path)
		layer = vector_dataset.GetLayer()

		#Create Memory Raster with given raster projection information
		#using a global variable here ##datapath## probably bad practice
		imagepath = r'%s/%s' % (datapath, img_number)
		tiffDriver = gdal.GetDriverByName('GTiff')
		theRast = tiffDriver.Create(imagepath, height, width, 1, 7)

		os.chmod(imagepath, 0777)

		theRast.SetProjection(proj)

		#Transformation should be original or densified            
		theRast.SetGeoTransform(transformation)
		band = theRast.GetRasterBand(1)
		band.SetNoDataValue(-999)

		#Rasterize
		vector_attribute = "ATTRIBUTE=%s" % (attribute_column)
		gdal.RasterizeLayer(theRast, [1], layer, options = [vector_attribute])

		#Get Data as Numpy Array
		vector_as_array = theRast.GetRasterBand(1).ReadAsArray()
		min,max,mean,stdev =band.GetStatistics(0,1)
		band.SetStatistics(min,max,mean,stdev) 

		return vector_as_array

	def vector_cleanup(shapefile_location):
		'''This function will delete a shapefile '''
		for i in shapefile_location:
			if os.path.exists(i):
				driver = ogr.GetDriverByName('ESRI Shapefile')
				driver.DeleteDataSource(i)

	#Step 0
	#datapath
	
	if not os.path.exists(datapath):
	  os.makedirs(datapath)
	
	shape_path = [r'%s/%s' %(datapath, r'areal_boundary_values.shp'), r'%s/%s' %(datapath, r'boundary.shp')]
	vector_cleanup(shape_path)

	#Step 1, use the raster metadata function to determine the raster table in the database from the raster variable. 
	datasets = raster_metadata(raster_variable_id)

	#Step 2, this function will return the raster metadata, using ST_Clip & ST_Union so it is potentially slow
	#metadata = raster_array_metadata(sample_geog_level_id, datasets, raster_band )
	#geo_transform = metadata["geoTransform"]

	metadata = new_raster_array_metadata(datasets)

	#Step 3, Create an ogr geometry object to be applied to the raster dataset
	query_results, area_data_type = get_area_data_values(sample_geog_level_id, area_data_id, datasets)

	postgis_layer_to_shapefile(query_results, metadata["geoProj"], shape_path[0])
	#Getting raster extent, needed for st_union
	rasterWidth, rasterHeight, rasterTransform = get_raster_extent(metadata, shape_path[0])
	rasterMetaDict= {"height": rasterHeight, "width": rasterWidth, "geoTransform" : rasterTransform }

	#Return the original number of features (geographic units) and original geometry, for densificiation
	num_features, original_geom_results = get_feature_count(sample_geog_level_id, area_data_id)

	postgis_layer_to_shapefile(original_geom_results, metadata["geoProj"], shape_path[1])

	#Step 4, Densify the raster using the heurestic and generate new tiff
	densifier_list = determine_raster_densification(shape_path, rasterMetaDict, metadata["geoProj"], num_features, area_data_type)

	#Step 5, Clean up. Remove all the temp files not necessary
	#vector_cleanup(shape_path)

	try:
		os.chmod(datapath, 0777)
	except:
		print "Unable to chmod '" + datapath + "'"
	
	#Step 6, Create a datapackage that 
	datapackage = []

	if len(densifier_list) > 1:
		#This packages data for the densified data
		for i in densifier_list:
			imagepath = r'%s/%s' % (datapath, i)
			if i == densifier_list[len(densifier_list)-1]:
				boolval = True
			else:
				boolval = False    
			datapackage.append([-999, i, metadata['srid'], boolval, imagepath, 'boundary_ids'])

		imagepath = r'%s/%s' % (datapath, 'attribute')
		datapackage.append([-999, i, metadata['srid'], True, imagepath, 'rasterization'])

	else:
		imagepath = r'%s/%s' % (datapath, '1')
		datapackage.append([-999, densifier_list[0], metadata['srid'], True, imagepath, 'boundary_ids'])
		imagepath = r'%s/%s' % (datapath, 'attribute')
		datapackage.append([-999, densifier_list[0], metadata['srid'], True, imagepath, 'rasterization'])
		#plpy.notice(datapackage)

	return datapackage
	$BODY$

LANGUAGE plpythonu VOLATILE;

  
  
-- SELECT * FROM terrapop_area_level_to_raster(277, 21, 1, 80, '/tmp/')
-- SELECT * FROM terrapop_area_level_to_raster_v2(236, 1, 30, 1, '/tmp/')

-- SELECT * FROM terrapop_area_level_to_raster(363, 1, 21, 1, '/tmp') -- 362
-- show log_destination;


-- select id, mnemonic, area_reference_id, second_area_reference_id, mnemonic_type from rasters_metadata_view where id = 30 -- 21
-- select id, mnemonic from raster_variables
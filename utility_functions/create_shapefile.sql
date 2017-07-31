/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/


CREATE OR REPLACE FUNCTION terrapop_postgis_to_shapefile( sample_geog_level_id bigint, shapefilepath text, shapefilename text) 
RETURNS text AS
    $BODY$ 

    from osgeo import ogr, osr
    import os

    
    def get_postgis_geometry(sgl_id):
        '''Query the database to return the geography information as text '''


        query = ''' SELECT gi.label as label, gi.code as geoid, ST_AsText(b.geom) as geom
        FROM sample_geog_levels sgl
        INNER JOIN geog_instances gi on sgl.id = gi.sample_geog_level_id
        INNER JOIN boundaries b on b.geog_instance_id = gi.id
        WHERE sgl.id = %s ''' % (sgl_id)

        plpy.notice(query)
        results = plpy.execute(query)

        return results

    def vector_cleanup(shapefile_location):
        '''This function will delete a shapefile '''
        
        if os.path.exists(shapefile_location):
            plpy.notice("shapefile exits")
            driver = ogr.GetDriverByName('ESRI Shapefile')
            driver.DeleteDataSource(shapefile_location)

    def get_boundary_projection(sgl_id):

        query  = ''' WITH boundary as
        (
        SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, 
        ST_AsText(bound.geog) as geom, ST_SRID(bound.geog::geometry) as srid
        FROM sample_geog_levels sgl
        inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
        inner join boundaries bound on bound.geog_instance_id = gi.id
        WHERE sgl.id = %s
        limit 1
        )
        SELECT srtext, proj4text 
        from spatial_ref_sys srs inner join boundary on srs.srid = boundary.srid ''' % (sgl_id)

        plpy.notice(query)
        results = plpy.execute(query)

        return results[0]["srtext"]

    def postgis_layer_to_shapefile(results, geoproj, shape_location):
            '''This function returns a geometry layer using ogr'''

            #This function will query the database and return the appropriate area data values
            #results = get_area_data_values(sample_geog_level_id, area_data_id, raster_data_tables)

            #Create Shapefile
            driver = ogr.GetDriverByName('ESRI Shapefile')

            postGISGeometry = driver.CreateDataSource(shape_location)
            srs = osr.SpatialReference()
            srs.ImportFromWkt(geoproj)

            layer = postGISGeometry.CreateLayer('postgis_boundaries', srs, geom_type=ogr.wkbMultiPolygon)

            fields = ["fid", "geoid"]
            for field in fields:
                newfield = ogr.FieldDefn(field, ogr.OFTInteger)
                layer.CreateField(newfield)

            #Create string field for name
            newfield = ogr.FieldDefn("label", ogr.OFTString)
            layer.CreateField(newfield)


            for r, rec in enumerate(results):
                feature = ogr.Feature(layer.GetLayerDefn())
                polygon = ogr.CreateGeometryFromWkt(rec['geom'])
                feature.SetGeometry(polygon)
                feature.SetField("fid", r)
                feature.SetField("geoid", rec['geoid'])
                feature.SetField("label", rec['label'])
                layer.CreateFeature(feature)
                feature.Destroy()

            reponse = 'PostGIS finds %s features that contain at least 1 raster pixel'  % (layer.GetFeatureCount())
            plpy.notice(reponse)

            postGISGeometry.Destroy()

    if not os.path.exists(shapefilepath):  
        os.makedirs(shapefilepath)

    os.chmod(shapefilepath, 0777)

    thepath = r"%s/%s.shp" % (shapefilepath, shapefilename)

    vector_cleanup(thepath)
        
    query_results = get_postgis_geometry(sample_geog_level_id)
    plpy.notice(thepath)
    prj4text = get_boundary_projection(sample_geog_level_id)
    postgis_layer_to_shapefile(query_results, prj4text, thepath)

    return thepath



    $BODY$

LANGUAGE plpythonu VOLATILE;
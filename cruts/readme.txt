Here is a brief overview of the functions within the CRU TS directory


The first step for conducting raster summarizations is getting or generating the raster template. The function terrapop_get_cruts_template, will returns a string that is the table name of the densified CRU TS region. This allows the function to have its own caching mechanism. 

1) Get CRU_TS Template     SELECT * FROM  terrapop_get_cruts_template(31, 1, '/tmp'); 
Sample_geog_level  --- 31 
Raster Variable id --- 1
directory workspace -- '/tmp'

This the main logic that of the function that might need to be altered in the future.

IF cruts_template.template_name IS NULL THEN
    cruts_table_name := format('climate.cruts_%s_%s', cruts_template.template_type, cruts_template.iso_code);
    cruts_template_name := format('climate.cruts_322_%s_template', cruts_template.template_type);
    
    RAISE NOTICE 'Creating new densified cruts template using %s at %s using sample_geog_id: %', cruts_template_name, cruts_table_name, most_geographies.id;

    PERFORM terrapop_create_dense_cruts( most_geographies.id, cruts_template_name, cruts_table_name, temp_path) ;
    
    EXECUTE format($$ UPDATE climate.cruts_322_countries SET %s = '%s' WHERE iso_code = '%s' $$, cruts_raster_variable, cruts_table_name, cruts_template.iso_code); 
ELSE
    RAISE NOTICE 'Raster template % exists', cruts_raster_variable;
    cruts_table_name := cruts_template.template_name;
END IF;

RETURN  cruts_table_name;

#########

The first function may call the following densifiation function, which is written in python. Lots of hard coded items in this function specifically for CRU_TS but you could adjust it

PERFORM terrapop_create_dense_cruts( most_geographies.id, cruts_template_name, cruts_table_name, temp_path) ;

This is the heuristic that is used in this function.

def cruts_raster_densification

while found_geometries < feature_count and boundary_lag_count != num_boundary_ids:
        
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

####### CRU TS data analyis is separated into two functions ######

SELECT * FROM terrapop_cruts_timepoint_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2010-06-01') ;
SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2000-01-16', '2005-01-16') ;

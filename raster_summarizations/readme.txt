This file provides a brief description of the raster summary functions currently utilized for TerraPop.


Each of the raster summary functions that utilize rasters stored within PostgreSQL utilize two functions.
The outer function, is the procedure that is called by Ruby directly and is called terrapop_<rastersummarizationtype>_summarization
The inner function, is called directly by the outerfunction and is named _tp_<rastersummmarizationtype>_summarization

The outer function must return the same field names that ruby expects.



This is the logic that is used for determining the raster summarization. Essentially each raster variable type is a type summarization. 

if mnemonic[datatype] == 'categorical': #1
    if mnemonic[srid] == 4326:
        terrapop_wgs84_categorical_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
        RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, mode double precision, num_categories bigint)

        	Underlying Function  _tp_wgs84_categorical_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint)
    else:
        terrapop_modis_categorical_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint)
        RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, mode double precision, num_categories bigint)

        	Underlying Function _tp_categorical_modis_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint) 

elif mnemonic[datatype] == 'binary':  #5
    if mnemonic[srid] == 4326:
        terrapop_wgs84_categorical_binary_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint)
        RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, percent_area double precision, total_area double precision)

        	Underlying Function _tp_wgs84_categorical_binary_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint) 
    else:
        terrapop_MODIS_categorical_binary_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
        RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, min double precision, max double precision, mean double precision, count bigint )
        	Underlying Function _tp_MODIS_categorical_binary_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint)

elif mnemonic[datatype] == 'continous': #2
    #All Continous (WorldClim) and GLI Yield
    terrapop_continuous_summarization( sample_geog_level_id bigint, raster_variable_id bigint) 
    RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, min double precision, max double precision, mean double precision, count bigint )

    	Underlying Function _tp_continuous_summarization( sample_table_name text, raster_variable_id bigint) 

elif mnemonic[datatype] == 'context_area_prop': #3
    #Harvested area
    terrapop_gli_harvested_summarization( sample_geog_level_id bigint, raster_variable_id bigint) 
    RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, percent_area double precision, total_area double precision)

    	Underlying Function _tp_gli_harvested_summarization( sample_table_name text, raster_variable_id bigint) 

elif mnemonic[datatype] == 'cont_ext_arearef': #4
    #All Area Reference Rasters
    terrapop_area_reference_summarization( sample_geog_level_id bigint, raster_variable_id bigint) 
    RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, total_area double precision)

    	Underlying Function _tp_area_reference_summarization( sample_table_name text, raster_variable_id bigint) 


elif menmonic[datatype] == 'cruts_netcdf': #7
	terrapop_cruts_data_sgl_check(sgl, raster_id)
	terrapop_cruts_data_time_point_analysis(sgl, raster_id, time)
	terrapop_cruts_data_time_interval_analysis(sgl, raster_id, begin_time, end_time)
    RETURNS TABLE (geog_id bigint, place_name text, place_code bigint, min double precision, max double precision, mean double precision, count bigint )





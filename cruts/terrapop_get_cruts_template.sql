/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_get_cruts_template(sample_geog_level_id bigint, raster_var_id bigint, temp_path text) 
RETURNS Text AS

$BODY$


DECLARE 
	cruts_variable_array text := '';
	query_string text := '';
	cruts_template RECORD;
	most_geographies RECORD;
	cruts_raster_variable text := '';
	cruts_table_name text := '';
	cruts_template_name text := '';

BEGIN

	-- Determine the sample geog level with the most geographies
	WITH cntry_table as
	(
	SELECT c.id, c.full_name as country, c.short_name as iso_code
	FROM sample_geog_levels sgl
	inner join country_levels cl on sgl.country_level_id = cl.id
	inner join countries c on cl.country_id = c.id
	WHERE sgl.id = sample_geog_level_id
	), all_geog_levels as
	(
	SELECT sgl.id
	FROM sample_geog_levels sgl
	inner join country_levels cl on sgl.country_level_id = cl.id
	inner join countries c on cl.country_id = c.id
	inner join cntry_table ct on c.id = ct.id
	)
	SELECT sgl.id, count(sgl.label) as num_units
	INTO most_geographies
	FROM sample_geog_levels sgl
	inner join all_geog_levels agl on sgl.id = agl.id
	inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
	inner join boundaries b on gi.id = b.geog_instance_id
	group by sgl.id
	order by 2 desc
	limit 1;

	
	-- Determine the cruts template column from the RasterVariable---
	SELECT netcdf_template as template
	INTO cruts_raster_variable
	FROM raster_variables rv
	WHERE rv.id = raster_var_id;

/*	SELECT split_part('climate.cruts_all_template', '.', 2) as template
	INTO cruts_raster_variable;
*/

	RAISE NOTICE '%', cruts_raster_variable;

	query_string := $$
		WITH country_data AS
		(
		SELECT c.id, c.full_name as country, c.short_name as iso_code
		FROM sample_geog_levels sgl
		inner join country_levels cl on sgl.country_level_id = cl.id
		inner join countries c on cl.country_id = c.id
		WHERE sgl.id = $$ || sample_geog_level_id ||$$
		)
		SELECT cd.id, cd.country, cd.iso_code, ccc.$$ ||cruts_raster_variable|| $$ as template_name,
		split_part( 'ccc.$$ ||cruts_raster_variable|| $$', '_', 3) as template_type
		FROM climate.cruts_322_countries ccc		
		INNER JOIN country_data cd on ccc.country_id = cd.id $$;

		RAISE NOTICE '%', query_string;

	EXECUTE query_string INTO cruts_template ;

	-- RAISE NOTICE '%', query_string;

	RAISE NOTICE '%', cruts_template.template_name;

	IF cruts_template.template_name IS NULL THEN
		
		cruts_table_name := format('climate.cruts_%s_%s', cruts_template.template_type, cruts_template.iso_code);
		cruts_template_name := format('climate.cruts_322_%s_template', cruts_template.template_type);
		
		RAISE NOTICE 'Creating new densified cruts template using %s at %s using sample_geog_id: %', cruts_template_name, cruts_table_name, most_geographies.id;

		PERFORM terrapop_create_dense_cruts( most_geographies.id, cruts_template_name, cruts_table_name, temp_path) ;
		
		--EXECUTE format($$ INSERT INTO climate.cruts_countries  (id, country, iso_code, template, %S) 
		--VALUES 
		--(cruts_template.id, cruts_template.country, cruts_template.iso_code, True, cruts_table_name) 
		--$$, cruts_raster_variable );

 		EXECUTE format($$ UPDATE climate.cruts_322_countries SET %s = '%s' WHERE iso_code = '%s' $$, cruts_raster_variable, cruts_table_name, cruts_template.iso_code);	

	ELSE
		RAISE NOTICE 'Raster template % exists', cruts_raster_variable;
		cruts_table_name := cruts_template.template_name;
	
	END IF;
	
	RETURN  cruts_table_name;
	
END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

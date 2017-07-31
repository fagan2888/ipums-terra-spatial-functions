/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_cruts_analysis(sample_geog_level_id bigint, cruts_dense_table text) 
RETURNS Text AS

$BODY$


DECLARE 
	cruts_variable_array text := '';
	query text := '';

BEGIN
	
	WITH climate_variable as 
	(
	SELECT array_agg('c.'|| variable_field_name) as climate_variables
	FROM climate.climate_variables
	where id in (1)
	)
	SELECT replace(replace(climate_variables::text, '{', ''), '}', '')
	INTO cruts_variable_array
	FROM climate_variable;

	RAISE NOTICE '%', cruts_variable_array;

	DROP TABLE IF EXISTS climate.cruts_user_summarization;


	query  := $$
	CREATE TABLE climate.cruts_user_summarization AS
	WITH cruts_dense AS
	(
	SELECT pixel_id, geom 
	FROM terrapop_extract_dense_cruts_data( $$ || sample_geog_level_id || $$, '/tmp') 
	), geographic_boundary as
	(
	SELECT bound.id as place_id, bound.description as place, bound.geom
	FROM sample_geog_levels sgl
	inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
	inner join boundaries bound on bound.geog_instance_id = gi.id
	WHERE sgl.id  = $$ || sample_geog_level_id || $$
	), geographic_cruts as
	(
	SELECT g.place_id, g.place, c.pixel_id, c.geom-- , row_number() over() as geo_id
	from geographic_boundary g inner join cruts_dense c on ST_Within(c.geom, g.geom)
	), cruts_temporal as
	(
	SELECT c.pixel_id, $$ || cruts_variable_array || $$
	FROM climate.cruts_322 c 
	WHERE c.month in (1) and c.year in (2000)
	)
	SELECT gc.place_id, gc.place, avg(ct.pre) as pre
	FROM cruts_temporal ct inner join geographic_cruts gc on ct.pixel_id = gc.pixel_id
	GROUP BY gc.place_id, gc.place, month $$;

	RAISE NOTICE  ' % ', query;

	execute query;

    RETURN 'SELECT * FROM climate.cruts_user_summarization';

END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

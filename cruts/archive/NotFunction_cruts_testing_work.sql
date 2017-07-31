/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

select * from raster_timepoints
select * from climate.cruts_countries where cruts_all_template <> ''

SELECT split_part('climate.cruts_322_all_template', '.', 2) as template
-- INTO cruts_raster_variable
FROM 



WITH country_data AS
(
SELECT c.id, c.full_name as country, c.short_name as iso_code
FROM sample_geog_levels sgl
inner join country_levels cl on sgl.country_level_id = cl.id
inner join countries c on cl.country_id = c.id
WHERE sgl.id = 31
)
SELECT cd.id, cd.country, cd.iso_code, split_part('climate.cruts_all_template', '.', 2) as template_name
FROM climate.cruts_countries ccc 
inner join country_data cd on ccc.country_id = cd.id

UPDATE SET = WHERE

SELECT * FROM  terrapop_get_cruts_template(31, 1, '/tmp/'); 
SELECT * FROM terrapop_cruts_timepoint_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2010-06-01') ;
-- 5.9 sec
SELECT * FROM terrapop_cruts_timepoint_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '06/01/2010') ;
-- 5.6 sec
SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2000-01-16', '2005-01-16') ;
-- 4 minutes 3 seconds
SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '01/16/2000', '01/16/2005') ;
-- 4 minutes 2 seconds

SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2000-01-01', '2005-02-05') ;
-- 4 minutes 7 seconds
SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '01/01/2000', '02/05/2005') ;
-- 4 minutes 8 seconds


SELECT * FROM terrapop_cruts_timeinterval_analysis(31, 1, 'climate.cruts_all_af', 'climate.cruts_322', '2000-01-01', '2003-02-05') ;

WITH cruts_template AS
(
SELECT terrapop_get_cruts_template as template_name
FROM  terrapop_get_cruts_template(75, 1, '/tmp') 
)
SELECT (terrapop_cruts_timepoint_analysis(75, 1, template_name, 'climate.cruts_322', '01/01/2000')).*
FROM cruts_template 


WITH cruts_template AS
(
SELECT terrapop_get_cruts_template as template_name
FROM  terrapop_get_cruts_template(31, 1, '/tmp/') 
)
SELECT (terrapop_cruts_timeinterval_analysis(31, 1, template_name, 'climate.cruts_322', '01/01/2000', '01/08/2005')).*
FROM cruts_template 


drop table if exists climate.cruts_322_pet_template ;
UPDATE climate.cruts_countries SET cruts_all_template = '' WHERE iso_code = 'af'

-- WITH country_data AS
-- (
-- SELECT c.id, c.full_name as country, c.short_name as iso_code
-- FROM sample_geog_levels sgl
-- inner join country_levels cl on sgl.country_level_id = cl.id
-- inner join countries c on cl.country_id = c.id
-- WHERE sgl.id = 31
-- )
-- SELECT cd.id, cd.country, cd.iso_code, cruts_all_template as template_name
-- FROM climate.cruts_countries ccc 
-- inner join country_data cd on ccc.country_id = cd.id


SELECT bound.id as place_id, bound.description as place, bound.geom
	FROM sample_geog_levels sgl
	inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
	inner join boundaries bound on bound.geog_instance_id = gi.id
	WHERE sgl.id  = 31

select * from climate.cruts_322_all_template limit 10
		WITH country_data AS
		(
		SELECT c.id, c.full_name as country, c.short_name as iso_code
		FROM sample_geog_levels sgl
		inner join country_levels cl on sgl.country_level_id = cl.id
		inner join countries c on cl.country_id = c.id
		WHERE sgl.id = 31
		)
		SELECT cd.id, cd.country, cd.iso_code, ccc.cruts_all_template as template_name,
		split_part( 'ccc.cruts_all_template', '_', 2) as template_type
		FROM climate.cruts_countries ccc
-- 		INTO cruts_template 
		inner join country_data cd on ccc.country_id = cd.id


WITH country_boundary as
(
SELECT sgl.id as sample_geog_level_id, ST_Buffer(ST_Collect(ST_ConvexHull(bound.geog::geometry)),.5) as geom
FROM sample_geog_levels sgl
inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
inner join boundaries bound on bound.geog_instance_id = gi.id
WHERE sgl.id  = 31
GROUP BY sgl.id
)
SELECT c.pixel_id, ST_asText(ST_Centroid(c.geom)) as geometry
FROM climate.cruts_grid_template c inner join country_boundary b on ST_Intersects(c.geom, b.geom)
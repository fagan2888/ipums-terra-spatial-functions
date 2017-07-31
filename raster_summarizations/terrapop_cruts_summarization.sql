/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/


CREATE TABLE climate.user_cruts_dense AS
SELECT pixel_id, geom 
FROM terrapop_extract_dense_cruts_data(312 , '/tmp'); 

CREATE INDEX climate_user_cruts_dense_geom_gist ON climate.user_cruts_dense USING gist(geom);


With geographic_boundary as
(
SELECT bound.id as place_id, bound.description as place, bound.geom
FROM sample_geog_levels sgl
inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
inner join boundaries bound on bound.geog_instance_id = gi.id
WHERE sgl.id  = 312
), geographic_cruts as
(
SELECT g.place_id, g.place, c.pixel_id, c.geom 
from geographic_boundary g inner join climate.user_cruts_dense c on ST_Within(c.geom, g.geom)
), cruts_temporal as
(
SELECT c.pixel_id, c.pre
FROM climate.cruts_322 c 
WHERE c.month in (1) and c.year in (2000,2001)
)
SELECT gc.place_id, gc.place, avg(ct.pre) as pre
FROM cruts_temporal ct inner join geographic_cruts gc on ct.pixel_id = gc.pixel_id
GROUP BY gc.place_id, gc.place;

-- DROP INDEX climate_user_cruts_dense_geom_gist;
-- DROP TABLE climate.user_cruts_dense;
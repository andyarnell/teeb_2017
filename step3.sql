
                        --set path for sql processing to act on tables in a specific schema within the database (normally defaults to public otherwise)
SET search_path=teeb_2017,lvb_wkshp1,public,topology;

--if postgis/postgresql running locally on desktop increase access to memory (RAM) 
SET work_mem TO 120000;
SET maintenance_work_mem TO 120000;
SET client_min_messages TO DEBUG;


--importance script which calculates importance for subsets: i)  all species ii) IUCN threatened (CR EN VU)  species, iii) mammals, iv) birds, and v) amphibians 
DROP TABLE IF EXISTS out_sppimp_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_sppimp_10km_teeb_s1_2050_cons_allsp AS 
SELECT /*osrd.species,*/ osrd.id_no, osrd.cell_id, osrd.cell_sp,
--CASE WHEN osra.sumofcell_suitarea_max_s1_2050_cons=0 
--THEN 0 ELSE ((osrd.cell_suitarea_max_s1_2050_cons/osra.sumofcell_suitarea_max_s1_2050_cons)*(osrun.sumofarea/spo.eoo_area)) END 
--AS sppimp_max, 
CASE WHEN osra.sumofcell_suitarea_eq_s1_2050_cons=0 
THEN 0 ELSE ((osrd.cell_suitarea_eq_s1_2050_cons/osra.sumofcell_suitarea_eq_s1_2050_cons)*(osrun.sumofarea/spo.eoo_area)) END 
AS sppimp_eq
--,CASE WHEN osra.sumofcell_suitarea_min_s1_2050_cons=0 
--THEN 0 ELSE ((osrd.cell_suitarea_min_s1_2050_cons/osra.sumofcell_suitarea_min_s1_2050_cons)*(osrun.sumofarea/spo.eoo_area)) END 
--AS sppimp_min
FROM
raw.species_eoo_gridbscale AS spo,
out_spp_calc_areacells_10km_teeb_s1_2050_cons AS osrd,
out_spp_allsuitarearegion_10km_teeb_s1_2050_cons AS osra,
out_spp_allarearegion_10km_teeb AS osrun
, /*statusandtaxonomy AS st*/ 
(
select distinct id_no, class as class_name, category as code
from
raw.species_eoo_gridbscale
) as st
WHERE
/*osrd.species = osra.species AND
osrun.species = osrd.species AND
spo.species = osrun.species AND
lower(st.friendly_name) = lower(osrd.species)*/
osrd.id_no = osra.id_no AND
osrun.id_no = osrd.id_no AND
spo.id_no = osrun.id_no AND
st.id_no = osrd.id_no
AND lower(st.class_name) in ('amphibia','aves','mammalia'); 
--view subset of result to check it worked - 
-- SELECT * FROM out_sppimp_10km_teeb_s1_2050_cons_allsp ORDER BY cell_sp LIMIT 1000;


--see how many species have values 
-- SELECT COUNT(1) from (select /*species as species*/ id_no from out_sppimp_10km_teeb_s1_2050_cons_allsp group by id_no/*species*/) as foo;


--should be same as the count on the input table out_spp_calc_areacells_10km_teeb_s1_2050_cons_allsp
-- SELECT count(1) from (select /*species as species*/ id_no from out_spp_calc_areacells_10km_teeb_s1_2050_cons_allsp group by id_no/*species*/) as foo;

 
--see if null rows are null in all columns
-- SELECT * FROM out_sppimp_10km_teeb_s1_2050_cons_allsp WHERE sppimp_eq IS NULL ORDER BY cell_sp;


--make final importance values for watersheds by grouping by watershed ids
DROP TABLE IF EXISTS out_cell_imp_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_imp_10km_teeb_s1_2050_cons_allsp AS
SELECT o.cell_id, SUM(o.sppimp_eq) AS cellimp_eq
--,SUM(o.sppimp_max) AS cellimp_max, SUM(o.sppimp_min) AS cellimp_min 
FROM out_sppimp_10km_teeb_s1_2050_cons_allsp as o
GROUP BY o.cell_id;


--join final results to watershed polygons for viewing
DROP TABLE IF EXISTS out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp AS 
SELECT o.*, p.the_geom as the_geom  
FROM 
out_cell_imp_10km_teeb_s1_2050_cons_allsp AS o,
cells_10km_teeb AS p 
WHERE o.cell_id=p.cell_id;

--convert to wgs84 for viewing purposes
ALTER TABLE out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp
ALTER COLUMN the_geom type geometry(MultiPolygon, 54009) using ST_Multi(the_geom);
ALTER TABLE out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);
  
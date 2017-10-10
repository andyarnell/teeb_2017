------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--STEP 2 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--set path for sql processing to act on tables in a specific schema within the database (normally defaults to public otherwise)
SET search_path=teeb_2017,lvb_wkshp2,public,topology;

--if postgis/postgresql running locally on desktop increase access to memory (RAM) 
SET work_mem TO 120000;
SET maintenance_work_mem TO 120000;
SET client_min_messages TO DEBUG;

/*
--full intersection (currently intersection in wgs84 and then areas calculated in mollweide. This version matched better, though not perfectly, with results for vector overlays than converting raster to Mollweide before intersecting)
drop table if exists raw_lc_areas_cells_10km_teeb_s1_2050_cons;
CREATE TABLE raw_lc_areas_cells_10km_teeb_s1_2050_cons AS
 SELECT cell_id, 
        sum(st_area(st_transform((gv).geom,54009))) AS area_lc_cell,
        (gv).val as lc
 FROM (SELECT cell_id, 
              --ST_Intersection(rast, the_geom) AS gv  
	st_dumpaspolygons((St_clip(rast,the_geom))) AS gv  --the st_dumpaspolygons is best if quick (but less accurate) method is needed
       FROM raw.lshift_regscale_lvb_s1_2050_cons,
            (
SELECT cell_id, 
st_buffer(st_transform(the_geom,4326),0) as the_geom  
FROM cells_10km_teeb
) as bob
       WHERE ST_Intersects(rast, the_geom)
      ) foo
group by (gv).val, cell_id;
*/


--make table for importing semi-raw landshift outputs
--(after processing rasters through seperate python script into csv tables --
--and then converting to long format with r script)
DROP TABLE IF EXISTS raw_lc_areas_cells_10km_teeb_s1_2050_cons;
CREATE TABLE raw_lc_areas_cells_10km_teeb_s1_2050_cons 
(
cell_id VARCHAR,
lc numeric,
area_lc_cell numeric
)
WITH (OIDS=FALSE);

  


ALTER TABLE raw_lc_areas_cells_10km_teeb_s1_2050_cons
  OWNER TO postgres;
  


COPY raw_lc_areas_cells_10km_teeb_s1_2050_cons
(cell_id,
lc,
area_lc_cell)
FROM
'C:\data\lshift_all\outputs\10km_teeb_s1_2050_cons.csv' CSV DELIMITER ',' HEADER;


Alter table raw_lc_areas_cells_10km_teeb_s1_2050_cons
ADD COLUMN id bigserial NOT NULL,
ADD constraint raw_lc_areas_cells_10km_teeb_s1_2050_cons_pkey PRIMARY KEY (id);



--import tables of landcover/landuse for scenarios and s1_2050
--use land cover lookup table (lc_lut) to link crosswalk values to all landshift values eg. 100 into 100,101,102...120) (N.B. for natureserve there is no change as glc2000 and no landshift values)
DROP TABLE IF EXISTS lc_areas_cells_10km_teeb_s1_2050_cons;
CREATE TABLE lc_areas_cells_10km_teeb_s1_2050_cons AS
SELECT 
b.cell_id::integer as cell_id, 
l.lc_lookup AS lc, 
b.area_lc_cell as area_lc_cell
FROM 
raw_lc_areas_cells_10km_teeb_s1_2050_cons as b, 
lc_lut_10km_teeb as l
where b.lc = l.lc_raw;

--this next table (out_spp_allsuitareacells_10km_teeb_s1_2050_cons) creation sql seems to take a lot of time compared to the rest of the simple functions (especially when many cells)
--so added steps before indexes to tables and created temp tables to speed this up
drop table if exists habitat_prefs_10km_teeb_clean;
create table habitat_prefs_10km_teeb_clean as
select distinct taxonid, /*species,*/ suitlc  
from habitat_prefs_10km_teeb as foo1,
--(select id_no, species from raw.species_eoo_gridbscale) as foo2
(select distinct id_no from species_intersecting_10km_teeb_temp) as foo2
where spchabimpdesc = 'Suitable' 
and foo1.taxonid = foo2.id_no;

create index habitat_prefs_10km_teeb_clean_suitlc_idx on habitat_prefs_10km_teeb_clean (suitlc);
create index habitat_prefs_10km_teeb_clean_taxonid_idx on habitat_prefs_10km_teeb_clean (taxonid);

create index lc_areas_cells_10km_teeb_s1_2050_cons_cons_lc_idx on lc_areas_cells_10km_teeb_s1_2050_cons (lc);
create index lc_areas_cells_10km_teeb_s1_2050_cons_cons_cell_id_idx on lc_areas_cells_10km_teeb_s1_2050_cons (cell_id);


DROP TABLE IF EXISTS out_spp_allsuitareacells_10km_teeb_s1_2050_cons;
CREATE TABLE out_spp_allsuitareacells_10km_teeb_s1_2050_cons AS 
SELECT
h.taxonid as id_no,
lc.cell_id as cell_id, 
sum(lc.area_lc_cell) AS sumofarea_lc_cell 
FROM 
(select * from lc_areas_cells_10km_teeb_s1_2050_cons) 
AS lc 
inner join 
habitat_prefs_10km_teeb_clean AS h 
on lc.lc = h.suitlc
GROUP BY h.taxonid, /*h.species,*/ lc.cell_id;

                             
--add column 
ALTER TABLE out_spp_allsuitareacells_10km_teeb_s1_2050_cons 
ADD cell_sp varchar;
--add unique id for combination of cell and species
UPDATE out_spp_allsuitareacells_10km_teeb_s1_2050_cons
SET 
cell_sp = (cell_id/*::numeric(20,4)*/)  || '_' || id_no /*firstofspecies*/;



-- add an index
ALTER TABLE out_spp_allsuitareacells_10km_teeb_s1_2050_cons
ADD COLUMN id bigserial NOT NULL,
ADD constraint out_spp_allsuitareacells_10km_teeb_s1_2050_cons_pkey PRIMARY KEY (id);



--view subset of result to check
-- SELECT * FROM out_spp_allsuitareacells_10km_teeb_s1_2050_cons LIMIT 1000; 
/*SELECT os.*, (os.sumofarea_lc_cell/c.cell_area) as prop, c.cell_area 
FROM (select * from out_spp_allsuitareacells_10km_teeb_s1_2050_cons) os, cells_10km_teeb as c  
WHERE c.cell_id=os.cell_id /*and (os.sumofarea_lc_cell/c.cell_area) >1 */ order by c.cell_id;
*/

/*
From Access notes:
To reduce the species table to just those species/watershed combinations where the species occurs in the watershed 
(i.e. the overlap proportion is greater than zero) and calculate the area of suitable habitat within the watershed for a species.
Currently the suitable areas table does not account for the overlap of the species and the watershed. This is included in this output table.
The area of suitable habitat can be calculated using:
(1) Equal distribution assumption: Assumes that the distribution of the land covers is equal across the species overlap. 
For example, a species overlapping half of a watershed where forest was its only suitable habitat would overlap half of the total forest area.
(2) Maximal distribution assumption: Assumes that the species preferentially occurs in suitable habitat
(3) Minimum distribution assumption: Assumes that the species overlaps unsuitable habitat
*/


--This query will create a table with equal, min and max suitable area options described above
DROP TABLE IF EXISTS  out_spp_calc_areacells_10km_teeb_s1_2050_cons;
CREATE TABLE out_spp_calc_areacells_10km_teeb_s1_2050_cons AS
select 
sp.id_no,
sp.cell_sp, 
sp.cell_prop, 
(sp.cell_prop::numeric) * op.sumofarea_lc_cell as cell_suitarea_eq_s1_2050_cons,
--CASE WHEN sp.area<op.sumofarea_lc_cell THEN
--sp.area ELSE op.sumofarea_lc_cell END as cell_suitarea_max_s1_2050_cons,
--CASE WHEN sp.area>((c.cell_area)-(op.sumofarea_lc_cell)) THEN
--(sp.area-(c.cell_area-op.sumofarea_lc_cell)) ELSE 0 END as cell_suitarea_min_s1_2050_cons,
sp.cell_id 
FROM 
species_overlap_10km_teeb AS sp 
--(select * from species_overlap_10km_teeb limit 1000000) as sp
INNER JOIN 
out_spp_allsuitareacells_10km_teeb_s1_2050_cons as op 
ON sp.cell_sp = op.cell_sp
INNER JOIN 
cells_10km_teeb AS c ON op.cell_id = c.cell_id
WHERE (((sp.cell_prop)<>0));

--add an id column as a primary key
ALTER TABLE out_spp_calc_areacells_10km_teeb_s1_2050_cons
ADD COLUMN id bigserial NOT NULL,
ADD constraint out_spp_calc_areacells_10km_teeb_s1_2050_cons_pkey PRIMARY KEY (id);


--check how many remain
-- SELECT count(1) from (select species from out_spp_calc_areacells_10km_teeb_s1_2050_cons group by species) as foo;

--view subset of result to check it worked - 
-- SELECT * FROM out_spp_calc_areacells_10km_teeb_s1_2050_cons LIMIT 1000;


--checking orignal species overlap with watershed areas and the proportions between them 
--occasionally a mismatch (cell_prop > 1) between these as calculated in different software --ideally all processing is in postgis so should agree
--SELECT sp.*, c.cell_area FROM species_overlap_10km_teeb sp, cells_10km_teeb c WHERE sp.cell_id = c.cell_id and sp.cell_prop >.9999; 

 
--Calculate the total area of suitable habitat in the region
DROP TABLE IF EXISTS  out_spp_allsuitarearegion_10km_teeb_s1_2050_cons;
CREATE TABLE out_spp_allsuitarearegion_10km_teeb_s1_2050_cons AS
SELECT /*o.species, */ o.id_no,
--SUM(o.cell_suitarea_max_s1_2050_cons) AS sumofcell_suitarea_max_s1_2050_cons, 
SUM(o.cell_suitarea_eq_s1_2050_cons) AS sumofcell_suitarea_eq_s1_2050_cons
--,SUM(o.cell_suitarea_min_s1_2050_cons) AS sumofcell_suitarea_min_s1_2050_cons 
FROM out_spp_calc_areacells_10km_teeb_s1_2050_cons AS o
GROUP BY o.id_no/*, o.species*/;



-------------------------------------------------------------------------------------------------------------------
--STEP 3
-------------------------------------------------------------------------------------------------------------------

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
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);

-------------------------------------------------------------------------------------------------------------------
--STEP 4
-------------------------------------------------------------------------------------------------------------------

--set path for sql processing to act on tables in a specific schema within the database (normally defaults to public otherwise)
SET search_path=teeb_2017,lvb_wkshp1,public,topology;

--if postgis/postgresql running locally on desktop increase access to memory (RAM) 
SET work_mem TO 120000;
SET maintenance_work_mem TO 120000;
SET client_min_messages TO DEBUG;

-- The first query outputs two fields:
--Sp_WS_Chg: Implements the full calculation (Equation D)
--Habt_Chg: Implements the first part of equation D. 
--loss species only, not caclualted yet --this field is used in subsequent queries to identify species which lose habitat.

DROP TABLE IF EXISTS out_spcell_change_10km_teeb_s1_2050_cons_allsp; 
CREATE TABLE out_spcell_change_10km_teeb_s1_2050_cons_allsp AS
SELECT 
case when a.cell_sp isnull then b.cell_sp else a.cell_sp end as cell_sp, --case when is used as outer joins (used as if data missing in one time period then no result)
case when a.cell_id isnull then b.cell_id else a.cell_id end as cell_id, --case when is used as outer joins (used as if dat missing in one time period then no result)
case when a.id_no isnull then b.id_no else a.id_no end as id_no, --case when is used as outer joins (used as if data missing in one time period then no result)
((coalesce(b.cell_suitarea_eq_s1_2050_cons,0) - coalesce(a.cell_suitarea_eq_baseline_cons,0))
/ c.sumofcell_suitarea_eq_baseline_cons 
) *(d.sumofarea/species_eoo.eoo_area) AS sp_cell_chg, 
((coalesce(b.cell_suitarea_eq_s1_2050_cons,0) - coalesce(a.cell_suitarea_eq_baseline_cons,0))
/c.sumofcell_suitarea_eq_baseline_cons) 
AS habt_chg 
FROM out_spp_allarearegion_10km_teeb as d 
INNER JOIN (
raw.species_eoo_gridbscale as species_eoo
INNER JOIN 
((out_spp_calc_areacells_10km_teeb_baseline_cons as a
FULL OUTER JOIN out_spp_calc_areacells_10km_teeb_s1_2050_cons as b
ON a.cell_sp = b.cell_sp) 
inner join
(
select * from out_spp_allsuitarearegion_10km_teeb_baseline_cons
where  sumofcell_suitarea_eq_baseline_cons> 0
) as c
ON (case when a.id_no isnull then b.id_no else a.id_no end) =  c.id_no) --case when is used as outer joins (used as if data missing in one time period then no result)
ON species_eoo.id_no = (case when a.id_no isnull then b.id_no else a.id_no end)) --case when is used as outer joins (used as if data missing in one time period then no result) 
ON d.id_no =  (case when a.id_no isnull then b.id_no else a.id_no end) --case when is used as outer joins (used as if data missing in one time period then no result)
inner join
(
select distinct id_no, class as class_name, category as code
from
raw.species_eoo_gridbscale
) as st
on (case when a.id_no isnull then b.id_no else a.id_no end) = st.id_no --case when is used as outer joins (used as if data missing in one time period then no result)
where lower(st.class_name) in ('amphibia','aves','mammalia');


--select *  from out_spcell_change_10km_teeb_s1_2050_cons_allsp order by sp_cell_chg desc; --habt_chg desc;
                                
--Calculate change in suitable habitat for All species (cell)
--cell scale changes are based on output tables from preceding step
--(i.e. out_spcell_change_...etc..
--These outputs must have been created prior to this step.
--cell scale change is calculated as the sum of change for all species which occur in that cell.
--for each scenario etc:

DROP TABLE  IF EXISTS out_cell_change_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_change_10km_teeb_s1_2050_cons_allsp AS
select a.cell_id, sum(a.sp_cell_chg) 
as cell_change
from out_spcell_change_10km_teeb_s1_2050_cons_allsp as a
group by a.cell_id 
order by cell_change desc;


-- make a table linking polygons to the results
DROP TABLE  IF EXISTS out_cell_change_shape_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_change_shape_10km_teeb_s1_2050_cons_allsp  AS 
SELECT o.*, p.the_geom as the_geom  
FROM out_cell_change_10km_teeb_s1_2050_cons_allsp AS o, cells_10km_teeb AS p 
WHERE o.cell_id=p.cell_id;


--convert to wgs84 for viewing purposes
ALTER TABLE out_cell_change_shape_10km_teeb_s1_2050_cons_allsp
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);
 
update out_cell_change_shape_10km_teeb_s1_2050_cons_allsp
set cell_change = coalesce(cell_change, 0);

----------------------------------------------
--Calculate loss in suitable habitat for All species (cell) - as with hacnge but considering only species that lose habitat
--cell scale loss are based on output tables from preceding step

DROP TABLE  IF EXISTS out_cell_loss_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_loss_10km_teeb_s1_2050_cons_allsp AS
select a.cell_id, sum(a.sp_cell_chg) 
as cell_loss
from
(
select cell_sp,
cell_id,
id_no,
(case when habt_chg > 0 then 0 else sp_cell_chg end) as sp_cell_chg, 
(case when habt_chg > 0 then 0 else habt_chg end) as habt_chg
from out_spcell_change_10km_teeb_s1_2050_cons_allsp
)as a
group by a.cell_id;


-- make a table linking polygons to the results
DROP TABLE  IF EXISTS out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp;
CREATE TABLE out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp  AS 
SELECT o.*, p.the_geom as the_geom  
FROM out_cell_loss_10km_teeb_s1_2050_cons_allsp AS o, cells_10km_teeb AS p 
WHERE o.cell_id=p.cell_id;


--convert to wgs84 for viewing purposes
ALTER TABLE out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);
 
update out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp
set cell_loss = coalesce(cell_loss, 0);

-----------------------------------------------------------------------------------------------------------
----------------------code for exporting shapefiles - just for normalised allsp currently 
/*

--For exporting using ogr2ogr (osgeo4w command line) into separate shapefiles for change maps

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_loss_shape_10km_teeb_s1_2050_cons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_change_shape_10km_teeb_s1_2050_cons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_change_shape_10km_teeb_s1_2050_cons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_imp_shape_10km_teeb_s1_2050_cons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_imp_shape_10km_teeb_baseline_cons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_imp_shape_10km_teeb_baseline_cons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"


*/


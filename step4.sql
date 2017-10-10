
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

DROP TABLE IF EXISTS out_spcell_change_10km_teeb_s3_2050_nocons_allsp; 
CREATE TABLE out_spcell_change_10km_teeb_s3_2050_nocons_allsp AS
SELECT 
case when a.cell_sp isnull then b.cell_sp else a.cell_sp end as cell_sp, --case when is used as outer joins (used as if data missing in one time period then no result)
case when a.cell_id isnull then b.cell_id else a.cell_id end as cell_id, --case when is used as outer joins (used as if dat missing in one time period then no result)
case when a.id_no isnull then b.id_no else a.id_no end as id_no, --case when is used as outer joins (used as if data missing in one time period then no result)
((coalesce(b.cell_suitarea_eq_s3_2050_nocons,0) - coalesce(a.cell_suitarea_eq_baseline_nocons,0))
/ c.sumofcell_suitarea_eq_baseline_nocons 
) *(d.sumofarea/species_eoo.eoo_area) AS sp_cell_chg, 
((coalesce(b.cell_suitarea_eq_s3_2050_nocons,0) - coalesce(a.cell_suitarea_eq_baseline_nocons,0))
/c.sumofcell_suitarea_eq_baseline_nocons) 
AS habt_chg 
FROM out_spp_allarearegion_10km_teeb as d 
INNER JOIN (
raw.species_eoo_gridbscale as species_eoo
INNER JOIN 
((out_spp_calc_areacells_10km_teeb_baseline_nocons as a
FULL OUTER JOIN out_spp_calc_areacells_10km_teeb_s3_2050_nocons as b
ON a.cell_sp = b.cell_sp) 
inner join
(
select * from out_spp_allsuitarearegion_10km_teeb_baseline_nocons
where  sumofcell_suitarea_eq_baseline_nocons> 0
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


--select *  from out_spcell_change_10km_teeb_s3_2050_nocons_allsp order by sp_cell_chg desc; --habt_chg desc;
                                
--Calculate change in suitable habitat for All species (cell)
--cell scale changes are based on output tables from preceding step
--(i.e. out_spcell_change_...etc..
--These outputs must have been created prior to this step.
--cell scale change is calculated as the sum of change for all species which occur in that cell.
--for each scenario etc:

DROP TABLE  IF EXISTS out_cell_change_10km_teeb_s3_2050_nocons_allsp;
CREATE TABLE out_cell_change_10km_teeb_s3_2050_nocons_allsp AS
select a.cell_id, sum(a.sp_cell_chg) 
as cell_change
from out_spcell_change_10km_teeb_s3_2050_nocons_allsp as a
group by a.cell_id 
order by cell_change desc;


-- make a table linking polygons to the results
DROP TABLE  IF EXISTS out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp;
CREATE TABLE out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp  AS 
SELECT o.*, p.the_geom as the_geom  
FROM out_cell_change_10km_teeb_s3_2050_nocons_allsp AS o, cells_10km_teeb AS p 
WHERE o.cell_id=p.cell_id;


--convert to wgs84 for viewing purposes
ALTER TABLE out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);
 
update out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp
set cell_change = coalesce(cell_change, 0);

----------------------------------------------
--Calculate loss in suitable habitat for All species (cell) - as with hacnge but considering only species that lose habitat
--cell scale loss are based on output tables from preceding step

DROP TABLE  IF EXISTS out_cell_loss_10km_teeb_s3_2050_nocons_allsp;
CREATE TABLE out_cell_loss_10km_teeb_s3_2050_nocons_allsp AS
select a.cell_id, sum(a.sp_cell_chg) 
as cell_loss
from
(
select cell_sp,
cell_id,
id_no,
(case when habt_chg > 0 then 0 else sp_cell_chg end) as sp_cell_chg, 
(case when habt_chg > 0 then 0 else habt_chg end) as habt_chg
from out_spcell_change_10km_teeb_s3_2050_nocons_allsp
)as a
group by a.cell_id;


-- make a table linking polygons to the results
DROP TABLE  IF EXISTS out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp;
CREATE TABLE out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp  AS 
SELECT o.*, p.the_geom as the_geom  
FROM out_cell_loss_10km_teeb_s3_2050_nocons_allsp AS o, cells_10km_teeb AS p 
WHERE o.cell_id=p.cell_id;


--convert to wgs84 for viewing purposes
ALTER TABLE out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp
 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) 
  USING ST_Transform(the_geom,4326);
 
update out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp
set cell_loss = coalesce(cell_loss, 0);

-----------------------------------------------------------------------------------------------------------
----------------------code for exporting shapefiles - just for normalised allsp currently 
/*

--For exporting using ogr2ogr (osgeo4w command line) into separate shapefiles for change maps

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_loss_shape_10km_teeb_s3_2050_nocons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_change_shape_10km_teeb_s3_2050_nocons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_imp_shape_10km_teeb_s3_2050_nocons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_imp_shape_10km_teeb_s3_2050_nocons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"

ogr2ogr --config FGDB_BULK_LOAD YES  -progress -f "ESRI Shapefile" -sql "SELECT * FROM teeb_2017.out_cell_imp_shape_10km_teeb_baseline_nocons_allsp" C:\Data\final_bd_results\teeb_2017\1km_lshift\cons PG:"host=localhost user=postgres password=Seltaeb1 dbname=biodiv_processing" -nln out_cell_imp_shape_10km_teeb_baseline_nocons_allsp -nlt POLYGON -lco "SHPT=POLYGON"  -a_srs "EPSG:4326"


*/

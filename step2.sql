
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

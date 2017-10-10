--metadata fro species numbers 
CREATE SCHEMA IF NOT EXISTS teeb_2017; 

--set path for sql processing to act on tables in a specific schema within the database (normally defaults to public otherwise)
SET search_path=teeb_2017, public,topology;

--if postgis/postgresql running locally on desktop increase access to memory (RAM) 
SET work_mem TO 120000;
SET maintenance_work_mem TO 120000;
SET client_min_messages TO DEBUG;

--all animals overlapping with regoion
select count(distinct id_no) from
species_intersecting_10km_teeb_temp
--species_overlap_10km_teeb
;


--add a normal index on column (as used in subsequent joins with large tables)
create index species_overlap_teeb_10km_id_no_index
ON species_overlap_10km_teeb (id_no);

--breakdown of classes for fauna
select count (distinct (foo1.id_no,foo2.class)), foo2.class from 
--species_overlap_teeb_10km as foo1  
species_intersecting_10km_teeb_temp as foo1
join 
(select distinct id_no, class from raw.species_seperate_polygons_gridbscale) as foo2
on foo1.id_no=foo2.id_no
group by foo2.class;

--with land cover and links
select count (distinct (foo1.id_no,foo2.class)), foo2.class from 
(select distinct id_no from (select count(distinct foo1.id_no) from 
out_spp_calc_areacells_teeb_10km_2013 as foo1,
teeb_endemic_subset_anmls_endmc as foo2 where foo1.id_no=foo2.id_no)) as foo1  
join 
(select distinct id_no, class from raw.species_seperate_polygons_gridbscale) as foo2
on foo1.id_no=foo2.id_no
group by foo2.class;

--overall counts
--with links to land cover
--by taxonomic class
select count (distinct (foo1.id_no,foo2.class)), foo2.class from 
out_spp_calc_areacells_10km_teeb_s1_2050_cons as foo1
join 
(select distinct id_no, class, category from raw.species_seperate_polygons_gridbscale) 
as foo2
on foo1.id_no=foo2.id_no
group by foo2.class;

--by status
select count (distinct (foo1.id_no,foo2.category)), foo2.category from 
out_spp_calc_areacells_10km_teeb_s1_2050_cons as foo1
join 
(select distinct id_no, class, category from raw.species_seperate_polygons_gridbscale) 
as foo2
on foo1.id_no=foo2.id_no
group by foo2.category;


select count(distinct id_no) from
teeb_endemic_subset_anmls_endmc
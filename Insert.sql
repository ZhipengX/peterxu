begin 
--insert zx_crime_description table
zx_sf_trans_data_pkg.p_insert_description;
--insert zx_crime_category table
zx_sf_trans_data_pkg.p_insert_category;
--save change
commit;
end;
/

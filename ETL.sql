--create table
CREATE TABLE zx_crime_category (
    category_pk      NUMBER NOT NULL,
    category_name    VARCHAR2(255),
    pdid             VARCHAR2(255),
    description_fk   NUMBER NOT NULL
);

ALTER TABLE zx_crime_category ADD CONSTRAINT zx_crime_category_pk PRIMARY KEY ( category_pk );

CREATE TABLE zx_crime_description (
    description_pk     NUMBER NOT NULL,
    description_name   VARCHAR2(255)
);

ALTER TABLE zx_crime_description ADD CONSTRAINT zx_crime_description_pk PRIMARY KEY ( description_pk );

--  ERROR: FK name length exceeds maximum allowed length(30) 
ALTER TABLE zx_crime_category
    ADD CONSTRAINT zx_crime_category_zx_crime_description_fk FOREIGN KEY ( description_fk )
        REFERENCES zx_crime_description ( description_pk );
--create trigger to generate primary key for category table 
create sequence seq_cate
create or replace trigger trig_bi_zx_crime_category
before insert on zx_crime_category
for each row
declare
v_category_pk zx_crime_category.category_pk%type;
begin
    if :NEW.category_pk is null then                 
    select seq_cate.nextval                              
    into v_category_pk                               
    from dual;
    :New.category_pk:= v_category_pk;
    end if;
end;  
--create trigger to generate primary key for description table
create sequence seq_descp
create or replace trigger trig_bi_zx_crime_description
before insert on zx_crime_description
for each row
declare
v_description_pk zx_crime_description.description_pk%type;
begin
    if :NEW.description_pk is null then                 
    select seq_descp.nextval                              
    into v_description_pk                              
    from dual;
    :New.description_pk:= v_description_pk;
    end if;
end;  
--- create view an view combine 2 tables
create or replace view zx_sf_crime
as select description_pk,category_pk,category_name,description_name,pdid
from zx_crime_category cate  join zx_crime_description des
on cate.description_fk=des.description_pk(+)
select * from zx_sf_crime
---package to insert, update, and delete view
create or replace package zx_insert_update_delete_pkg
is 
procedure p_insert(i_description_name varchar2, i_category_name varchar2,i_pdid varchar2);
procedure p_update_description(i_description_name varchar2, i_description_pk number);
procedure p_update_category(i_category_name varchar2,i_description_fk number);
procedure p_update_pdid(i_pdid varchar2, i_description_fk number);
procedure p_delete(i_description_fk number);
function  f_COUNT_DISTINCT_DESCRIPTION_NAME(i_description_name varchar2) return number;
end zx_insert_update_delete_pkg;
create or replace package body zx_insert_update_delete_pkg
is 
--function to judge if value is exist
function f_COUNT_DISTINCT_DESCRIPTION_NAME(i_description_name varchar2) 
return number 
is v_count number;
begin 
 select count(*) into v_count from zx_crime_description where 'ABORTION' not in (select description_name from zx_crime_description);
 return v_count;
end;
--procedure to insert value
procedure p_insert(i_description_name varchar2, i_category_name varchar2,i_pdid varchar2) is 
v_description_pk number;
v_count number;
begin 
v_count:=f_COUNT_DISTINCT_DESCRIPTION_NAME(i_description_name);
if v_count!=0 then
insert into zx_crime_description(description_name)
    values(i_description_name)
    returning description_pk into v_description_pk;
    
insert into zx_crime_category(category_name,pdid,description_fk) values(i_category_name,i_pdid,v_description_pk);
end if;
end;
--procedure to update description_name
procedure p_update_description(i_description_name varchar2, i_description_pk number)
is 
begin 
    update zx_crime_description
    set description_name=i_description_name
    where description_pk=i_description_pk;
end;    
--procedure to uodate category_name
procedure p_update_category(i_category_name varchar2,i_description_fk number)
is 
begin 
    update zx_crime_category
    set category_name=i_category_name
    where description_fk=i_description_fk;
end;  
--procedure to update pdid
 procedure p_update_pdid(i_pdid varchar2, i_description_fk number)
is 
begin 
    update zx_crime_category
    set pdid=i_pdid
    where  description_fk=i_description_fk;
end;  
--procedure to delete value
procedure p_delete(i_description_fk number)
is 
begin 
delete from zx_crime_category
where description_fk=i_description_fk;
delete from zx_crime_description
where description_pk=i_description_fk;
end;
end zx_insert_update_delete_pkg;



--- create trigger to insert, update, and delete view
CREATE or replace TRIGGER trig_zx_sf_crime 
  INSTEAD OF INSERT OR UPDATE OR DELETE --To handle all 3 DMLs
  ON zx_sf_crime --The name of your view
  REFERENCING NEW AS NEW OLD AS OLD 
DECLARE 
--Use this to declare any variables
rec_category zx_crime_category%rowtype;

BEGIN
    rec_category.description_fk:= :old.description_pk;
    rec_category.category_pk:=:old.category_pk;
  if inserting then --Inserting is a special keyword
    --Write any code you need to handle INSERT
     --You will need to use :NEW for all *new* values
    zx_insert_update_delete_pkg.p_insert(:new.description_name, :new.category_name,:new.pdid);
   
  elsif updating then --Updating is a special keyword
      --Write any code you need to handle UPDATE
      --You will need to use :OLD.your_primary_key to refer to your primary key 
      --You will need to use :NEW for any *new* values, and :OLD for any existing values     
    zx_insert_update_delete_pkg.p_update_description(:new.description_name,rec_category.description_fk);
    zx_insert_update_delete_pkg.p_update_category(:new.category_name, rec_category.description_fk);
    zx_insert_update_delete_pkg.p_update_pdid(:new.pdid,rec_category.description_fk);
   
  elsif deleting then --Deleting is a special keyword
      --Write any code you need to handle DELETE
      --You will need to use :OLD.your_primary_key to refer to your primary key
     zx_insert_update_delete_pkg.p_delete(rec_category.description_fk);
  end if;  
END;
--- create package to migrate data from stage to table
create or replace package zx_sf_trans_data_pkg
is 
function  f_count_description return number;
function  f_count_category return number;
procedure p_insert_description;
procedure p_insert_category;
end zx_sf_trans_data_pkg;

create or replace package body zx_sf_trans_data_pkg
is 
--function f_count_description to judge whether row already exist in description table
function f_count_description
return number
is v_count number;
begin
select count(*) into v_count from(
    select distinct descript from sf_crime_stage
        minus
    select description_name
    from zx_crime_description);
    return v_count;
end;  
--function f_count_category to judge whether row already exist in category table
function f_count_category
return number
is v_count number;
begin
select count(*) into v_count from(
    select category_name,pdid,description_pk
    from sf_crime_stage stg,
    zx_crime_description des
    where stg.descript=des.description_name
    minus
    select category_name,pdid,description_fk
    from zx_crime_category);
    return v_count;
end;    
--procedure p_insert_description to transfer value from stage to description table

procedure p_insert_description
is v_count number;
begin
v_count:=f_count_description; 
if v_count!=0 then
declare 
cursor cur_desc is 
    select seq_descp.nextval,descript from(
    select distinct descript from sf_crime_stage
        minus
    select description_name
    from zx_crime_description);

    
    type description_tt is table of zx_crime_description%rowtype index by pls_integer;
    my_description_list description_tt;
begin
 open cur_desc;
    loop
        my_description_list.delete;
        fetch cur_desc bulk collect into my_description_list limit 1000;
            forall indx in 1..my_description_list.count
            insert into zx_crime_description
            values my_description_list(indx);
            commit; --free up the memory
        exit when cur_desc%notfound;
    end loop;
    close cur_desc;
end;
end if;
end;
--procedure p_insert_description to transfer value from stage to category table
procedure p_insert_category
is v_count number;
begin
v_count:=f_count_description; 
if v_count!=0 then
declare
cursor cur_category is 
    select seq_cate.nextval,category_name,pdid,
    description_pk description_fk
    from (
    select category_name,pdid,description_pk
    from sf_crime_stage stg,
    zx_crime_description des
    where stg.descript=des.description_name
    minus
    select category_name,pdid,description_fk
    from zx_crime_category);
    
    type category_tt is table of zx_crime_category%rowtype index by pls_integer;
    my_category_list category_tt;
    
begin
open cur_category;
    loop
        my_category_list.delete;
        fetch cur_category bulk collect into  my_category_list limit 1000;
        forall indx in 1.. my_category_list.count
        insert into zx_crime_category
        values  my_category_list(indx);
        commit; --free up memory;
        exit when cur_category%notfound;
   end loop;
   close cur_category;
end;
end if;
end;
end zx_sf_trans_data_pkg;
--insert data into description table 
create or replace procedure p_insert_to_description
is 
begin
zx_sf_trans_data_pkg.p_insert_description;
end;

--insert data into category table
create or replace procedure p_insert_to_category
is 
begin 
zx_sf_trans_data_pkg.p_insert_category;
end;





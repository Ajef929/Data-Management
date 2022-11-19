-- This is IMPORTANT: Set your active schema!
use schema nyc311.<your_schema>;

-- The semantics of processing dates in Snowflake are a bit different compared to MySQL.
-- Therefore we need to set these two policy variables to make the results match our expectations.
alter session set week_of_year_policy = 0;
alter session set week_start = 0;
show parameters;
-- This is an example query to illustrate the results of those date-processing functions.
select '2010-01-01T00:00:00Z'::timestamp as tstamp,
       week(tstamp) as "WEEK",
       weekiso(tstamp) as "WEEK ISO",
       weekofyear(tstamp) as "WEEK OF YEAR",
       yearofweek(tstamp) as "YEAR OF WEEK",
       yearofweekiso(tstamp) as "YEAR OF WEEK ISO";

-- Profiling/exploring the updates.
-- How many yearweek values all together?
with yw as (select distinct (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek from nyc311.service_request_all order by yearweek)
select count(*) from yw;

-- How many yearweek values prior to or in 202206?
select count(*) from
(select *, (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek from nyc311.service_request_all where yearweek <= 202206); -- 27,769,609

-- How many yearweek values after 202206?
select count(*) from
(select *, (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek from nyc311.service_request_all where yearweek > 202206); -- 54,4242

-- Comparing the totals.
select 27769609 + 544242; -- 28,313,851
select count(*) from nyc311.service_request_all; -- 28,313,851

-- This is the old service_request table, now in VIEW form.
create or replace view nyc311.service_request as
select *, (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek from nyc311.service_request_all where yearweek <= 202206;
select count(*) from nyc311.service_request;

-- These are just the updated rows, after week 202206.
create or replace view nyc311.service_request_updates as
select *, (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek from nyc311.service_request_all where yearweek > 202206;
select count(*) from nyc311.service_request_updates;

-- Same table as in MySQL nyc311 schema.
create or replace table sr_complaint_type_summary as
select lower("Complaint_Type") as complaint_type, count(*) as count
from nyc311."SERVICE_REQUEST"
group by complaint_type;
select count(*) from sr_complaint_type_summary; -- 467
select count(*) from nyc311.sr_complaint_type_summary; --465

-- Same table as in MySQL nyc311 schema.
create or replace table sr_incident_zip_summary as
select "Incident_Zip" as incident_zip, count(*) as count
from nyc311."SERVICE_REQUEST"
group by incident_zip;
select count(*) from sr_incident_zip_summary; -- 2,836
select count(*) from nyc311.sr_complaint_zip_summary; -- 2,833

-- Dimension: YEARWEEK
create or replace table dim_yearweek ( yearweek number(8, 0) primary key );
-- There's no yearweek(...) function in Snowflake, so we have to do this as follows:
insert into dim_yearweek
select distinct (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek
from nyc311."SERVICE_REQUEST"
order by yearweek;

-- Dimension: AGENCY
create or replace table dim_agency ( agency_id number(8, 0) autoincrement start 3001 increment 1 primary key, agency_name varchar(5) );
-- TASK
-- Write the insert statement as per the nyc311_data_warehouse_build.sql script for this dimension.
insert into dim_agency (agency_name)
with agency as (select distinct "Agency" as agency_name from nyc311."SERVICE_REQUEST" order by agency_name)
select agency_name from agency;

-- Dimension: LOCATION
create or replace table dim_location ( location_zip varchar(5) primary key );
-- TASK
-- Write the insert statement as per the nyc311_data_warehouse_build.sql script for this dimension.
insert into dim_location select "Zip" as location_zip from nyc311."ZIP_CODE_NYC_BOROUGH" order by location_zip;

-- Dimension REQUEST TYPE
create or replace table dim_request_type ( type_id number(8, 0) primary key, type_name varchar(30) );
-- TASK
-- Write the insert statement as per the nyc311_data_warehouse_build.sql script for this dimension.
insert into dim_request_type select "ID" as type_id, "Type" as type_name from nyc311."REF_SR_TYPE_NYC311_OPEN_DATA_26";

-- This is where we may do data cleansing to remap "BAD" Incident_Zip values to the reference values, where possible.
create or replace table map_incident_zip_nyc_borough as
select "Zip", incident_zip, count
from sr_incident_zip_summary
left join nyc311."ZIP_CODE_NYC_BOROUGH"
on incident_zip = "Zip";
select count(*) from map_incident_zip_nyc_borough; -- 2,836
select count(*) from nyc311.map_incident_zip_nyc_borough; -- 2,833

-- Regular expressions in Snowflake work as in MySQL.
select count, complaint_type, lower(regexp_replace(complaint_type, '[^[:alnum:]]+', '')) as complaint_type_stripped
from sr_complaint_type_summary;

-- This is where we may do data cleansing to remap "BAD" Complaint_Type values to the reference values, where possible.
create or replace table map_complaint_type_open_nyc311 as
select ID as "Type_ID", complaint_type, count from sr_complaint_type_summary
left join nyc311."REF_SR_TYPE_NYC311_OPEN_DATA_26" on lower(regexp_replace(complaint_type, '[^[:alnum:]]+', '')) = lower(regexp_replace("Type", '[^[:alnum:]]+', ''));
select count(*) from map_complaint_type_open_nyc311; -- 467
select count(*) from nyc311.map_complaint_type_open_nyc311; -- 465

-- This is the view with "clean(er)" data for Incident_Zip and Complaint_Type values.
create or replace view sr_full as
select "Unique_Key" as unique_key,
    yearweek,
    "Created_Date" as created_date, "Closed_Date" as closed_date, "Agency" as agency,
    "Agency_Name" as agency_name,
    "Type_ID" as complaint_type_id, "Descriptor" as descriptor, "Location_Type" as location_type,
    "Zip" as incident_zip_id, "Incident_Address" as incident_address, "Street_Name" as street_name,
    "Cross_Street_1" as cross_street_1, "Cross_Street_2" as cross_street_2,
    "Intersection_Street_1" as intersection_street_1, "Intersection_Street_2" as intersection_street_2,
    "Address_Type" as address_type, "City" as city, "Landmark" as landmark,
    "Facility_Type" as facility_type, "Status" as status, "Due_Date" as due_date,
    "Resolution_Description" as resulution_description,
    "Resolution_Action_Updated_Date" as resolution_action_updated_date,
    "Community_Board" as community_board, "BBL" as bbl, "Borough" as borough,
    "X_Coordinate_(State Plane)" as x_coordinate_state_plane,
    "Y_Coordinate_(State Plane)" as y_coordinate_state_plane,
    "Open_Data_Channel_Type" as open_data_channel_type,
    "Park_Facility_Name" as park_facility_name, "Park_Borough" as park_borough, "Vehicle_Type" as vehicle_type,
    "Taxi_Company_Borough" as taxi_company_borough,
    "Taxi_Pick_Up_Location" as taxi_pick_up_location, "Bridge_Highway_Name" as bridge_highway_name,
    "Bridge_Highway_Direction" as bridge_highway_direction,
    "Road_Ramp" as road_ramp, "Bridge_Highway_Segment" as bridge_highway_segment,
    "Latitude" as latitude, "Longitude" as longitude, "Location" as location
from nyc311."SERVICE_REQUEST"
left join map_complaint_type_open_nyc311
    on map_complaint_type_open_nyc311.complaint_type = lower(nyc311."SERVICE_REQUEST"."Complaint_Type")
left join map_incident_zip_nyc_borough
    on map_incident_zip_nyc_borough."Zip" = nyc311."SERVICE_REQUEST"."Incident_Zip";
select count(*) from sr_full; -- 27,769,609
select count(*) from nyc311.service_request; -- 27,769,609
-- The fact table.
create or replace table fact_service_quality (
  agency_id number(8, 0) not null,
  location_zip varchar(5) NOT null,
  type_id number(8, 0) not null,
  yearweek number(8, 0) not null,
  count int not null default 0,
  avg float default null,
  min int default null,
  max int default null,
  primary key (agency_id, location_zip, type_id,yearweek)
  , constraint agency_dim foreign key (agency_id) references dim_agency (agency_id)
  , constraint location_dim foreign key (location_zip) references dim_location (location_zip)
  , constraint quest_type_dim foreign key (type_id) references dim_request_type (type_id)
  , constraint yearweek_dim foreign key (yearweek) references dim_yearweek (yearweek)
);

-- Populating the fact table.
insert into fact_service_quality (agency_id, location_zip, type_id, yearweek, count, avg, min, max)
select dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id,
    sr_full.yearweek,
    count(*),
    avg(timestampdiff(hour, created_date, closed_date)),
    min(timestampdiff(hour, created_date, closed_date)),
    max(timestampdiff(hour, created_date, closed_date))
from sr_full
inner join dim_agency dim_agency on sr_full.Agency = dim_agency.agency_name
inner join dim_location dim_location on sr_full.incident_zip_id = dim_location.location_zip
inner join dim_request_type dim_request_type on sr_full.complaint_type_id = dim_request_type.type_id
inner join dim_yearweek dim_yearweek on sr_full.yearweek = dim_yearweek.yearweek
group by dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, sr_full.yearweek;

-- Number of cells/rows in the data warehouse.
select count(*) from fact_service_quality; -- 1,914,246

-- In theory there could be this many cell/rows: 126,397,440
select (select count(*) from dim_agency) as agency_count, (select count(*) from dim_location) as zip_count, (select count(*) from dim_request_type) as type_count, (select count(*) from dim_yearweek) as yearweek_count;

-- Same calculation with all permutaions for the four dimensions.
select count(*) from (
select agency_id, location_zip, type_id, yearweek
from dim_agency
cross join dim_location
cross join dim_request_type
cross join dim_yearweek) as T; -- 126,397,440


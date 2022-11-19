
### Dimension AGENCY
CREATE TABLE nyc311wh.dim_agency (
  agency_id int NOT NULL AUTO_INCREMENT,
  agency_name varchar(5) DEFAULT NULL,
  PRIMARY KEY (agency_id)
);
ALTER TABLE nyc311wh.dim_agency AUTO_INCREMENT = 3001;
INSERT INTO nyc311wh.dim_agency(agency_name) SELECT DISTINCT Agency AS agency_name FROM nyc311.`service_request` ORDER BY agency_name;

### Dimension LOCATION
CREATE TABLE nyc311wh.dim_location (
  location_zip varchar(5) NOT NULL,
  PRIMARY KEY (location_zip)
);
INSERT INTO nyc311wh.dim_location (location_zip) SELECT Zip FROM nyc311.zip_code_nyc_borough;

### Dimension REQUEST TYPE
CREATE TABLE nyc311wh.dim_request_type (
  type_id int NOT NULL,
  type_name varchar(30) NOT NULL,
  PRIMARY KEY (type_id)
);
INSERT INTO nyc311wh.dim_request_type (type_id, type_name) SELECT ID, `Type` FROM nyc311.ref_sr_type_nyc311_open_data_26;

### Dimension YEARWEEK
CREATE TABLE nyc311wh.dim_yearweek (
  yearweek int NOT NULL,
  PRIMARY KEY (yearweek)
);
INSERT INTO nyc311wh.dim_yearweek SELECT DISTINCT yearweek(`Created Date`) AS yearweek FROM nyc311.`service_request` ORDER BY yearweek;

### Create the zip map table — mapping valid ZIP codes to IDs.
CREATE TABLE map_incident_zip_nyc_borough AS
SELECT Zip, `Incident Zip`, Count FROM sr_incident_zip_summary
LEFT JOIN zip_code_nyc_borough ON `Incident Zip` = Zip;

### Create the complaint type map table — mapping vaild complaint types to complaint type IDs.
CREATE TABLE map_complaint_type_open_nyc311 AS
SELECT ID AS `Type ID`, `Complaint Type`, Count FROM sr_complaint_type_summary
LEFT JOIN ref_sr_type_nyc311_open_data_26 ON LOWER(REGEXP_REPLACE(`Complaint Type`, '[^[:alnum:]]+', '')) = LOWER(REGEXP_REPLACE(`Type`, '[^[:alnum:]]+', ''));

### Create the view for ALL SRs — replacing the `Complaint Type` with `Complaint Type ID` from complaint map table and `Incident Zip` with `Incident Zip ID` from ZIP map table?
CREATE VIEW sr_full AS
SELECT `Unique Key`, `Created Date`, `Closed Date`, Agency, `Agency Name`, `Type ID` `Complaint Type ID`, `Descriptor`, `Location Type`, Zip `Incident Zip ID`, `Incident Address`, `Street Name`, `Cross Street 1`, `Cross Street 2`, `Intersection Street 1`, `Intersection Street 2`, `Address Type`, City, Landmark, `Facility Type`, `Status`, `Due Date`, `Resolution Description`, `Resolution Action Updated Date`, `Community Board`, BBL, Borough, `X Coordinate (State Plane)`, `Y Coordinate (State Plane)`, `Open Data Channel Type`, `Park Facility Name`, `Park Borough`, `Vehicle Type`, `Taxi Company Borough`, `Taxi Pick Up Location`, `Bridge Highway Name`, `Bridge Highway Direction`, `Road Ramp`, `Bridge Highway Segment`, Latitude, Longitude, Location
FROM service_request
LEFT JOIN map_complaint_type_open_nyc311 ON map_complaint_type_open_nyc311.`Complaint Type` = service_request.`Complaint Type`
LEFT JOIN map_incident_zip_nyc_borough ON map_incident_zip_nyc_borough.Zip = service_request.`Incident Zip`;
-- SELECT COUNT(*) FROM service_request; -- 27,761,935 -- Just checking.
-- SELECT COUNT(*) FROM sr_full; -- 27,761,935 -- Just checking.

### The service quality fact table.
CREATE TABLE nyc311wh.fact_service_quality (
  agency_id int NOT NULL,
  location_zip varchar(5) NOT NULL,
  type_id int NOT NULL,
  yearweek int NOT NULL,
  count int NOT NULL DEFAULT '0',
  avg float DEFAULT NULL,
  min int DEFAULT NULL,
  max int DEFAULT NULL,
  PRIMARY KEY (agency_id,location_zip,type_id,yearweek),
  KEY location_dim_idx (location_zip),
  KEY quest_type_dim_idx (type_id),
  KEY yearweek_dim_idx (yearweek),
  CONSTRAINT agency_dim FOREIGN KEY (agency_id) REFERENCES dim_agency (agency_id),
  CONSTRAINT location_dim FOREIGN KEY (location_zip) REFERENCES dim_location (location_zip),
  CONSTRAINT quest_type_dim FOREIGN KEY (type_id) REFERENCES dim_request_type (type_id),
  CONSTRAINT yearweek_dim FOREIGN KEY (yearweek) REFERENCES dim_yearweek (yearweek)
);

### Populate the fact table.
INSERT INTO nyc311wh.fact_service_quality (agency_id, location_zip, type_id, yearweek, count, avg, min, max)
SELECT dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, yearweek(`Created Date`), count(*), avg(TIMESTAMPDIFF(HOUR, `Created Date`, `Closed Date`)), min(TIMESTAMPDIFF(HOUR, `Created Date`, `Closed Date`)), max(TIMESTAMPDIFF(HOUR, `Created Date`, `Closed Date`))
FROM sr_full
INNER JOIN nyc311wh.dim_agency dim_agency ON sr_full.Agency = dim_agency.agency_name
INNER JOIN nyc311wh.dim_location dim_location ON sr_full.`Incident Zip ID` = dim_location.location_zip
INNER JOIN nyc311wh.dim_request_type dim_request_type ON sr_full.`Complaint Type ID` = dim_request_type.type_id
INNER JOIN nyc311wh.dim_yearweek dim_yearweek ON yearweek(sr_full.`Created Date`) = dim_yearweek.yearweek
GROUP BY dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, dim_yearweek.yearweek;

### Slicing.
SELECT yearweek, SUM(count) FROM nyc311wh.fact_service_quality
WHERE type_id = (SELECT type_id FROM nyc311wh.dim_request_type WHERE type_name = 'Rodent')
AND location_zip IN (SELECT Zip FROM nyc311.zip_code_nyc_borough WHERE Borough = 'Manhattan')
GROUP BY yearweek;

### Calculating the potential number of cells in the fact quality table: 126,397,440 cells.
SELECT 32 * 240 * 26 * 633;
SELECT count(*) FROM (
SELECT agency_id, location_zip, type_id, yearweek
FROM nyc311wh.dim_agency
CROSS JOIN nyc311wh.dim_location
CROSS JOIN nyc311wh.dim_request_type
CROSS JOIN nyc311wh.dim_yearweek) AS T;
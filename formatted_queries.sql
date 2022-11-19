##1) missing due dates
SELECT '10k sample' AS sample,
       round(100 * (sum(CASE
                            WHEN `due date` IS NULL THEN 1
                            ELSE 0
                        END) / count(*)), 2) AS `% missing due date`
FROM nyc311.sr_10k
UNION ALL
SELECT '1k sample',
       round(100 * (sum(CASE
                            WHEN `due date` IS NULL THEN 1
                            ELSE 0
                        END) / count(*)), 2) AS `% missing due date`
FROM nyc311.sr_1k;

#2)created date AFTER closed date
SELECT '10k sample' AS sample,
       100 * round((sum(CASE
                            WHEN `created date` > `closed date` THEN 1
                            ELSE 0
                        END) /count(`unique key`)),3) AS '% Created Date After Closed Date'
FROM nyc311.sr_10k AS t1
UNION ALL
SELECT '1k sample',
       100 * round((sum(CASE
                            WHEN `created date` > `closed date` THEN 1
                            ELSE 0
                        END) /count(`unique key`)),3) AS '% Created Date After Closed Date'
FROM nyc311.sr_1k AS t1;

## 3) due date BEFORE created date
SELECT '10k sample' AS sample,
       100 * round((sum(CASE
                            WHEN `due date` < `created date` THEN 1
                            ELSE 0
                        END) /count(`unique key`)),3) AS '% Due Date Before Created Date'
FROM nyc311.sr_10k AS t1
UNION ALL
SELECT '1k sample',
       100 * round((sum(CASE
                            WHEN `due date` < `created date` THEN 1
                            ELSE 0
                        END) /count(`unique key`)),3)
FROM nyc311.sr_1k AS t1;

#4) status NOT MATCH closed date
SELECT @totalrows:= count(`unique key`)
FROM nyc311.sr_10k;


SELECT @totalrows2:= count(`unique key`)
FROM nyc311.sr_1k;


SELECT '10k' AS 'sample',
       sum(`% status not match closed date `) AS ` % status not match closed date`
FROM
  (SELECT 'Closed Null but Status Closed' AS 'Match Fail Type',
          100 * count(`unique key`) / @totalrows AS `% status not match closed date `
   FROM nyc311.sr_10k
   WHERE `closed date` IS NULL
     AND `status` = 'Closed'
   UNION ALL #select `unique key`,
             `closed date`,
             `status`
   FROM SELECT 'Closed Value but Status Not Closed' AS 'type',
               100 * count(`unique key`) / @totalrows
   FROM nyc311.sr_10k
   WHERE `closed date` IS NOT NULL
     AND `status` in ('Open',
                      'Assigned',
                      'Pending',
                      'In Progress')) AS t2
UNION ALL
SELECT '1k',
       sum(`% status not match closed date `) AS ` % status not match closed date`
FROM
  (SELECT 'Closed Null but Status Closed' AS 'Match Fail Type',
          100 * count(`unique key`) / @totalrows2 AS `% status not match closed date `
   FROM nyc311.sr_1k
   WHERE `closed date` IS NULL
     AND `status` = 'Closed'
   UNION ALL #select `unique key`,
             `closed date`,
             `status`
   FROM SELECT 'Closed Value but Status Not Closed' AS 'type',
               100 * count(`unique key`) / @totalrows2
   FROM nyc311.sr_1k
   WHERE `closed date` IS NOT NULL
     AND `status` in ('Open',
                      'Assigned',
                      'Pending',
                      'In Progress')) AS t2;

##5) zip missingness
SELECT @totalcounts2:= sum(`count`)
FROM nyc311.sr_incident_zip_summary;


SELECT 100 * sum(COUNT) / @totalcounts2 AS `% missing zip`
FROM nyc311.sr_incident_zip_summary a
WHERE `incident zip` IS NULL
  OR `incident zip` in ("");

##6) zips that ARE not in the reference dataset 'zip_code_nyc_borough' WITH totalcounts AS
  (SELECT sum(COUNT) AS totalcount
   FROM nyc311.sr_incident_zip_summary)
SELECT 100 * sum(COUNT) /
  (SELECT sum(totalcount)
   FROM (TABLE totalcounts) c) AS `% zip no borough`
FROM (`nyc311`.`sr_incident_zip_summary` `a`
      LEFT JOIN `nyc311`.`zip_code_nyc_borough` `b` ON ((`a`.`incident zip` = `b`.`zip`)))
WHERE (`b`.`zip` IS NULL);

##or alternatively

WITH reftable1 AS
  (SELECT a.count,
          c.borough
   FROM nyc311.sr_incident_zip_summary a
   JOIN nyc311.map_incident_zip_nyc_borough b ON a.`incident zip` = b.`incident zip`
   LEFT JOIN nyc311.zip_code_nyc_borough c ON b.`zip` = c.`zip`)
SELECT 100* sum(COUNT) /
  (SELECT sum(COUNT)
   FROM (TABLE reftable1) a2) AS `% zip no borough`
FROM (TABLE reftable1) a1
WHERE borough IS NULL ##7) invaid zip codes
  SELECT sum(COUNT) / @totalcounts3 * 100 AS `%invalid zip codes`
  FROM nyc311.sr_incident_zip_summary WHERE NOT (`incident zip` regexp '^[0-9]{5}$');

#and NOT `incident zip` IS NULL;

##it seems based ON the map_incident_zip_nyc_borough TABLE that the VALID format
FOR a zip IS a 5 digit number ##complaint TYPE field quality ## 8) complaint types not in reference DATA
SELECT (sum(COUNT) /
          (SELECT sum(COUNT)
           FROM nyc311.sr_complaint_type_summary)) * 100 AS 'No Reference Match %'
FROM nyc311.sr_complaint_type_summary
WHERE `complaint type` not IN
    (SELECT `type`
     FROM nyc311.ref_sr_type_nyc311_open_data_26
     UNION SELECT `problem_area`
     FROM nyc311.ref_problem_areas_nyc311_portal_57) ;

## 9) borough zip consistency. percentage OF boroughs in EACH TABLE that DO NOT refer TO the same incident zip int ##the DATA sample
SELECT '10k sample'AS sample,
       sum(CASE
               WHEN a.borough != b.borough THEN 1
               ELSE 0
           END) / count(`unique key`) * 100 AS `%unmatched boroughs`
FROM nyc311.service_request_sample_10k a
JOIN nyc311.zip_code_nyc_borough b ON a.`incident zip` = b.`zip`
UNION
SELECT '1k sample'AS sample,
       sum(CASE
               WHEN a.borough != b.borough THEN 1
               ELSE 0
           END) / count(`unique key`) * 100 AS `%unmatched boroughs`
FROM nyc311.service_request_sample_1k a
JOIN nyc311.zip_code_nyc_borough b ON a.`incident zip` = b.`zip`;

##10 ) park boroughs that are inconsistent with reference data

WITH reftable AS
  (SELECT `borough`
   FROM nyc311.zip_code_nyc_borough)
SELECT '10k' AS sample,
       (count(*) /
          (SELECT count(*)
           FROM nyc311.service_request_sample_10k)) * 100 AS `% not match park boroughs`
FROM nyc311.service_request_sample_10k
WHERE `park borough` not in (TABLE reftable)
UNION
SELECT '1K',
       (count(*) /
          (SELECT count(*)
           FROM nyc311.service_request_sample_1k)) * 100 AS `% invalid park boroughs`
FROM nyc311.service_request_sample_1k
WHERE `park borough` not in (TABLE reftable);

##11)park borough incompleteness 

WITH reftable AS
  (SELECT `borough`
   FROM nyc311.zip_code_nyc_borough)
SELECT '10k' AS sample,
       (count(*) /
          (SELECT count(*)
           FROM nyc311.service_request_sample_10k)) * 100 AS `% missing park boroughs`
FROM nyc311.service_request_sample_10k
WHERE `park borough` IS NULL
UNION
SELECT '1k' AS sample,
       (count(*) /
          (SELECT count(*)
           FROM nyc311.service_request_sample_1k)) * 100 AS `% missing park boroughs`
FROM nyc311.service_request_sample_1k
WHERE `park borough` IS NULL;

## 12) testing the completeness OF the START dates

SELECT 100 * count(*)/ 10000 AS `% complete created date `
FROM nyc311.sr_10k
WHERE `created date` IS NOT NULL ;

/* /*the subquery itself
SELECT 'Closed Null but Status Closed' AS 'Match Fail Type',
       100 * count(`unique key`) / @totalrows AS `% status not match closed date `
FROM nyc311.sr_10k
WHERE `closed date` IS NULL
  AND `status` = 'Closed'
UNION ALL #select `unique key`,
          `closed date`,
          `status`
FROM
SELECT 'Closed Value but Status Not Closed' AS 'type',
       100 * count(`unique key`) / @totalrows
FROM nyc311.sr_10k
WHERE `closed date` IS NOT NULL
  AND `status` in ('Open',
                   'Assigned',
                   'Pending',
                   'In Progress');

##another way TO WRITE this ##association BETWEEN service request types / problem areas AND "the available values" ###how many requests DO NOT fall INTO these categories. this falls UNDER DATA consistency
SELECT @totalcounts4:= sum(`count`)
FROM nyc311.sr_complaint_type_summary;


SELECT sum(COUNT) AS `freq. not existing type category`,
       100 * sum(COUNT) / @totalcounts4 AS `% not existing type category requests`
FROM nyc311.sr_complaint_type_summary
WHERE `complaint type` not in
    (SELECT `type`
     FROM nyc311.ref_sr_type_nyc311_open_data_26);

##6)
SELECT @totalcounts3 := sum(COUNT)
FROM nyc311.sr_incident_zip_summary;


SELECT sum(a.count) * 100 / @totalcounts3 AS `no borough %`
FROM nyc311.sr_incident_zip_summary a
JOIN nyc311.map_incident_zip_nyc_borough b ON a.`incident zip` = b.`incident zip` #left
JOIN nyc311.zip_code_nyc_borough c ON b.`zip` = c.`zip`;


WHERE a.`incident zip` IS NOT NULL
  AND b.`zip` IS NULL;


SELECT 'Complaint Type' AS 'Reference Match %',
       (sum(COUNT) /
          (SELECT sum(COUNT)
           FROM nyc311.sr_complaint_type_summary)) * 100 AS value
FROM nyc311.sr_complaint_type_summary
WHERE `complaint type` IN
    (SELECT `type`
     FROM nyc311.ref_sr_type_nyc311_open_data_26) -- 11,024,708 / 27,736,190 = 39.7485
UNION ALL ##it seems there ARE SOME problem areas that have filtered through TO complaint types AS well. will TREAT these AS SIMILAR things
SELECT 'Problem Area',
       (sum(COUNT) /
          (SELECT sum(COUNT)
           FROM nyc311.sr_complaint_type_summary)) * 100
FROM nyc311.sr_complaint_type_summary
WHERE `complaint type` in
    (SELECT `problem_area`
     FROM nyc311.ref_problem_areas_nyc311_portal_57);
-- Count orphan records
-- change schema, parent_table, child_table, and key
SELECT 
  COUNT(*) as n_orphans 
FROM schema.child_table c
WHERE NOT EXISTS (SELECT 
		    key 
		  FROM schema.parent_table p 
		  WHERE p.key = c.key);

-- Count replication errors
-- change schema, original_table, derived_table, key, and field_name
SELECT 
  COUNT(*) as n_rep_errors
FROM schema.original_table a 
INNER JOIN schema.derived_table ON a.key = b.key
WHERE a.field_name != b.field_name;

-- Valueset validation; returns count of invalid records for a given field
-- change field_name, schema, and table
SELECT 
  SUM(CASE WHEN field_name NOT IN valueset THEN 1 ELSE 0 END) as n_invalid
FROM schema.table

-- Count extreme values for a numeric field
-- change schema, table, field_name, low_val, and high_val
SELECT
  COUNT(*) as n_extreme
FROM schema.table
WHERE field_name NOT BETWEEN low_val AND high_val;

-- Calculate events per encounter type
-- change schema, table, provide enc_type_vals
SELECT
  a.enc_type, a.n, b.n_enc, (a.n / b.enc_type) AS ratio
FROM (
  SELECT
    enc_type, COUNT(*) as n
  FROM schema.table
  WHERE enc_type IN enc_type_vals
  GROUP BY enc_type
  ) a
FULL OUTER JOIN (
  SELECT field_type, COUNT(*) as n_enc
  FROM schema.ENCOUNTER
  GROUP BY enc_type
  ) b
ON a.enc_type = b.enc_type;

-- Calculate percentage of patients with encounters who have diagnosis (or procedure) records
-- change schema and table (diagnosis or procedure table)
SELECT 
  100 * ROUND((n / n_enc), 3) as perc_patients_with_records
FROM (
  SELECT
    COUNT(DISTINCT patid) as n, 1 as id
  FROM schema.table
  ) a
LEFT JOIN (
  SELECT
    COUNT(DISTINCT patid) as n_enc, 1 as id
  FROM schema.ENCOUNTER
  ) b
ON a.id = b.id;

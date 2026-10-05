-- verify_counts.sql — Validate landing layer row counts against Kaggle
--
-- Expected counts per Kaggle dataset documentation:
--   events:          2,756,101
--   category_tree:   1,669
--   item_properties: 20,275,902
--
-- All three rows should return 'PASS'.

SELECT
  'events' AS table_name,
  COUNT(*) AS actual_count,
  2756101 AS expected_count,
  IF(COUNT(*) = 2756101, 'PASS', 'FAIL') AS result
FROM `<PROJECT>.<DATASET>.events`

UNION ALL

SELECT
  'category_tree',
  COUNT(*),
  1669,
  IF(COUNT(*) = 1669, 'PASS', 'FAIL')
FROM `<PROJECT>.<DATASET>.category_tree`

UNION ALL

SELECT
  'item_properties',
  COUNT(*),
  20275902,
  IF(COUNT(*) = 20275902, 'PASS', 'FAIL')
FROM `<PROJECT>.<DATASET>.item_properties`;
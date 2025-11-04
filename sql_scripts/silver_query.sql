-- 1. Incremental load into Silver
INSERT INTO `ai-health-risk-system.health_data.health_silver`
SELECT
  JSON_VALUE(payload, '$.Patient_ID') AS Patient_ID,
  INITCAP(TRIM(JSON_VALUE(payload, '$.Name'))) AS Name,
  SAFE_CAST(JSON_VALUE(payload, '$.Age') AS INT64) AS Age,
  CASE
    WHEN LOWER(JSON_VALUE(payload, '$.Gender')) IN ('male', 'm') THEN 'Male'
    WHEN LOWER(JSON_VALUE(payload, '$.Gender')) IN ('female', 'f') THEN 'Female'
    ELSE 'Unknown'
  END AS Gender,
  SAFE_CAST(JSON_VALUE(payload, '$.HeartRate') AS FLOAT64) AS HeartRate,
  SAFE_CAST(JSON_VALUE(payload, '$.BloodPressure') AS FLOAT64) AS BloodPressure,
  SAFE_CAST(JSON_VALUE(payload, '$.Temperature') AS FLOAT64) AS Temperature,
  TRIM(JSON_VALUE(payload, '$.MedicalCondition')) AS MedicalCondition,
  SAFE_CAST(JSON_VALUE(payload, '$.RiskScore') AS FLOAT64) AS RiskScore,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', JSON_VALUE(payload, '$.DateOfAdmission')) AS DateOfAdmission,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', JSON_VALUE(payload, '$.EventTimestamp')) AS EventTimestamp,
  CASE
    WHEN SAFE_CAST(JSON_VALUE(payload, '$.Age') AS INT64) BETWEEN 0 AND 120
      AND SAFE_CAST(JSON_VALUE(payload, '$.HeartRate') AS FLOAT64) BETWEEN 40 AND 180
      AND SAFE_CAST(JSON_VALUE(payload, '$.BloodPressure') AS FLOAT64) BETWEEN 80 AND 200
      AND SAFE_CAST(JSON_VALUE(payload, '$.Temperature') AS FLOAT64) BETWEEN 90 AND 110
      AND SAFE_CAST(JSON_VALUE(payload, '$.RiskScore') AS FLOAT64) BETWEEN 0 AND 1
    THEN 'VALID'
    ELSE 'INVALID'
  END AS DataQualityFlag,
  IngestionID,
  IngestionTS,
  CURRENT_TIMESTAMP() AS ProcessingTS
FROM `ai-health-risk-system.health_data.health_raw` r
WHERE JSON_VALUE(payload, '$.Patient_ID') IS NOT NULL
  AND SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', JSON_VALUE(payload, '$.EventTimestamp')) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM `ai-health-risk-system.health_data.health_silver` s
      WHERE s.Patient_ID = JSON_VALUE(r.payload, '$.Patient_ID')
        AND s.EventTimestamp = SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', JSON_VALUE(r.payload, '$.EventTimestamp'))
  );

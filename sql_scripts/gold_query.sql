-- 2. Incremental load into Gold
INSERT INTO `ai-health-risk-system.health_data.health_gold`
SELECT *
FROM `ai-health-risk-system.health_data.health_silver` s
WHERE s.DataQualityFlag = 'VALID'
  AND NOT EXISTS (
      SELECT 1
      FROM `ai-health-risk-system.health_data.health_gold` g
      WHERE g.Patient_ID = s.Patient_ID
        AND g.EventTimestamp = s.EventTimestamp
  );

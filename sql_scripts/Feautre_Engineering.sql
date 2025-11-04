-- Step 1: Prepare features for modeling
CREATE OR REPLACE VIEW `ai-health-risk-system.health_data.health_gold_features` AS
SELECT
  Patient_ID,
  Name,
  Age,
  Gender,
  SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
  SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
  SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
  SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore,
  MedicalCondition,
  DateOfAdmission
FROM `ai-health-risk-system.health_data.health_gold`
WHERE RiskScore IS NOT NULL;

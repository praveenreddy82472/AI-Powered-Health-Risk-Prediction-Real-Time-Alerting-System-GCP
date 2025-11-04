-- Step 1: Predict HeartRate
WITH pred_heart_rate AS (
  SELECT
    Patient_ID,
    predicted_HeartRate
  FROM ML.PREDICT(MODEL `ai-health-risk-system.health_data.model_heart_rate`,
    (
      SELECT
        Patient_ID,
        Age,
        SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
        SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
        SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore
      FROM `ai-health-risk-system.health_data.health_gold_features`
    )
  )
),

-- Step 2: Predict BloodPressure
pred_blood_pressure AS (
  SELECT
    Patient_ID,
    predicted_BloodPressure
  FROM ML.PREDICT(MODEL `ai-health-risk-system.health_data.model_BloodPressure`,
    (
      SELECT
        Patient_ID,
        Age,
        SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
        SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
        SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore
      FROM `ai-health-risk-system.health_data.health_gold_features`
    )
  )
),

-- Step 3: Predict Temperature
pred_temperature AS (
  SELECT
    Patient_ID,
    predicted_Temperature
  FROM ML.PREDICT(MODEL `ai-health-risk-system.health_data.model_Temperature`,
    (
      SELECT
        Patient_ID,
        Age,
        SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
        SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
        SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore
      FROM `ai-health-risk-system.health_data.health_gold_features`
    )
  )
),

-- Step 4: Predict RiskScore
pred_risk_score AS (
  SELECT
    Patient_ID,
    predicted_RiskScore
  FROM ML.PREDICT(MODEL `ai-health-risk-system.health_data.model_RiskScore`,
    (
      SELECT
        Patient_ID,
        Age,
        SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
        SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
        SAFE_CAST(Temperature AS FLOAT64) AS Temperature
      FROM `ai-health-risk-system.health_data.health_gold_features`
    )
  )
)

-- Step 5: Join all predictions
SELECT
  g.Patient_ID,
  g.Name,
  g.Age,
  g.Gender,
  g.MedicalCondition,
  g.HeartRate,
  g.BloodPressure,
  g.Temperature,
  g.RiskScore,
  h.predicted_HeartRate,
  b.predicted_BloodPressure,
  t.predicted_Temperature,
  r.predicted_RiskScore
FROM `ai-health-risk-system.health_data.health_gold_features` g
LEFT JOIN pred_heart_rate h USING (Patient_ID)
LEFT JOIN pred_blood_pressure b USING (Patient_ID)
LEFT JOIN pred_temperature t USING (Patient_ID)
LEFT JOIN pred_risk_score r USING (Patient_ID);

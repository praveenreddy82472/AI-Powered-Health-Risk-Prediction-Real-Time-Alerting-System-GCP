CREATE OR REPLACE MODEL `ai-health-risk-system.health_data.model_heart_rate`
OPTIONS(
  MODEL_TYPE='BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS=['HeartRate'],
  DATA_SPLIT_METHOD='RANDOM',
  DATA_SPLIT_EVAL_FRACTION=0.2,
  MAX_ITERATIONS=50
) AS
SELECT
  Age,
  SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
  SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
  SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore,
  HeartRate
FROM `ai-health-risk-system.health_data.health_gold_features`;


CREATE OR REPLACE MODEL `ai-health-risk-system.health_data.model_BloodPressure`
OPTIONS(
  MODEL_TYPE='BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS=['BloodPressure'],
  DATA_SPLIT_METHOD='RANDOM',
  DATA_SPLIT_EVAL_FRACTION=0.2,
  MAX_ITERATIONS=50
) AS
SELECT
  Age,
  SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
  SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
  SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore,
  BloodPressure
FROM `ai-health-risk-system.health_data.health_gold_features`;


CREATE OR REPLACE MODEL `ai-health-risk-system.health_data.model_Temperature`
OPTIONS(
  MODEL_TYPE='BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS=['Temperature'],
  DATA_SPLIT_METHOD='RANDOM',
  DATA_SPLIT_EVAL_FRACTION=0.2,
  MAX_ITERATIONS=50
) AS
SELECT
  Age,
  SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
  SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
  SAFE_CAST(RiskScore AS FLOAT64) AS RiskScore,
  Temperature
FROM `ai-health-risk-system.health_data.health_gold_features`;


CREATE OR REPLACE MODEL `ai-health-risk-system.health_data.model_RiskScore`
OPTIONS(
  MODEL_TYPE='BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS=['RiskScore'],
  DATA_SPLIT_METHOD='RANDOM',
  DATA_SPLIT_EVAL_FRACTION=0.2,
  MAX_ITERATIONS=50
) AS
SELECT
  Age,
  SAFE_CAST(HeartRate AS FLOAT64) AS HeartRate,
  SAFE_CAST(BloodPressure AS FLOAT64) AS BloodPressure,
  SAFE_CAST(Temperature AS FLOAT64) AS Temperature,
  RiskScore
FROM `ai-health-risk-system.health_data.health_gold_features`;


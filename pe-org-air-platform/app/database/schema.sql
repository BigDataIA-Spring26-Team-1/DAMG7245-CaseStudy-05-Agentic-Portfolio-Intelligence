-- ==========================================
-- ORGAIR PLATFORM FOUNDATION - SNOWFLAKE DDL
-- (Snowflake-compatible: no CHECK, no INDEX)
-- ==========================================
 
-- =========================================================
-- CS1: Core Foundation
-- =========================================================
 
-- Industries table
CREATE TABLE IF NOT EXISTS industries (
    id STRING PRIMARY KEY,
    name STRING NOT NULL UNIQUE,
    sector STRING NOT NULL,
    hr_base NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id STRING PRIMARY KEY,
    name STRING NOT NULL,
    ticker STRING UNIQUE,
    industry_id STRING,
    position_factor NUMBER(4,3) DEFAULT 0.0,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_companies_industry
        FOREIGN KEY (industry_id) REFERENCES industries(id)
);
 
-- Assessments table
CREATE TABLE IF NOT EXISTS assessments (
    id STRING PRIMARY KEY,
    company_id STRING NOT NULL,
    assessment_type STRING NOT NULL,
    assessment_date DATE NOT NULL,
    status STRING DEFAULT 'draft',
    primary_assessor STRING,
    secondary_assessor STRING,
    vr_score NUMBER(5,2),
    confidence_lower NUMBER(5,2),
    confidence_upper NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_assessments_company
        FOREIGN KEY (company_id) REFERENCES companies(id)
);
 
-- Dimension scores table
CREATE TABLE IF NOT EXISTS dimension_scores (
    id STRING PRIMARY KEY,
    assessment_id STRING NOT NULL,
    dimension STRING NOT NULL,
    score NUMBER(5,2) NOT NULL,
    weight NUMBER(4,3),
    confidence NUMBER(4,3) DEFAULT 0.8,
    evidence_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_dimension_scores_assessment
        FOREIGN KEY (assessment_id) REFERENCES assessments(id),
    CONSTRAINT uq_assessment_dimension UNIQUE (assessment_id, dimension)
);
 
-- =========================================================
-- CS2: Evidence Collection (Documents + Chunks)
-- =========================================================
 
CREATE TABLE IF NOT EXISTS documents (
  id STRING PRIMARY KEY,
  company_id STRING NOT NULL,
  ticker STRING NOT NULL,
  filing_type STRING NOT NULL,          -- 10-K / 10-Q / 8-K
  filing_date DATE NOT NULL,
  source_url STRING,
  local_path STRING,
  s3_key STRING,
  content_hash STRING,
  word_count INT,
  chunk_count INT,
  status STRING DEFAULT 'pending',
  error_message STRING,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  processed_at TIMESTAMP_NTZ,
  CONSTRAINT uq_documents_content_hash UNIQUE (content_hash),
  CONSTRAINT fk_documents_company
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
 
CREATE TABLE IF NOT EXISTS document_chunks (
  id STRING PRIMARY KEY,
  document_id STRING NOT NULL,
  chunk_index INT NOT NULL,
  content STRING NOT NULL,
  section STRING,
  start_char INT,
  end_char INT,
  word_count INT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT uq_doc_chunk UNIQUE (document_id, chunk_index),
  CONSTRAINT fk_document_chunks_document
    FOREIGN KEY (document_id) REFERENCES documents(id)
);
 
-- =========================================================
-- CS1 Seed: Industry reference rows (idempotent)
-- =========================================================
 
MERGE INTO industries t
USING (
    SELECT '550e8400-e29b-41d4-a716-446655440001' AS id, 'Manufacturing' AS name, 'Industrials' AS sector, 72 AS hr_base UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440002', 'Healthcare Services', 'Healthcare', 78 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440003', 'Business Services', 'Services', 75 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440004', 'Retail', 'Consumer', 70 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440005', 'Financial Services', 'Financial', 80
) s
ON t.id = s.id
WHEN NOT MATCHED THEN
INSERT (id, name, sector, hr_base)
VALUES (s.id, s.name, s.sector, s.hr_base);
 
-- =========================================================
-- CS2: External Signals
-- =========================================================
 
CREATE TABLE IF NOT EXISTS external_signals (
  id STRING PRIMARY KEY,
  company_id STRING NOT NULL,
  ticker STRING NOT NULL,
  signal_type STRING NOT NULL,          -- jobs/news/patents/tech
  source STRING NOT NULL,               -- greenhouse/google_news_rss/uspto/etc
  title STRING,
  url STRING,
  published_at TIMESTAMP_NTZ,
  collected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  content_text STRING,
  content_hash STRING,
  metadata VARIANT,
  CONSTRAINT uq_external_signals_hash UNIQUE (content_hash),
  CONSTRAINT fk_external_signals_company
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
 
CREATE TABLE IF NOT EXISTS company_signal_summaries (
  id STRING PRIMARY KEY,
  company_id STRING NOT NULL,
  ticker STRING NOT NULL,
  as_of_date DATE NOT NULL,
 
  summary_text STRING NOT NULL,
  signal_count INT DEFAULT 0,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT uq_company_signal_summary_company_day UNIQUE (company_id, as_of_date),
  CONSTRAINT fk_company_signal_summaries_company
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
 
-- =========================================================
-- CS3: Scoring Engine (Scores + Config + Audit)
-- =========================================================
 
CREATE TABLE IF NOT EXISTS scoring_runs (
  id STRING PRIMARY KEY,
  run_timestamp TIMESTAMP_NTZ NOT NULL,
  companies_scored VARIANT,
  model_version STRING,
  parameters_json VARIANT,
  status STRING,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE TABLE IF NOT EXISTS org_air_scores (
  id STRING PRIMARY KEY,
  company_id STRING NOT NULL,
  assessment_id STRING,
  vr_score NUMBER(5,2),
  synergy_bonus NUMBER(5,2),
  talent_penalty NUMBER(5,2),
  sem_lower NUMBER(5,2),
  sem_upper NUMBER(5,2),
  composite_score NUMBER(5,2),
  score_band STRING,
  dimension_breakdown_json VARIANT,
  scoring_run_id STRING,
  scored_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT fk_org_air_scores_company
    FOREIGN KEY (company_id) REFERENCES companies(id),
  CONSTRAINT fk_org_air_scores_assessment
    FOREIGN KEY (assessment_id) REFERENCES assessments(id),
  CONSTRAINT fk_org_air_scores_run
    FOREIGN KEY (scoring_run_id) REFERENCES scoring_runs(id)
);
 
CREATE TABLE IF NOT EXISTS scoring_audit_log (
  id STRING PRIMARY KEY,
  scoring_run_id STRING NOT NULL,
  company_id STRING,
  step_name STRING NOT NULL,
  input_json VARIANT,
  output_json VARIANT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT fk_scoring_audit_log_run
    FOREIGN KEY (scoring_run_id) REFERENCES scoring_runs(id),
  CONSTRAINT fk_scoring_audit_log_company
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
 
CREATE TABLE IF NOT EXISTS sector_baselines (
  id STRING PRIMARY KEY,
  sector_name STRING NOT NULL,
  dimension STRING,
  weight NUMBER(4,3),
  hr_baseline_value NUMBER(5,2),
  version STRING NOT NULL,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE TABLE IF NOT EXISTS synergy_config (
  id STRING PRIMARY KEY,
  dimension_a STRING NOT NULL,
  dimension_b STRING NOT NULL,
  synergy_type STRING NOT NULL,  -- positive | negative
  threshold NUMBER(5,2) NOT NULL,
  magnitude NUMBER(5,2) NOT NULL,
  version STRING NOT NULL,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
CREATE TABLE IF NOT EXISTS talent_penalty_config (
  id STRING PRIMARY KEY,
  hhi_threshold_mild NUMBER(5,3) NOT NULL,
  hhi_threshold_severe NUMBER(5,3) NOT NULL,
  penalty_factor_mild NUMBER(5,3) NOT NULL,
  penalty_factor_severe NUMBER(5,3) NOT NULL,
  min_sample_size INT NOT NULL,
  version STRING NOT NULL,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
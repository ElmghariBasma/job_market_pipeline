-- models/silver/silver_jobs.sql
-- ============================================================
-- Couche Silver Snowflake
-- ============================================================

{{ config(materialized='table', schema='silver') }}

WITH

base AS (
    -- Référence directe à la table bronze.raw_jobs
    SELECT 
        id AS job_id,
        loaded_at,
        scraped_at,
        source,
        raw_json:url::STRING AS url,
        raw_json:titre::STRING AS titre_raw,
        raw_json:entreprise::STRING AS entreprise_raw,
        raw_json:ville::STRING AS ville_raw,
        raw_json:description::STRING AS description_raw,
        raw_json:secteur::STRING AS secteur_raw,
        raw_json:niveau_etudes::STRING AS niveau_etudes_raw,
        raw_json:experience::STRING AS experience_raw,
        raw_json:nb_postes::STRING AS nb_postes,
        raw_json:date_publication::STRING AS date_publication_raw,
        raw_json:date_limite::STRING AS date_limite_raw,
        raw_json:type_contrat::STRING AS type_contrat_raw,
        raw_json:teletravail::STRING AS teletravail_raw
    FROM {{ source('bronze', 'raw_jobs') }}
),

villes AS (
    SELECT ville_raw, ville_normalisee, region
    FROM {{ ref('villes_mapping') }}
),

joined AS (
    SELECT
        b.job_id,
        b.loaded_at,
        b.scraped_at,
        b.source,
        b.url,

        INITCAP(TRIM(b.titre_raw))                          AS titre,
        TRIM(b.entreprise_raw)                              AS entreprise,
        COALESCE(v.ville_normalisee, INITCAP(TRIM(b.ville_raw))) AS ville,
        COALESCE(v.region, 'Non précisé')                  AS region,
        b.ville_raw,
        TRIM(b.description_raw)                             AS description,
        TRIM(b.secteur_raw)                                 AS secteur,
        TRIM(b.niveau_etudes_raw)                           AS niveau_etudes,
        b.experience_raw,
        TRY_CAST(b.nb_postes AS NUMBER)                     AS nb_postes,

        TRY_TO_DATE(b.date_publication_raw, 'DD/MM/YYYY')  AS date_publication,
        TRY_TO_DATE(b.date_limite_raw, 'DD/MM/YYYY')       AS date_limite,

        b.type_contrat_raw,
        b.teletravail_raw

    FROM base b
    LEFT JOIN villes v
        ON LOWER(TRIM(b.ville_raw)) = LOWER(v.ville_raw)
),

-- (le reste de ton code SQL - contrats, experience, tech_flag, deduplication - garde-le tel quel)
-- ... mais n'oublie pas de mettre les bonnes colonnes dans la SELECT finale

contrats AS (
    SELECT *,
        CASE
            WHEN LOWER(type_contrat_raw) LIKE '%cdi%'        THEN 'CDI'
            WHEN LOWER(type_contrat_raw) LIKE '%cdd%'        THEN 'CDD'
            WHEN LOWER(type_contrat_raw) LIKE '%stage%'      THEN 'Stage'
            WHEN LOWER(type_contrat_raw) LIKE '%freelance%'  THEN 'Freelance'
            WHEN LOWER(type_contrat_raw) LIKE '%alternance%' THEN 'Alternance'
            WHEN LOWER(type_contrat_raw) LIKE '%interim%'
              OR LOWER(type_contrat_raw) LIKE '%intérim%'    THEN 'Intérim'
            ELSE COALESCE(TRIM(type_contrat_raw), 'Non précisé')
        END AS type_contrat,

        CASE
            WHEN LOWER(teletravail_raw) IN ('oui','yes','hybride','partiel','full')
                THEN TRUE
            WHEN LOWER(teletravail_raw) IN ('non','no')
                THEN FALSE
            ELSE NULL
        END AS is_teletravail

    FROM joined
),

experience AS (
    SELECT *,

        -- REGEXP_SUBSTR Snowflake : extraire le premier nombre
        -- Pattern : "3 à 5 ans" → 3
        CASE
            WHEN REGEXP_LIKE(experience_raw, '.*\\d+\\s*(à|a|-)\\s*\\d+\\s*ans?.*')
                THEN TRY_TO_NUMBER(REGEXP_SUBSTR(experience_raw, '(\\d+)', 1, 1))
            WHEN LOWER(experience_raw) LIKE '%débutant%'
              OR LOWER(experience_raw) LIKE '%sans expérience%'
                THEN 0
            ELSE NULL
        END AS experience_min_ans,

        CASE
            WHEN REGEXP_LIKE(experience_raw, '.*\\d+\\s*(à|a|-)\\s*(\\d+)\\s*ans?.*')
                THEN TRY_TO_NUMBER(REGEXP_SUBSTR(experience_raw, '\\d+\\s*(à|a|-)\\s*(\\d+)', 1, 1, 'e', 2))
            ELSE NULL
        END AS experience_max_ans

    FROM contrats
),

tech_flag AS (
    SELECT *,
        CASE WHEN
            LOWER(secteur)     LIKE '%informatique%'
            OR LOWER(secteur)  LIKE '%internet%'
            OR LOWER(secteur)  LIKE '%multimédia%'
            OR LOWER(secteur)  LIKE '%telecom%'
            OR LOWER(secteur)  LIKE '%data%'
            OR LOWER(titre)    LIKE '%data%'
            OR LOWER(titre)    LIKE '%engineer%'
            OR LOWER(titre)    LIKE '%développeur%'
            OR LOWER(titre)    LIKE '%developer%'
            OR LOWER(titre)    LIKE '%devops%'
            OR LOWER(titre)    LIKE '%cloud%'
            OR LOWER(titre)    LIKE '%python%'
            OR LOWER(titre)    LIKE '%machine learning%'
            OR LOWER(titre)    LIKE '%fullstack%'
            OR LOWER(titre)    LIKE '%backend%'
            OR LOWER(titre)    LIKE '%frontend%'
            OR LOWER(titre)    LIKE '%java%'
            OR LOWER(titre)    LIKE '%software%'
            OR LOWER(description) LIKE '%spark%'
            OR LOWER(description) LIKE '%hadoop%'
            OR LOWER(description) LIKE '%kafka%'
            OR LOWER(description) LIKE '%docker%'
            OR LOWER(description) LIKE '%big data%'
            OR LOWER(description) LIKE '%machine learning%'
        THEN TRUE
        ELSE FALSE
        END AS is_tech_job

    FROM experience
),

deduplication AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY url
            ORDER BY scraped_at DESC
        ) AS rn
    FROM tech_flag
    WHERE url IS NOT NULL
)

SELECT
    job_id, loaded_at, scraped_at, date_publication, date_limite,
    source, url, titre, entreprise,
    ville, ville_raw, region,
    type_contrat, is_teletravail,
    experience_raw, experience_min_ans, experience_max_ans,
    niveau_etudes, secteur, description, nb_postes,
    is_tech_job,
    NULL::FLOAT  AS salaire_min_mad,
    NULL::FLOAT  AS salaire_max_mad

FROM deduplication
WHERE rn = 1
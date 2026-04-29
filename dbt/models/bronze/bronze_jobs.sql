-- models/bronze/bronze_jobs.sql
-- ============================================================
-- Couche Bronze Snowflake
-- Extrait les champs du VARIANT (JSON natif Snowflake)
-- La syntaxe : raw_json:champ::TYPE
-- ============================================================

{{ config(materialized='view', schema='bronze') }}

SELECT
    id                                              AS job_id,
    loaded_at,
    scraped_at,
    source,

    -- Extraction depuis le type VARIANT Snowflake
    -- Syntaxe : colonne:champ::type_cible
    raw_json:titre::VARCHAR                         AS titre_raw,
    raw_json:entreprise::VARCHAR                    AS entreprise_raw,
    raw_json:ville::VARCHAR                         AS ville_raw,
    raw_json:type_contrat::VARCHAR                  AS type_contrat_raw,
    raw_json:experience::VARCHAR                    AS experience_raw,
    raw_json:niveau_etudes::VARCHAR                 AS niveau_etudes_raw,
    raw_json:secteur::VARCHAR                       AS secteur_raw,
    raw_json:description::VARCHAR                   AS description_raw,
    raw_json:teletravail::VARCHAR                   AS teletravail_raw,
    raw_json:url::VARCHAR                           AS url,
    raw_json:date_publication::VARCHAR              AS date_publication_raw,
    raw_json:date_limite::VARCHAR                   AS date_limite_raw,
    raw_json:nb_postes::INT                         AS nb_postes,

    raw_json                                        AS raw_json

FROM {{ source('bronze', 'raw_jobs') }}
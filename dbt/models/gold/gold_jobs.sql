-- models/gold/gold_jobs.sql
-- ============================================================
-- Couche Gold — Table analytique finale
-- Toutes les offres enrichies, prêtes pour Metabase/Superset
-- ============================================================

{{ config(materialized='table', schema='gold') }}

SELECT
    job_id,
    source,
    date_publication,
    date_limite,
    titre,
    entreprise,
    ville,
    region,
    secteur,
    type_contrat,
    is_teletravail,
    experience_raw,
    experience_min_ans,
    experience_max_ans,
    niveau_etudes,
    nb_postes,
    is_tech_job,
    description,
    url,

    -- Enrichissement Gold : catégorie d'expérience lisible
    CASE
        WHEN experience_min_ans = 0              THEN 'Débutant'
        WHEN experience_min_ans BETWEEN 1 AND 2  THEN 'Junior (1-2 ans)'
        WHEN experience_min_ans BETWEEN 3 AND 5  THEN 'Intermédiaire (3-5 ans)'
        WHEN experience_min_ans BETWEEN 6 AND 10 THEN 'Confirmé (6-10 ans)'
        WHEN experience_min_ans > 10             THEN 'Expert (10+ ans)'
        ELSE 'Non précisé'
    END AS categorie_experience,

    -- Mois de publication (utile pour les tendances temporelles)
    DATE_TRUNC('month', date_publication) AS mois_publication,
    YEAR(date_publication)                AS annee_publication,
    MONTH(date_publication)               AS mois_numero

FROM {{ ref('silver_jobs') }}


-- ============================================================
-- models/gold/gold_competences.sql
-- Extrait les compétences tech depuis la description
-- Une ligne par compétence par offre (format long)
-- ============================================================
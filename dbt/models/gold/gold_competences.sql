-- models/gold/gold_competences.sql
-- ============================================================
-- Extrait les compétences tech depuis la description (format long)
-- Une ligne par compétence détectée par offre
-- Utile pour : "Top 20 compétences les plus demandées au Maroc"
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH offres AS (
    SELECT job_id, titre, description, ville, region, secteur,
           date_publication, is_tech_job
    FROM {{ ref('silver_jobs') }}
    WHERE description IS NOT NULL
),

competences_raw AS (
    -- On crée une ligne par compétence en utilisant LATERAL FLATTEN
    -- sur un tableau de mots-clés à chercher dans la description
    SELECT
        o.job_id,
        o.titre,
        o.ville,
        o.region,
        o.secteur,
        o.date_publication,
        o.is_tech_job,
        comp.value::VARCHAR AS competence
    FROM offres o,
    LATERAL FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'Python', 'SQL', 'Java', 'JavaScript', 'TypeScript',
        'Scala', 'R', 'C++', 'C#', 'PHP', 'Go', 'Rust',
        'Spark', 'Hadoop', 'Kafka', 'Hive', 'Flink',
        'Airflow', 'dbt', 'Luigi',
        'Docker', 'Kubernetes', 'Terraform', 'Ansible',
        'AWS', 'Azure', 'GCP', 'Google Cloud',
        'Snowflake', 'BigQuery', 'Redshift', 'PostgreSQL',
        'MySQL', 'MongoDB', 'Redis', 'Elasticsearch',
        'Power BI', 'Tableau', 'Looker', 'Metabase',
        'TensorFlow', 'PyTorch', 'Scikit-learn', 'Keras',
        'Machine Learning', 'Deep Learning', 'NLP',
        'Git', 'CI/CD', 'Jenkins', 'GitLab',
        'React', 'Angular', 'Vue', 'Node.js',
        'Spring', 'Django', 'Flask', 'FastAPI',
        'Linux', 'Shell', 'PowerShell',
        'Excel', 'Power Query', 'VBA',
        'Agile', 'Scrum', 'JIRA'
    )) comp
    WHERE CONTAINS(LOWER(o.description), LOWER(comp.value::VARCHAR))
)

SELECT
    job_id,
    competence,
    titre,
    ville,
    region,
    secteur,
    date_publication,
    is_tech_job,
    COUNT(*) OVER (PARTITION BY competence) AS nb_offres_avec_competence
FROM competences_raw
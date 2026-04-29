# Plateforme d'Analyse du Marché de l'Emploi au Maroc

![Architecture ELT](docs/screenshots/architecture.png)

## 📋 Description

Ce projet met en place un **pipeline ELT automatisé** pour analyser le marché de l'emploi au Maroc. Il scrape quotidiennement les offres d'emploi, les stocke dans Snowflake, les transforme avec dbt, et les visualise dans Power BI.

### Problématique
- Offres d'emploi tech dispersées sur plusieurs sites
- Absence de vision centralisée sur les compétences recherchées
- Difficulté à suivre les tendances (villes, contrats, salaires)

### Objectifs
- ✅ Centraliser automatiquement les offres d'emploi 
- ✅ Nettoyer et standardiser les données
- ✅ Extraire des tendances (compétences, villes, contrats)
- ✅ Permettre la recherche et la visualisation

---

## 🏗️ Architecture
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ SOURCE │ │ SCRAPER │ │ ORCHESTRATEUR│ │
│ │ │ │ │ │ │ │
│ │ rekrute.ma │ ────► │ Python + │ ────► │ Airflow │ │
│ │ │ │ BeautifulSoup│ │ (Docker) │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
│ │
│ │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│ │ DATA WAREHOUSE│ │ TRANSFORMATION│ │ VISUALISATION│ │
│ │ │ │ │ │ │ │
│ │ Snowflake │ ────► │ dbt │ ────► │ Power BI │ │
│ │ Bronze/Silver/Gold│ │ (SQL) │ │ │ │
│ └──────────────┘ └──────────────┘ └──────────────┘ │
│ │
│ ⚡ ELT : Extract → Load → Transform │
└─────────────────────────────────────────────────────────────────────────────────────┘


### Modèle en Médaillon (Bronze/Silver/Gold)

| Couche | Contenu | Rôle |
|--------|---------|------|
| **BRONZE** | JSON brut des offres | Stockage原始, aucune transformation |
| **SILVER** | Données nettoyées | Villes normalisées, contrats typés, déduplication |
| **GOLD** | Données agrégées | KPIs, top compétences, tendances |

---

## 🛠️ Technologies utilisées

| Catégorie | Technologie | Rôle |
|-----------|-------------|------|
| **Scraping** | Python + BeautifulSoup | Extraction des offres depuis rekrute.ma |
| **Orchestration** | Apache Airflow | Automatisation quotidienne (0 6 * * *) |
| **Conteneurisation** | Docker | Environnement reproductible |
| **Stockage** | Snowflake | Data Warehouse cloud (JSON natif) |
| **Transformation** | dbt | Nettoyage et modélisation SQL |
| **Visualisation** | Power BI | Dashboard interactif |

---

## 📊 Résultats

### Statistiques (avril 2026)

| Métrique | Valeur |
|----------|--------|
| Offres scrapées (brutes) | 305 |
| Offres tech (Silver/Gold) | 200 |
| Compétences identifiées | 60+ |
| Villes couvertes | 15 |

### Top compétences recherchées

| Compétence | Nombre d'offres |
|------------|-----------------|
| Python | 134 |
| SQL | 196 |
| Spark | 206 |
| Docker | 87 |
| AWS | 78 |

### Villes les plus dynamiques

| Ville | Offres tech |
|-------|-------------|
| Casablanca | 326 |
| Rabat | 210 |
| Marrakech | 100 |
| Tanger | 75 |

### Types de contrat

| Contrat | Pourcentage |
|---------|-------------|
| CDI | 68% |
| CDD | 18% |
| Stage | 9% |
| Freelance | 5% |

---

## 🚀 Installation

### Prérequis

- Docker Desktop
- Python 3.8+
- Compte Snowflake (gratuit)
- Git

### Étapes

```bash
# 1. Cloner le projet
git clone https://github.com/ElmghariBasma/job_market_pipeline.git
cd job_market_pipeline

# 2. Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos identifiants Snowflake

# 3. Configurer dbt
cp dbt/profiles.yml.example dbt/profiles.yml
# Éditer avec vos identifiants Snowflake

# 4. Lancer Airflow
docker-compose -f docker-compose-simple.yml up -d

# 5. Accéder à l'interface Airflow
# http://localhost:8080 (admin/admin)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
from airflow.operators.dummy import DummyOperator  # ← AJOUTEZ CETTE LIGNE

# Ajoutez le chemin des plugins
sys.path.append('/opt/airflow/plugins')

# Importez vos fonctions (pas le script entier)
from test_rekrute import scrape_rekrute_tech, scrape_all_keywords
from merge_json import fusionner_json
from load_to_snowflake_bronze import charger_vers_snowflake

default_args = {
    'owner': 'Basma',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

def run_scrape(**context):
    """Fonction appelée par Airflow pour le scraping"""
    print("🚀 Démarrage du scraping...")
    
    # Utilisez la fonction qui scrape tous les mots-clés
    result = scrape_all_keywords(pages=5, output_dir='/opt/airflow/data/bronze')
    
    print(f"✅ Scraping terminé : {len(result)} offres trouvées")
    return f"Scraping terminé : {len(result)} offres"

def run_merge(**context):
    """Fonction pour fusionner les JSON"""
    print("🔗 Fusion des fichiers JSON...")
    
    result = fusionner_json(repertoire='/opt/airflow/data/bronze')
    
    if result:
        print(f"✅ Fusion terminée : {result['meta']['total_uniques']} offres uniques")
    else:
        print("⚠️ Aucun fichier trouvé")
    
    return "Fusion terminée"

def run_load(**context):
    """Fonction pour charger dans Snowflake"""
    print("☁️ Chargement vers Snowflake...")
    
    fichier_json = '/opt/airflow/data/bronze/rekrute_merged.json'
    result = charger_vers_snowflake(fichier_json)
    
    print(f"✅ Chargement terminé : {result} offres chargées")
    return f"Chargement terminé : {result} offres"

with DAG(
    'rekrute_pipeline_complet',
    default_args=default_args,
    description='Pipeline complet scraping rekrute.ma → Snowflake → dbt',
    schedule_interval='0 6 * * *',  # Tous les jours à 6h
    catchup=False,
    tags=['emploi', 'maroc', 'scraping'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    scrape = PythonOperator(
        task_id='scrape_rekrute',
        python_callable=run_scrape,
    )
    
    merge = PythonOperator(
        task_id='merge_json',
        python_callable=run_merge,
    )
    
    validate = PythonOperator(
        task_id='validate_bronze',
        python_callable=lambda: None,  # À implémenter
    )
    
    load = PythonOperator(
        task_id='load_snowflake_bronze',
        python_callable=run_load,
    )
    
    dbt_run = DummyOperator(task_id='dbt_run')
    dbt_test = DummyOperator(task_id='dbt_test')
    
    end = DummyOperator(task_id='end')
    
    start >> scrape >> merge >> validate >> load >> dbt_run >> dbt_test >> end
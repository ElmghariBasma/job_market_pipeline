"""
load_to_snowflake_bronze.py
===========================
Charge le fichier rekrute_merged.json dans Snowflake (couche Bronze).

Installation :
    pip install snowflake-connector-python

Lancement :
    python load_to_snowflake_bronze.py [fichier_json]

Usage depuis un autre script :
    from load_to_snowflake_bronze import charger_vers_snowflake
"""

import json
import sys
import os
from datetime import datetime, timezone

# ── Configuration Snowflake (à remplacer par vos identifiants) ──
SNOWFLAKE_CONFIG = {
    "account": "emgfqhx-tg96851",
    "user": "Basma",
    "password": "CGeH8uRfSGFwLaz",
    "warehouse": "EMPLOI_WH",
    "database": "EMPLOI_MAROC",
    "schema": "BRONZE",
}

def charger_vers_snowflake(filepath):
    """
    Charge un fichier JSON vers Snowflake
    
    Args:
        filepath: Chemin vers le fichier JSON fusionné
    
    Returns:
        int: Nombre d'offres chargées, ou None en cas d'erreur
    """
    import snowflake.connector
    
    print(f"\n{'='*50}")
    print("  CHARGEMENT BRONZE → Snowflake")
    print(f"{'='*50}")

    # Lire le JSON
    if not os.path.exists(filepath):
        print(f"  ✗ Fichier non trouvé : {filepath}")
        return None
    
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    offres = data.get("offres", [])
    print(f"  → {len(offres)} offres à charger")

    # Connexion Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        print("  ✓ Connexion Snowflake établie")
    except Exception as e:
        print(f"  ✗ Erreur connexion : {e}")
        return None

    # Insertion
    inseres = 0
    erreurs = 0
    
    for offre in offres:
        try:
            cur.execute("""
                INSERT INTO bronze.raw_jobs (source, scraped_at, raw_json)
                SELECT
                    %s,
                    %s::TIMESTAMP_TZ,
                    PARSE_JSON(%s)
            """, (
                offre.get("source", "rekrute.ma"),
                offre.get("scraped_at"),
                json.dumps(offre, ensure_ascii=False),
            ))
            inseres += 1
        except Exception as e:
            erreurs += 1
            print(f"  ✗ Erreur insertion: {e}")

    conn.commit()

    # Vérification
    cur.execute("SELECT COUNT(*) FROM bronze.raw_jobs")
    total = cur.fetchone()[0]

    print(f"  ✓ {inseres} offres insérées")
    print(f"  ✓ {erreurs} erreurs")
    print(f"  ✓ Total en base : {total} offres")

    cur.close()
    conn.close()
    print(f"{'='*50}\n")
    
    return inseres

# ── Ne s'exécute que si on lance le script directement ──
if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "rekrute_merged.json"
    charger_vers_snowflake(filepath)
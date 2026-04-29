"""
merge_json.py
=============
Fusionne tous les fichiers rekrute_test_*.json en un seul,
avec déduplication par URL.

Usage:
    python merge_json.py
    OU depuis un autre script: from merge_json import fusionner_json
"""

import json
import glob
import os
from datetime import datetime, timezone

def fusionner_json(repertoire="."):
    """
    Fusionne tous les fichiers JSON dans le dossier spécifié
    
    Args:
        repertoire: Dossier contenant les fichiers JSON (par défaut: dossier courant)
    
    Returns:
        dict: Le dictionnaire fusionné avec les métadonnées, ou None si aucun fichier
    """
    # ── Trouver tous les fichiers JSON du scraper ─────────────────────────────────
    ancien_repertoire = os.getcwd()
    os.chdir(repertoire)
    
    fichiers = glob.glob("rekrute_tech_*.json")
    
    if not fichiers:
        print("Aucun fichier rekrute_tech_*.json trouvé dans ce dossier.")
        os.chdir(ancien_repertoire)
        return None
    
    print(f"\n{len(fichiers)} fichiers trouvés :")
    
    # ── Fusionner avec déduplication par URL ──────────────────────────────────────
    toutes_offres = []
    urls_vues = set()
    total_brut = 0
    
    for fichier in sorted(fichiers):
        with open(fichier, encoding="utf-8") as f:
            data = json.load(f)
        
        offres = data.get("offres", [])
        total_brut += len(offres)
        nouvelles = 0
        
        for offre in offres:
            url = offre.get("url")
            if url and url not in urls_vues:
                urls_vues.add(url)
                toutes_offres.append(offre)
                nouvelles += 1
        
        print(f"  {fichier:<45} {len(offres):>4} offres  →  {nouvelles} nouvelles")
    
    # ── Sauvegarder le fichier fusionné ──────────────────────────────────────────
    output = {
        "meta": {
            "source": "rekrute.ma",
            "fichiers_sources": fichiers,
            "total_brut": total_brut,
            "total_uniques": len(toutes_offres),
            "doublons_retires": total_brut - len(toutes_offres),
            "merged_at": datetime.now(timezone.utc).isoformat(),
        },
        "offres": toutes_offres,
    }
    
    nom_sortie = "rekrute_merged.json"
    with open(nom_sortie, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    # ── Rapport ───────────────────────────────────────────────────────────────────
    print(f"""
{'='*50}
  Offres brutes (total)  : {total_brut}
  Doublons retirés       : {total_brut - len(toutes_offres)}
  Offres uniques         : {len(toutes_offres)}
  Fichier de sortie      : {nom_sortie}
{'='*50}
""")
    
    os.chdir(ancien_repertoire)
    return output

# ── Ne s'exécute que si on lance le script directement ──
if __name__ == "__main__":
    fusionner_json()
"""
test_rekrute.py
===============
Scraping des offres d'emploi tech sur rekrute.ma

Usage:
    python test_rekrute.py
    OU depuis un autre script: from test_rekrute import scrape_rekrute_tech
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import random
from datetime import datetime, timezone
from urllib.parse import urljoin
import sys
sys.stdout.reconfigure(line_buffering=True)

# ====================== CONFIG ======================
BASE_URL = "https://www.rekrute.com/offres.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Connection": "keep-alive",
}
# Mots-clés élargis pour TOUT le domaine Tech
TECH_KEYWORDS = [
    "data engineer", "data scientist", "big data", "machine learning", "ia", "intelligence artificielle",
    "devops", "cloud", "azure", "aws", "gcp", "kubernetes", "docker", "spark", "kafka", "airflow", "databricks",
    "python", "java", "spring", "javascript", "react", "angular", "nodejs", "php", "symfony", 
    "fullstack", "backend", "frontend", "sql", "oracle", "pl/sql", "power bi", "tableau", 
    "etl", "elt", "cybersecurity", "sécurité", "ingenieur", "développeur", "architecte", 
    "administrateur", "support technique", "devsecops"
]


def normalize_ville(ville: str) -> str:
    if not ville: 
        return None
    v = ville.lower().strip()
    mapping = {
        "casa": "Casablanca", 
        "casablanca": "Casablanca", 
        "rabat": "Rabat", 
        "sale": "Salé", 
        "technopolis": "Technopolis (Salé)", 
        "fes": "Fès", 
        "marrakech": "Marrakech", 
        "tanger": "Tanger", 
        "kenitra": "Kénitra", 
        "agadir": "Agadir"
    }
    return mapping.get(v, ville.title())

def extract_salary(text: str) -> str:
    if not text: 
        return None
    m = re.search(r'(\d{1,3}[.,\s]?\d{3})\s*(?:MAD|DH|dirham|dhs)', text, re.I)
    return m.group(0) if m else None

def extract_tech_skills(text: str) -> list:
    if not text: 
        return []
    lower = text.lower()
    return list(set(kw.title() for kw in TECH_KEYWORDS if kw in lower))

def is_tech_offer(offre: dict) -> bool:
    text = f"{offre.get('titre','')} {offre.get('description','')} {offre.get('description_complete','')}".lower()
    return any(kw in text for kw in TECH_KEYWORDS)

# ====================== DETAIL SCRAPING ======================
def scrape_job_detail(url: str) -> dict:
    try:
        time.sleep(random.uniform(2.2, 4.5))
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        selectors = [
            "div#job-description", 
            "div.description-offre", 
            "div.contenu", 
            "section.job-details", 
            "div.job-description", 
            ".offre-description", 
            "div.text-justify"
        ]
        
        full_desc = ""
        for sel in selectors:
            tag = soup.select_one(sel)
            if tag:
                full_desc = tag.get_text(separator=" ", strip=True)
                break
        if not full_desc:
            full_desc = soup.get_text(separator=" ", strip=True)[:4000]

        return {
            "description_complete": full_desc,
            "salaire_raw": extract_salary(full_desc),
            "skills": extract_tech_skills(full_desc),
            "detail_scraped": True
        }
    except Exception as e:
        print(f"❌ Erreur détail {url[-50:]} : {type(e).__name__}")
        return {"description_complete": "", "salaire_raw": None, "skills": [], "detail_scraped": False}

# ====================== PARSE CARTE ======================
def parse_offre(card) -> dict:
    titre_tag = card.select_one("a.titreJob")
    if not titre_tag:
        return None

    titre_brut = titre_tag.get_text(strip=True)
    href = titre_tag.get("href", "")
    url = urljoin("https://www.rekrute.com", href) if href.startswith("/") else href

    titre, ville_raw = None, None
    if "|" in titre_brut:
        parts = [p.strip() for p in titre_brut.split("|", 1)]
        titre = parts[0]
        ville_raw = parts[1] if len(parts) > 1 else None

    entreprise_tag = card.select_one(".company-name, .recruteur")
    entreprise = entreprise_tag.get_text(strip=True) if entreprise_tag else None

    # Description courte
    description_short = ""
    for div in card.select("div.info"):
        span = div.select_one('span[style*="color"]')
        if span:
            description_short = span.get_text(separator=" ", strip=True)[:450]
            break

    offre = {
        "source": "rekrute.ma",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "titre": titre,
        "entreprise": entreprise,
        "url": url,
        "ville": normalize_ville(ville_raw),
        "description": description_short,
    }

    # Scraping détail
    if url:
        detail = scrape_job_detail(url)
        offre.update(detail)

    return offre

# ====================== MAIN ======================
def scrape_rekrute_tech(pages=12, keyword="data", output_dir="."):
    """
    Scrape les offres tech sur rekrute.ma
    
    Args:
        pages: Nombre de pages à scraper
        keyword: Mot-clé de recherche
        output_dir: Dossier où sauvegarder les fichiers JSON
    
    Returns:
        list: Liste des offres scrapées
    """
    print(f"\n🚀 Scraping Tech - Mot-clé: '{keyword}' | Pages: {pages}")
    # Ajoutez un délai avant la première requête
    time.sleep(random.uniform(2, 5))
    session = requests.Session()
    offres_tech = []
    urls_vues = set()

    for p in range(1, pages + 1):
        params = {"s": 1, "p": p, "o": 1, "q": keyword}
        try:
            
            r = session.get(BASE_URL, params=params, headers=HEADERS, timeout=30, allow_redirects=True)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("li.post-id")

            print(f"Page {p}/{pages} → {len(cards)} cartes")

            for card in cards:
                offre = parse_offre(card)
                if offre and offre.get("url") and offre["url"] not in urls_vues:
                    urls_vues.add(offre["url"])
                    if is_tech_offer(offre):
                        offres_tech.append(offre)
                        print(f"✅ Tech : {offre['titre'][:75]}...")

            time.sleep(random.uniform(5, 10))

        except Exception as e:
            print(f"❌ Erreur page {p}: {type(e).__name__}")
            time.sleep(8)

    # Sauvegarde
    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"rekrute_tech_v4_{keyword}_{date_str}.json"
    
    # Sauvegarde dans le dossier spécifié
    import os
    full_path = os.path.join(output_dir, filename) if output_dir != "." else filename
    
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {
                "keyword": keyword,
                "pages": pages,
                "total_tech": len(offres_tech),
                "extracted_at": datetime.now(timezone.utc).isoformat()
            },
            "offres": offres_tech
        }, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 Terminé ! {len(offres_tech)} offres TECH dans → {filename}\n")
    return offres_tech

def scrape_all_keywords(pages=10, output_dir="."):
    """
    Scrape tous les mots-clés prédéfinis
    
    Args:
        pages: Nombre de pages par mot-clé
        output_dir: Dossier de sortie
    """
    keywords = ["data", "informatique", "développeur", "devops", "cloud", "ingenieur", "java", "python"]
    toutes_offres = []
    
    for kw in keywords:
        offres = scrape_rekrute_tech(pages=pages, keyword=kw, output_dir=output_dir)
        toutes_offres.extend(offres)
        time.sleep(random.uniform(10, 15))
    
    return toutes_offres

# ====================== LANCEMENT ======================
# Ne s'exécute que si on lance le script directement (pas si importé par Airflow)
if __name__ == "__main__":
    scrape_all_keywords(pages=10)
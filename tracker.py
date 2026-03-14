#!/usr/bin/env python3
"""
GSC Ranking Tracker
===================
Récupère les données Google Search Console pour plusieurs sites,
détecte les variations de positions et génère un rapport Excel + texte.

Usage :
    python tracker.py                  # Tous les sites
    python tracker.py --site avis-malin.fr   # Un seul site
    python tracker.py --config mon_config.yaml
"""

import argparse
import io
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Forcer la sortie UTF-8 sur Windows (sinon les emojis crashent en cp1252)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import httpx
import pandas as pd
import yaml
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Scope nécessaire pour lire les données Search Console
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


# ============================================================
# 1. AUTHENTIFICATION
# ============================================================

def get_gsc_service(credentials_file: str, token_file: str):
    """
    Crée un service GSC authentifié.
    - Charge le token existant si disponible
    - Sinon, lance le flow OAuth2 et sauvegarde le token
    - Gère le refresh automatique
    """
    creds = None

    # Charger le token existant
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # Si pas de token valide, authentifier
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("[AUTH] Refresh du token...")
            creds.refresh(Request())
        else:
            if not os.path.exists(credentials_file):
                print(f"[ERREUR] Fichier credentials introuvable : {credentials_file}")
                print("         Voir README.md pour les instructions de configuration.")
                sys.exit(1)
            print("[AUTH] Première connexion - un navigateur va s'ouvrir...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)

        # Sauvegarder le token pour les prochains lancements
        with open(token_file, "w") as f:
            f.write(creds.to_json())
        print("[AUTH] Token sauvegardé.")

    return build("searchconsole", "v1", credentials=creds)


# ============================================================
# 2. COLLECTE DES DONNÉES
# ============================================================

def get_week_bounds(weeks_ago: int) -> tuple[str, str]:
    """
    Retourne (start_date, end_date) pour une semaine donnée.
    Semaine 0 = semaine dernière complète (lundi→dimanche).
    On décale de 3 jours supplémentaires car GSC a ~3j de latence.
    """
    today = datetime.now().date()
    # Dernier dimanche (fin de semaine dernière)
    last_sunday = today - timedelta(days=today.weekday() + 1)
    # Décaler de 3 jours pour la latence GSC
    last_sunday = last_sunday - timedelta(days=3)

    end = last_sunday - timedelta(weeks=weeks_ago)
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


def fetch_gsc_data(service, site_url: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Récupère les données GSC pour un site sur une période donnée.
    Dimensions : query + page.
    Retourne un DataFrame avec : query, page, clicks, impressions, ctr, position.
    """
    all_rows = []
    start_row = 0
    row_limit = 25000  # Max par requête API

    while True:
        request_body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": ["query", "page"],
            "rowLimit": row_limit,
            "startRow": start_row,
        }

        response = service.searchanalytics().query(
            siteUrl=site_url, body=request_body
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            all_rows.append({
                "query": row["keys"][0],
                "page": row["keys"][1],
                "clicks": row["clicks"],
                "impressions": row["impressions"],
                "ctr": round(row["ctr"] * 100, 2),  # Convertir en %
                "position": round(row["position"], 1),
            })

        start_row += row_limit
        # Si on a reçu moins que la limite, c'est la fin
        if len(rows) < row_limit:
            break

    return pd.DataFrame(all_rows)


def collect_site_data(service, site_url: str, weeks_history: int) -> dict:
    """
    Collecte les données sur N semaines pour un site.
    Retourne un dict {semaine_index: DataFrame}.
    """
    data = {}
    site_name = site_url.replace("sc-domain:", "").replace("https://", "")

    for w in range(weeks_history):
        start, end = get_week_bounds(w)
        print(f"  [{site_name}] Semaine -{w} : {start} → {end}...", end=" ")
        df = fetch_gsc_data(service, site_url, start, end)
        print(f"{len(df)} lignes")
        data[w] = df

    return data


# ============================================================
# 3. DÉTECTIONS AUTOMATIQUES
# ============================================================

def analyze_site(data: dict, thresholds: dict) -> dict:
    """
    Compare semaine 0 (la plus récente) vs semaine 1 (précédente).
    Génère les 5 segments de détection.
    Retourne un dict avec les DataFrames de chaque segment + données brutes.
    """
    current = data.get(0, pd.DataFrame())
    previous = data.get(1, pd.DataFrame())

    if current.empty:
        return {
            "current": current,
            "progressions": pd.DataFrame(),
            "drops": pd.DataFrame(),
            "quickwins": pd.DataFrame(),
            "low_ctr": pd.DataFrame(),
            "new_queries": pd.DataFrame(),
        }

    # ---- Merge semaine N et N-1 pour comparer ----
    if not previous.empty:
        merged = current.merge(
            previous[["query", "page", "position", "clicks", "impressions"]],
            on=["query", "page"],
            how="left",
            suffixes=("", "_prev"),
        )
        # Calcul de la variation (négatif = amélioration de position)
        merged["position_change"] = merged["position"] - merged["position_prev"]
        merged["clicks_change"] = merged["clicks"] - merged["clicks_prev"]
    else:
        merged = current.copy()
        merged["position_prev"] = None
        merged["position_change"] = None
        merged["clicks_change"] = None

    # ---- 🟢 Progressions > X positions ----
    gain_threshold = thresholds["position_gain"]
    progressions = merged[
        merged["position_change"].notna()
        & (merged["position_change"] < -gain_threshold)
    ].copy()
    progressions["position_change"] = progressions["position_change"].round(1)
    progressions = progressions.sort_values("position_change")

    # ---- 🔴 Chutes > X positions ----
    drop_threshold = thresholds["position_drop"]
    drops = merged[
        merged["position_change"].notna()
        & (merged["position_change"] > drop_threshold)
    ].copy()
    drops["position_change"] = drops["position_change"].round(1)
    drops = drops.sort_values("position_change", ascending=False)

    # ---- 🎯 Quick wins : position 4-12 + impressions > seuil ----
    qw_min = thresholds["quickwin_pos_min"]
    qw_max = thresholds["quickwin_pos_max"]
    qw_imp = thresholds["quickwin_impressions_min"]
    quickwins = current[
        (current["position"] >= qw_min)
        & (current["position"] <= qw_max)
        & (current["impressions"] >= qw_imp)
    ].sort_values("impressions", ascending=False)

    # ---- 👁️ Mauvais CTR ----
    ctr_imp = thresholds["low_ctr_impressions_min"]
    ctr_thresh = thresholds["low_ctr_threshold"]
    low_ctr = current[
        (current["impressions"] >= ctr_imp)
        & (current["ctr"] < ctr_thresh)
    ].sort_values("impressions", ascending=False)

    # ---- 🆕 Nouvelles requêtes (dans current mais pas dans previous) ----
    if not previous.empty:
        prev_queries = set(previous["query"].unique())
        new_queries = current[
            ~current["query"].isin(prev_queries)
        ].sort_values("impressions", ascending=False)
    else:
        new_queries = pd.DataFrame()

    return {
        "current": merged,
        "progressions": progressions,
        "drops": drops,
        "quickwins": quickwins,
        "low_ctr": low_ctr,
        "new_queries": new_queries,
    }


# ============================================================
# 4. STOCKAGE SQLite (pour le dashboard)
# ============================================================

DB_FILE = "gsc_data.db"


def init_db(db_path: str):
    """Crée la base de données et les tables si elles n'existent pas."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site TEXT NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            query TEXT NOT NULL,
            page TEXT NOT NULL,
            clicks INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            ctr REAL DEFAULT 0,
            position REAL DEFAULT 0,
            collected_at TEXT NOT NULL,
            UNIQUE(site, week_start, query, page)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site TEXT NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            total_queries INTEGER DEFAULT 0,
            total_clicks INTEGER DEFAULT 0,
            total_impressions INTEGER DEFAULT 0,
            avg_position REAL DEFAULT 0,
            progressions INTEGER DEFAULT 0,
            drops INTEGER DEFAULT 0,
            quickwins INTEGER DEFAULT 0,
            low_ctr INTEGER DEFAULT 0,
            new_queries INTEGER DEFAULT 0,
            collected_at TEXT NOT NULL,
            UNIQUE(site, week_start)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_weekly_site ON weekly_data(site, week_start)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_summary_site ON weekly_summary(site, week_start)")
    conn.commit()
    conn.close()


def save_to_db(db_path: str, site_url: str, data: dict, analysis: dict):
    """
    Sauvegarde les données hebdomadaires et le résumé dans SQLite.
    Utilise INSERT OR REPLACE pour éviter les doublons.
    """
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    short_name = site_url.replace("sc-domain:", "").replace("https://", "").rstrip("/")

    # Sauvegarder les données brutes de chaque semaine
    for week_idx, df in data.items():
        if df.empty:
            continue
        start, end = get_week_bounds(week_idx)
        records = []
        for _, row in df.iterrows():
            records.append((
                short_name, start, end,
                row["query"], row["page"],
                int(row["clicks"]), int(row["impressions"]),
                float(row["ctr"]), float(row["position"]),
                now,
            ))
        conn.executemany("""
            INSERT OR REPLACE INTO weekly_data
            (site, week_start, week_end, query, page, clicks, impressions, ctr, position, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

    # Sauvegarder le résumé de la semaine courante
    current = analysis["current"]
    if not current.empty:
        start, end = get_week_bounds(0)
        conn.execute("""
            INSERT OR REPLACE INTO weekly_summary
            (site, week_start, week_end, total_queries, total_clicks, total_impressions,
             avg_position, progressions, drops, quickwins, low_ctr, new_queries, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            short_name, start, end,
            len(current),
            int(current["clicks"].sum()),
            int(current["impressions"].sum()),
            round(float(current["position"].mean()), 1),
            len(analysis["progressions"]),
            len(analysis["drops"]),
            len(analysis["quickwins"]),
            len(analysis["low_ctr"]),
            len(analysis["new_queries"]),
            now,
        ))

    conn.commit()
    conn.close()


def push_to_supabase(supabase_config: dict, site_url: str, data: dict, analysis: dict):
    """
    Pousse les données vers Supabase via l'API REST (httpx).
    Utilise UPSERT (on_conflict) pour éviter les doublons.
    """
    url = supabase_config["url"]
    key = supabase_config["key"]
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    short_name = site_url.replace("sc-domain:", "").replace("https://", "").rstrip("/")
    now = datetime.now().isoformat()

    # Pousser les données brutes par semaine (par lots de 500)
    for week_idx, df in data.items():
        if df.empty:
            continue
        start, end = get_week_bounds(week_idx)
        records = []
        for _, row in df.iterrows():
            records.append({
                "site": short_name,
                "week_start": start,
                "week_end": end,
                "query": row["query"],
                "page": row["page"],
                "clicks": int(row["clicks"]),
                "impressions": int(row["impressions"]),
                "ctr": float(row["ctr"]),
                "position": float(row["position"]),
                "collected_at": now,
            })

        # Envoyer par lots de 500 lignes
        batch_size = 500
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            resp = httpx.post(
                f"{url}/rest/v1/weekly_data",
                headers=headers,
                json=batch,
                timeout=30.0,
            )
            if resp.status_code not in (200, 201):
                print(f"    [SUPABASE] Erreur weekly_data semaine {start}: {resp.status_code}")

    # Pousser le résumé de la semaine courante
    current = analysis["current"]
    if not current.empty:
        start, end = get_week_bounds(0)
        summary = {
            "site": short_name,
            "week_start": start,
            "week_end": end,
            "total_queries": len(current),
            "total_clicks": int(current["clicks"].sum()),
            "total_impressions": int(current["impressions"].sum()),
            "avg_position": round(float(current["position"].mean()), 1),
            "progressions": len(analysis["progressions"]),
            "drops": len(analysis["drops"]),
            "quickwins": len(analysis["quickwins"]),
            "low_ctr": len(analysis["low_ctr"]),
            "new_queries": len(analysis["new_queries"]),
            "collected_at": now,
        }
        resp = httpx.post(
            f"{url}/rest/v1/weekly_summary",
            headers=headers,
            json=summary,
            timeout=30.0,
        )
        if resp.status_code not in (200, 201):
            print(f"    [SUPABASE] Erreur weekly_summary: {resp.status_code}")


# ============================================================
# 5. GÉNÉRATION DES RAPPORTS
# ============================================================

def generate_excel(all_results: dict, output_dir: str) -> str:
    """
    Génère un fichier Excel avec :
    - Un onglet par site (données complètes de la semaine)
    - Un onglet "Résumé" avec les KPIs de chaque site
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = os.path.join(output_dir, f"gsc_report_{today_str}.xlsx")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        summary_rows = []

        for site_name, analysis in all_results.items():
            short_name = site_name.replace("sc-domain:", "").replace("https://", "").rstrip("/")
            # Nettoyer les caractères interdits dans les noms d'onglets Excel
            # et tronquer à 31 chars (limite Excel)
            sheet_name = short_name.replace("/", "").replace("\\", "")[:31]

            # --- Onglet principal : données complètes ---
            current = analysis["current"]
            if not current.empty:
                # Colonnes à exporter
                cols = ["query", "page", "clicks", "impressions", "ctr", "position"]
                if "position_prev" in current.columns:
                    cols += ["position_prev", "position_change", "clicks_change"]
                current[cols].to_excel(writer, sheet_name=sheet_name, index=False)

            # --- Onglets segments (un sous-onglet par détection) ---
            segments = {
                "Progressions": analysis["progressions"],
                "Chutes": analysis["drops"],
                "Quick Wins": analysis["quickwins"],
                "Mauvais CTR": analysis["low_ctr"],
                "Nouvelles": analysis["new_queries"],
            }
            for seg_name, seg_df in segments.items():
                seg_sheet = f"{short_name[:20]}_{seg_name}".replace("/", "")[:31]
                if not seg_df.empty:
                    seg_df.to_excel(writer, sheet_name=seg_sheet, index=False)

            # --- Ligne résumé ---
            summary_rows.append({
                "Site": short_name,
                "Requêtes totales": len(current),
                "Clicks total": current["clicks"].sum() if not current.empty else 0,
                "Impressions total": current["impressions"].sum() if not current.empty else 0,
                "Progressions (>3 pos)": len(analysis["progressions"]),
                "Chutes (>3 pos)": len(analysis["drops"]),
                "Quick Wins": len(analysis["quickwins"]),
                "Mauvais CTR": len(analysis["low_ctr"]),
                "Nouvelles requêtes": len(analysis["new_queries"]),
            })

        # --- Onglet Résumé global ---
        if summary_rows:
            pd.DataFrame(summary_rows).to_excel(
                writer, sheet_name="Résumé Global", index=False
            )

    return filename


def generate_text_summary(all_results: dict) -> str:
    """
    Génère un résumé texte condensé (pour email ou Slack).
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"📊 GSC Ranking Tracker - Rapport du {today_str}",
        "=" * 55,
        "",
    ]

    for site_name, analysis in all_results.items():
        short_name = site_name.replace("sc-domain:", "").replace("https://", "")
        current = analysis["current"]

        lines.append(f"🌐 {short_name}")
        lines.append("-" * 40)

        if current.empty:
            lines.append("  Aucune donnée disponible.")
            lines.append("")
            continue

        total_clicks = current["clicks"].sum()
        total_impressions = current["impressions"].sum()
        avg_pos = current["position"].mean()

        lines.append(f"  Requêtes : {len(current)} | Clicks : {total_clicks:,}"
                     f" | Impressions : {total_impressions:,} | Pos. moy : {avg_pos:.1f}")
        lines.append("")

        # Progressions
        prog = analysis["progressions"]
        if not prog.empty:
            lines.append(f"  🟢 Progressions (>{analysis.get('gain_threshold', 3)} pos) : {len(prog)}")
            for _, row in prog.head(5).iterrows():
                lines.append(f"     ↗ {row['query'][:50]} : {row['position']:.0f}"
                             f" (était {row.get('position_prev', '?'):.0f},"
                             f" {row['position_change']:+.0f})")
            if len(prog) > 5:
                lines.append(f"     ... et {len(prog) - 5} autres")
            lines.append("")

        # Chutes
        drops = analysis["drops"]
        if not drops.empty:
            lines.append(f"  🔴 Chutes (>{analysis.get('drop_threshold', 3)} pos) : {len(drops)}")
            for _, row in drops.head(5).iterrows():
                lines.append(f"     ↘ {row['query'][:50]} : {row['position']:.0f}"
                             f" (était {row.get('position_prev', '?'):.0f},"
                             f" {row['position_change']:+.0f})")
            if len(drops) > 5:
                lines.append(f"     ... et {len(drops) - 5} autres")
            lines.append("")

        # Quick wins
        qw = analysis["quickwins"]
        if not qw.empty:
            lines.append(f"  🎯 Quick Wins : {len(qw)}")
            for _, row in qw.head(5).iterrows():
                lines.append(f"     → {row['query'][:50]} : pos {row['position']:.0f}"
                             f" | {row['impressions']} imp | {row['ctr']:.1f}% CTR")
            if len(qw) > 5:
                lines.append(f"     ... et {len(qw) - 5} autres")
            lines.append("")

        # Mauvais CTR
        lc = analysis["low_ctr"]
        if not lc.empty:
            lines.append(f"  👁️ Mauvais CTR : {len(lc)}")
            for _, row in lc.head(5).iterrows():
                lines.append(f"     ⚠ {row['query'][:50]} : {row['ctr']:.1f}% CTR"
                             f" | {row['impressions']} imp | pos {row['position']:.0f}")
            if len(lc) > 5:
                lines.append(f"     ... et {len(lc) - 5} autres")
            lines.append("")

        # Nouvelles requêtes
        nq = analysis["new_queries"]
        if not nq.empty:
            lines.append(f"  🆕 Nouvelles requêtes : {len(nq)}")
            for _, row in nq.head(5).iterrows():
                lines.append(f"     + {row['query'][:50]} : pos {row['position']:.0f}"
                             f" | {row['impressions']} imp | {row['clicks']} clicks")
            if len(nq) > 5:
                lines.append(f"     ... et {len(nq) - 5} autres")
            lines.append("")

        lines.append("")

    return "\n".join(lines)


# ============================================================
# 6. MAIN
# ============================================================

def load_config(config_path: str) -> dict:
    """Charge la configuration depuis un fichier YAML."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description="GSC Ranking Tracker - Suivi hebdomadaire des positions Search Console"
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Chemin vers le fichier de configuration (défaut: config.yaml)"
    )
    parser.add_argument(
        "--site", default=None,
        help="Tracker un seul site (nom de domaine, ex: avis-malin.fr)"
    )
    args = parser.parse_args()

    # Charger la config
    config = load_config(args.config)
    sites = config["sites"]
    weeks = config["weeks_history"]
    thresholds = config["thresholds"]
    output_dir = config["output_dir"]
    credentials_file = config["credentials_file"]
    token_file = config["token_file"]
    supabase_config = config.get("supabase")

    # Filtrer sur un seul site si --site est utilisé
    if args.site:
        # Accepter le nom de domaine seul ou le format complet
        site_filter = args.site
        sites = [s for s in sites if site_filter in s]
        if not sites:
            print(f"[ERREUR] Site '{args.site}' non trouvé dans la config.")
            print(f"         Sites disponibles : {config['sites']}")
            sys.exit(1)

    # Créer le dossier de sortie
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Initialiser la base SQLite
    db_path = os.path.join(os.path.dirname(os.path.abspath(args.config)), DB_FILE)
    init_db(db_path)

    # Connexion à l'API GSC
    print("=" * 55)
    print("📊 GSC Ranking Tracker")
    print("=" * 55)
    service = get_gsc_service(credentials_file, token_file)
    print("[OK] Connecté à Google Search Console.\n")

    # Collecter et analyser chaque site
    all_results = {}
    for site_url in sites:
        site_name = site_url.replace("sc-domain:", "").replace("https://", "").rstrip("/")
        print(f"\n🌐 Collecte des données pour {site_name}...")
        print("-" * 40)

        try:
            site_data = collect_site_data(service, site_url, weeks)
            analysis = analyze_site(site_data, thresholds)
            all_results[site_url] = analysis

            # Sauvegarder dans SQLite (local)
            save_to_db(db_path, site_url, site_data, analysis)

            # Pousser vers Supabase (dashboard en ligne)
            if supabase_config:
                try:
                    push_to_supabase(supabase_config, site_url, site_data, analysis)
                    print(f"  ☁️  Données envoyées vers Supabase")
                except Exception as e:
                    print(f"  ⚠️  Supabase: {e}")

            print(f"  ✅ {site_name} : {len(analysis['current'])} requêtes analysées")
        except Exception as e:
            print(f"  ❌ Erreur sur {site_name} : {e}")
            print(f"     Ce site est ignoré, passage au suivant.")

    # Générer les rapports
    print("\n" + "=" * 55)
    print("📝 Génération des rapports...")

    # Rapport Excel
    excel_path = generate_excel(all_results, output_dir)
    print(f"  📊 Excel : {excel_path}")

    # Résumé texte
    text_summary = generate_text_summary(all_results)
    text_path = os.path.join(output_dir, f"gsc_summary_{datetime.now().strftime('%Y-%m-%d')}.txt")
    with open(text_path, "w", encoding="utf-8") as f:
        f.write(text_summary)
    print(f"  📄 Texte : {text_path}")

    # Afficher le résumé dans la console
    print("\n" + text_summary)
    print("\n✅ Rapport terminé !")


if __name__ == "__main__":
    main()

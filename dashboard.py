"""
GSC Ranking Tracker - Dashboard
================================
Dashboard Streamlit connecté à Supabase.
Déployable sur Streamlit Community Cloud.

Usage local :  streamlit run dashboard.py
"""

import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pandas as pd
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="GSC Ranking Tracker",
    page_icon="📊",
    layout="wide",
)

# Credentials Supabase (via .streamlit/secrets.toml en local,
# ou via les Settings de Streamlit Cloud en production)
SUPABASE_URL = st.secrets["supabase"]["url"]
SUPABASE_KEY = st.secrets["supabase"]["key"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

# GSC OAuth
GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
CREDENTIALS_FILE = Path(__file__).parent / "credentials.json"
TOKEN_FILE = Path(__file__).parent / "token.json"


def get_gsc_connection_status():
    """Vérifie si on a un token GSC valide. Retourne (connected, creds)."""
    if not TOKEN_FILE.exists():
        return False, None
    try:
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), GSC_SCOPES)
        if creds and creds.valid:
            return True, creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            TOKEN_FILE.write_text(creds.to_json())
            return True, creds
    except Exception:
        pass
    return False, None


def sync_gsc_sites(creds):
    """Récupère les sites GSC et les synchronise dans Supabase. Retourne (new_count, all_sites)."""
    service = build("searchconsole", "v1", credentials=creds)
    response = service.sites().list().execute()
    gsc_sites = response.get("siteEntry", [])

    if not gsc_sites:
        return 0, []

    # Sites déjà dans Supabase
    resp = httpx.get(
        f"{SUPABASE_URL}/rest/v1/sites",
        headers=HEADERS,
        params={"select": "url"},
        timeout=10.0,
    )
    existing_urls = set()
    if resp.status_code == 200:
        existing_urls = {s["url"] for s in resp.json()}

    new_count = 0
    all_sites = []
    for site in gsc_sites:
        site_url = site["siteUrl"]
        if not site_url.endswith("/") and not site_url.startswith("sc-domain:"):
            site_url += "/"
        name = site_url.replace("sc-domain:", "").replace("https://", "").replace("http://", "").rstrip("/")
        all_sites.append({"url": site_url, "name": name})

        if site_url not in existing_urls:
            resp = httpx.post(
                f"{SUPABASE_URL}/rest/v1/sites",
                headers={**HEADERS, "Content-Type": "application/json", "Prefer": "return=minimal"},
                json={"url": site_url, "name": name, "active": False},
                timeout=10.0,
            )
            if resp.status_code in (200, 201):
                new_count += 1

    return new_count, all_sites


def run_tracker_collect(site: str | None = None):
    """Lance tracker.py pour collecter les données. Si site est fourni, ne collecte que ce site."""
    tracker_path = Path(__file__).parent / "tracker.py"
    cmd = [sys.executable, str(tracker_path)]
    if site:
        cmd.extend(["--site", site])
    result = subprocess.run(
        cmd,
        cwd=str(tracker_path.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,  # 5 min max
    )
    return result.returncode, result.stdout, result.stderr


# ============================================================
# REQUÊTES SUPABASE
# ============================================================

def _fetch(table: str, params: dict) -> pd.DataFrame:
    """Requête GET générique vers l'API REST Supabase."""
    resp = httpx.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params,
        timeout=15.0,
    )
    if resp.status_code != 200:
        st.error(f"Erreur Supabase ({table}): {resp.status_code}")
        return pd.DataFrame()
    data = resp.json()
    return pd.DataFrame(data) if data else pd.DataFrame()


@st.cache_data(ttl=300)
def get_all_summaries() -> pd.DataFrame:
    """Tous les résumés hebdomadaires, triés par site et semaine."""
    df = _fetch("weekly_summary", {"select": "*", "order": "site.asc,week_start.asc"})
    if not df.empty:
        df["week_start"] = pd.to_datetime(df["week_start"])
        df["week_end"] = pd.to_datetime(df["week_end"])
    return df


@st.cache_data(ttl=300)
def get_sites() -> list:
    """Liste des sites disponibles (données existantes + sites actifs)."""
    sites = set()
    df = get_all_summaries()
    if not df.empty:
        sites.update(df["site"].unique().tolist())
    # Ajouter les sites actifs de la table sites (même sans données)
    resp = httpx.get(
        f"{SUPABASE_URL}/rest/v1/sites",
        headers=HEADERS,
        params={"select": "name", "active": "eq.true"},
        timeout=10.0,
    )
    if resp.status_code == 200:
        for s in resp.json():
            sites.add(s["name"])
    return sorted(sites)


@st.cache_data(ttl=300)
def get_week_data(site: str, week_start: str, limit: int = 500) -> pd.DataFrame:
    """Données détaillées pour un site et une semaine."""
    return _fetch("weekly_data", {
        "select": "query,page,clicks,impressions,ctr,position",
        "site": f"eq.{site}",
        "week_start": f"eq.{week_start}",
        "order": "impressions.desc",
        "limit": str(limit),
    })


@st.cache_data(ttl=300)
def get_query_evolution(site: str, query: str) -> pd.DataFrame:
    """Évolution de position d'une requête dans le temps."""
    df = _fetch("weekly_data", {
        "select": "week_start,position,clicks,impressions,ctr",
        "site": f"eq.{site}",
        "query": f"eq.{query}",
        "order": "week_start.asc",
    })
    if not df.empty:
        df["week_start"] = pd.to_datetime(df["week_start"])
    return df


@st.cache_data(ttl=300)
def search_queries(site: str, search_term: str) -> list:
    """Recherche de requêtes contenant un terme (via ilike)."""
    df = _fetch("weekly_data", {
        "select": "query",
        "site": f"eq.{site}",
        "query": f"ilike.*{search_term}*",
        "limit": "20",
    })
    if df.empty:
        return []
    return sorted(df["query"].unique().tolist())


@st.cache_data(ttl=300)
def get_available_weeks(site: str) -> list:
    """Semaines disponibles pour un site (depuis les résumés)."""
    summaries = get_all_summaries()
    site_data = summaries[summaries["site"] == site].sort_values("week_start", ascending=False)
    return site_data["week_start"].dt.strftime("%Y-%m-%d").tolist()


def compare_weeks(site: str, week_current: str, week_previous: str) -> pd.DataFrame:
    """Compare deux semaines et calcule les variations."""
    current = get_week_data(site, week_current, limit=5000)
    previous = get_week_data(site, week_previous, limit=5000)

    if current.empty:
        return pd.DataFrame()
    if previous.empty:
        return current

    previous = previous.rename(columns={
        "clicks": "clicks_prev",
        "impressions": "impressions_prev",
        "ctr": "ctr_prev",
        "position": "position_prev",
    })
    merged = current.merge(previous[["query", "page", "clicks_prev", "impressions_prev", "position_prev"]],
                           on=["query", "page"], how="left")
    merged["pos_change"] = merged["position"] - merged["position_prev"]
    merged["clicks_change"] = merged["clicks"] - merged["clicks_prev"]
    return merged


# ============================================================
# COMPOSANTS UI
# ============================================================

def color_position_change(val):
    """Colore les variations (vert = progression, rouge = chute)."""
    if pd.isna(val):
        return ""
    if val < -3:
        return "color: #22c55e; font-weight: bold"
    elif val > 3:
        return "color: #ef4444; font-weight: bold"
    return ""


# ============================================================
# PAGES
# ============================================================

def page_overview():
    """Vue globale de tous les sites."""
    st.header("Vue globale")

    all_summaries = get_all_summaries()
    all_sites = get_sites()

    if not all_sites:
        st.warning("Aucun site. Connectez votre compte GSC depuis **Gestion des sites**.")
        return

    sites_with_data = set(all_summaries["site"].unique()) if not all_summaries.empty else set()

    # Sites actifs sans données
    sites_pending = [s for s in all_sites if s not in sites_with_data]
    if sites_pending:
        st.info(
            f"**{len(sites_pending)} site(s) en attente de collecte** : "
            + ", ".join(sites_pending)
            + "\n\nActivez-les depuis **Gestion des sites** pour lancer la collecte automatiquement."
        )

    sites = sorted(sites_with_data)
    if not sites:
        return

    # KPIs par site (dernière semaine)
    for site in sites:
        site_data = all_summaries[all_summaries["site"] == site].sort_values("week_start")
        if site_data.empty:
            continue

        latest = site_data.iloc[-1]
        with st.expander(f"**{site}** — Semaine du {latest['week_start'].strftime('%Y-%m-%d')}", expanded=True):
            cols = st.columns(5)
            cols[0].metric("Requêtes", f"{int(latest['total_queries']):,}")
            cols[1].metric("Clicks", f"{int(latest['total_clicks']):,}")
            cols[2].metric("Impressions", f"{int(latest['total_impressions']):,}")
            cols[3].metric("Position moy.", f"{latest['avg_position']:.1f}")

            if len(site_data) >= 2:
                prev = site_data.iloc[-2]
                delta_clicks = int(latest['total_clicks'] - prev['total_clicks'])
                cols[1].caption(f"{'↗' if delta_clicks >= 0 else '↘'} {delta_clicks:+,}")

            has_trends = "trends_up" in latest.index and "trends_down" in latest.index
            alert_col_count = 7 if has_trends else 5
            alert_cols = st.columns(alert_col_count)
            alert_cols[0].metric("🟢 Progress.", int(latest["progressions"]))
            alert_cols[1].metric("🔴 Chutes", int(latest["drops"]))
            alert_cols[2].metric("🎯 Quick Wins", int(latest["quickwins"]))
            alert_cols[3].metric("👁️ Mauvais CTR", int(latest["low_ctr"]))
            alert_cols[4].metric("🆕 Nouvelles", int(latest["new_queries"]))
            if has_trends:
                alert_cols[5].metric("📈 Tendance +", int(latest["trends_up"]))
                alert_cols[6].metric("📉 Tendance -", int(latest["trends_down"]))

    # Graphiques comparatifs — toggle semaine/mois
    time_view = st.radio("Granularité", ["Par semaine", "Par mois"], horizontal=True, key="overview_time_view")

    if time_view == "Par mois":
        chart_data = all_summaries.copy()
        chart_data["month"] = chart_data["week_start"].dt.to_period("M").dt.to_timestamp()
        monthly = chart_data.groupby(["month", "site"]).agg(
            total_clicks=("total_clicks", "sum"),
            total_impressions=("total_impressions", "sum"),
            avg_position=("avg_position", "mean"),
        ).reset_index()
        monthly["avg_position"] = monthly["avg_position"].round(1)

        st.subheader("Évolution des clicks (mensuel)")
        chart = monthly.pivot_table(index="month", columns="site", values="total_clicks").fillna(0)
        if not chart.empty:
            st.line_chart(chart)

        st.subheader("Évolution de la position moyenne (mensuel)")
        chart = monthly.pivot_table(index="month", columns="site", values="avg_position").fillna(0)
        if not chart.empty:
            st.line_chart(chart)
            st.caption("Plus bas = mieux")
    else:
        st.subheader("Évolution des clicks")
        chart = all_summaries.pivot_table(index="week_start", columns="site", values="total_clicks").fillna(0)
        if not chart.empty:
            st.line_chart(chart)

        st.subheader("Évolution de la position moyenne")
        chart = all_summaries.pivot_table(index="week_start", columns="site", values="avg_position").fillna(0)
        if not chart.empty:
            st.line_chart(chart)
            st.caption("Plus bas = mieux")


def page_site_detail():
    """Détail d'un site avec comparaison semaine N vs N-1."""
    sites = get_sites()
    if not sites:
        st.warning("Aucune donnée.")
        return

    site = st.selectbox("Site", sites)
    all_summaries = get_all_summaries()
    history = all_summaries[all_summaries["site"] == site].sort_values("week_start")

    if history.empty:
        st.warning(f"Pas de données pour {site}.")
        return

    latest = history.iloc[-1]

    # KPIs
    st.subheader(f"Semaine du {latest['week_start'].strftime('%Y-%m-%d')}")
    cols = st.columns(4)
    if len(history) >= 2:
        prev = history.iloc[-2]
        cols[0].metric("Clicks", f"{int(latest['total_clicks']):,}",
                       delta=f"{int(latest['total_clicks'] - prev['total_clicks']):+,}")
        cols[1].metric("Impressions", f"{int(latest['total_impressions']):,}",
                       delta=f"{int(latest['total_impressions'] - prev['total_impressions']):+,}")
        cols[2].metric("Position moy.", f"{latest['avg_position']:.1f}",
                       delta=f"{latest['avg_position'] - prev['avg_position']:.1f}", delta_color="inverse")
        cols[3].metric("Requêtes", f"{int(latest['total_queries']):,}",
                       delta=f"{int(latest['total_queries'] - prev['total_queries']):+,}")
    else:
        cols[0].metric("Clicks", f"{int(latest['total_clicks']):,}")
        cols[1].metric("Impressions", f"{int(latest['total_impressions']):,}")
        cols[2].metric("Position moy.", f"{latest['avg_position']:.1f}")
        cols[3].metric("Requêtes", f"{int(latest['total_queries']):,}")

    # Graphiques d'évolution
    st.subheader("Évolution")
    detail_time_view = st.radio("Granularité", ["Par semaine", "Par mois"], horizontal=True, key="detail_time_view")

    if detail_time_view == "Par mois":
        chart_history = history.copy()
        chart_history["month"] = chart_history["week_start"].dt.to_period("M").dt.to_timestamp()
        monthly_hist = chart_history.groupby("month").agg(
            total_clicks=("total_clicks", "sum"),
            total_impressions=("total_impressions", "sum"),
            avg_position=("avg_position", "mean"),
            progressions=("progressions", "sum"),
            drops=("drops", "sum"),
            quickwins=("quickwins", "sum"),
            low_ctr=("low_ctr", "sum"),
            new_queries=("new_queries", "sum"),
        ).reset_index()
        monthly_hist["avg_position"] = monthly_hist["avg_position"].round(1)
        display_hist = monthly_hist.set_index("month")
    else:
        display_hist = history.set_index("week_start")

    tab1, tab2, tab3 = st.tabs(["Clicks & Impressions", "Position", "Alertes"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.line_chart(display_hist["total_clicks"])
        with c2:
            st.line_chart(display_hist["total_impressions"])

    with tab2:
        st.line_chart(display_hist["avg_position"])
        st.caption("Plus bas = mieux")

    with tab3:
        det_cols = ["progressions", "drops", "quickwins", "low_ctr", "new_queries"]
        available_det = [c for c in det_cols if c in display_hist.columns]
        st.bar_chart(display_hist[available_det])

    # Comparaison N vs N-1
    st.subheader("Comparaison semaine N vs N-1")
    weeks = get_available_weeks(site)

    if len(weeks) >= 2:
        comparison = compare_weeks(site, weeks[0], weeks[1])

        if not comparison.empty and "pos_change" in comparison.columns:
            filter_type = st.radio("Segment", [
                "Toutes", "🟢 Progressions", "🔴 Chutes",
                "🎯 Quick Wins", "👁️ Mauvais CTR", "🆕 Nouvelles", "🔄 Cannibalisation"
            ], horizontal=True)

            search = st.text_input("Rechercher une requête", "")
            filtered = comparison.copy()

            if filter_type == "🟢 Progressions":
                filtered = filtered[filtered["pos_change"] < -3]
            elif filter_type == "🔴 Chutes":
                filtered = filtered[filtered["pos_change"] > 3]
            elif filter_type == "🎯 Quick Wins":
                filtered = filtered[(filtered["position"] >= 4) & (filtered["position"] <= 12) & (filtered["impressions"] >= 100)]
                # Add priority_score column
                expected_ctr_map = {1: 30, 2: 15, 3: 10, 4: 7, 5: 5, 6: 4, 7: 3, 8: 2.5, 9: 2, 10: 1.5, 11: 1, 12: 1}
                if not filtered.empty:
                    filtered = filtered.copy()
                    filtered["expected_ctr"] = filtered["position"].apply(
                        lambda p: expected_ctr_map.get(int(round(p)), 1)
                    )
                    filtered["priority_score"] = (
                        filtered["impressions"]
                        * (1 / filtered["position"])
                        * (filtered["expected_ctr"] - filtered["ctr"]).clip(lower=0)
                    ).round(1)
                    filtered = filtered.sort_values("priority_score", ascending=False)
            elif filter_type == "👁️ Mauvais CTR":
                filtered = filtered[(filtered["impressions"] >= 200) & (filtered["ctr"] < 3)]
            elif filter_type == "🆕 Nouvelles":
                filtered = filtered[filtered["position_prev"].isna()]
            elif filter_type == "🔄 Cannibalisation":
                # Detect cannibalization: queries with 2+ distinct pages (ignoring # fragments)
                filtered = filtered.copy()
                filtered["page_clean"] = filtered["page"].str.split("#").str[0]
                cannibal_queries = filtered.groupby("query").filter(lambda g: g["page_clean"].nunique() >= 2)
                filtered = cannibal_queries.sort_values(["query", "position"])
                filtered = filtered.drop(columns=["page_clean"])

            if search:
                filtered = filtered[filtered["query"].str.contains(search, case=False, na=False)]

            display_cols = ["query", "page", "clicks", "impressions", "ctr", "position", "position_prev", "pos_change", "clicks_change"]
            if filter_type == "🎯 Quick Wins" and "priority_score" in filtered.columns:
                display_cols = ["query", "page", "clicks", "impressions", "ctr", "position", "priority_score", "position_prev", "pos_change", "clicks_change"]
            available = [c for c in display_cols if c in filtered.columns]

            if "pos_change" in available:
                st.dataframe(
                    filtered[available].head(300).style.map(color_position_change, subset=["pos_change"]),
                    use_container_width=True, height=500,
                )
            else:
                st.dataframe(filtered[available].head(300), use_container_width=True, height=500)

            st.caption(f"{len(filtered)} requêtes")
    else:
        st.info("Pas assez de semaines pour comparer.")


def page_query_tracker():
    """Suivi de l'évolution d'une requête spécifique."""
    sites = get_sites()
    if not sites:
        st.warning("Aucune donnée.")
        return

    c1, c2 = st.columns([1, 2])
    with c1:
        site = st.selectbox("Site", sites, key="qt_site")
    with c2:
        query_search = st.text_input("Requête à suivre")

    if not query_search:
        st.info("Tapez une requête pour voir son évolution.")
        return

    matches = search_queries(site, query_search)
    if not matches:
        st.warning(f"Aucune requête trouvée contenant '{query_search}'.")
        return

    selected = st.selectbox("Résultats", matches)
    evolution = get_query_evolution(site, selected)

    if evolution.empty:
        st.warning("Pas de données.")
        return

    st.subheader(f"« {selected} »")

    c1, c2, c3 = st.columns(3)
    c1.metric("Position actuelle", f"{evolution.iloc[-1]['position']:.1f}")
    c2.metric("Clicks", int(evolution.iloc[-1]["clicks"]))
    c3.metric("Impressions", int(evolution.iloc[-1]["impressions"]))

    if len(evolution) > 1:
        change = evolution.iloc[-1]["position"] - evolution.iloc[-2]["position"]
        c1.caption(f"Variation : {change:+.1f}")

    col1, col2 = st.columns(2)
    with col1:
        st.caption("Position")
        st.line_chart(evolution.set_index("week_start")["position"])
    with col2:
        st.caption("Clicks & Impressions")
        st.line_chart(evolution.set_index("week_start")[["clicks", "impressions"]])

    with st.expander("Données brutes"):
        st.dataframe(evolution, use_container_width=True)


def page_admin_sites():
    """Gestion des sites trackés."""
    st.header("Gestion des sites")

    # ── Connexion GSC ──────────────────────────────────────────
    st.subheader("🔗 Connexion Google Search Console")

    connected, creds = get_gsc_connection_status()

    if connected:
        st.success("Connecté à Google Search Console")
        col_sync, col_collect, col_disconnect = st.columns(3)
        if col_sync.button("🔄 Synchroniser les sites"):
            with st.spinner("Synchronisation en cours..."):
                new_count, all_sites = sync_gsc_sites(creds)
            if new_count > 0:
                st.success(f"{new_count} nouveau(x) site(s) importé(s) sur {len(all_sites)} trouvés !")
            else:
                st.info(f"Tous les {len(all_sites)} sites sont déjà synchronisés.")
            st.cache_data.clear()
            st.rerun()
        if col_collect.button("🚀 Collecter les données"):
            with st.spinner("Collecte en cours... (cela peut prendre quelques minutes)"):
                returncode, stdout, stderr = run_tracker_collect()
            if returncode == 0:
                st.success("Collecte terminée !")
                with st.expander("Détails"):
                    st.code(stdout)
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Erreur lors de la collecte.")
                with st.expander("Détails de l'erreur"):
                    st.code(stderr or stdout)
        if col_disconnect.button("🔌 Déconnecter"):
            TOKEN_FILE.unlink(missing_ok=True)
            st.cache_data.clear()
            st.rerun()
    else:
        if not CREDENTIALS_FILE.exists():
            st.error(
                "Fichier `credentials.json` introuvable dans le dossier du projet.\n\n"
                "Créez un projet Google Cloud, activez l'API Search Console, "
                "et téléchargez les identifiants OAuth."
            )
        else:
            st.info("Connectez votre compte Google pour importer automatiquement vos sites GSC.")

            if st.button("Se connecter à Google Search Console"):
                flow = Flow.from_client_secrets_file(
                    str(CREDENTIALS_FILE),
                    scopes=GSC_SCOPES,
                    redirect_uri="http://localhost",
                )
                auth_url, state = flow.authorization_url(
                    access_type="offline",
                    prompt="consent",
                )
                st.session_state["gsc_auth_url"] = auth_url

            if st.session_state.get("gsc_auth_url"):
                st.markdown("**Étapes :**")
                st.markdown(f"1. [Cliquez ici pour autoriser l'accès GSC]({st.session_state['gsc_auth_url']})")
                st.markdown("2. Autorisez l'accès dans la fenêtre Google")
                st.markdown("3. Vous serez redirigé vers une page qui **ne charge pas** — c'est normal")
                st.markdown("4. Copiez l'**URL complète** depuis la barre d'adresse et collez-la ci-dessous :")

                redirect_url = st.text_input(
                    "URL de redirection (commençant par http://localhost?...)",
                    key="gsc_redirect_url",
                )
                if redirect_url and "code=" in redirect_url:
                    try:
                        parsed = urlparse(redirect_url)
                        code = parse_qs(parsed.query)["code"][0]

                        flow = Flow.from_client_secrets_file(
                            str(CREDENTIALS_FILE),
                            scopes=GSC_SCOPES,
                            redirect_uri="http://localhost",
                        )
                        flow.fetch_token(code=code)
                        new_creds = flow.credentials
                        TOKEN_FILE.write_text(new_creds.to_json())

                        # Auto-sync des sites
                        new_count, all_sites = sync_gsc_sites(new_creds)
                        st.success(
                            f"Connecté ! {len(all_sites)} site(s) trouvé(s) dans GSC, "
                            f"{new_count} nouveau(x) importé(s)."
                        )
                        st.session_state.pop("gsc_auth_url", None)
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erreur d'authentification : {e}")

    st.markdown("---")

    # ── Liste des sites ────────────────────────────────────────
    resp = httpx.get(
        f"{SUPABASE_URL}/rest/v1/sites",
        headers=HEADERS,
        params={"select": "*", "order": "name.asc"},
        timeout=15.0,
    )
    sites_data = resp.json() if resp.status_code == 200 else []

    all_summaries = get_all_summaries()
    sites_with_data = set()
    if not all_summaries.empty:
        sites_with_data = set(all_summaries["site"].unique())

    active_sites = [s for s in sites_data if s["active"]]
    inactive_sites = [s for s in sites_data if not s["active"]]

    if active_sites:
        st.subheader(f"🟢 Sites actifs ({len(active_sites)})")
        for site in active_sites:
            short_name = site["url"].replace("sc-domain:", "").replace("https://", "").replace("http://", "").rstrip("/")
            has_data = short_name in sites_with_data
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            col1.markdown(f"**{site['name']}** — `{site['url']}`")
            col2.markdown("📊 Données" if has_data else "⏳ En attente")
            if col3.button("Désactiver", key=f"deactivate_{site['id']}"):
                httpx.patch(
                    f"{SUPABASE_URL}/rest/v1/sites",
                    headers={**HEADERS, "Prefer": "return=minimal"},
                    params={"id": f"eq.{site['id']}"},
                    json={"active": False},
                    timeout=10.0,
                )
                st.cache_data.clear()
                st.rerun()
            if col4.button("🗑️", key=f"del_{site['id']}"):
                httpx.delete(
                    f"{SUPABASE_URL}/rest/v1/sites",
                    headers=HEADERS,
                    params={"id": f"eq.{site['id']}"},
                    timeout=10.0,
                )
                st.cache_data.clear()
                st.rerun()
        st.markdown("---")

    if inactive_sites:
        st.subheader(f"🔴 Sites inactifs ({len(inactive_sites)})")
        st.caption("Ces sites ne sont pas collectés. Activez-les pour les inclure dans le prochain tracking.")
        for site in inactive_sites:
            col1, col2, col3 = st.columns([3, 1, 1])
            col1.markdown(f"**{site['name']}** — `{site['url']}`")
            if col2.button("Activer", key=f"activate_{site['id']}"):
                httpx.patch(
                    f"{SUPABASE_URL}/rest/v1/sites",
                    headers={**HEADERS, "Prefer": "return=minimal"},
                    params={"id": f"eq.{site['id']}"},
                    json={"active": True},
                    timeout=10.0,
                )
                # Collecter les données immédiatement pour ce site
                with st.spinner(f"Collecte des données pour {site['name']}..."):
                    returncode, stdout, stderr = run_tracker_collect(site['name'])
                if returncode == 0:
                    st.success(f"Site **{site['name']}** activé et données collectées !")
                else:
                    st.warning(f"Site activé mais erreur lors de la collecte. Détails : {stderr or stdout}")
                st.cache_data.clear()
                st.rerun()
            if col3.button("🗑️", key=f"del_inactive_{site['id']}"):
                httpx.delete(
                    f"{SUPABASE_URL}/rest/v1/sites",
                    headers=HEADERS,
                    params={"id": f"eq.{site['id']}"},
                    timeout=10.0,
                )
                st.cache_data.clear()
                st.rerun()
        st.markdown("---")

    # ── Ajout manuel ───────────────────────────────────────────
    st.subheader("Ajouter un site manuellement")
    st.caption("Le format doit correspondre exactement à celui de Google Search Console (ex: `https://monsite.fr/`).")

    with st.form("add_site"):
        new_url = st.text_input("URL du site (ex: https://monsite.fr/)")
        new_name = st.text_input("Nom court (ex: monsite.fr)")
        submitted = st.form_submit_button("Ajouter")

        if submitted and new_url and new_name:
            new_url = new_url.strip()
            if new_url.startswith("https://http://") or new_url.startswith("https://https://"):
                new_url = "https://" + new_url.split("://", 2)[-1]
            elif new_url.startswith("http://http://") or new_url.startswith("http://https://"):
                new_url = "https://" + new_url.split("://", 2)[-1]
            if not new_url.startswith(("https://", "http://", "sc-domain:")):
                new_url = f"https://{new_url}"
            if not new_url.endswith("/") and not new_url.startswith("sc-domain:"):
                new_url += "/"

            resp = httpx.post(
                f"{SUPABASE_URL}/rest/v1/sites",
                headers={**HEADERS, "Prefer": "return=minimal"},
                json={"url": new_url, "name": new_name.strip(), "active": True},
                timeout=10.0,
            )
            if resp.status_code in (200, 201):
                # Collecter les données immédiatement
                with st.spinner(f"Collecte des données pour {new_name.strip()}..."):
                    returncode, stdout, stderr = run_tracker_collect(new_name.strip())
                if returncode == 0:
                    st.success(f"Site **{new_name}** ajouté et données collectées !")
                else:
                    st.warning(f"Site ajouté mais erreur lors de la collecte. Détails : {stderr or stdout}")
                st.cache_data.clear()
                st.rerun()
            elif resp.status_code == 409:
                st.error("Ce site existe déjà.")
            else:
                st.error(f"Erreur: {resp.status_code} — {resp.text}")


@st.cache_data(ttl=300)
def get_cannibalization_data(site: str, week_start: str) -> pd.DataFrame:
    """Récupère les données de cannibalisation pour un site et une semaine."""
    df = get_week_data(site, week_start, limit=5000)
    if df.empty:
        return pd.DataFrame()
    # Find queries with 2+ pages
    pages_per_query = df.groupby("query").filter(lambda g: g["page"].nunique() >= 2)
    if pages_per_query.empty:
        return pd.DataFrame()
    return pages_per_query.sort_values(["query", "position"])


def page_cannibalization():
    """Page dédiée à la détection de cannibalisation."""
    st.header("Cannibalisation")
    st.caption("Requêtes pour lesquelles plusieurs pages de votre site se font concurrence dans les résultats de recherche.")

    sites = get_sites()
    if not sites:
        st.warning("Aucune donnée.")
        return

    site = st.selectbox("Site", sites, key="cannibal_site")
    weeks = get_available_weeks(site)
    if not weeks:
        st.warning("Pas de données.")
        return

    week = st.selectbox("Semaine", weeks, format_func=lambda w: f"Semaine du {w}", key="cannibal_week")

    data = get_week_data(site, week, limit=5000)
    if data.empty:
        st.warning("Pas de données pour cette semaine.")
        return

    # Normaliser les URLs en supprimant les fragments (#...)
    data = data.copy()
    data["page_clean"] = data["page"].str.split("#").str[0]

    # Detect cannibalized queries (2+ pages distinctes sans les #)
    query_pages = data.groupby("query").agg(
        page_count=("page_clean", "nunique"),
        total_clicks=("clicks", "sum"),
        total_impressions=("impressions", "sum"),
    ).reset_index()
    cannibalized = query_pages[query_pages["page_count"] >= 2].sort_values("total_impressions", ascending=False)

    if cannibalized.empty:
        st.success("Aucune cannibalisation détectée pour cette semaine.")
        return

    # Summary KPIs
    cols = st.columns(3)
    cols[0].metric("Requêtes cannibalisées", len(cannibalized))
    cols[1].metric("Impressions impactées", f"{int(cannibalized['total_impressions'].sum()):,}")
    cols[2].metric("Clicks impactés", f"{int(cannibalized['total_clicks'].sum()):,}")

    st.markdown("---")

    # Multi-site summary if viewing all sites
    all_summaries = get_all_summaries()
    if len(sites) > 1:
        st.subheader("Cannibalisation par site")
        site_cannibal_counts = []
        for s in sites:
            s_weeks = get_available_weeks(s)
            if s_weeks:
                s_data = get_week_data(s, s_weeks[0], limit=5000)
                if not s_data.empty:
                    s_data = s_data.copy()
                    s_data["page_clean"] = s_data["page"].str.split("#").str[0]
                    s_qp = s_data.groupby("query").agg(page_count=("page_clean", "nunique")).reset_index()
                    count = len(s_qp[s_qp["page_count"] >= 2])
                    site_cannibal_counts.append({"Site": s, "Requêtes cannibalisées": count})
        if site_cannibal_counts:
            st.dataframe(pd.DataFrame(site_cannibal_counts), use_container_width=True)
        st.markdown("---")

    # Detailed view per cannibalized query
    st.subheader(f"Détail — {len(cannibalized)} requêtes cannibalisées")

    search_cannibal = st.text_input("Filtrer les requêtes", "", key="cannibal_search")
    display_queries = cannibalized
    if search_cannibal:
        display_queries = display_queries[display_queries["query"].str.contains(search_cannibal, case=False, na=False)]

    for _, qrow in display_queries.head(50).iterrows():
        query = qrow["query"]
        query_data = data[data["query"] == query].copy()
        # Agréger par page nettoyée (sans #fragment)
        query_data_grouped = query_data.groupby("page_clean").agg(
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
            ctr=("ctr", "mean"),
            position=("position", "min"),
        ).reset_index().rename(columns={"page_clean": "page"}).sort_values("position")
        query_data_grouped["ctr"] = query_data_grouped["ctr"].round(2)
        with st.expander(
            f"**{query}** — {int(qrow['page_count'])} pages | "
            f"{int(qrow['total_impressions']):,} imp | {int(qrow['total_clicks']):,} clicks"
        ):
            st.dataframe(
                query_data_grouped[["page", "clicks", "impressions", "ctr", "position"]],
                use_container_width=True,
                hide_index=True,
            )

    if len(display_queries) > 50:
        st.caption(f"Affichage limité aux 50 premières requêtes sur {len(display_queries)}.")


def color_trend_change(val):
    """Colore les variations de tendance (vert = amélioration, rouge = déclin)."""
    if pd.isna(val):
        return ""
    if val < 0:
        return "color: #22c55e; font-weight: bold"
    elif val > 0:
        return "color: #ef4444; font-weight: bold"
    return ""


@st.cache_data(ttl=300)
def get_multi_week_data(site: str, num_weeks: int = 4) -> dict:
    """Récupère les données des N dernières semaines pour un site."""
    weeks = get_available_weeks(site)
    result = {}
    for i, week in enumerate(weeks[:num_weeks]):
        df = get_week_data(site, week, limit=5000)
        if not df.empty:
            result[i] = {"week_start": week, "data": df}
    return result


def compute_trends(multi_week: dict, min_impressions: int = 50) -> tuple:
    """
    Calcule les tendances haussières et baissières sur 3+ semaines consécutives.
    Retourne (trends_up_df, trends_down_df).
    """
    if len(multi_week) < 3:
        return pd.DataFrame(), pd.DataFrame()

    # Build position history: (query, page) -> {week_idx: position}
    position_history = {}
    week_indices = sorted(multi_week.keys())

    for w_idx in week_indices:
        df = multi_week[w_idx]["data"]
        for _, row in df.iterrows():
            key = (row["query"], row["page"])
            if key not in position_history:
                position_history[key] = {}
            position_history[key][w_idx] = row["position"]

    # Current week data for impressions/clicks
    current_df = multi_week[0]["data"] if 0 in multi_week else pd.DataFrame()

    trend_records = []
    for (query, page), positions in position_history.items():
        if 0 not in positions:
            continue

        # Ordered from oldest to newest
        ordered_weeks = sorted([w for w in week_indices if w in positions], reverse=True)
        pos_sequence = [(w, positions[w]) for w in ordered_weeks]

        if len(pos_sequence) < 3:
            continue

        consecutive_improving = 0
        consecutive_worsening = 0

        for i in range(1, len(pos_sequence)):
            prev_pos = pos_sequence[i - 1][1]
            curr_pos = pos_sequence[i][1]
            if curr_pos < prev_pos:
                consecutive_improving += 1
                consecutive_worsening = 0
            elif curr_pos > prev_pos:
                consecutive_worsening += 1
                consecutive_improving = 0
            else:
                consecutive_improving = 0
                consecutive_worsening = 0

        oldest_week = max(week_indices)
        position_oldest = positions.get(oldest_week)
        position_current = positions[0]

        # Get impressions/clicks from current week
        curr_row = current_df[(current_df["query"] == query) & (current_df["page"] == page)]
        impressions = int(curr_row["impressions"].iloc[0]) if not curr_row.empty else 0
        clicks = int(curr_row["clicks"].iloc[0]) if not curr_row.empty else 0

        if impressions < min_impressions:
            continue

        total_change = round(position_current - position_oldest, 1) if position_oldest is not None else None

        if consecutive_improving >= 3:
            trend_records.append({
                "query": query,
                "page": page,
                "position": round(position_current, 1),
                "position_4w_ago": round(position_oldest, 1) if position_oldest else None,
                "total_change": total_change,
                "weeks_trending": consecutive_improving,
                "impressions": impressions,
                "clicks": clicks,
                "direction": "up",
            })
        elif consecutive_worsening >= 3:
            trend_records.append({
                "query": query,
                "page": page,
                "position": round(position_current, 1),
                "position_4w_ago": round(position_oldest, 1) if position_oldest else None,
                "total_change": total_change,
                "weeks_trending": consecutive_worsening,
                "impressions": impressions,
                "clicks": clicks,
                "direction": "down",
            })

    if not trend_records:
        return pd.DataFrame(), pd.DataFrame()

    trends_df = pd.DataFrame(trend_records)
    trends_up = trends_df[trends_df["direction"] == "up"].drop(columns=["direction"]).copy()
    trends_up = trends_up.sort_values("total_change")
    trends_down = trends_df[trends_df["direction"] == "down"].drop(columns=["direction"]).copy()
    trends_down = trends_down.sort_values("total_change", ascending=False)

    return trends_up, trends_down


def page_trends():
    """Page de détection des tendances multi-semaines."""
    st.header("Tendances")
    st.caption("Requêtes dont la position s'améliore ou se dégrade de manière continue sur 3+ semaines consécutives.")

    sites = get_sites()
    if not sites:
        st.warning("Aucune donnée.")
        return

    site = st.selectbox("Site", sites, key="trends_site")
    weeks = get_available_weeks(site)

    if len(weeks) < 3:
        st.warning("Il faut au moins 3 semaines de données pour détecter des tendances.")
        return

    min_impressions = st.number_input(
        "Impressions minimum", min_value=0, value=50, step=10, key="trends_min_imp",
        help="Filtrer les requêtes ayant au moins ce nombre d'impressions sur la semaine en cours."
    )

    multi_week = get_multi_week_data(site, num_weeks=4)
    if len(multi_week) < 3:
        st.warning("Pas assez de semaines avec des données pour ce site.")
        return

    weeks_labels = [multi_week[w]["week_start"] for w in sorted(multi_week.keys())]
    st.info(f"Analyse sur {len(multi_week)} semaines : {weeks_labels[-1]} ... {weeks_labels[0]}")

    trends_up, trends_down = compute_trends(multi_week, min_impressions=min_impressions)

    # Summary metrics
    cols = st.columns(2)
    cols[0].metric("📈 Requêtes en hausse", len(trends_up))
    cols[1].metric("📉 Requêtes en baisse", len(trends_down))

    st.markdown("---")

    display_cols = ["query", "page", "position", "position_4w_ago", "total_change", "weeks_trending", "impressions", "clicks"]

    # Tendances haussières
    st.subheader("📈 Tendances haussières")
    st.caption("Requêtes dont la position s'améliore depuis 3+ semaines consécutives.")
    if trends_up.empty:
        st.info("Aucune tendance haussière détectée.")
    else:
        search_up = st.text_input("Filtrer les requêtes en hausse", "", key="trends_up_search")
        display_up = trends_up.copy()
        if search_up:
            display_up = display_up[display_up["query"].str.contains(search_up, case=False, na=False)]

        available_up = [c for c in display_cols if c in display_up.columns]
        st.dataframe(
            display_up[available_up].head(200).style.map(color_trend_change, subset=["total_change"]),
            use_container_width=True,
            height=min(400, 35 * len(display_up) + 40),
        )
        st.caption(f"{len(display_up)} requêtes en tendance haussière")

    st.markdown("---")

    # Tendances baissières
    st.subheader("📉 Tendances baissières")
    st.caption("Requêtes dont la position se dégrade depuis 3+ semaines consécutives.")
    if trends_down.empty:
        st.info("Aucune tendance baissière détectée.")
    else:
        search_down = st.text_input("Filtrer les requêtes en baisse", "", key="trends_down_search")
        display_down = trends_down.copy()
        if search_down:
            display_down = display_down[display_down["query"].str.contains(search_down, case=False, na=False)]

        available_down = [c for c in display_cols if c in display_down.columns]
        st.dataframe(
            display_down[available_down].head(200).style.map(color_trend_change, subset=["total_change"]),
            use_container_width=True,
            height=min(400, 35 * len(display_down) + 40),
        )
        st.caption(f"{len(display_down)} requêtes en tendance baissière")


def page_data_export():
    """Export des données brutes."""
    sites = get_sites()
    if not sites:
        st.warning("Aucune donnée.")
        return

    site = st.selectbox("Site", sites, key="exp_site")
    weeks = get_available_weeks(site)

    if not weeks:
        st.warning("Pas de données.")
        return

    week = st.selectbox("Semaine", weeks, format_func=lambda w: f"Semaine du {w}")
    data = get_week_data(site, week, limit=5000)

    st.subheader(f"{site} — Semaine du {week}")
    st.caption(f"{len(data)} requêtes")

    c1, c2 = st.columns(2)
    with c1:
        min_imp = st.number_input("Impressions min.", 0, value=0)
    with c2:
        max_pos = st.number_input("Position max.", 1, 100, value=100)

    filtered = data[(data["impressions"] >= min_imp) & (data["position"] <= max_pos)]
    st.dataframe(filtered, use_container_width=True, height=500)

    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button("Télécharger CSV", csv, f"gsc_{site}_{week}.csv", "text/csv")


# ============================================================
# APP PRINCIPALE
# ============================================================

# Sidebar navigation
st.sidebar.title("📊 GSC Tracker")
st.sidebar.markdown("---")

page = st.sidebar.radio("Navigation", [
    "Vue globale",
    "Détail par site",
    "Suivi de requête",
    "Tendances",
    "Cannibalisation",
    "Export données",
    "Gestion des sites",
])

st.sidebar.markdown("---")
st.sidebar.caption("GSC Ranking Tracker v2.0")
if st.sidebar.button("Rafraîchir les données"):
    st.cache_data.clear()
    st.rerun()

# Routeur
if page == "Vue globale":
    page_overview()
elif page == "Détail par site":
    page_site_detail()
elif page == "Suivi de requête":
    page_query_tracker()
elif page == "Tendances":
    page_trends()
elif page == "Cannibalisation":
    page_cannibalization()
elif page == "Export données":
    page_data_export()
elif page == "Gestion des sites":
    page_admin_sites()

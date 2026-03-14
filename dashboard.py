"""
GSC Ranking Tracker - Dashboard
================================
Dashboard Streamlit connecté à Supabase.
Déployable sur Streamlit Community Cloud.

Usage local :  streamlit run dashboard.py
"""

import httpx
import pandas as pd
import streamlit as st

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
    """Liste des sites disponibles."""
    df = get_all_summaries()
    return sorted(df["site"].unique().tolist()) if not df.empty else []


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
    if all_summaries.empty:
        st.warning("Aucune donnée. Lancez `python tracker.py` pour collecter les données.")
        return

    sites = sorted(all_summaries["site"].unique())

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

            alert_cols = st.columns(5)
            alert_cols[0].metric("🟢 Progress.", int(latest["progressions"]))
            alert_cols[1].metric("🔴 Chutes", int(latest["drops"]))
            alert_cols[2].metric("🎯 Quick Wins", int(latest["quickwins"]))
            alert_cols[3].metric("👁️ Mauvais CTR", int(latest["low_ctr"]))
            alert_cols[4].metric("🆕 Nouvelles", int(latest["new_queries"]))

    # Graphiques comparatifs
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
    tab1, tab2, tab3 = st.tabs(["Clicks & Impressions", "Position", "Alertes"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.line_chart(history.set_index("week_start")["total_clicks"])
        with c2:
            st.line_chart(history.set_index("week_start")["total_impressions"])

    with tab2:
        st.line_chart(history.set_index("week_start")["avg_position"])
        st.caption("Plus bas = mieux")

    with tab3:
        det_cols = ["week_start", "progressions", "drops", "quickwins", "low_ctr", "new_queries"]
        st.bar_chart(history[det_cols].set_index("week_start"))

    # Comparaison N vs N-1
    st.subheader("Comparaison semaine N vs N-1")
    weeks = get_available_weeks(site)

    if len(weeks) >= 2:
        comparison = compare_weeks(site, weeks[0], weeks[1])

        if not comparison.empty and "pos_change" in comparison.columns:
            filter_type = st.radio("Segment", [
                "Toutes", "🟢 Progressions", "🔴 Chutes",
                "🎯 Quick Wins", "👁️ Mauvais CTR", "🆕 Nouvelles"
            ], horizontal=True)

            search = st.text_input("Rechercher une requête", "")
            filtered = comparison.copy()

            if filter_type == "🟢 Progressions":
                filtered = filtered[filtered["pos_change"] < -3]
            elif filter_type == "🔴 Chutes":
                filtered = filtered[filtered["pos_change"] > 3]
            elif filter_type == "🎯 Quick Wins":
                filtered = filtered[(filtered["position"] >= 4) & (filtered["position"] <= 12) & (filtered["impressions"] >= 100)]
            elif filter_type == "👁️ Mauvais CTR":
                filtered = filtered[(filtered["impressions"] >= 200) & (filtered["ctr"] < 3)]
            elif filter_type == "🆕 Nouvelles":
                filtered = filtered[filtered["position_prev"].isna()]

            if search:
                filtered = filtered[filtered["query"].str.contains(search, case=False, na=False)]

            display_cols = ["query", "page", "clicks", "impressions", "ctr", "position", "position_prev", "pos_change", "clicks_change"]
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
    st.caption("Ajoutez, activez ou désactivez des sites directement depuis ce dashboard.")

    # Charger les sites depuis Supabase
    resp = httpx.get(
        f"{SUPABASE_URL}/rest/v1/sites",
        headers=HEADERS,
        params={"select": "*", "order": "name.asc"},
        timeout=15.0,
    )
    sites_data = resp.json() if resp.status_code == 200 else []

    # Afficher les sites existants
    if sites_data:
        st.subheader("Sites actuels")
        for site in sites_data:
            col1, col2, col3 = st.columns([3, 1, 1])
            col1.markdown(f"**{site['name']}** — `{site['url']}`")
            status = "Actif" if site["active"] else "Inactif"
            col2.markdown(f"{'🟢' if site['active'] else '🔴'} {status}")

            # Bouton activer/désactiver
            btn_label = "Désactiver" if site["active"] else "Activer"
            if col3.button(btn_label, key=f"toggle_{site['id']}"):
                httpx.patch(
                    f"{SUPABASE_URL}/rest/v1/sites",
                    headers={**HEADERS, "Prefer": "return=minimal"},
                    params={"id": f"eq.{site['id']}"},
                    json={"active": not site["active"]},
                    timeout=10.0,
                )
                st.rerun()
        st.markdown("---")

    # Formulaire d'ajout
    st.subheader("Ajouter un site")
    st.info(
        "Le format doit correspondre exactement à celui de Google Search Console. "
        "Lancez `python list_sites.py` pour voir les URLs exactes de votre compte."
    )

    with st.form("add_site"):
        new_url = st.text_input("URL du site (ex: https://monsite.fr/)")
        new_name = st.text_input("Nom court (ex: monsite.fr)")
        submitted = st.form_submit_button("Ajouter")

        if submitted and new_url and new_name:
            # Vérifier que l'URL se termine par /
            if not new_url.endswith("/"):
                new_url += "/"
            if not new_url.startswith("https://"):
                new_url = f"https://{new_url}"

            resp = httpx.post(
                f"{SUPABASE_URL}/rest/v1/sites",
                headers={**HEADERS, "Prefer": "return=minimal"},
                json={"url": new_url, "name": new_name, "active": True},
                timeout=10.0,
            )
            if resp.status_code in (200, 201):
                st.success(f"Site {new_name} ajouté !")
                st.rerun()
            elif resp.status_code == 409:
                st.error("Ce site existe déjà.")
            else:
                st.error(f"Erreur: {resp.status_code} — {resp.text}")

    # Section suppression
    if sites_data:
        st.markdown("---")
        st.subheader("Supprimer un site")
        site_to_delete = st.selectbox(
            "Site à supprimer",
            options=sites_data,
            format_func=lambda s: f"{s['name']} ({s['url']})",
            key="delete_site",
        )
        if st.button("Supprimer", type="secondary"):
            httpx.delete(
                f"{SUPABASE_URL}/rest/v1/sites",
                headers=HEADERS,
                params={"id": f"eq.{site_to_delete['id']}"},
                timeout=10.0,
            )
            st.success(f"Site {site_to_delete['name']} supprimé.")
            st.rerun()


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
elif page == "Export données":
    page_data_export()
elif page == "Gestion des sites":
    page_admin_sites()

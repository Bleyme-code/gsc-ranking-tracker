# GSC Ranking Tracker

Suivi automatisé des positions Google Search Console avec détection des progressions, chutes, quick wins et nouvelles requêtes.

## 1. Prérequis

- Python 3.10+
- Un compte Google avec accès aux propriétés Search Console

## 2. Configuration Google Cloud (credentials.json)

### Étape 1 : Créer un projet Google Cloud

1. Aller sur https://console.cloud.google.com/
2. Cliquer sur **Sélectionner un projet** > **Nouveau projet**
3. Nommer le projet (ex: `gsc-ranking-tracker`) et cliquer **Créer**

### Étape 2 : Activer l'API Search Console

1. Dans le menu, aller à **API et services** > **Bibliothèque**
2. Chercher **Google Search Console API**
3. Cliquer **Activer**

### Étape 3 : Configurer l'écran de consentement OAuth

1. Aller à **API et services** > **Écran de consentement OAuth**
2. Choisir **Externe** et cliquer **Créer**
3. Remplir le nom de l'application et l'email
4. Dans **Champs d'application**, ajouter : `https://www.googleapis.com/auth/webmasters.readonly`
5. Dans **Utilisateurs tests**, ajouter votre adresse Gmail
6. Enregistrer

### Étape 4 : Créer les identifiants OAuth2

1. Aller à **API et services** > **Identifiants**
2. Cliquer **Créer des identifiants** > **ID client OAuth**
3. Type d'application : **Application de bureau**
4. Nommer (ex: `gsc-tracker`) et cliquer **Créer**
5. **Télécharger le JSON** et le renommer en `credentials.json`
6. Placer `credentials.json` à la racine de ce projet

## 3. Installation

```bash
cd "E:\GSC Ranking Tracker"
pip install -r requirements.txt
```

## 4. Première exécution

```bash
python tracker.py
```

Un navigateur s'ouvrira pour l'authentification Google. Connectez-vous et autorisez l'accès. Le token sera sauvegardé dans `token.json` pour les exécutions suivantes.

## 5. Utilisation

### Tous les sites

```bash
python tracker.py
```

### Un seul site

```bash
python tracker.py --site avis-malin.fr
```

### Fichier de config personnalisé

```bash
python tracker.py --config ma_config.yaml
```

## 6. Automatisation (cron)

### Linux / Mac - Chaque lundi à 8h

```bash
crontab -e
```

Ajouter :

```
0 8 * * 1 cd /chemin/vers/gsc-tracker && /usr/bin/python3 tracker.py >> rapports/cron.log 2>&1
```

### Windows - Planificateur de tâches

1. Ouvrir le **Planificateur de tâches**
2. **Créer une tâche de base**
3. Déclencheur : **Hebdomadaire**, lundi, 08:00
4. Action : **Démarrer un programme**
   - Programme : `python`
   - Arguments : `tracker.py`
   - Démarrer dans : `E:\GSC Ranking Tracker`

## 7. Rapports générés

Les rapports sont créés dans le dossier `./rapports/` :

- `gsc_report_YYYY-MM-DD.xlsx` : rapport Excel complet
  - Un onglet par site avec toutes les requêtes
  - Onglets par segment (progressions, chutes, quick wins...)
  - Onglet "Résumé Global" avec KPIs
- `gsc_summary_YYYY-MM-DD.txt` : résumé texte (pour email/Slack)

## 8. Configuration (config.yaml)

Modifier `config.yaml` pour :

- Ajouter/retirer des sites
- Ajuster les seuils de détection
- Changer le dossier de sortie

## 9. Structure des fichiers

```
GSC Ranking Tracker/
├── tracker.py          # Script principal
├── config.yaml         # Configuration
├── requirements.txt    # Dépendances Python
├── credentials.json    # (à créer) Identifiants Google OAuth2
├── token.json          # (auto-généré) Token d'authentification
└── rapports/           # (auto-créé) Dossier des rapports
```

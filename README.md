# Collecteur Vélib'

Vélib' Métropole publie en temps réel l'état de ses stations, mais n'en garde aucun historique. Ce dépôt le constitue : il relève automatiquement la disponibilité des **1 526 stations** (vélos mécaniques, électriques, bornes libres, état de la station) et archive chaque relevé dans des fichiers CSV.

L'historique est collecté depuis le **2 novembre 2026** et s'enrichit tout seul, sans ordinateur allumé, grâce à GitHub Actions.

Même principe que le dépôt `lovasoa/historique-velib-opendata`, avec en plus une appli locale de suivi.

**Source :** flux GBFS ouvert de Vélib' Métropole (licence ODbL, sans clé d'accès).

## Les données

| Fichier | Contenu |
|---|---|
| [`velib/data/AAAA-MM-JJ.csv`](velib/data) | Un fichier par jour, horodatage en UTC |
| [`velib/data/stations.csv`](velib/data/stations.csv) | Le référentiel des stations (nom, capacité, position) |
| [`velib/data/etat_courant.json`](velib/data/etat_courant.json) | Le dernier état connu, qui sert à repérer ce qui a changé |

### Ce que contient chaque relevé

| Colonne | Sens |
|---|---|
| `horodatage_utc` | moment du relevé |
| `code` | numéro de station affiché sur la borne |
| `mecaniques`, `electriques`, `velos` | vélos disponibles |
| `bornes_libres` | places pour rendre un vélo |
| `hors_service_estime` | capacité − vélos − bornes libres (vélos ou bornes indisponibles) |
| `installee`, `location_ok`, `retour_ok` | station ouverte, location possible, retour possible |

Une station n'est réécrite que si son état a changé depuis le relevé précédent : l'historique reste complet, mais 5 à 10 fois plus léger. Pour reconstituer l'état du réseau à un instant T, on prend la dernière ligne de chaque station avant T.

## Fonctionnement

### Collecte automatique (GitHub Actions)

Le fichier [`.github/workflows/releve-velib.yml`](.github/workflows/releve-velib.yml) lance un relevé programmé toutes les 10 minutes sur les serveurs de GitHub, puis enregistre les nouvelles lignes dans le dépôt. GitHub peut espacer ces passages aux heures chargées.

Pour reproduire le dispositif dans un autre dépôt :

1. Pousser le dossier `velib/` et le dossier `.github/`.
2. Dans le dépôt : *Settings → Actions → General → Workflow permissions* → cocher **Read and write permissions**.
3. Onglet *Actions* → « Relevé Vélib' » → **Run workflow** pour un premier essai.

Sur un dépôt public, GitHub Actions est gratuit. GitHub peut désactiver les tâches planifiées d'un dépôt resté 60 jours sans activité manuelle : un commit de temps en temps suffit à les garder actives.

### Appli locale

```bash
python3 velib/velib_collecteur.py
```

Puis ouvrir http://localhost:8767 :

- chiffres clés du réseau (vélos mécaniques et électriques, bornes libres, stations vides ou pleines, places hors service estimées) ;
- carte des stations colorées selon leur taux de remplissage ;
- réglages : intervalle entre relevés (5 min par défaut), mode de stockage ;
- export CSV par période et par station (séparateur `;`, s'ouvre directement dans Excel).

Les données vont dans `velib.sqlite` (tables `stations`, `releves`, `etat_courant`, `passages`). Les relevés ne tournent que tant que le script est lancé. Pour le laisser tourner en continu sous Linux, une ligne dans `crontab -e` suffit :

```
@reboot cd /chemin/vers/velib && python3 velib_collecteur.py >> collecteur.log 2>&1
```

Aucune dépendance à installer : Python 3.8 ou plus suffit.

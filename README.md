Collecteur Vélib'
Stocke l'historique des disponibilités de toutes les stations Vélib' Métropole

(vélos mécaniques, électriques, bornes libres, état de la station), que Vélib'

publie en temps réel mais n'archive pas. Même principe que le dépôt

lovasoa/historique-velib-opendata, avec en plus une appli locale.
Source : flux GBFS ouvert de Vélib' Métropole (licence ODbL, sans clé).
Option 1 : appli locale
python3 velib_collecteur.py

Puis ouvrir http://localhost:8767 :
·	chiffres clés du réseau (vélos méca/élec, bornes libres, stations vides ou pleines, places hors service estimées) ;
·	carte des stations colorées selon leur taux de remplissage ;
·	réglages : intervalle entre relevés (5 min par défaut), mode de stockage ;
·	export CSV par période et par station (séparateur ;, s'ouvre directement dans Excel).
Les données vont dans velib.sqlite (tables stations, releves, etat_courant, passages).

Les relevés ne tournent que tant que le script est lancé.
Pour le laisser tourner en continu sous Linux, une ligne dans crontab -e suffit :
@reboot cd /chemin/vers/velib && python3 velib_collecteur.py >> collecteur.log 2>&1

Option 2 : GitHub Actions (sans ordinateur allumé)
1.	Créer un dépôt GitHub et y pousser velib_collecteur.py et le dossier .github/.
2.	Dans le dépôt : Settings → Actions → General → Workflow permissions → cocher Read and write permissions.
3.	Onglet Actions → « Relevé Vélib' » → Run workflow pour un premier essai.
Ensuite, toutes les 10 minutes, GitHub relève l'état et l'ajoute dans

data/AAAA-MM-JJ.csv (un fichier par jour, horodatage en UTC), avec

data/stations.csv pour le référentiel des stations.
Sur un dépôt public, GitHub Actions est gratuit. GitHub peut désactiver les

tâches planifiées d'un dépôt resté 60 jours sans activité manuelle : un commit

de temps en temps suffit à les garder actives.
Ce qui est stocké
Colonne	Sens
horodatage_utc	moment du relevé
code	numéro de station affiché sur la borne
mecaniques, electriques, velos	vélos disponibles
bornes_libres	places pour rendre un vélo
hors_service_estime	capacité − vélos − bornes libres (vélos ou bornes indisponibles)
installee, location_ok, retour_ok	station ouverte, location possible, retour possible

Par défaut, une station n'est réécrite que si son état a changé depuis le

relevé précédent. Pour reconstituer l'état complet à un instant T, on prend

la dernière ligne de chaque station avant T. Le mode « tout » (dans les

réglages ou config.json) écrit toutes les stations à chaque relevé.
Ordre de grandeur en mode changements, relevé toutes les 5 min : quelques

dizaines de Mo par mois.

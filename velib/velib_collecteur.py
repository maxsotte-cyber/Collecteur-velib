#!/usr/bin/env python3
"""
Collecteur Vélib' Métropole
===========================

Relève régulièrement l'état de toutes les stations Vélib' (flux GBFS ouvert,
sans clé) et stocke l'historique, que Vélib' ne publie pas.

Deux façons de l'utiliser :

1. Appli locale (par défaut)
       python3 velib_collecteur.py
   -> relevés automatiques en tâche de fond, stockage dans velib.sqlite,
      interface sur http://localhost:8767 (état, carte, réglages, export CSV).

2. Relevé unique vers des CSV (pour GitHub Actions, cron, etc.)
       python3 velib_collecteur.py --une-fois --csv data
   -> ajoute les changements du moment dans data/AAAA-MM-JJ.csv et sort.

Par défaut, seules les stations dont l'état a changé depuis le relevé
précédent sont enregistrées : l'historique reste complet, mais 5 à 10 fois
plus léger.

Aucune dépendance externe : Python 3.8+ suffit.
"""

import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ICI = os.path.dirname(os.path.abspath(__file__))
FICHIER_CONFIG = os.path.join(ICI, "config.json")

CONFIG_DEFAUT = {
    "intervalle_minutes": 5,
    "mode": "changements",          # "changements" ou "tout"
    "port": 8767,
    "base": "velib.sqlite",
    "url_stations": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
    "url_etat": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
    "releves_actifs": True,
}

CHAMPS_ETAT = ["mecaniques", "electriques", "velos", "bornes_libres",
               "installee", "location_ok", "retour_ok"]

COLONNES_EXPORT = ["horodatage_utc", "station_id", "code", "nom", "capacite",
                   "mecaniques", "electriques", "velos", "bornes_libres",
                   "hors_service_estime", "installee", "location_ok",
                   "retour_ok", "derniere_maj_station_utc"]


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def charger_config():
    cfg = dict(CONFIG_DEFAUT)
    if os.path.exists(FICHIER_CONFIG):
        try:
            with open(FICHIER_CONFIG, encoding="utf-8") as f:
                cfg.update(json.load(f))
        except (OSError, ValueError):
            pass
    return cfg


def sauver_config(cfg):
    with open(FICHIER_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def maintenant():
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# Lecture du flux GBFS
# --------------------------------------------------------------------------

def lire_json(url, delai=30):
    req = urllib.request.Request(url, headers={
        "User-Agent": "collecteur-velib/1.0 (projet data personnel)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=delai) as r:
        return json.loads(r.read().decode("utf-8"))


def _entier(v, defaut=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return defaut


def _types_velos(st):
    """Vélib' publie la répartition sous forme de liste [{'mechanical': n}, {'ebike': n}]."""
    meca, elec = 0, 0
    types = st.get("num_bikes_available_types")
    if isinstance(types, list):
        for t in types:
            if isinstance(t, dict):
                meca += _entier(t.get("mechanical"))
                elec += _entier(t.get("ebike"))
    elif isinstance(types, dict):
        meca = _entier(types.get("mechanical"))
        elec = _entier(types.get("ebike"))
    return meca, elec


def lire_stations(data):
    out = []
    for s in data.get("data", {}).get("stations", []):
        out.append({
            "station_id": str(s.get("station_id")),
            "code": str(s.get("stationCode") or ""),
            "nom": s.get("name") or "",
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "capacite": _entier(s.get("capacity"), None),
        })
    return out


def lire_etat(data):
    out = []
    for s in data.get("data", {}).get("stations", []):
        meca, elec = _types_velos(s)
        velos = _entier(s.get("num_bikes_available", s.get("numBikesAvailable")), meca + elec)
        bornes = _entier(s.get("num_docks_available", s.get("numDocksAvailable")))
        lr = s.get("last_reported")
        derniere = iso(datetime.fromtimestamp(int(lr), timezone.utc)) if lr else None
        out.append({
            "station_id": str(s.get("station_id")),
            "mecaniques": meca,
            "electriques": elec,
            "velos": velos,
            "bornes_libres": bornes,
            "installee": _entier(s.get("is_installed"), 1),
            "location_ok": _entier(s.get("is_renting"), 1),
            "retour_ok": _entier(s.get("is_returning"), 1),
            "derniere_maj": derniere,
        })
    return out


def signature(e):
    return tuple(e[k] for k in CHAMPS_ETAT)


def hors_service(capacite, velos, bornes):
    if capacite is None:
        return None
    return max(0, capacite - velos - bornes)


# --------------------------------------------------------------------------
# Stockage SQLite (appli locale)
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    code TEXT, nom TEXT, lat REAL, lon REAL, capacite INTEGER,
    vue_le TEXT
);
CREATE TABLE IF NOT EXISTS releves (
    horodatage TEXT NOT NULL,
    station_id TEXT NOT NULL,
    mecaniques INTEGER, electriques INTEGER, velos INTEGER,
    bornes_libres INTEGER,
    installee INTEGER, location_ok INTEGER, retour_ok INTEGER,
    derniere_maj TEXT
);
CREATE INDEX IF NOT EXISTS idx_releves_ts ON releves(horodatage);
CREATE INDEX IF NOT EXISTS idx_releves_station ON releves(station_id, horodatage);
CREATE TABLE IF NOT EXISTS etat_courant (
    station_id TEXT PRIMARY KEY,
    mecaniques INTEGER, electriques INTEGER, velos INTEGER,
    bornes_libres INTEGER,
    installee INTEGER, location_ok INTEGER, retour_ok INTEGER,
    derniere_maj TEXT, horodatage TEXT
);
CREATE TABLE IF NOT EXISTS passages (
    horodatage TEXT, ok INTEGER, stations INTEGER,
    lignes_ecrites INTEGER, duree_s REAL, erreur TEXT
);
"""


def ouvrir_base(chemin):
    con = sqlite3.connect(chemin, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


class Collecteur:
    def __init__(self, cfg):
        self.cfg = cfg
        chemin = cfg["base"]
        if not os.path.isabs(chemin):
            chemin = os.path.join(ICI, chemin)
        self.chemin_base = chemin
        self.con = ouvrir_base(chemin)
        self.verrou = threading.Lock()
        self.derniere_maj_stations = None
        self.prochain = None
        self.en_cours = False

    def maj_stations(self, force=False):
        h = maintenant()
        if (not force and self.derniere_maj_stations
                and h - self.derniere_maj_stations < timedelta(hours=12)):
            return
        stations = lire_stations(lire_json(self.cfg["url_stations"]))
        with self.con:
            self.con.executemany(
                """INSERT INTO stations VALUES (:station_id,:code,:nom,:lat,:lon,:capacite,:vue)
                   ON CONFLICT(station_id) DO UPDATE SET code=excluded.code, nom=excluded.nom,
                   lat=excluded.lat, lon=excluded.lon, capacite=excluded.capacite, vue_le=excluded.vue_le""",
                [dict(s, vue=iso(h)) for s in stations])
        self.derniere_maj_stations = h

    def collecter(self):
        with self.verrou:
            self.en_cours = True
            debut = time.time()
            h = iso(maintenant())
            try:
                self.maj_stations()
                etats = lire_etat(lire_json(self.cfg["url_etat"]))
                precedent = {
                    r["station_id"]: tuple(r[k] for k in CHAMPS_ETAT)
                    for r in self.con.execute("SELECT * FROM etat_courant")
                }
                tout = self.cfg.get("mode") == "tout"
                a_ecrire = [e for e in etats
                            if tout or precedent.get(e["station_id"]) != signature(e)]
                connues = {r[0] for r in self.con.execute("SELECT station_id FROM stations")}
                nouvelles = any(e["station_id"] not in connues for e in etats)
                with self.con:
                    self.con.executemany(
                        """INSERT INTO releves VALUES (:h,:station_id,:mecaniques,:electriques,:velos,
                           :bornes_libres,:installee,:location_ok,:retour_ok,:derniere_maj)""",
                        [dict(e, h=h) for e in a_ecrire])
                    self.con.executemany(
                        """INSERT OR REPLACE INTO etat_courant VALUES (:station_id,:mecaniques,:electriques,
                           :velos,:bornes_libres,:installee,:location_ok,:retour_ok,:derniere_maj,:h)""",
                        [dict(e, h=h) for e in etats])
                    self.con.execute("INSERT INTO passages VALUES (?,?,?,?,?,?)",
                                     (h, 1, len(etats), len(a_ecrire), round(time.time() - debut, 2), None))
                if nouvelles:
                    self.maj_stations(force=True)
                return {"ok": True, "stations": len(etats), "lignes": len(a_ecrire)}
            except Exception as ex:
                msg = f"{type(ex).__name__}: {ex}"
                with self.con:
                    self.con.execute("INSERT INTO passages VALUES (?,?,?,?,?,?)",
                                     (h, 0, 0, 0, round(time.time() - debut, 2), msg))
                return {"ok": False, "erreur": msg}
            finally:
                self.en_cours = False

    def boucle(self):
        while True:
            if self.cfg.get("releves_actifs", True):
                self.collecter()
            minutes = max(1, int(self.cfg.get("intervalle_minutes", 5)))
            # On cale les relevés sur des heures rondes (…:00, :05, :10…)
            t = time.time()
            pas = minutes * 60
            cible = (int(t // pas) + 1) * pas
            self.prochain = iso(datetime.fromtimestamp(cible, timezone.utc))
            time.sleep(max(1, cible - t))

    # ---- lectures pour l'interface ----

    def resume(self):
        c = self.con
        r = {}
        r["stations"] = c.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
        r["lignes"] = c.execute("SELECT COUNT(*) FROM releves").fetchone()[0]
        p = c.execute("SELECT MIN(horodatage), MAX(horodatage) FROM releves").fetchone()
        r["debut"], r["fin"] = p[0], p[1]
        r["passages_ok"] = c.execute("SELECT COUNT(*) FROM passages WHERE ok=1").fetchone()[0]
        r["passages_ko"] = c.execute("SELECT COUNT(*) FROM passages WHERE ok=0").fetchone()[0]
        t = c.execute("""SELECT SUM(e.mecaniques), SUM(e.electriques), SUM(e.bornes_libres),
                                SUM(CASE WHEN s.capacite IS NULL THEN 0
                                         ELSE MAX(0, s.capacite - e.velos - e.bornes_libres) END),
                                SUM(CASE WHEN e.velos=0 AND e.installee=1 THEN 1 ELSE 0 END),
                                SUM(CASE WHEN e.bornes_libres=0 AND e.installee=1 THEN 1 ELSE 0 END),
                                SUM(CASE WHEN e.installee=0 OR e.location_ok=0 THEN 1 ELSE 0 END),
                                MAX(e.horodatage)
                         FROM etat_courant e LEFT JOIN stations s USING(station_id)""").fetchone()
        r.update(mecaniques=t[0] or 0, electriques=t[1] or 0, bornes=t[2] or 0,
                 hors_service=t[3] or 0, vides=t[4] or 0, pleines=t[5] or 0,
                 fermees=t[6] or 0, dernier_releve=t[7])
        r["taille_mo"] = round(os.path.getsize(self.chemin_base) / 1e6, 1) if os.path.exists(self.chemin_base) else 0
        r["derniers_passages"] = [dict(x) for x in c.execute(
            "SELECT * FROM passages ORDER BY rowid DESC LIMIT 12")]
        r["prochain"] = self.prochain
        r["en_cours"] = self.en_cours
        r["config"] = {k: self.cfg[k] for k in ("intervalle_minutes", "mode", "releves_actifs")}
        return r

    def carte(self):
        return [dict(x) for x in self.con.execute(
            """SELECT s.station_id, s.code, s.nom, s.lat, s.lon, s.capacite,
                      e.mecaniques, e.electriques, e.velos, e.bornes_libres,
                      e.installee, e.location_ok
               FROM stations s JOIN etat_courant e USING(station_id)
               WHERE s.lat IS NOT NULL""")]

    def export(self, du=None, au=None, code=None):
        sql = """SELECT r.horodatage, r.station_id, s.code, s.nom, s.capacite,
                        r.mecaniques, r.electriques, r.velos, r.bornes_libres,
                        r.installee, r.location_ok, r.retour_ok, r.derniere_maj
                 FROM releves r LEFT JOIN stations s USING(station_id) WHERE 1=1"""
        args = []
        if du:
            sql += " AND r.horodatage >= ?"; args.append(du + "T00:00:00Z")
        if au:
            sql += " AND r.horodatage <= ?"; args.append(au + "T23:59:59Z")
        if code:
            sql += " AND s.code = ?"; args.append(code)
        sql += " ORDER BY r.horodatage, s.code"
        for r in self.con.execute(sql, args):
            yield [r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8],
                   hors_service(r[4], r[7], r[8]), r[9], r[10], r[11], r[12]]


# --------------------------------------------------------------------------
# Mode relevé unique vers CSV (GitHub Actions)
# --------------------------------------------------------------------------

def releve_csv(cfg, dossier):
    os.makedirs(dossier, exist_ok=True)
    h = maintenant()
    stations = lire_stations(lire_json(cfg["url_stations"]))
    etats = lire_etat(lire_json(cfg["url_etat"]))
    infos = {s["station_id"]: s for s in stations}

    # Référentiel des stations, réécrit à chaque passage
    with open(os.path.join(dossier, "stations.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["station_id", "code", "nom", "lat", "lon", "capacite"])
        for s in sorted(stations, key=lambda s: s["code"]):
            w.writerow([s["station_id"], s["code"], s["nom"], s["lat"], s["lon"], s["capacite"]])

    # Dernier état connu, pour n'écrire que les changements
    chemin_etat = os.path.join(dossier, "etat_courant.json")
    precedent = {}
    if os.path.exists(chemin_etat):
        with open(chemin_etat, encoding="utf-8") as f:
            precedent = {k: tuple(v) for k, v in json.load(f).items()}
    tout = cfg.get("mode") == "tout"
    a_ecrire = [e for e in etats if tout or precedent.get(e["station_id"]) != signature(e)]

    chemin_jour = os.path.join(dossier, h.strftime("%Y-%m-%d") + ".csv")
    nouveau = not os.path.exists(chemin_jour)
    with open(chemin_jour, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nouveau:
            w.writerow(COLONNES_EXPORT)
        for e in a_ecrire:
            s = infos.get(e["station_id"], {})
            cap = s.get("capacite")
            w.writerow([iso(h), e["station_id"], s.get("code", ""), s.get("nom", ""), cap,
                        e["mecaniques"], e["electriques"], e["velos"], e["bornes_libres"],
                        hors_service(cap, e["velos"], e["bornes_libres"]),
                        e["installee"], e["location_ok"], e["retour_ok"], e["derniere_maj"]])

    with open(chemin_etat, "w", encoding="utf-8") as f:
        json.dump({e["station_id"]: list(signature(e)) for e in etats}, f)

    print(f"{iso(h)} : {len(etats)} stations, {len(a_ecrire)} lignes ajoutées à {chemin_jour}")


# --------------------------------------------------------------------------
# Interface web locale
# --------------------------------------------------------------------------

PAGE = r"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Collecteur Vélib'</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<style>
:root{--fond:#f6f7f4;--carte:#fff;--texte:#1d2320;--doux:#66706a;--trait:#e1e4df;
--vert:#2f8f5b;--bleu:#2f6fb0;--rouge:#c4473a;--orange:#d08a1f}
@media (prefers-color-scheme:dark){:root{--fond:#141816;--carte:#1d2220;--texte:#e8ece9;
--doux:#9aa49e;--trait:#2d3430}}
*{box-sizing:border-box}body{margin:0;background:var(--fond);color:var(--texte);
font:15px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1150px;margin:0 auto;padding:20px 16px 40px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:0 0 10px;color:var(--doux);
text-transform:uppercase;letter-spacing:.04em}
.sous{color:var(--doux);margin-bottom:18px}
.grille{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:16px}
.kpi,.bloc{background:var(--carte);border:1px solid var(--trait);border-radius:10px;padding:12px 14px}
.kpi b{display:block;font-size:22px;font-variant-numeric:tabular-nums}
.kpi span{color:var(--doux);font-size:13px}
.deux{display:grid;grid-template-columns:2fr 1fr;gap:12px;margin-bottom:12px}
@media(max-width:800px){.deux{grid-template-columns:1fr}}
#carte{height:440px;border-radius:8px}
table{width:100%;border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums}
td,th{padding:5px 6px;border-bottom:1px solid var(--trait);text-align:left}
.ok{color:var(--vert)}.ko{color:var(--rouge)}
label{display:block;font-size:13px;color:var(--doux);margin:8px 0 3px}
input,select,button{font:inherit;padding:6px 8px;border-radius:6px;border:1px solid var(--trait);
background:var(--fond);color:var(--texte)}
button{background:var(--bleu);color:#fff;border:0;cursor:pointer;margin-top:10px}
button.second{background:transparent;color:var(--texte);border:1px solid var(--trait)}
.legende span{display:inline-block;width:10px;height:10px;border-radius:50%;margin:0 4px 0 10px}
.msg{font-size:13px;color:var(--doux);margin-top:8px;min-height:18px}
</style></head><body><main>
<h1>Collecteur Vélib'</h1>
<div class="sous" id="sous">Chargement…</div>
<div class="grille" id="kpis"></div>
<div class="deux">
  <div class="bloc"><h2>État actuel du réseau</h2><div id="carte"></div>
    <div class="legende" style="font-size:12px;margin-top:6px;color:var(--doux)">
      Taux de remplissage :<span style="background:var(--rouge)"></span>vide
      <span style="background:var(--orange)"></span>&lt; 25 %
      <span style="background:var(--vert)"></span>25–75 %
      <span style="background:var(--bleu)"></span>&gt; 75 %
      <span style="background:#888"></span>fermée</div></div>
  <div>
    <div class="bloc" style="margin-bottom:12px"><h2>Réglages</h2>
      <label>Intervalle entre deux relevés (minutes)</label>
      <input id="intervalle" type="number" min="1" max="60" style="width:80px">
      <label>Que stocker ?</label>
      <select id="mode"><option value="changements">Seulement les stations qui ont changé</option>
      <option value="tout">Toutes les stations à chaque relevé</option></select>
      <label><input type="checkbox" id="actifs"> Relevés automatiques actifs</label>
      <button onclick="sauver()">Enregistrer</button>
      <button class="second" onclick="collecter()">Relever maintenant</button>
      <div class="msg" id="msg"></div></div>
    <div class="bloc"><h2>Export CSV</h2>
      <label>Du</label><input type="date" id="du">
      <label>Au</label><input type="date" id="au">
      <label>Code station (facultatif)</label><input id="code" placeholder="ex. 16107" style="width:120px">
      <br><button onclick="exporter()">Télécharger</button></div>
  </div>
</div>
<div class="bloc"><h2>Derniers relevés</h2><table><thead><tr><th>Heure (locale)</th><th>Statut</th>
<th>Stations</th><th>Lignes écrites</th><th>Durée</th><th>Erreur</th></tr></thead><tbody id="passages"></tbody></table></div>
</main>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
const nf=new Intl.NumberFormat('fr-FR');
const loc=s=>s?new Date(s).toLocaleString('fr-FR'):'—';
let carte=null,couche=null,reglagesCharges=false;
function couleur(s){if(!s.installee||!s.location_ok)return'#888';
 const tot=s.velos+s.bornes_libres; if(s.velos===0)return getComputedStyle(document.documentElement).getPropertyValue('--rouge');
 const p=tot?s.velos/tot:0; const v=p<.25?'--orange':p<=.75?'--vert':'--bleu';
 return getComputedStyle(document.documentElement).getPropertyValue(v);}
async function charger(){
 const r=await (await fetch('api/resume')).json();
 document.getElementById('sous').textContent=
  `Dernier relevé : ${loc(r.dernier_releve)} · prochain : ${r.en_cours?'en cours…':loc(r.prochain)} · `+
  `historique depuis ${loc(r.debut)} · base ${r.taille_mo} Mo`;
 const k=[['Stations suivies',r.stations],['Vélos mécaniques',r.mecaniques],['Vélos électriques',r.electriques],
  ['Bornes libres',r.bornes],['Places hors service (est.)',r.hors_service],['Stations vides',r.vides],
  ['Stations pleines',r.pleines],['Lignes stockées',r.lignes]];
 document.getElementById('kpis').innerHTML=k.map(([l,v])=>`<div class="kpi"><b>${nf.format(v||0)}</b><span>${l}</span></div>`).join('');
 document.getElementById('passages').innerHTML=r.derniers_passages.map(p=>
  `<tr><td>${loc(p.horodatage)}</td><td class="${p.ok?'ok':'ko'}">${p.ok?'OK':'Échec'}</td><td>${nf.format(p.stations)}</td>
   <td>${nf.format(p.lignes_ecrites)}</td><td>${p.duree_s} s</td><td>${p.erreur||''}</td></tr>`).join('');
 if(!reglagesCharges){document.getElementById('intervalle').value=r.config.intervalle_minutes;
  document.getElementById('mode').value=r.config.mode;document.getElementById('actifs').checked=r.config['releves_actifs'];
  reglagesCharges=true;}
 await dessinerCarte();
}
async function dessinerCarte(){
 if(typeof L==='undefined'){document.getElementById('carte').textContent='Carte indisponible (pas de connexion au CDN).';return;}
 const st=await (await fetch('api/carte')).json();
 if(!carte){carte=L.map('carte').setView([48.8566,2.3522],12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18,attribution:'© OpenStreetMap'}).addTo(carte);}
 if(couche)couche.remove();
 couche=L.layerGroup(st.map(s=>L.circleMarker([s.lat,s.lon],{radius:5,weight:0,fillOpacity:.85,fillColor:couleur(s)})
  .bindPopup(`<b>${s.nom}</b> (${s.code})<br>${s.mecaniques} méca · ${s.electriques} élec<br>${s.bornes_libres} bornes libres / ${s.capacite??'?'}`))).addTo(carte);
}
async function sauver(){
 const corps={intervalle_minutes:+document.getElementById('intervalle').value,mode:document.getElementById('mode').value,
  'releves_actifs':document.getElementById('actifs').checked};
 await fetch('api/reglages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(corps)});
 document.getElementById('msg').textContent='Réglages enregistrés (pris en compte au prochain cycle).';}
async function collecter(){document.getElementById('msg').textContent='Relevé en cours…';
 const r=await (await fetch('api/collecter',{method:'POST'})).json();
 document.getElementById('msg').textContent=r.ok?`OK : ${r.stations} stations, ${r.lignes} lignes écrites.`:`Échec : ${r.erreur}`;charger();}
function exporter(){const p=new URLSearchParams();
 for(const id of['du','au','code']){const v=document.getElementById(id).value.trim();if(v)p.set(id,v);}
 location.href='export.csv?'+p.toString();}
charger();setInterval(charger,60000);
</script></body></html>"""


def fabriquer_gestionnaire(col):
    class Gestionnaire(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, obj, code=200):
            b = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = dict(urllib.parse.parse_qsl(u.query))
            if u.path in ("/", "/index.html"):
                b = PAGE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
            elif u.path == "/api/resume":
                self._json(col.resume())
            elif u.path == "/api/carte":
                self._json(col.carte())
            elif u.path == "/export.csv":
                nom = "velib_" + (q.get("du") or "debut") + "_" + (q.get("au") or "fin") + ".csv"
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="{nom}"')
                self.end_headers()
                tampon = io.StringIO()
                w = csv.writer(tampon, delimiter=";")
                self.wfile.write("\ufeff".encode("utf-8"))  # pour Excel
                w.writerow(COLONNES_EXPORT)
                for i, ligne in enumerate(col.export(q.get("du"), q.get("au"), q.get("code"))):
                    w.writerow(ligne)
                    if i % 5000 == 0:
                        self.wfile.write(tampon.getvalue().encode("utf-8"))
                        tampon.seek(0); tampon.truncate()
                self.wfile.write(tampon.getvalue().encode("utf-8"))
            else:
                self._json({"erreur": "introuvable"}, 404)

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            if u.path == "/api/collecter":
                self._json(col.collecter())
            elif u.path == "/api/reglages":
                n = int(self.headers.get("Content-Length") or 0)
                try:
                    d = json.loads(self.rfile.read(n) or b"{}")
                except ValueError:
                    return self._json({"ok": False}, 400)
                if "intervalle_minutes" in d:
                    col.cfg["intervalle_minutes"] = max(1, min(60, int(d["intervalle_minutes"])))
                if d.get("mode") in ("changements", "tout"):
                    col.cfg["mode"] = d["mode"]
                if "releves_actifs" in d:
                    col.cfg["releves_actifs"] = bool(d["releves_actifs"])
                sauver_config(col.cfg)
                self._json({"ok": True})
            else:
                self._json({"erreur": "introuvable"}, 404)

    return Gestionnaire


# --------------------------------------------------------------------------
# Lancement
# --------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Collecteur de disponibilités Vélib'")
    p.add_argument("--une-fois", action="store_true", help="faire un seul relevé puis quitter")
    p.add_argument("--csv", metavar="DOSSIER", help="avec --une-fois : écrire en CSV dans ce dossier au lieu de SQLite")
    p.add_argument("--port", type=int, help="port de l'interface (défaut 8767)")
    a = p.parse_args()

    cfg = charger_config()
    if not os.path.exists(FICHIER_CONFIG) and not a.une_fois:
        sauver_config(cfg)
    if a.port:
        cfg["port"] = a.port

    if a.une_fois:
        if a.csv:
            releve_csv(cfg, a.csv)
        else:
            r = Collecteur(cfg).collecter()
            print(r)
            sys.exit(0 if r["ok"] else 1)
        return

    col = Collecteur(cfg)
    threading.Thread(target=col.boucle, daemon=True).start()
    serveur = ThreadingHTTPServer(("127.0.0.1", cfg["port"]), fabriquer_gestionnaire(col))
    print(f"Collecteur Vélib' lancé : http://localhost:{cfg['port']}  (Ctrl+C pour arrêter)")
    print(f"Base : {col.chemin_base} · relevé toutes les {cfg['intervalle_minutes']} min")
    try:
        serveur.serve_forever()
    except KeyboardInterrupt:
        print("\nArrêt.")


if __name__ == "__main__":
    main()

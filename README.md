# 🚕 Chicago Taxi Trips — Pipeline de Data Engineering

## 1. Présentation du projet

Ce projet consiste à concevoir et déployer un pipeline de Data Engineering de bout en bout à partir du jeu de données **Chicago Taxi Trips**, disponible via l'API publique Socrata de la ville de Chicago.

L'objectif est de collecter, traiter et transformer les données selon une architecture **Medallion (Bronze, Silver, Gold)**, puis de rendre les indicateurs accessibles à travers une API REST et un tableau de bord interactif.

Le pipeline couvre la période du **1er janvier au 31 mars 2023 (Q1 2023)**.

## 2. Architecture

Le projet repose sur une architecture modulaire orchestrée par Dagster.

```text
                 Socrata API
                      |
                      v
              Bronze Layer
               (Raw Data)
                      |
                      v
              Silver Layer
          (Cleaning & Quality)
                      |
                      v
               Gold Layer
          (Business Aggregations)
                      |
                      v
                  MinIO
                (Data Lake)
                      |
                      v
                  DuckDB
                      |
                      v
                 FastAPI
                      |
                      v
                 Streamlit
                 Dashboard

          Dagster orchestrates
          the pipeline assets
```

## 3. Technologies utilisées

| Composant | Technologie |
|---|---|
| Source de données | Socrata API |
| Orchestration | Dagster |
| Traitement distribué | PySpark |
| Stockage Data Lake | MinIO |
| Stockage des métadonnées Dagster | PostgreSQL |
| Requêtage analytique | DuckDB |
| API REST | FastAPI |
| Dashboard | Streamlit |
| Conteneurisation | Docker & Docker Compose |
| Format de stockage | Parquet |

## 4. Architecture Medallion

### Bronze — Ingestion des données

- Extraction des données depuis l'API Socrata.
- Ingestion des trajets sur la période janvier–mars 2023.
- Pagination des résultats pour récupérer les données.
- Conservation des données brutes au format Parquet, sans transformation métier.

Emplacement :
`bronze/taxi_trips/quarter=2023-Q1/`

### Silver — Nettoyage et transformation

- Conversion des types de données.
- Nettoyage et normalisation des colonnes.
- Suppression des doublons.
- Gestion des valeurs manquantes et vérification de la qualité des données.
- Création de colonnes dérivées, notamment la date de prise en charge, l'heure et la durée du trajet.
- Calcul du revenu du trajet.

Emplacement :
`silver/taxi_trips/quarter=2023-Q1/`

### Gold — Agrégations métier

Quatre jeux de données analytiques sont produits :

| Dataset | Description |
|---|---|
| `daily_trip_performance` | Indicateurs quotidiens : nombre de trajets, revenus et moyennes |
| `payment_analysis` | Analyse des trajets et revenus par moyen de paiement |
| `pickup_area_analysis` | Analyse des trajets par zone de prise en charge |
| `hourly_trip_analysis` | Analyse de l'activité selon l'heure de la journée |

Emplacement :
`gold/taxi_trips/quarter=2023-Q1/`

## 5. API et Dashboard

### API REST — FastAPI

L'API expose les données Gold à travers les endpoints suivants :

| Endpoint | Description |
|---|---|
| `/health` | Vérification de l'état de l'API |
| `/api/daily-performance` | Performance quotidienne |
| `/api/payments` | Analyse des moyens de paiement |
| `/api/pickup-areas` | Analyse par zone de prise en charge |
| `/api/hourly-analysis` | Analyse par heure |

Documentation interactive :
`http://127.0.0.1:8000/docs`

### Dashboard — Streamlit

Le dashboard est organisé en quatre onglets :

- **Overview** : indicateurs globaux et évolution quotidienne.
- **Payments** : répartition des trajets et des revenus par moyen de paiement.
- **Geography** : analyse des zones de prise en charge.
- **Hourly** : évolution de l'activité selon l'heure.

## 6. Structure du projet

```text
taxi-trips-pipeline/
│
├── api/
│   ├── main.py
│   ├── database.py
│   └── sql/
│       ├── daily_trip_performance.sql
│       ├── payment_analysis.sql
│       ├── pickup_area_analysis.sql
│       └── hourly_trip_analysis.sql
│
├── dashboard/
│   └── app.py
│
├── src/
│   ├── definitions.py
│   └── jobs/
│       ├── bronze_layer/
│       │   └── fetch_data.py
│       ├── silver_layer/
│       │   ├── cleaning.py
│       │   └── silver_transform.py
│       └── gold_layer/
│           └── gold_transform.py
│
├── dagster_home/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── workspace.yaml
└── README.md
```

## 7. Installation et exécution

### Prérequis

- Git
- Docker Desktop
- Docker Compose
- Python 3.10 ou supérieur

### Étape 1 — Cloner le projet

```bash
git clone https://github.com/chadia08/Taxi_Trips_Pipeline.git
cd Taxi_Trips_Pipeline
```

### Étape 2 — Démarrer les services Docker

```bash
docker compose up -d --build
```

Vérifier que les conteneurs sont démarrés :

```bash
docker compose ps
```

Les interfaces disponibles sont :

- Dagster : `http://localhost:3000`
- MinIO Console : `http://localhost:9001`

### Étape 3 — Matérialiser les assets Dagster

La matérialisation peut être lancée depuis le terminal avec :

```bash
docker compose exec dagster dagster asset materialize --workspace /opt/dagster/app/workspace.yaml --select chicago_taxi_bronze chicago_taxi_silver chicago_taxi_gold
```

Cette commande lance la matérialisation des trois assets du pipeline. Dagster respecte les dépendances entre les assets : Bronze, puis Silver, puis Gold.

Il est également possible de lancer la matérialisation depuis l'interface Dagster :

1. Ouvrir `http://localhost:3000`.
2. Accéder à la vue **Assets**.
3. Sélectionner les assets du pipeline.
4. Lancer la matérialisation.

**Important — Durée de la matérialisation Bronze**

La matérialisation de l'asset `chicago_taxi_bronze` peut prendre un certain temps. En effet, cette étape récupère un volume important de données depuis l'API Socrata, correspondant à l'ensemble des trajets enregistrés entre janvier et mars 2023.

La durée dépend notamment du temps de réponse de l'API et du volume de données à télécharger. Il est donc normal que cette première étape soit plus longue que les transformations Silver et Gold.

Une fois Bronze terminée, les étapes suivantes peuvent exploiter les données stockées dans MinIO.

### Étape 4 — Vérifier les données dans MinIO

Ouvrir la console MinIO :

`http://localhost:9001`

Vérifier la présence des fichiers dans le bucket `datalake` :

- `bronze/`
- `silver/`
- `gold/`

### Étape 5 — Lancer l'API

Dans un terminal, créer et activer un environnement virtuel si nécessaire, puis installer les dépendances de l'API.

```bash
uvicorn api.main:app --reload
```

Tester la documentation interactive :

`http://127.0.0.1:8000/docs`

### Étape 6 — Lancer le Dashboard

Dans un autre terminal :

```bash
streamlit run dashboard/app.py
```

Le dashboard sera accessible à l'adresse :

`http://localhost:8501`

## 8. Qualité des données

Plusieurs contrôles sont appliqués durant la transformation Silver :

- Vérification des identifiants des trajets.
- Suppression des doublons.
- Vérification de la cohérence des dates et durées.
- Contrôle des valeurs numériques et financières.
- Gestion des valeurs manquantes.
- Normalisation des données catégorielles.

## 9. Choix techniques et perspectives d'amélioration

### Choix techniques

- **Parquet** : format colonnaire adapté aux traitements analytiques.
- **MinIO** : stockage objet compatible S3 pour construire un Data Lake local.
- **PySpark** : traitement et transformation de volumes importants de données.
- **Dagster** : orchestration des étapes et gestion des dépendances entre assets.
- **DuckDB** : interrogation analytique directe des fichiers Parquet.
- **FastAPI et Streamlit** : séparation entre l'accès aux données et leur visualisation.

### Améliorations possibles

- Rendre l'ingestion incrémentale et idempotente.
- Ajouter des tests automatisés de qualité des données.
- Mettre en place une surveillance des exécutions et des alertes.
- Ajouter des paramètres pour gérer plusieurs périodes d'ingestion.
- Déployer les composants sur une infrastructure cloud.

## 10. Auteur

**Chadia El Kharmoudi**

Data Engineering | Data Science | Artificial Intelligence

GitHub : https://github.com/chadia08
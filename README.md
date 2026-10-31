# Chicago Taxi Trips – Pipeline Data Engineering de bout en bout

Ce projet consiste à concevoir et développer un pipeline Data Engineering de bout en bout à partir du jeu de données Chicago Taxi Trips.

L'objectif est de construire un pipeline de données reproductible suivant l'architecture **Medallion (Bronze, Silver, Gold)**, permettant d'ingérer, de transformer et de préparer les données pour leur exploitation analytique.

Le pipeline repose sur Dagster pour l'orchestration, PySpark pour le traitement des données et MinIO pour le stockage objet. L'ensemble de l'infrastructure est exécuté localement à l'aide de Docker Compose.

---

## 1. Objectifs du projet

L'objectif principal est de mettre en place un pipeline Batch permettant de transformer des données brutes de trajets de taxis en données fiables, structurées et exploitables pour l'analyse.

Le pipeline est organisé en trois couches :

- **Bronze :** ingestion et stockage des données brutes.
- **Silver :** nettoyage, normalisation, déduplication et contrôle de la qualité des données.
- **Gold :** transformations métier, agrégations et calcul d'indicateurs analytiques (KPIs).

Les données de la couche Gold sont destinées à être exploitées via SQL, des API et des dashboards.

## 2. Architecture

Le projet suit une architecture Medallion composée de trois couches de traitement.

```text
                  CHICAGO DATA PORTAL
                         API
                          |
                          | Ingestion Batch
                          v
                 +------------------+
                 |      BRONZE      |
                 |  Données brutes  |
                 |      MinIO       |
                 +------------------+
                          |
                          | PySpark
                          v
                 +------------------+
                 |      SILVER      |
                 | Données nettoyées|
                 |      MinIO       |
                 +------------------+
                          |
                          | PySpark
                          v
                 +------------------+
                 |       GOLD       |
                 | Données analytiques
                 |      MinIO       |
                 +------------------+
                          |
                          v
              +--------------------------+
              |    COUCHE D'EXPOSITION   |
              |                          |
              |      SQL / API /         |
              |       Dashboard          |
              +--------------------------+


    DAGSTER     : Orchestration du pipeline
    POSTGRESQL  : Stockage des métadonnées Dagster
    DOCKER      : Exécution de l'infrastructure locale
```

Dagster orchestre les différentes étapes du pipeline et gère les dépendances entre les couches Bronze, Silver et Gold.

## 3. Technologies utilisées

| Technologie | Rôle |
|---|---|
| Python | Développement du pipeline |
| Dagster | Orchestration et gestion des dépendances |
| Apache Spark / PySpark | Traitement et transformation des données |
| MinIO | Stockage objet et Data Lake |
| PostgreSQL | Stockage des métadonnées et de l'historique Dagster |
| Parquet | Format de stockage colonne |
| Docker Compose | Déploiement de l'infrastructure locale |
| DuckDB | Exécution de requêtes SQL sur les fichiers Parquet |
| FastAPI | Exposition des données via des API REST |
| Streamlit | Création d'un dashboard interactif |

## 4. Source des données

**Chicago Taxi Trips**

Source officielle : [Chicago Data Portal – Taxi Trips](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew/about_data)

Les données sont récupérées depuis l'API du portail Open Data de la ville de Chicago.

La première extraction porte sur le premier trimestre de l'année 2023 (Q1 2023).

L'ingestion utilise une pagination afin de récupérer les données par lots.

## 5. Architecture Medallion

### 5.1. Bronze Layer – Données brutes

Cette couche constitue le point d'entrée des données dans le Data Lake.

Les principales opérations sont :

- Récupération des données depuis l'API Chicago Data Portal.
- Extraction par pagination.
- Conservation des données sources sans modification métier.
- Stockage des données au format Parquet dans MinIO.
- Organisation des données selon des partitions temporelles.

### 5.2. Silver Layer – Données nettoyées

Cette couche vise à améliorer la qualité et la cohérence des données.

Les transformations prévues comprennent :

- Conversion et validation des types de données.
- Gestion des valeurs manquantes et invalides.
- Suppression des doublons.
- Normalisation de certaines colonnes.
- Application de règles de qualité.
- Création d'attributs utiles aux analyses.

Les données nettoyées sont ensuite stockées dans MinIO au format Parquet.

### 5.3. Gold Layer – Données analytiques

Cette couche transforme les données Silver en jeux de données orientés métier.

Les traitements prévus comprennent :

- Agrégations métier.
- Calcul d'indicateurs de performance (KPIs).
- Analyse des revenus et des trajets.
- Préparation de tables analytiques destinées à la consommation.

Les données Gold sont stockées dans MinIO au format Parquet.

## 6. Orchestration du pipeline

Dagster est utilisé pour orchestrer les différentes étapes du pipeline.

Il permet notamment de :

- Définir les assets correspondant aux différentes couches.
- Gérer les dépendances entre les transformations.
- Suivre les exécutions et consulter les logs.
- Identifier les erreurs de traitement.
- Relancer les étapes nécessaires.

Le pipeline suit l'enchaînement suivant :

**Bronze → Silver → Gold**

Chaque couche dépend de la réussite de la couche précédente.

## 7. Prérequis

Avant de lancer le projet, les outils suivants doivent être installés :

- Git
- Docker Desktop
- Docker Compose
- Une connexion Internet pour récupérer les données depuis l'API Chicago.

Une configuration disposant de suffisamment de mémoire pour exécuter les traitements PySpark est également recommandée.

## 8. Installation et lancement

### Étape 1 – Cloner le repository

```bash
git clone https://github.com/chadia08/Taxi_Trips_Pipeline.git

cd Taxi_Trips_Pipeline
```

### Étape 2 – Configurer les variables d'environnement

Créer un fichier `.env` à partir du modèle `.env.example`, si celui-ci est fourni.

Renseigner les variables nécessaires à la connexion aux services, notamment MinIO.

### Étape 3 – Construire et démarrer les services

```bash
docker compose up -d --build
```

Cette commande démarre les différents services définis dans Docker Compose.

### Étape 4 – Vérifier les conteneurs

```bash
docker compose ps
```

### Étape 5 – Accéder à Dagster

Ouvrir l'interface Dagster dans le navigateur :

[http://localhost:3000](http://localhost:3000)

Depuis l'interface, accéder au **Catalog**, sélectionner les assets du pipeline, puis déclencher leur matérialisation (*Materialize*).

Il est également possible de lancer la matérialisation depuis la ligne de commande :

```bash
docker compose exec dagster dagster asset materialize -m src.definitions
```

Dagster orchestre l'exécution des différentes couches en respectant les dépendances définies entre les assets : la couche Bronze est exécutée avant Silver, puis Gold, sous réserve que les assets soient correctement sélectionnés et que les étapes précédentes se terminent avec succès.

### Étape 6 – Accéder à MinIO

Ouvrir la console MinIO :

http://localhost:900
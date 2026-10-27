# EcommerceAnalytics

Pipeline d'analyse de données e-commerce distribué, écrit en **Scala** avec **Apache Spark**.

L'application lit quatre sources de données (CSV, JSON, Parquet), valide et nettoie les enregistrements, les enrichit (jointures, caractéristiques temporelles, fonctions de fenêtrage), calcule des indicateurs métier (KPI par marchand, cohortes de rétention), puis écrit les résultats aux formats CSV et Parquet.

> Projet final du module *Data Engineer – Spark & Scala* — Groupe 3.
> Composition de l'équipe et répartition des rôles : voir [EQUIPE.md](EQUIPE.md).
> Décisions techniques et suivi des contributions : voir [CONTRIBUTIONS.md](CONTRIBUTIONS.md).

---

## Sommaire

1. [Versions utilisées](#1-versions-utilisées)
2. [Prérequis et installation](#2-prérequis-et-installation)
3. [Structure du projet](#3-structure-du-projet)
4. [Compilation et génération du JAR](#4-compilation-et-génération-du-jar)
5. [Exécution locale avec SBT](#5-exécution-locale-avec-sbt)
6. [Déploiement avec spark-submit](#6-déploiement-avec-spark-submit)
7. [Résultats produits](#7-résultats-produits)
8. [Résolution des problèmes courants](#8-résolution-des-problèmes-courants)
9. [Mesure des optimisations](#9-mesure-des-optimisations)

---

## 1. Versions utilisées

| Outil   | Version  | Remarque                                                       |
|---------|----------|----------------------------------------------------------------|
| Java    | 11 ou 17 | **Obligatoire** : Spark 3.5 ne démarre pas avec Java 23 ou plus |
| Scala   | 2.12.18  | Doit correspondre à la version de Scala de la distribution Spark |
| SBT     | 1.9.9    | Fixée dans `project/build.properties`                          |
| Spark   | 3.5.1    | Distribution « pre-built for Apache Hadoop 3.3 and later »     |

> **Important :** la version de Scala du projet (2.12) doit être identique à celle avec laquelle Spark a été compilé. Les distributions Spark 3.5.x téléchargeables par défaut utilisent Scala 2.12. Une incompatibilité ne se voit pas à la compilation mais provoque des erreurs `NoSuchMethodError` à l'exécution.

---

## 2. Prérequis et installation

### 2.1 Java (JDK 11 ou 17)

**Linux / macOS** — via [SDKMAN!](https://sdkman.io) (recommandé) :

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk list java                 # repérer un identifiant 17.x « tem » (Temurin)
sdk install java 17.0.10-tem  # adapter l'identifiant à celui affiché
```

**Ubuntu / Debian** (alternative) :

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
```

**Windows** : installer le JDK 17 Temurin depuis <https://adoptium.net>, en cochant l'option *Set JAVA_HOME variable*.

Vérification :

```bash
java -version    # doit afficher 11.x ou 17.x
```

### 2.2 SBT et Scala

SBT télécharge automatiquement le compilateur Scala 2.12.18 indiqué dans `build.sbt` : **l'installation séparée de Scala n'est pas indispensable** pour compiler le projet. Elle reste utile pour disposer de la console `scala`.

**Linux / macOS** (SDKMAN!) :

```bash
sdk install sbt 1.9.9
sdk install scala 2.12.18   # facultatif
```

**macOS** (Homebrew, alternative) :

```bash
brew install sbt
```

**Windows** : télécharger l'installeur `.msi` de sbt sur <https://www.scala-sbt.org/download.html>, ou avec Scoop :

```powershell
scoop install sbt
```

Vérification :

```bash
sbt --version
scala -version   # si Scala a été installé
```

### 2.3 Apache Spark 3.5.1

Spark n'est **pas nécessaire pour `sbt run`** (SBT récupère lui-même les librairies Spark). Il est en revanche **obligatoire pour `spark-submit`**.

**Linux / macOS :**

```bash
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 ~/spark

# À ajouter dans ~/.bashrc ou ~/.zshrc
export SPARK_HOME=~/spark
export PATH="$SPARK_HOME/bin:$PATH"
```

Puis recharger le shell : `source ~/.bashrc`.

**Windows :**

1. Télécharger `spark-3.5.1-bin-hadoop3.tgz` à la même adresse et l'extraire dans `C:\spark`.
2. Télécharger `winutils.exe` et `hadoop.dll` correspondant à Hadoop 3.3, les placer dans `C:\hadoop\bin`.
3. Définir les variables d'environnement `SPARK_HOME=C:\spark` et `HADOOP_HOME=C:\hadoop`, puis ajouter `%SPARK_HOME%\bin` et `%HADOOP_HOME%\bin` au `PATH`.

Vérification :

```bash
spark-submit --version   # doit afficher « version 3.5.1 » et « Using Scala version 2.12 »
```

---

## 3. Structure du projet

```
EcommerceAnalytics/
├── build.sbt                       # Dépendances et configuration du build
├── project/
│   ├── build.properties            # Version de sbt
│   └── plugins.sbt                 # Plugin sbt-assembly
├── README.md
├── EQUIPE.md
├── CONTRIBUTIONS.md
└── src/
    ├── main/
    │   ├── scala/com/ecommerce/
    │   │   ├── analytics/          # Ingestion, validation, transformations, analytique, MainApp
    │   │   ├── models/             # Case classes (Transaction, User, Product, Merchant)
    │   │   └── utils/              # SparkSessionBuilder, ConfigLoader
    │   └── resources/
    │       ├── application.conf    # Configuration externalisée
    │       └── data/               # Jeux de données d'entrée
    │           ├── transactions.csv
    │           ├── users.json
    │           ├── products.parquet/   (répertoire de fichiers Parquet)
    │           └── merchants.csv
    └── test/scala/                 # Tests unitaires
```

Les chemins des fichiers d'entrée et de sortie, les seuils de validation et les paramètres Spark sont définis dans `src/main/resources/application.conf`. Aucun de ces paramètres n'est codé en dur dans le code Scala.

---

## 4. Compilation et génération du JAR

Toutes les commandes se lancent depuis la **racine du projet** (le dossier contenant `build.sbt`).

```bash
# Compiler le code source (le premier lancement télécharge les dépendances)
sbt compile

# Lancer les tests unitaires
sbt test

# Générer le JAR exécutable
sbt assembly
```

Le JAR est produit à l'emplacement suivant :

```
target/scala-2.12/EcommerceAnalytics-1.0.0.jar
```

Pour repartir d'un état propre :

```bash
sbt clean assembly
```

> **Pourquoi `assembly` ?** Le JAR produit embarque notre code ainsi que la librairie Typesafe Config. Spark est déclaré en `provided` : il n'est pas inclus dans le JAR car il est fourni par `spark-submit`. Justification détaillée dans [CONTRIBUTIONS.md](CONTRIBUTIONS.md).

---

## 5. Exécution locale avec SBT

```bash
sbt run
```

L'application s'exécute en mode local (`local[*]`, soit tous les cœurs de la machine) et utilise les données de `src/main/resources/data/`. Les résultats sont écrits dans le dossier `output/` à la racine du projet.

**Passer un argument à l'application** (exécution d'une seule étape — question bonus 6.2) :

```bash
sbt "run ingestion"
sbt "run transformation"
sbt "run analytics"
sbt "run all"
```

En mode modulaire, chaque étape enregistre ses résultats dans `output/intermediate/` (Parquet) pour l'étape suivante : `ingestion` doit donc être lancée au moins une fois avant `transformation`, et `transformation` avant `analytics`. Ensuite, chaque membre peut relancer uniquement son étape. Un argument inconnu (ou `help`) affiche la liste des valeurs acceptées.

**Utiliser un autre fichier de configuration** sans modifier le projet :

```bash
sbt 'set javaOptions += "-Dconfig.file=/chemin/vers/mon.conf"' run
```

> L'application est exécutée dans une JVM séparée (`fork := true`) avec 2 Go de mémoire et les options nécessaires à Spark sous Java 17 ; ces réglages sont déjà présents dans `build.sbt`.

**Suivre l'exécution** : pendant que l'application tourne, l'interface web Spark est accessible sur <http://localhost:4040> (jobs, stages, plan d'exécution, cache).

---

## 6. Déploiement avec spark-submit

Générer d'abord le JAR (`sbt assembly`), puis vérifier que `spark-submit --version` affiche bien Spark 3.5.x / Scala 2.12.

### 6.1 Test du JAR en local

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --driver-memory 2g \
  target/scala-2.12/EcommerceAnalytics-1.0.0.jar
```

Cette commande valide le JAR avant tout déploiement sur un cluster.

### 6.2 Préparer les données et la configuration pour le cluster

Sur un cluster, les fichiers d'entrée doivent être accessibles **par tous les nœuds** (HDFS, S3 ou un stockage partagé), et non depuis le disque local d'une machine.

```bash
# Exemple avec HDFS
hdfs dfs -mkdir -p /data/ecommerce
hdfs dfs -put src/main/resources/data/* /data/ecommerce/
```

Créer ensuite un fichier de configuration dédié au cluster, par exemple `cluster.conf` :

```hocon
include "application.conf"

app {
  spark.master = "yarn"
  spark.shuffle.partitions = 200

  data {
    input {
      transactions = "hdfs:///data/ecommerce/transactions.csv"
      users        = "hdfs:///data/ecommerce/users.json"
      products     = "hdfs:///data/ecommerce/products.parquet"
      merchants    = "hdfs:///data/ecommerce/merchants.csv"
    }
    output.path = "hdfs:///output/ecommerce/"
  }
}
```

### 6.3 Lancement sur un cluster YARN

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --files cluster.conf \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=cluster.conf" \
  target/scala-2.12/EcommerceAnalytics-1.0.0.jar all
```

- `--files cluster.conf` envoie le fichier de configuration sur le nœud qui exécute le driver.
- `-Dconfig.file=cluster.conf` indique à Typesafe Config de l'utiliser à la place de la configuration embarquée.
- Le dernier argument (`all`) est transmis à l'application.

### 6.4 Lancement sur un cluster Spark Standalone

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master spark://<hote-master>:7077 \
  --deploy-mode client \
  --executor-memory 4g \
  --total-executor-cores 8 \
  --driver-java-options "-Dconfig.file=cluster.conf" \
  target/scala-2.12/EcommerceAnalytics-1.0.0.jar
```

En mode `client`, le driver tourne sur la machine qui lance la commande : `cluster.conf` doit donc s'y trouver.

> **Priorité du paramètre master :** une valeur passée avec `--master` à `spark-submit` doit l'emporter sur celle du fichier de configuration. Le `SparkSessionBuilder` n'applique la valeur `app.spark.master` que si aucun master n'a été fourni par `spark-submit`.

---

## 7. Résultats produits

Les résultats sont écrits dans le répertoire défini par `app.data.output.path` (par défaut `output/`) :

| Sous-dossier | Contenu | Question | Formats |
|--------------|---------|----------|---------|
| `data_quality_report/` | Rapport de qualité : lignes lues, valides, rejetées, taux de rejet, valeurs nulles, références orphelines | 2.4, 2.5 | CSV |
| `rejected/<dataset>/` | Lignes rejetées avec la colonne `rejection_reason` | 2.2 | CSV, Parquet |
| `enriched_transactions/` | Transactions enrichies (jointures, variables temporelles, fenêtres, suspicion) ; relues par l'étape `analytics` | 3.2 à 3.4 | Parquet |
| `merchant_kpis/` | KPI par marchand (CA, transactions, clients, classements, commission, répartition par âge, taux de suspectes) | 4.1 | CSV, Parquet |
| `cohort_retention/` | Matrice de rétention par cohorte et par période, CA et revenu moyen | 4.2 | CSV, Parquet |
| `rfm_segmentation/` | Scores R, F, M et segment par utilisateur | 4.3 | CSV, Parquet |
| `rfm_vs_customer_segment/` | Tableau croisé segment RFM × segment déclaré | 4.3 | CSV, Parquet |
| `top_products/` | Top 10 des produits par CA | 4.4 | CSV, Parquet |
| `sales_by_category_region/` | CA par catégorie et région, part dans la région | 4.4 | CSV, Parquet |
| `sales_by_payment_period/` | CA par méthode de paiement et période de la journée | 4.4 | CSV, Parquet |
| `performance/<mode>/` | Durée de chaque étape, avec et sans optimisation | 5.3 | CSV |

Les résultats en CSV sont regroupés en un seul fichier (`part-00000-….csv`) pour être ouverts directement dans un tableur.

Lecture rapide d'un résultat Parquet dans `spark-shell` :

```scala
spark.read.parquet("output/merchant_kpis/parquet").show(20, truncate = false)
```

---

## 8. Résolution des problèmes courants

| Symptôme | Cause probable | Solution |
|----------|----------------|----------|
| `java.lang.IllegalAccessError ... sun.nio.ch` | Spark sous Java 17 sans les options `--add-opens` | Utiliser `sbt run` (options déjà dans `build.sbt`) ou Java 11 |
| `NoSuchMethodError: scala...` | Versions de Scala différentes entre le JAR et Spark | Utiliser une distribution Spark 3.5.x compilée en Scala 2.12 |
| `ClassNotFoundException: org.apache.spark...` | JAR lancé avec `java -jar` au lieu de `spark-submit` | Toujours lancer le JAR avec `spark-submit` |
| `Path does not exist: ...` | Mauvais répertoire courant ou chemins de `application.conf` | Lancer depuis la racine du projet ; vérifier les chemins de la configuration |
| `Could not locate executable winutils.exe` | Windows sans `HADOOP_HOME` | Installer `winutils.exe` (voir section 2.3) |
| `OutOfMemoryError: Java heap space` | Mémoire du driver insuffisante | Augmenter `-Xmx` dans `build.sbt` ou `--driver-memory` |
| `No main class detected` | `MainApp` absent ou dans un autre package | Vérifier `package com.ecommerce.analytics` dans `MainApp.scala` |

---

## 9. Mesure des optimisations

Les optimisations (cache, persist, broadcast) sont activées par défaut. Un paramètre unique permet de les désactiver pour mesurer leur effet. Dans ce mode, le broadcast automatique de Spark est également désactivé, afin que la comparaison soit honnête.

```bash
# Avec optimisations (défaut)
sbt run

# Sans optimisation
sbt 'set javaOptions += "-Dapp.optimization.enabled=false"' run
```

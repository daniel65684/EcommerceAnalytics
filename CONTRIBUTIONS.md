# Journal de contribution – Projet EcommerceAnalytics (Groupe 3)

> Les champs `<...>` sont à compléter par chaque membre. Les charges de travail, les difficultés et les relectures doivent refléter ce qui s'est réellement passé : le jury les rapproche de l'historique Git (`git log`).

Composition de l'équipe : voir [EQUIPE.md](EQUIPE.md).

---

## 1. Tableau récapitulatif des responsabilités

| Question | Intitulé | Niveau | Responsable | Relecteur |
|----------|----------|--------|-------------|-----------|
| 0.1 | Constitution du groupe | Tronc commun | Groupe | — |
| 0.2 | Suivi des versions (Git) | Tronc commun | Chaque membre | — |
| 0.3 | Journal de contribution | Tronc commun | Groupe | — |
| 1.1 | Structure de projet SBT | Tronc commun | Membre A | Membre C |
| 1.2 | Configuration de `build.sbt` | Tronc commun | Membre A | Membre C |
| 1.3 | Documentation `README.md` | Tronc commun | Membre A | Membre C |
| 2.1 | Ingestion multi-format | Tronc commun | Membre A | Membre B |
| 2.2 | Validation des données | Tronc commun | Membre A | Membre B |
| 2.3 | Gestion d'erreurs et résumé | Tronc commun | Membre A | Membre B |
| 2.4 | Rapport de qualité | Tronc commun | Membre A | Membre B |
| 2.5 | Intégrité référentielle | Bonus | Membre A | Membre B |
| 3.1 | UDF `extractTimeFeatures` | Tronc commun | Membre B | Membre A |
| 3.2 | `enrichTransactionData` | Tronc commun | Membre B | Membre A |
| 3.3 | Analyse par partition Window | Tronc commun | Membre B | Membre A |
| 3.4 | Transactions suspectes | Bonus | Membre B | Membre A |
| 4.1 | KPI par marchand | Tronc commun | Membre C | Membre B |
| 4.2 | Analyse de cohortes | Tronc commun | Membre C | Membre B |
| 4.3 | Segmentation RFM | Bonus | Membre C | Membre B |
| 4.4 | Produits et catégories | Bonus | Membre C | Membre B |
| 5.1 | Optimisation du stockage | Tronc commun | Membre C | Membres A et B |
| 5.2 | Optimisation des jointures | Tronc commun | Membre C | Membres A et B |
| 5.3 | Mesure des optimisations | Bonus | Membre C | Membres A et B |
| 6.1 | Application principale | Tronc commun | Membre C | Membres A et B |
| 6.2 | Exécution modulaire | Bonus | Membre C | Membres A et B |
| 7.1 | `application.conf` | Tronc commun | Membre A | Membre C |
| 8.1 | Archive | Tronc commun | Groupe | — |
| 8.2 | Soutenance | Tronc commun | Groupe | — |

---

## 2. Charge de travail et difficultés

### Membre A – Data Ingestion & Platform Engineer

**Charge estimée :** `<nombre>` heures

**Difficultés rencontrées :**
- `<difficulté et solution>`
- *Exemples possibles, à ne garder que s'ils vous concernent :* Spark 3.5 qui refuse de démarrer sous Java 23+ (`getSubject is not supported`), résolu en imposant un JDK 11 ou 17 ; lanceur sbt 2.x qui ignorait la configuration tant que `project/build.properties` manquait ; types inférés incompatibles avec les case classes (`age` en `bigint`, `establishment_date` en entier), résolus par l'alignement de schéma `alignTo`.

### Membre B – Data Transformation Engineer

**Charge estimée :** `<nombre>` heures

**Difficultés rencontrées :**
- `<difficulté et solution>`
- *Exemples possibles :* colonnes homonymes (`category`, `name`) provoquant des ambiguïtés après jointure ; `countDistinct` interdit sur une fenêtre, remplacé par `size(collect_set(...))` ; définition d'une fenêtre « 7 jours » cohérente (secondes ou jours calendaires).

### Membre C – Analytics & Performance Engineer

**Charge estimée :** `<nombre>` heures

**Difficultés rencontrées :**
- `<difficulté et solution>`
- *Exemples possibles :* noms de colonnes refusés par Parquet (espaces dans le tableau croisé RFM) ; recalculs multiples visibles dans les logs avant la mise en cache ; mesure avant/après faussée par le broadcast automatique de Spark.

---

## 3. Décisions techniques du groupe

### D1 – Versions : Spark 3.5.1, Scala 2.12.18, Java 11 ou 17
Spark 3.5 est la dernière branche 3.x stable ; sa distribution par défaut est compilée en Scala 2.12, d'où ce choix pour éviter toute incompatibilité binaire (`NoSuchMethodError`) entre notre JAR et le cluster. Java 11 ou 17 est obligatoire : Spark 3.5 ne démarre pas à partir de Java 23.

### D2 – JAR exécutable : sbt-assembly plutôt que `sbt package` (Question 1.2)
`sbt package` ne produit qu'un JAR contenant notre code, sans Typesafe Config : l'application échouerait au lancement sauf à fournir chaque dépendance via `--jars`. sbt-assembly produit un JAR autonome. Spark et Scala étant fournis par `spark-submit`, ils sont exclus (`provided`), ce qui garde le JAR léger et évite les conflits de versions.

### D3 – Case classes : champs numériques en `Option` (Question 2.1)
Les données contiennent des valeurs manquantes ; un `Double` ou un `Int` Scala ne peut pas valoir `null` et `.as[T]` échouerait (`Null value appeared in non-nullable field`). Avec `Option`, une valeur manquante devient `None` et c'est la validation qui la rejette proprement. Les dates restent en `String`, comme dans le sujet.

### D4 – Lecture : schéma explicite, puis alignement sur les case classes (Question 2.1)
`transactions.csv` utilise un schéma explicite en mode `PERMISSIVE` : une valeur illisible devient `null` au lieu d'arrêter la lecture. Pour les sources à schéma inféré (JSON, CSV marchands, Parquet), la fonction `alignTo` convertit chaque colonne au type de la case class (ex. `age` bigint → int) ; les objets JSON imbriqués sont aplatis.

### D5 – Validation : rejets conservés et motivés (Question 2.2)
Une valeur manquante est considérée comme une violation de la règle. Les bornes sont incluses (16 et 100 ans, note 1 et 5, commission 0 et 1). La colonne `rejection_reason` liste toutes les règles violées, pas seulement la première. Tous les seuils proviennent de `application.conf`.

### D6 – Intégrité référentielle (Question 2.5, bonus)
On contrôle les transactions valides face aux référentiels avant validation : un utilisateur rejeté pour un revenu incohérent existe tout de même. Un identifiant `null` n'est pas compté comme orphelin, car il figure déjà dans `nb_valeurs_nulles`. Le comptage utilise des jointures `left_anti`.

### D7 – Jointures d'enrichissement (Question 3.2)
| Table | Jointure | Justification |
|-------|----------|---------------|
| transactions | table de départ | Chaque ligne du résultat est une vente. |
| users | LEFT | Une vente valide est conservée même si son client est absent ou rejeté, afin de ne pas sous-estimer le CA. |
| products | LEFT | Même raison : un produit absent du catalogue ne doit pas faire disparaître la vente (cas observé : produit `P04990`, 78 765 € de CA). |
| merchants | LEFT | Même raison ; les références manquantes sont mesurées par la Question 2.5. |

Les référentiels utilisés sont ceux après validation. Les colonnes homonymes sont renommées avant jointure (`product_category`, `merchant_name`...).

### D8 – Ambiguïtés du sujet tranchées (Parties 3)
- **25 ans** n'appartient à aucune tranche (« moins de 25 », puis « 26 à 44 ») : rattaché à « Jeune ». Âge inconnu : « Inconnu ».
- **0h–6h** n'est pas défini : rattaché à « Night ».
- **Heures ouvrées** : de 9h00 à 16h59 (`9 <= heure < 17`).
- **Jours et mois** en anglais, par cohérence avec « Morning » / « Night ».
- **Timestamp** analysé en mode strict : une date impossible donne `null`, sans arrêter le job.

### D9 – Fenêtres glissantes (Question 3.3)
Le montant cumulé utilise une fenêtre en secondes (les 7 jours précédant exactement la transaction). Le statut « actif » utilise une fenêtre de 7 jours calendaires, plus naturelle pour compter des jours distincts ; `countDistinct` étant interdit sur une fenêtre, on utilise `size(collect_set(...))`. Le délai entre achats est exprimé en jours (2 décimales) ; la valeur en secondes est conservée pour le seuil de 5 minutes de la Question 3.4.

### D10 – Transactions suspectes (Question 3.4, bonus)
Le panier moyen est calculé sur tout l'historique de l'utilisateur. Une condition non évaluable (valeur `null`, par exemple pas de transaction précédente) compte comme fausse. Les quatre seuils sont paramétrés dans `app.rules.suspicious`.

### D11 – Analytique (Partie 4)
La répartition par tranche d'âge (4.1) utilise une agrégation conditionnelle (`sum(when(...))`) plutôt qu'un pivot : un seul passage sur les données et des noms de colonnes stables. Le classement utilise `rank()` (ex aequo au même rang). Les transactions sans `merchant_id` sont exclues des KPI marchands. Le revenu moyen par cohorte (4.2) est rapporté aux utilisateurs actifs de la période ; en cas d'égalité de rétention, la cohorte la plus grande l'emporte.

### D12 – Règles de segmentation RFM (Question 4.3, bonus)
Scores de 1 à 5 par `ntile(5)` ; pour la récence, tri décroissant afin que les clients les plus récents obtiennent 5. Règles évaluées dans l'ordre, couvrant tous les cas :

| Segment | Règle | Logique métier |
|---------|-------|----------------|
| Champions | R ≥ 4, F ≥ 4, M ≥ 4 | Récents, fréquents et gros acheteurs |
| Nouveaux | R ≥ 4, F ≤ 2 | Arrivés récemment, peu d'achats |
| Clients fidèles | R ≥ 3, F ≥ 3 | Achètent régulièrement |
| Perdus | R ≤ 2, F ≤ 2 | Anciens et peu actifs |
| À risque | autres cas | Notamment les bons clients qui ne reviennent plus |

Validation observée : les Champions sont majoritairement déclarés Premium/VIP et les Perdus majoritairement Budget.

### D13 – Formats de sortie
Chaque résultat analytique est écrit en Parquet (format colonnaire typé, relu efficacement par Spark) et en CSV regroupé en un seul fichier (`coalesce(1)`, lisible dans un tableur). Les transactions enrichies sont écrites en Parquet seul (volume important, usage interne). Les noms de colonnes refusés par Parquet sont nettoyés, et les tableaux convertis en texte pour le CSV.

### D14 – Stratégie d'optimisation (Partie 5)
`cache()` sur les DataFrame réutilisés (sources, résultats de validation, résultats analytiques affichés puis écrits deux fois) ; `persist(MEMORY_AND_DISK_SER)` sur les transactions enrichies, la table la plus volumineuse, réutilisée par toutes les analyses. `unpersist()` d'abord sur les résultats « feuilles », puis sur le reste en fin de pipeline : libérer un DataFrame dont dépend un autre cache ferait perdre le contenu de ce dernier. `broadcast()` sur `users`, `products` et `merchants`. `spark.sql.shuffle.partitions = 8` au lieu de 200 en local. Pour la mesure 5.3, le mode sans optimisation désactive aussi le broadcast automatique de Spark (`autoBroadcastJoinThreshold = -1`), sans quoi la comparaison serait faussée.

### D15 – Application principale (Partie 6)
L'object s'appelle `MainApp` (et non `EcommerceAnalyticsApp`) pour respecter l'arborescence imposée et les commandes `spark-submit` de la Question 6.2. L'argument est contrôlé avant le démarrage de Spark. L'étape `analytics` relit les transactions enrichies d'une exécution précédente, ce qui permet de tester la Partie 4 sans relancer les Parties 2 et 3.

### D16 – Configuration externalisée (Partie 7)
Les chemins pointent vers `src/main/resources/data/` (et non `data/` comme dans l'exemple du sujet), car l'arborescence imposée y place les données ; Spark ne pouvant pas lire un fichier situé dans le JAR, les chemins sont relatifs à la racine du projet. Chaque lecture a une valeur par défaut, utilisée si la clé est absente ou invalide, avec un avertissement. Les définitions imposées par le sujet (tranches d'âge, périodes de la journée) restent dans le code.

---

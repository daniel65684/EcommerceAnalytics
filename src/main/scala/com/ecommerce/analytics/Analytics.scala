package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.util.control.NonFatal

/**
 * Partie 4 - Analytique business (Questions 4.1 à 4.4).
 * Toutes les fonctions prennent en entrée le DataFrame enrichi de la Partie 3.
 *
 * Responsable : Membre C
 */
object Analytics {

  /** Tranches d'âge de la Partie 3 -> suffixe de colonne sans espace ni accent. */
  private val AgeGroups: Seq[(String, String)] = Seq(
    "Jeune"     -> "jeune",
    "Adulte"    -> "adulte",
    "Âge Moyen" -> "age_moyen",
    "Senior"    -> "senior",
    "Inconnu"   -> "inconnu"
  )

  // ===================================================================
  // Question 4.1 - Rapport détaillé par marchand
  // ===================================================================

  /**
   * Une ligne par marchand : CA, transactions, clients uniques, panier moyen,
   * commission, classements (catégorie, région), répartition du CA par
   * tranche d'âge et, si la colonne is_suspicious existe, taux de suspectes.
   *
   * La répartition par âge est calculée par agrégation conditionnelle
   * (sum(when(...))) : un seul passage sur les données, au lieu d'un pivot
   * suivi d'une jointure.
   * Les transactions sans merchant_id sont exclues de ce rapport.
   */
  def merchantKpis(enriched: DataFrame): DataFrame = {
    val ageAggs: Seq[Column] = AgeGroups.map { case (label, suffix) =>
      round(sum(when(col("tranche_age") === label, col("amount")).otherwise(0.0)), 2).as(s"ca_$suffix")
    }

    // Bonus (nécessite la Question 3.4)
    val suspicionAgg: Seq[Column] =
      if (enriched.columns.contains("is_suspicious"))
        Seq(round(avg(col("is_suspicious")) * 100, 2).as("taux_suspectes_pct"))
      else Seq.empty

    val aggs: Seq[Column] = Seq(
      round(sum("amount"), 2).as("ca_total"),
      count(lit(1)).as("nb_transactions"),
      countDistinct("user_id").as("nb_clients_uniques"),
      round(avg("amount"), 2).as("montant_moyen"),
      round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale")
    ) ++ ageAggs ++ suspicionAgg

    val perMerchant = enriched
      .filter(col("merchant_id").isNotNull)
      .groupBy("merchant_id", "merchant_name", "merchant_category", "merchant_region", "commission_rate")
      .agg(aggs.head, aggs.tail: _*)

    // Répartition en pourcentage du CA du marchand
    val withPct = AgeGroups.foldLeft(perMerchant) { case (acc, (_, suffix)) =>
      acc.withColumn(s"pct_ca_$suffix", round(col(s"ca_$suffix") / col("ca_total") * 100, 2))
    }

    // rank : deux marchands à CA égal ont le même rang
    val byCategory = Window.partitionBy("merchant_category").orderBy(col("ca_total").desc)
    val byRegion   = Window.partitionBy("merchant_region").orderBy(col("ca_total").desc)

    withPct
      .withColumn("rang_ca_categorie", rank().over(byCategory))
      .withColumn("rang_ca_region", rank().over(byRegion))
      .orderBy(col("ca_total").desc)
  }

  // ===================================================================
  // Question 4.2 - Analyse de cohortes
  // ===================================================================

  /**
   * Matrice de rétention au format long : une ligne par (cohort_month, period_index)
   * avec taille de la cohorte, utilisateurs actifs, taux de rétention et,
   * en bonus, CA de la période et revenu moyen par utilisateur actif.
   */
  def cohortAnalysis(enriched: DataFrame): DataFrame = {
    val tx = enriched
      .filter(col("user_id").isNotNull && col("transaction_date").isNotNull)
      .select(col("user_id"), col("amount"), trunc(col("transaction_date"), "month").as("transaction_month"))
      // Mois de la première transaction de l'utilisateur
      .withColumn("cohort_month", min("transaction_month").over(Window.partitionBy("user_id")))
      // Deux premiers jours de mois -> nombre entier de mois
      .withColumn("period_index", months_between(col("transaction_month"), col("cohort_month")).cast("int"))

    val cohortSizes = tx.groupBy("cohort_month").agg(countDistinct("user_id").as("taille_cohorte"))

    tx.groupBy("cohort_month", "period_index")
      .agg(
        countDistinct("user_id").as("nb_utilisateurs_actifs"),
        round(sum("amount"), 2).as("ca_periode")
      )
      .join(SparkOptimizations.broadcastIfEnabled(cohortSizes), Seq("cohort_month"))
      .withColumn("taux_retention_pct",
        round(col("nb_utilisateurs_actifs") / col("taille_cohorte") * 100, 2))
      .withColumn("revenu_moyen_par_utilisateur",
        round(col("ca_periode") / col("nb_utilisateurs_actifs"), 2))
      .withColumn("cohort_month", date_format(col("cohort_month"), "yyyy-MM"))
      .select("cohort_month", "taille_cohorte", "period_index", "nb_utilisateurs_actifs",
        "taux_retention_pct", "ca_periode", "revenu_moyen_par_utilisateur")
      .orderBy("cohort_month", "period_index")
  }

  /**
   * Cohorte ayant la meilleure rétention à `period` mois.
   * En cas d'égalité, la cohorte la plus grande l'emporte (plus significative).
   */
  def bestCohortAt(matrix: DataFrame, period: Int = 3): Option[Row] =
    matrix.filter(col("period_index") === period)
      .orderBy(col("taux_retention_pct").desc, col("taille_cohorte").desc)
      .limit(1)
      .collect()
      .headOption

  /** Vue "tableau" pour l'affichage : une ligne par cohorte, une colonne M0, M1... par période. */
  def retentionMatrixWide(matrix: DataFrame): DataFrame = {
    val wide = matrix.groupBy("cohort_month", "taille_cohorte")
      .pivot("period_index")
      .agg(first("taux_retention_pct"))
    wide.columns.filter(_.forall(_.isDigit))
      .foldLeft(wide)((acc, c) => acc.withColumnRenamed(c, s"M$c"))
      .orderBy("cohort_month")
  }

  // ===================================================================
  // Question 4.3 (BONUS) - Segmentation RFM
  // ===================================================================

  /**
   * Récence (jours depuis la dernière transaction, par rapport à la date la plus
   * récente du jeu de données), Fréquence (nb de transactions), Montant (CA).
   * Scores de 1 (faible) à 5 (excellent) par quintiles avec ntile(5) :
   *  - R : tri par récence décroissante -> les plus récents obtiennent 5 ;
   *  - F et M : tri croissant -> les plus forts obtiennent 5.
   * Fenêtre sans partition : Spark regroupe les ~12 000 utilisateurs sur
   * une seule partition, acceptable à ce volume.
   */
  def rfmSegmentation(enriched: DataFrame): DataFrame = {
    val tx = enriched.filter(col("user_id").isNotNull && col("transaction_date").isNotNull)

    val referenceDate = tx.agg(max("transaction_date")).first().getDate(0)

    val perUser = tx.groupBy("user_id").agg(
      max("transaction_date").as("derniere_transaction"),
      count(lit(1)).as("frequence"),
      round(sum("amount"), 2).as("montant"),
      first("customer_segment", ignoreNulls = true).as("customer_segment")
    ).withColumn("recence_jours", datediff(lit(referenceDate), col("derniere_transaction")))

    perUser
      .withColumn("score_r", ntile(5).over(Window.orderBy(col("recence_jours").desc)))
      .withColumn("score_f", ntile(5).over(Window.orderBy(col("frequence").asc)))
      .withColumn("score_m", ntile(5).over(Window.orderBy(col("montant").asc)))
      .withColumn("score_rfm", concat(col("score_r").cast("string"), col("score_f").cast("string"), col("score_m").cast("string")))
      .withColumn("segment_rfm", rfmSegment(col("score_r"), col("score_f"), col("score_m")))
  }

  /**
   * Règles d'affectation (évaluées dans l'ordre, couvrent tous les cas) :
   *  1. Champions       : R >= 4, F >= 4, M >= 4 (récents, fréquents, dépensiers)
   *  2. Nouveaux        : R >= 4, F <= 2         (récents mais peu d'achats)
   *  3. Clients fidèles : R >= 3, F >= 3         (achètent souvent et encore récemment)
   *  4. Perdus          : R <= 2, F <= 2         (anciens et peu actifs)
   *  5. À risque        : tous les autres cas, en particulier R <= 2 avec F >= 3
   *                       (bons clients qui ne reviennent plus) et R = 3 avec F <= 2.
   */
  def rfmSegment(r: Column, f: Column, m: Column): Column =
    when(r >= 4 && f >= 4 && m >= 4, "Champions")
      .when(r >= 4 && f <= 2, "Nouveaux")
      .when(r >= 3 && f >= 3, "Clients fidèles")
      .when(r <= 2 && f <= 2, "Perdus")
      .otherwise("À risque")

  /** Tableau croisé : segment RFM (lignes) x customer_segment déclaré (colonnes). */
  def rfmVsDeclaredSegment(rfm: DataFrame): DataFrame =
    rfm.withColumn("customer_segment", coalesce(col("customer_segment"), lit("Inconnu")))
      .groupBy("segment_rfm")
      .pivot("customer_segment")
      .count()
      .na.fill(0)
      .orderBy("segment_rfm")

  // ===================================================================
  // Question 4.4 (BONUS) - Produits et catégories
  // ===================================================================

  /** Top N des produits par CA, avec note moyenne et niveau de stock. */
  def topProducts(enriched: DataFrame, n: Int = 10): DataFrame =
    enriched.filter(col("product_id").isNotNull)
      .groupBy("product_id", "product_name", "product_category")
      .agg(
        round(sum("amount"), 2).as("ca_total"),
        count(lit(1)).as("nb_transactions"),
        round(avg("product_rating"), 2).as("note_moyenne"),
        max("product_stock").as("stock")
      )
      .orderBy(col("ca_total").desc)
      .limit(n)

  /**
   * CA et nombre de transactions par catégorie (de la transaction) et par région
   * (du marchand), avec la part de chaque catégorie dans le CA de sa région.
   */
  def salesByCategoryAndRegion(enriched: DataFrame): DataFrame =
    enriched
      .groupBy(
        coalesce(col("category"), lit("Inconnue")).as("categorie"),
        coalesce(col("merchant_region"), lit("Inconnue")).as("region"))
      .agg(round(sum("amount"), 2).as("ca_total"), count(lit(1)).as("nb_transactions"))
      .withColumn("pct_ca_region",
        round(col("ca_total") / sum("ca_total").over(Window.partitionBy("region")) * 100, 2))
      .orderBy(col("region"), col("ca_total").desc)

  /** Répartition du CA par méthode de paiement et période de la journée. */
  def salesByPaymentAndPeriod(enriched: DataFrame): DataFrame = {
    val byPaymentPeriod = enriched
      .groupBy(
        coalesce(col("payment_method"), lit("Inconnue")).as("payment_method"),
        coalesce(col("day_period"), lit("Inconnue")).as("day_period"))
      .agg(round(sum("amount"), 2).as("ca_total"), count(lit(1)).as("nb_transactions"))
    // Total calculé à part (1 ligne) : évite une fenêtre globale sans partition
    val total = byPaymentPeriod.agg(sum("ca_total").as("ca_global"))
    byPaymentPeriod.crossJoin(total)
      .withColumn("pct_ca_total", round(col("ca_total") / col("ca_global") * 100, 2))
      .drop("ca_global")
      .orderBy(col("ca_total").desc)
  }

  // ===================================================================
  // Écriture des résultats (CSV + Parquet)
  // ===================================================================

  /**
   * Écrit un résultat dans <output>/<name>/parquet et <output>/<name>/csv.
   * Les noms de colonnes sont nettoyés (Parquet refuse espaces, virgules...)
   * et les tableaux sont convertis en texte pour le CSV.
   */
  def save(df: DataFrame, name: String): Unit = {
    val root  = ConfigLoader.getString("app.data.output.path", "output/").stripSuffix("/")
    val clean = df.columns.foldLeft(df) { (acc, c) =>
      val safe = c.replaceAll("[ ,;{}()\\n\\t=]", "_")
      if (safe == c) acc else acc.withColumnRenamed(c, safe)
    }
    try {
      clean.write.mode("overwrite").parquet(s"$root/$name/parquet")
      toCsvFriendly(clean).coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(s"$root/$name/csv")
      println(s"[Écriture] $name -> $root/$name (parquet + csv)")
    } catch {
      case NonFatal(e) => System.err.println(s"[Écriture] ERREUR pour $name : ${e.getMessage}")
    }
  }

  private def toCsvFriendly(df: DataFrame): DataFrame =
    df.select(df.schema.fields.toSeq.map { f =>
      f.dataType match {
        case _: ArrayType => concat_ws("|", col(s"`${f.name}`")).as(f.name)
        case _            => col(s"`${f.name}`")
      }
    }: _*)
}
package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.SparkSessionBuilder
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Question 6.1 - Application principale (EcommerceAnalyticsApp).
 * L'object s'appelle MainApp pour respecter l'arborescence imposée
 * (MainApp.scala) ; choix documenté dans CONTRIBUTIONS.md.
 *
 * Question 6.2 (bonus) - Exécution d'une seule étape :
 *   spark-submit --class com.ecommerce.analytics.MainApp app.jar [ingestion|transformation|analytics|all]
 *
 * Responsable : Membre C
 */
object MainApp {

  // ===================================================================
  // Question 6.2 - Arguments acceptés
  // ===================================================================
  private val Steps: Map[String, String] = Map(
    "ingestion"      -> "Partie 2 : lecture, validation, rapport de qualité",
    "transformation" -> "Parties 2 et 3 : ingestion puis enrichissement des transactions",
    "analytics"      -> "Partie 4 : analyses (relit les transactions enrichies si elles existent)",
    "all"            -> "Pipeline complet (valeur par défaut)"
  )

  private val Usage: String =
    s"""Usage : MainApp [étape]
       |
       |Étapes disponibles :
       |${Seq("ingestion", "transformation", "analytics", "all").map(s => f"  $s%-15s ${Steps(s)}").mkString("\n")}
       |
       |Exemple : spark-submit --class com.ecommerce.analytics.MainApp EcommerceAnalytics-1.0.0.jar analytics
       |""".stripMargin

  /** Nom du jeu enrichi écrit par "transformation" et "all", relu par "analytics". */
  private val EnrichedOutput = "enriched_transactions"

  // ===================================================================
  // Structures de résultats
  // ===================================================================
  private case class IngestionOutput(
                                      transactions:  Option[ValidationResult[Transaction]],
                                      users:         Option[ValidationResult[User]],
                                      products:      Option[ValidationResult[Product]],
                                      merchants:     Option[ValidationResult[Merchant]],
                                      results:       Seq[ValidationResult[_]],
                                      qualityReport: DataFrame
                                    ) {
    /** Les quatre jeux validés, si les quatre sources ont pu être chargées. */
    def allSources: Option[(ValidationResult[Transaction], ValidationResult[User],
      ValidationResult[Product], ValidationResult[Merchant])] =
      for (t <- transactions; u <- users; p <- products; m <- merchants) yield (t, u, p, m)
  }

  private case class AnalyticsOutput(
                                      merchantKpis:          DataFrame,
                                      cohorts:               DataFrame,
                                      rfm:                   DataFrame,
                                      rfmVsDeclaredSegment:  DataFrame,
                                      topProducts:           DataFrame,
                                      salesByCategoryRegion: DataFrame,
                                      salesByPaymentPeriod:  DataFrame
                                    )

  // ===================================================================
  // Point d'entrée
  // ===================================================================
  def main(args: Array[String]): Unit = {
    // 6.2 : l'argument est contrôlé AVANT de démarrer Spark
    val step = args.headOption.map(_.trim.toLowerCase).getOrElse("all")
    if (Set("-h", "--help", "help").contains(step)) { println(Usage); return }
    if (!Steps.contains(step) || args.length > 1) {
      System.err.println(s"Argument non reconnu : ${args.mkString(" ")}\n")
      System.err.println(Usage)
      sys.exit(2)
    }

    val spark = SparkSessionBuilder.build()
    SparkOptimizations.configure(spark)
    println(s"[MainApp] Étape demandée : $step")

    val timer  = new StageTimer
    var failed = false

    // 6.1 : gestion globale des erreurs, arrêt propre de Spark dans tous les cas
    try {
      step match {
        case "ingestion"      => runIngestionOnly(spark, timer)
        case "transformation" => runUpToTransformation(spark, timer)
        case "analytics"      => runAnalyticsOnly(spark, timer)
        case "all"            => runAll(spark, timer)
      }
      timer.printSummary(s"$step - ${SparkOptimizations.modeLabel}")
      if (step == "all") timer.save(spark, SparkOptimizations.modeLabel) // mesures 5.3
    } catch {
      case e: Exception =>
        failed = true
        System.err.println(s"[MainApp] Échec du pipeline : ${e.getClass.getSimpleName} - ${e.getMessage}")
        e.printStackTrace()
    } finally {
      SparkOptimizations.releaseAll() // 5.1 : libération explicite du cache
      spark.stop()
      println("[MainApp] SparkSession arrêtée.")
    }

    if (failed) sys.exit(1) // code retour non nul : l'échec est visible par spark-submit
  }

  // ===================================================================
  // Scénarios d'exécution
  // ===================================================================

  private def runIngestionOnly(spark: SparkSession, timer: StageTimer): Unit = {
    val validation = new DataValidation(spark)
    val ingestion  = timer.time("1. Ingestion et validation")(runIngestion(spark, validation))
    timer.time("4. Écriture")(writeIngestion(validation, ingestion))
  }

  private def runUpToTransformation(spark: SparkSession, timer: StageTimer): Unit = {
    val validation = new DataValidation(spark)
    val ingestion  = timer.time("1. Ingestion et validation")(runIngestion(spark, validation))
    ingestion.allSources match {
      case Some((t, u, p, m)) =>
        val enriched = timer.time("2. Transformation")(runTransformation(t, u, p, m))
        timer.time("4. Écriture") {
          writeIngestion(validation, ingestion)
          Analytics.saveParquet(enriched, EnrichedOutput)
        }
      case None => sourcesMissing(validation, ingestion, timer)
    }
  }

  /**
   * Relit les transactions enrichies d'une exécution précédente si elles
   * existent : le membre C teste ses analyses sans relancer les Parties 2 et 3.
   * Sinon, ingestion et transformation sont exécutées d'abord.
   */
  private def runAnalyticsOnly(spark: SparkSession, timer: StageTimer): Unit = {
    val path = Analytics.outputPath(EnrichedOutput)
    val enrichedOpt: Option[DataFrame] =
      if (pathExists(spark, path)) {
        println(s"[MainApp] Transactions enrichies relues depuis $path (exécution précédente).")
        Some(timer.time("2. Chargement des transactions enrichies") {
          val df = SparkOptimizations.persistLarge(spark.read.parquet(path), "transformation:enrichies")
          println(f"[MainApp] ${df.count()}%,d transactions enrichies chargées")
          df
        })
      } else {
        println(s"[MainApp] $path introuvable : exécution préalable de l'ingestion et de la transformation.")
        val validation = new DataValidation(spark)
        val ingestion  = timer.time("1. Ingestion et validation")(runIngestion(spark, validation))
        ingestion.allSources match {
          case Some((t, u, p, m)) => Some(timer.time("2. Transformation")(runTransformation(t, u, p, m)))
          case None               => sourcesMissing(validation, ingestion, timer); None
        }
      }

    enrichedOpt.foreach { enriched =>
      val analytics = timer.time("3. Analytique")(runAnalytics(enriched))
      timer.time("4. Écriture")(writeAnalytics(analytics))
    }
  }

  private def runAll(spark: SparkSession, timer: StageTimer): Unit = {
    val validation = new DataValidation(spark)
    val ingestion  = timer.time("1. Ingestion et validation")(runIngestion(spark, validation))
    ingestion.allSources match {
      case Some((t, u, p, m)) =>
        val enriched  = timer.time("2. Transformation")(runTransformation(t, u, p, m))
        val analytics = timer.time("3. Analytique")(runAnalytics(enriched))
        timer.time("4. Écriture") {
          writeIngestion(validation, ingestion)
          Analytics.saveParquet(enriched, EnrichedOutput)
          writeAnalytics(analytics)
        }
      case None => sourcesMissing(validation, ingestion, timer)
    }
  }

  private def sourcesMissing(validation: DataValidation, ingestion: IngestionOutput, timer: StageTimer): Unit = {
    System.err.println("[MainApp] Au moins une source est indisponible : transformations et analyses ignorées.")
    timer.time("4. Écriture")(writeIngestion(validation, ingestion))
  }

  // ===================================================================
  // Étape 1 - Ingestion, validation, rapport de qualité (Partie 2)
  // ===================================================================
  private def runIngestion(spark: SparkSession, validation: DataValidation): IngestionOutput = {
    println("\n===== PARTIE 2 : INGESTION ET VALIDATION =====")
    val ingestion = new DataIngestion(spark)

    val txLoaded = ingestion.loadTransactions()
    val usLoaded = ingestion.loadUsers()
    val prLoaded = ingestion.loadProducts()
    val meLoaded = ingestion.loadMerchants()

    val tx = txLoaded.map(validation.validateTransactions)
    val us = usLoaded.map(validation.validateUsers)
    val pr = prLoaded.map(validation.validateProducts)
    val me = meLoaded.map(validation.validateMerchants)

    val results    = Seq[Option[ValidationResult[_]]](tx, us, pr, me).flatten
    val baseReport = validation.buildQualityReport(results)

    // 2.5 (bonus) : uniquement si les quatre sources sont disponibles
    val report = (for {
      t <- tx; u <- usLoaded; p <- prLoaded; m <- meLoaded
    } yield validation.addReferentialIntegrity(baseReport, t.valid, u.data, p.data, m.data))
      .getOrElse(baseReport)

    println("\n----- Rapport de qualité des données -----")
    report.show(truncate = false)

    println("----- Motifs de rejet -----")
    results.foreach { r =>
      println(s"-- ${r.name}")
      r.rejected.groupBy("rejection_reason").count().orderBy(col("count").desc).show(truncate = false)
    }

    IngestionOutput(tx, us, pr, me, results, report)
  }

  // ===================================================================
  // Étape 2 - Transformations (Partie 3)
  // ===================================================================
  private def runTransformation(t: ValidationResult[Transaction], u: ValidationResult[User],
                                p: ValidationResult[Product], m: ValidationResult[Merchant]): DataFrame = {
    println("\n===== PARTIE 3 : TRANSFORMATIONS =====")
    val enriched  = DataTransformation.enrichTransactionData(t.valid, u.valid, p.valid, m.valid)
    val behaviour = DataTransformation.addBehaviourFeatures(enriched)

    // 5.1 : DataFrame le plus volumineux, réutilisé par toutes les analyses
    val result = SparkOptimizations.persistLarge(
      DataTransformation.detectSuspiciousTransactions(behaviour), "transformation:enrichies")

    // Action : matérialise le cache (et donne du sens au chronométrage)
    println(f"[Transformation] ${result.count()}%,d transactions enrichies")

    println("----- 3.1 / 3.2 Caractéristiques temporelles, rang, tranche d'âge -----")
    result.select("transaction_id", "user_id", "amount", "hour", "day_of_week", "month", "is_weekend",
        "day_period", "is_working_hours", "tranche_age", "rang_transaction_utilisateur",
        "nb_transactions_utilisateur")
      .show(10, truncate = false)

    println("----- 3.3 Fenêtres glissantes -----")
    result.select("user_id", "transaction_ts", "amount", "montant_cumule_7j", "nb_jours_actifs_7j",
        "is_active_user", "jours_depuis_achat_precedent")
      .orderBy("user_id", "transaction_ts")
      .show(10, truncate = false)

    println("----- 3.4 Transactions suspectes -----")
    DataTransformation.showSuspiciousSummary(result)
    result
  }

  // ===================================================================
  // Étape 3 - Analytique (Partie 4)
  // ===================================================================
  private def runAnalytics(enriched: DataFrame): AnalyticsOutput = {
    println("\n===== PARTIE 4 : ANALYTIQUE =====")
    // 5.1 : chaque résultat est affiché PUIS écrit deux fois (Parquet, CSV) :
    // sans cache, il serait recalculé à chaque fois.
    def cached(df: DataFrame, name: String): DataFrame =
      SparkOptimizations.cacheIfEnabled(df, s"analytique:$name")

    println("----- 4.1 KPI par marchand (top 10) -----")
    val kpis = cached(Analytics.merchantKpis(enriched), "kpis")
    kpis.show(10, truncate = false)

    println("----- 4.2 Cohortes : taux de rétention (%) -----")
    val cohorts = cached(Analytics.cohortAnalysis(enriched), "cohortes")
    Analytics.retentionMatrixWide(cohorts).show(36, truncate = false)
    val months = Analytics.retentionPeriodMonths
    Analytics.bestCohortAt(cohorts, months) match {
      case Some(r) => println(s"Meilleure rétention à $months mois : cohorte ${r.getAs[String]("cohort_month")} " +
        s"(${r.getAs[Double]("taux_retention_pct")} % de ${r.getAs[Long]("taille_cohorte")} utilisateurs)")
      case None    => println(s"Aucune cohorte n'a $months mois d'historique.")
    }

    println("\n----- 4.3 Segmentation RFM -----")
    val rfm = cached(Analytics.rfmSegmentation(enriched), "rfm")
    rfm.groupBy("segment_rfm").count().orderBy(col("count").desc).show(truncate = false)
    val rfmCross = cached(Analytics.rfmVsDeclaredSegment(rfm), "rfm_croise")
    rfmCross.show(truncate = false)

    println("----- 4.4 Produits et catégories -----")
    val top10   = cached(Analytics.topProducts(enriched), "top_produits")
    val catReg  = cached(Analytics.salesByCategoryAndRegion(enriched), "categorie_region")
    val payment = cached(Analytics.salesByPaymentAndPeriod(enriched), "paiement_periode")
    top10.show(truncate = false)
    catReg.show(20, truncate = false)
    payment.show(30, truncate = false)

    AnalyticsOutput(kpis, cohorts, rfm, rfmCross, top10, catReg, payment)
  }

  // ===================================================================
  // Étape 4 - Écriture des résultats (CSV + Parquet)
  // ===================================================================
  private def writeIngestion(validation: DataValidation, ingestion: IngestionOutput): Unit = {
    validation.saveQualityReport(ingestion.qualityReport)
    validation.saveRejected(ingestion.results)
  }

  private def writeAnalytics(analytics: AnalyticsOutput): Unit = {
    Analytics.save(analytics.merchantKpis,          "merchant_kpis")
    Analytics.save(analytics.cohorts,               "cohort_retention")
    Analytics.save(analytics.rfm,                   "rfm_segmentation")
    Analytics.save(analytics.rfmVsDeclaredSegment,  "rfm_vs_customer_segment")
    Analytics.save(analytics.topProducts,           "top_products")
    Analytics.save(analytics.salesByCategoryRegion, "sales_by_category_region")
    Analytics.save(analytics.salesByPaymentPeriod,  "sales_by_payment_period")
    // Les résultats analytiques sont des "feuilles" : on peut les libérer
    // dès qu'ils sont écrits, sans affecter d'autre cache.
    SparkOptimizations.release("analytique:")
  }

  /** Teste l'existence d'un répertoire (local, HDFS ou S3) via l'API Hadoop. */
  private def pathExists(spark: SparkSession, path: String): Boolean =
    try {
      val p = new Path(path)
      p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
    } catch {
      case _: Exception => false
    }
}

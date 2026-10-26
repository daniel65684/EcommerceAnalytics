package com.ecommerce.analytics

import com.ecommerce.utils.SparkSessionBuilder
import org.apache.spark.sql.functions.col

/**
 * VERSION TEMPORAIRE - test des Parties 2, 3 et 4.
 * Le membre C la remplacera par l'application complète (Question 6.1).
 */
object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.build()
    try {
      val ingestion  = new DataIngestion(spark)
      val validation = new DataValidation(spark)

      // 2.1 + 2.3 : chargement sécurisé et nombre de lignes lues
      val txLoaded = ingestion.loadTransactions()
      val usLoaded = ingestion.loadUsers()
      val prLoaded = ingestion.loadProducts()
      val meLoaded = ingestion.loadMerchants()

      // 2.2 + 2.3 : validation et nombre de lignes valides
      val tx = txLoaded.map(validation.validateTransactions)
      val us = usLoaded.map(validation.validateUsers)
      val pr = prLoaded.map(validation.validateProducts)
      val me = meLoaded.map(validation.validateMerchants)

      val results = Seq[Option[ValidationResult[_]]](tx, us, pr, me).flatten

      // 2.4 : rapport de qualité
      val baseReport = validation.buildQualityReport(results)

      // 2.5 (bonus) : uniquement si les quatre sources ont été chargées
      val report = (for {
        t <- tx; u <- usLoaded; p <- prLoaded; m <- meLoaded
      } yield validation.addReferentialIntegrity(baseReport, t.valid, u.data, p.data, m.data))
        .getOrElse(baseReport)

      println("\n===== RAPPORT DE QUALITÉ DES DONNÉES =====")
      report.show(truncate = false)
      validation.saveQualityReport(report)
      validation.saveRejected(results)

      println("\n===== EXEMPLES DE LIGNES REJETÉES =====")
      results.foreach { r =>
        println(s"-- ${r.name}")
        r.rejected.select("rejection_reason").groupBy("rejection_reason").count().show(truncate = false)
      }

      // ===== PARTIE 3 : transformations (nécessite les quatre sources) =====
      for (t <- tx; u <- us; p <- pr; m <- me) {
        // 3.1 : test rapide de l'UDF sur des cas limites
        println("\n===== 3.1 UDF extractTimeFeatures =====")
        Seq("20240706143000", "20240701021822", "", null, "abc", "20241345990000")
          .foreach(s => println(f"${String.valueOf(s)}%-16s -> ${TimeFeatures.extract(s)}"))

        // 3.2 + 3.3 (+ 3.4 bonus)
        val enriched   = DataTransformation.enrichTransactionData(t.valid, u.valid, p.valid, m.valid)
        val behaviour  = DataTransformation.addBehaviourFeatures(enriched)
        val suspicious = DataTransformation.detectSuspiciousTransactions(behaviour).cache()

        println("\n===== 3.2 Transactions enrichies =====")
        suspicious.select("transaction_id", "user_id", "amount", "hour", "day_of_week", "month",
            "is_weekend", "day_period", "is_working_hours", "tranche_age",
            "rang_transaction_utilisateur", "nb_transactions_utilisateur")
          .show(10, truncate = false)

        println("===== 3.3 Fenêtres glissantes =====")
        suspicious.select("user_id", "transaction_ts", "amount", "montant_cumule_7j",
            "nb_jours_actifs_7j", "is_active_user", "jours_depuis_achat_precedent")
          .orderBy("user_id", "transaction_ts")
          .show(15, truncate = false)

        println("===== 3.4 Transactions suspectes =====")
        DataTransformation.showSuspiciousSummary(suspicious)

        // ===== PARTIE 4 : analytique business =====
        println("\n===== 4.1 KPI par marchand (top 10 par CA) =====")
        val kpis = Analytics.merchantKpis(suspicious)
        kpis.select("merchant_id", "merchant_name", "merchant_category", "merchant_region", "ca_total",
            "nb_transactions", "nb_clients_uniques", "montant_moyen", "commission_totale",
            "rang_ca_categorie", "rang_ca_region", "taux_suspectes_pct")
          .show(10, truncate = false)

        println("===== 4.2 Cohortes : taux de rétention (%) =====")
        val cohorts = Analytics.cohortAnalysis(suspicious)
        Analytics.retentionMatrixWide(cohorts).show(24, truncate = false)
        Analytics.bestCohortAt(cohorts, 3) match {
          case Some(r) => println(s"Meilleure rétention à 3 mois : cohorte ${r.getAs[String]("cohort_month")} " +
            s"(${r.getAs[Double]("taux_retention_pct")} % de ${r.getAs[Long]("taille_cohorte")} utilisateurs)")
          case None    => println("Aucune cohorte n'a 3 mois d'historique.")
        }

        println("\n===== 4.3 Segmentation RFM =====")
        val rfm = Analytics.rfmSegmentation(suspicious)
        rfm.groupBy("segment_rfm").count().orderBy(col("count").desc).show(truncate = false)
        val rfmCross = Analytics.rfmVsDeclaredSegment(rfm)
        rfmCross.show(truncate = false)

        println("===== 4.4 Produits et catégories =====")
        val top10    = Analytics.topProducts(suspicious)
        val catReg   = Analytics.salesByCategoryAndRegion(suspicious)
        val payment  = Analytics.salesByPaymentAndPeriod(suspicious)
        top10.show(truncate = false)
        catReg.show(20, truncate = false)
        payment.show(30, truncate = false)

        // Écriture des résultats
        Analytics.save(kpis, "merchant_kpis")
        Analytics.save(cohorts, "cohort_retention")
        Analytics.save(rfm, "rfm_segmentation")
        Analytics.save(rfmCross, "rfm_vs_customer_segment")
        Analytics.save(top10, "top_products")
        Analytics.save(catReg, "sales_by_category_region")
        Analytics.save(payment, "sales_by_payment_period")
      }
    } catch {
      case e: Exception =>
        System.err.println(s"[MainApp] Échec du pipeline : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

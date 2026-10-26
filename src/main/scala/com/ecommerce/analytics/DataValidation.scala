package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}

import scala.util.control.NonFatal

/**
 * Résultat de la validation d'un dataset (Question 2.2) :
 * les lignes valides (typées) et les lignes rejetées (avec rejection_reason),
 * plus les compteurs utilisés par le rapport de qualité.
 */
case class ValidationResult[T](
                                name:          String,
                                valid:         Dataset[T],
                                rejected:      DataFrame,
                                readCount:     Long,
                                validCount:    Long,
                                rejectedCount: Long,
                                nullCount:     Long
                              ) {
  /** Pourcentage de lignes rejetées, arrondi à deux décimales. */
  def rejectionRate: Double =
    if (readCount == 0) 0.0
    else BigDecimal(rejectedCount * 100.0 / readCount)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
}

/** Une ligne du rapport de qualité (Question 2.4). */
case class DataQualityRecord(
                              dataset:            String,
                              nb_lignes_lues:     Long,
                              nb_lignes_valides:  Long,
                              nb_lignes_rejetees: Long,
                              taux_rejet:         Double,
                              nb_valeurs_nulles:  Long
                            )

/**
 * Questions 2.2, 2.4 et 2.5 (bonus).
 * Tous les seuils proviennent de application.conf.
 *
 * Responsable : Membre A
 */
class DataValidation(spark: SparkSession) {

  import spark.implicits._

  // -------------------------------------------------------------------
  // Seuils de validation (application.conf, avec valeurs par défaut)
  // -------------------------------------------------------------------
  private val minAmount       = ConfigLoader.getDouble("app.validation.transaction.min-amount", 0.0)
  private val timestampLength = ConfigLoader.getInt("app.validation.transaction.timestamp-length", 14)
  private val minAge          = ConfigLoader.getInt("app.validation.user.min-age", 16)
  private val maxAge          = ConfigLoader.getInt("app.validation.user.max-age", 100)
  private val minIncome       = ConfigLoader.getDouble("app.validation.user.min-income", 0.0)
  private val minPrice        = ConfigLoader.getDouble("app.validation.product.min-price", 0.0)
  private val minRating       = ConfigLoader.getDouble("app.validation.product.min-rating", 1.0)
  private val maxRating       = ConfigLoader.getDouble("app.validation.product.max-rating", 5.0)
  private val minCommission   = ConfigLoader.getDouble("app.validation.merchant.min-commission", 0.0)
  private val maxCommission   = ConfigLoader.getDouble("app.validation.merchant.max-commission", 1.0)

  private val outputRoot = ConfigLoader.getString("app.data.output.path", "output/").stripSuffix("/")

  // -------------------------------------------------------------------
  // Question 2.2 - Fonctions de validation
  // Chaque règle = (condition de VIOLATION, motif de rejet).
  // Une valeur manquante (null) est considérée comme une violation.
  // -------------------------------------------------------------------

  /** amount > 0 et timestamp de 14 caractères. */
  def validateTransactions(in: LoadedDataset[Transaction]): ValidationResult[Transaction] =
    splitValidRejected(in, Seq(
      (col("amount").isNull || col("amount") <= minAmount) ->
        s"amount manquant ou <= $minAmount",
      (col("timestamp").isNull || length(col("timestamp")) =!= timestampLength) ->
        s"timestamp manquant ou différent de $timestampLength caractères"
    ))

  /** age entre 16 et 100 (bornes incluses) et annual_income > 0. */
  def validateUsers(in: LoadedDataset[User]): ValidationResult[User] =
    splitValidRejected(in, Seq(
      (col("age").isNull || col("age") < minAge || col("age") > maxAge) ->
        s"age manquant ou hors de [$minAge, $maxAge]",
      (col("annual_income").isNull || col("annual_income") <= minIncome) ->
        s"annual_income manquant ou <= $minIncome"
    ))

  /** price > 0 et rating entre 1 et 5 (bornes incluses). */
  def validateProducts(in: LoadedDataset[Product]): ValidationResult[Product] =
    splitValidRejected(in, Seq(
      (col("price").isNull || col("price") <= minPrice) ->
        s"price manquant ou <= $minPrice",
      (col("rating").isNull || col("rating") < minRating || col("rating") > maxRating) ->
        s"rating manquant ou hors de [$minRating, $maxRating]"
    ))

  /** commission_rate entre 0 et 1 (bornes incluses). */
  def validateMerchants(in: LoadedDataset[Merchant]): ValidationResult[Merchant] =
    splitValidRejected(in, Seq(
      (col("commission_rate").isNull || col("commission_rate") < minCommission ||
        col("commission_rate") > maxCommission) ->
        s"commission_rate manquant ou hors de [$minCommission, $maxCommission]"
    ))

  /**
   * Sépare un dataset en lignes valides et lignes rejetées.
   * rejection_reason liste TOUTES les règles violées, séparées par " ; "
   * (concat_ws ignore les règles respectées, qui produisent null).
   */
  private def splitValidRejected[T: Encoder](in: LoadedDataset[T],
                                             rules: Seq[(Column, String)]): ValidationResult[T] = {
    val df      = in.data.toDF()
    val reasons = concat_ws(" ; ", rules.map { case (violated, msg) => when(violated, lit(msg)) }: _*)

    // Mis en cache : réutilisé pour les valides, les rejetées et les comptages.
    val tagged = SparkOptimizations.cacheIfEnabled(
      df.withColumn("rejection_reason", when(reasons =!= "", reasons)), s"validation:${in.name}")

    val valid    = tagged.filter(col("rejection_reason").isNull).drop("rejection_reason").as[T]
    val rejected = tagged.filter(col("rejection_reason").isNotNull)

    val validCount    = valid.count()
    val rejectedCount = rejected.count()
    val nullCount     = countNulls(df)

    // Question 2.3 : nombre de lignes valides après validation
    println(f"[Validation] ${in.name}%-12s : $validCount%,d valides / ${in.rowCount}%,d lues " +
      f"($rejectedCount%,d rejetées)")
    if (rejectedCount == 0)
      System.err.println(s"[Validation] ATTENTION ${in.name} : 0 ligne rejetée, vérifier les règles.")

    ValidationResult(in.name, valid, rejected, in.rowCount, validCount, rejectedCount, nullCount)
  }

  /** Nombre total de valeurs null, toutes colonnes confondues. */
  private def countNulls(df: DataFrame): Long = {
    val perColumn = df.columns.toSeq.map(c => sum(when(col(s"`$c`").isNull, 1).otherwise(0)))
    df.select(coalesce(perColumn.reduce(_ + _), lit(0L)).cast("long")).first().getLong(0)
  }

  // -------------------------------------------------------------------
  // Question 2.4 - Rapport de qualité
  // -------------------------------------------------------------------

  /** Une ligne par dataset validé. */
  def buildQualityReport(results: Seq[ValidationResult[_]]): DataFrame =
    results.map { r =>
      DataQualityRecord(r.name, r.readCount, r.validCount, r.rejectedCount, r.rejectionRate, r.nullCount)
    }.toDF()

  /** Sauvegarde du rapport en un seul fichier CSV dans <output>/data_quality_report. */
  def saveQualityReport(report: DataFrame): Unit = {
    val path = s"$outputRoot/data_quality_report"
    try {
      report.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
      println(s"[Qualité] Rapport sauvegardé dans $path")
    } catch {
      case NonFatal(e) => System.err.println(s"[Qualité] ERREUR d'écriture du rapport : ${e.getMessage}")
    }
  }

  /** Sauvegarde des lignes rejetées (Parquet + CSV) dans <output>/rejected/<dataset>. */
  def saveRejected(results: Seq[ValidationResult[_]]): Unit =
    results.foreach { r =>
      val base = s"$outputRoot/rejected/${r.name}"
      try {
        r.rejected.write.mode("overwrite").parquet(s"$base/parquet")
        toCsvFriendly(r.rejected).coalesce(1)
          .write.mode("overwrite").option("header", "true").csv(s"$base/csv")
      } catch {
        case NonFatal(e) => System.err.println(s"[Qualité] ERREUR d'écriture des rejets ${r.name} : ${e.getMessage}")
      }
    }

  /** Le CSV ne supporte pas les tableaux : preferred_categories devient "Books|Electronics". */
  private def toCsvFriendly(df: DataFrame): DataFrame =
    df.select(df.schema.fields.toSeq.map { f =>
      f.dataType match {
        case _: ArrayType => concat_ws("|", col(f.name)).as(f.name)
        case _            => col(f.name)
      }
    }: _*)

  // -------------------------------------------------------------------
  // Question 2.5 (BONUS) - Intégrité référentielle
  // -------------------------------------------------------------------

  /**
   * Ajoute au rapport le nombre de transactions dont user_id, product_id ou
   * merchant_id n'existe pas dans le référentiel correspondant.
   *
   * Choix documentés dans CONTRIBUTIONS.md :
   *  - on contrôle les transactions VALIDES ;
   *  - les référentiels sont pris AVANT validation : un utilisateur rejeté
   *    (ex : revenu incohérent) existe tout de même ;
   *  - une référence null n'est pas comptée ici (déjà comptée dans nb_valeurs_nulles).
   * Les trois colonnes ne sont remplies que pour la ligne "transactions".
   */
  def addReferentialIntegrity(report: DataFrame,
                              transactions: Dataset[Transaction],
                              users: Dataset[User],
                              products: Dataset[Product],
                              merchants: Dataset[Merchant]): DataFrame = {
    val tx = transactions.toDF()
    val orphanUsers     = countOrphans(tx, users.toDF(), "user_id")
    val orphanProducts  = countOrphans(tx, products.toDF(), "product_id")
    val orphanMerchants = countOrphans(tx, merchants.toDF(), "merchant_id")

    println(f"[Intégrité] Références orphelines : user_id=$orphanUsers%,d, " +
      f"product_id=$orphanProducts%,d, merchant_id=$orphanMerchants%,d")

    val isTransactions = col("dataset") === "transactions"
    report
      .withColumn("nb_user_id_orphelins",     when(isTransactions, lit(orphanUsers)))
      .withColumn("nb_product_id_orphelins",  when(isTransactions, lit(orphanProducts)))
      .withColumn("nb_merchant_id_orphelins", when(isTransactions, lit(orphanMerchants)))
  }

  /** left_anti : garde les transactions dont la clé n'a aucune correspondance. */
  private def countOrphans(tx: DataFrame, ref: DataFrame, key: String): Long =
    tx.select(key).filter(col(key).isNotNull)
      .join(SparkOptimizations.broadcastIfEnabled(ref.select(key).distinct()), Seq(key), "left_anti")
      .count()
}
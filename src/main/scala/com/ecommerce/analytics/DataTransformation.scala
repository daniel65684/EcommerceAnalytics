package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Questions 3.2, 3.3 et 3.4 (bonus).
 * Responsable : Membre B
 */
object DataTransformation {

  private val SecondsPerDay = 86400L

  // Règles métier lues dans application.conf (section app.rules), avec valeurs par défaut
  private val windowDays        = ConfigLoader.getInt("app.rules.window-days", 7)
  private val activeUserMinDays = ConfigLoader.getInt("app.rules.active-user-min-days", 5)
  private val suspiciousPct     = ConfigLoader.getDouble("app.rules.suspicious.amount-over-average-pct", 300.0)
  private val suspiciousDelay   = ConfigLoader.getLong("app.rules.suspicious.max-delay-seconds", 300L)
  private val suspiciousPayment = ConfigLoader.getString("app.rules.suspicious.payment-method", "CRYPTO")
  private val suspiciousSignals = ConfigLoader.getInt("app.rules.suspicious.min-signals", 2)

  // ===================================================================
  // Question 3.2 - enrichTransactionData
  // ===================================================================

  /**
   * Joint les transactions aux trois référentiels, ajoute les caractéristiques
   * temporelles, le rang et le total de transactions par utilisateur, et la
   * tranche d'âge.
   *
   * Jointures : les transactions sont la table de départ ; users, products et
   * merchants sont joints en LEFT (justification dans CONTRIBUTIONS.md).
   *
   * Les colonnes homonymes (category, name...) sont renommées avant jointure
   * pour éviter toute ambiguïté.
   */
  def enrichTransactionData(transactions: Dataset[Transaction],
                            users:        Dataset[User],
                            products:     Dataset[Product],
                            merchants:    Dataset[Merchant]): DataFrame = {

    val usersDf = users.toDF()
      .withColumnRenamed("city", "user_city")
      .withColumnRenamed("registration_date", "user_registration_date")

    val productsDf = products.toDF()
      .drop("merchant_id") // déjà présent dans la transaction
      .withColumnRenamed("name", "product_name")
      .withColumnRenamed("category", "product_category")
      .withColumnRenamed("price", "product_price")
      .withColumnRenamed("rating", "product_rating")
      .withColumnRenamed("stock", "product_stock")

    val merchantsDf = merchants.toDF()
      .withColumnRenamed("name", "merchant_name")
      .withColumnRenamed("category", "merchant_category")
      .withColumnRenamed("region", "merchant_region")
      .withColumnRenamed("establishment_date", "merchant_establishment_date")

    // Question 5.2 : les trois référentiels sont petits (12 000, 6 000 et 600 lignes)
    // face à ~138 000 transactions -> broadcast, aucune redistribution des transactions.
    val joined = transactions.toDF()
      .join(SparkOptimizations.broadcastIfEnabled(usersDf),     Seq("user_id"),     "left")
      .join(SparkOptimizations.broadcastIfEnabled(productsDf),  Seq("product_id"),  "left")
      .join(SparkOptimizations.broadcastIfEnabled(merchantsDf), Seq("merchant_id"), "left")

    // UDF : la struct est éclatée en 6 colonnes (hour, day_of_week, ...)
    val withTime = joined
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("transaction_date", to_date(col("transaction_ts")))

    // Fenêtres par utilisateur.
    // transaction_id départage deux transactions à la même seconde :
    // le rang est ainsi identique d'une exécution à l'autre.
    val byUserOrdered = Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))
    val byUser        = Window.partitionBy("user_id")

    withTime
      .withColumn("rang_transaction_utilisateur", row_number().over(byUserOrdered))
      .withColumn("nb_transactions_utilisateur",  count(lit(1)).over(byUser))
      .withColumn("tranche_age", ageGroup(col("age")))
  }

  /**
   * Tranches d'âge. Le sujet laisse 25 ans sans tranche ("moins de 25",
   * puis "26 à 44") : nous rattachons 25 ans à "Jeune" (choix documenté).
   * Un âge inconnu (utilisateur absent ou rejeté) donne "Inconnu".
   */
  def ageGroup(age: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    when(age.isNull, "Inconnu")
      .when(age <= 25, "Jeune")
      .when(age <= 44, "Adulte")
      .when(age <= 64, "Âge Moyen")
      .otherwise("Senior")

  // ===================================================================
  // Question 3.3 - Analyse par partition Window
  // ===================================================================

  /**
   * À appliquer sur le résultat de enrichTransactionData.
   *  - montant_cumule_7j : somme des montants de l'utilisateur sur les
   *    7 derniers jours, transaction courante incluse (fenêtre en secondes) ;
   *  - is_active_user : 1 si l'utilisateur a acheté au moins 5 jours
   *    calendaires distincts sur les 7 jours se terminant le jour courant ;
   *  - jours_depuis_achat_precedent : délai (en jours, 2 décimales) depuis
   *    la transaction précédente du même utilisateur, via lag ; null pour
   *    la première transaction.
   */
  def addBehaviourFeatures(enriched: DataFrame): DataFrame = {
    val withKeys = enriched
      .withColumn("ts_seconds", col("transaction_ts").cast("long"))
      .withColumn("day_index", datediff(col("transaction_date"), lit("1970-01-01")))

    // ]t - 7 jours ; t] : 7 jours glissants exacts jusqu'à la transaction
    val last7DaysSeconds = Window.partitionBy("user_id")
      .orderBy(col("ts_seconds"))
      .rangeBetween(-(windowDays * SecondsPerDay) + 1, Window.currentRow)

    // Jour courant + 6 jours précédents : 7 jours calendaires
    val last7CalendarDays = Window.partitionBy("user_id")
      .orderBy(col("day_index"))
      .rangeBetween(-(windowDays - 1).toLong, Window.currentRow)

    val byUserOrdered = Window.partitionBy("user_id").orderBy(col("ts_seconds"), col("transaction_id"))

    withKeys
      .withColumn("montant_cumule_7j", round(sum(col("amount")).over(last7DaysSeconds), 2))
      // countDistinct n'est pas autorisé sur une fenêtre : size(collect_set) le remplace
      .withColumn("nb_jours_actifs_7j", size(collect_set(col("transaction_date")).over(last7CalendarDays)))
      .withColumn("is_active_user", when(col("nb_jours_actifs_7j") >= activeUserMinDays, 1).otherwise(0))
      .withColumn("secondes_depuis_achat_precedent",
        col("ts_seconds") - lag(col("ts_seconds"), 1).over(byUserOrdered))
      .withColumn("jours_depuis_achat_precedent",
        round(col("secondes_depuis_achat_precedent") / SecondsPerDay.toDouble, 2))
  }

  // ===================================================================
  // Question 3.4 (BONUS) - Détection de transactions suspectes
  // ===================================================================

  /**
   * À appliquer sur le résultat de addBehaviourFeatures.
   * Panier moyen = moyenne de TOUTES les transactions de l'utilisateur
   * (historique complet, choix documenté).
   * is_suspicious = 1 si au moins 2 des 4 conditions sont vraies.
   * Une condition non évaluable (valeur null) compte comme fausse.
   */
  def detectSuspiciousTransactions(df: DataFrame): DataFrame = {
    val byUser = Window.partitionBy("user_id")

    val withGap = df
      .withColumn("panier_moyen_utilisateur", avg(col("amount")).over(byUser))
      .withColumn("ecart_panier_moyen_pct",
        round((col("amount") - col("panier_moyen_utilisateur")) / col("panier_moyen_utilisateur") * 100, 2))

    val conditions = Seq(
      col("ecart_panier_moyen_pct") > suspiciousPct,
      col("day_period") === "Night",
      col("secondes_depuis_achat_precedent") < suspiciousDelay,
      upper(trim(col("payment_method"))) === suspiciousPayment.toUpperCase
    )
    val nbConditions = conditions.map(c => when(c, 1).otherwise(0)).reduce(_ + _)

    withGap
      .withColumn("nb_signaux_suspicion", nbConditions)
      .withColumn("is_suspicious", when(col("nb_signaux_suspicion") >= suspiciousSignals, 1).otherwise(0))
  }

  /** Affiche le nombre de transactions suspectes et les 20 montants les plus élevés. */
  def showSuspiciousSummary(df: DataFrame): Unit = {
    val suspicious = df.filter(col("is_suspicious") === 1)
    println(f"[Suspicion] Transactions suspectes détectées : ${suspicious.count()}%,d")
    suspicious
      .select("transaction_id", "user_id", "amount", "ecart_panier_moyen_pct", "day_period",
        "secondes_depuis_achat_precedent", "payment_method", "nb_signaux_suspicion")
      .orderBy(col("amount").desc)
      .show(20, truncate = false)
  }
}

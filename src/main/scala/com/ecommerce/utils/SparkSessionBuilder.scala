package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/**
 * Crée la SparkSession à partir de application.conf (Questions 6.1 et 7.1).
 * Responsable : Membre A
 */
object SparkSessionBuilder {

  /** Valeurs appliquées si la section app.spark.conf ne les redéfinit pas. */
  private val DefaultSparkConf: Map[String, String] = Map(
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED"
  )

  def build(): SparkSession = {
    // Spark 3.5 ne démarre pas au-delà de Java 17 (erreur "getSubject is not supported")
    val javaVersion = System.getProperty("java.specification.version")
    if (javaVersion.split("\\.").last.toInt > 17)
      throw new IllegalStateException(
        s"Java $javaVersion détecté : Spark 3.5 exige Java 8, 11 ou 17. Voir README, section 2.1.")

    val extraConf = DefaultSparkConf ++ ConfigLoader.getStringMap("app.spark.conf")

    val builder = extraConf.foldLeft(
      SparkSession.builder()
        .appName(ConfigLoader.getString("app.name", "EcommerceAnalytics"))
        .config("spark.sql.shuffle.partitions", ConfigLoader.getInt("app.spark.shuffle.partitions", 8).toLong)
    ) { case (b, (key, value)) => b.config(key, value) }

    // Si spark-submit a déjà fourni --master, on le respecte ;
    // sinon (sbt run, IntelliJ) on prend la valeur de la configuration.
    val withMaster =
      if (sys.props.contains("spark.master")) builder
      else builder.master(ConfigLoader.getString("app.spark.master", "local[*]"))

    val spark = withMaster.getOrCreate()
    spark.sparkContext.setLogLevel(ConfigLoader.getString("app.spark.log-level", "WARN"))
    spark
  }
}

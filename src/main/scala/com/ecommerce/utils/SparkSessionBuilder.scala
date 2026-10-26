package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Crée la SparkSession à partir des paramètres de application.conf. */
object SparkSessionBuilder {

  def build(): SparkSession = {
    // Spark 3.5 ne démarre pas au-delà de Java 17 (erreur "getSubject is not supported")
    val javaVersion = System.getProperty("java.specification.version")
    if (javaVersion.split("\\.").last.toInt > 17)
      throw new IllegalStateException(
        s"Java $javaVersion détecté : Spark 3.5 exige Java 8, 11 ou 17. Voir README, section 2.1.")

    val builder = SparkSession.builder()
      .appName(ConfigLoader.getString("app.name", "EcommerceAnalytics"))
      .config("spark.sql.shuffle.partitions",
        ConfigLoader.getInt("app.spark.shuffle.partitions", 8).toLong)
      // Une date impossible donne null avec to_timestamp au lieu d'une erreur
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    // Si spark-submit a déjà fourni --master, on le respecte ;
    // sinon (sbt run, IntelliJ) on prend la valeur de la configuration.
    val withMaster =
      if (sys.props.contains("spark.master")) builder
      else builder.master(ConfigLoader.getString("app.spark.master", "local[*]"))

    val spark = withMaster.getOrCreate()
    spark.sparkContext.setLogLevel("WARN") // console lisible
    spark
  }
}

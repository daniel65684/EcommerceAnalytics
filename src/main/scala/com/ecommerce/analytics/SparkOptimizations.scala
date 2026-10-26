package com.ecommerce.analytics

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Partie 5 - Optimisations Spark (Questions 5.1 et 5.2).
 *
 * Toutes les optimisations passent par cet objet et dépendent de
 * application.conf, ce qui permet de les désactiver d'un seul paramètre
 * pour la mesure avant / après (Question 5.3) :
 *   -Dapp.optimization.enabled=false
 *
 * Responsable : Membre C
 */
object SparkOptimizations {

  /** Interrupteur général (5.3), puis interrupteurs individuels. */
  val enabled: Boolean          = ConfigLoader.getBoolean("app.optimization.enabled", true)
  val cacheEnabled: Boolean     = enabled && ConfigLoader.getBoolean("app.optimization.enable-cache", true)
  val broadcastEnabled: Boolean = enabled && ConfigLoader.getBoolean("app.optimization.enable-broadcast", true)

  def modeLabel: String =
    if (cacheEnabled || broadcastEnabled) "avec_optimisation" else "sans_optimisation"

  /** DataFrame mis en cache, dans l'ordre, pour pouvoir les libérer explicitement. */
  private val persisted = mutable.LinkedHashMap.empty[String, Dataset[_]]

  /**
   * À appeler juste après la création de la SparkSession.
   * Sans broadcast, on désactive aussi le broadcast AUTOMATIQUE de Spark
   * (tables < 10 Mo) : sinon Spark diffuserait quand même les petites
   * tables et la comparaison avant / après serait faussée.
   */
  def configure(spark: SparkSession): Unit = {
    if (!broadcastEnabled) spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    println(s"[Optimisation] mode=$modeLabel | cache=$cacheEnabled | broadcast=$broadcastEnabled | " +
      s"shuffle.partitions=${spark.conf.get("spark.sql.shuffle.partitions")}")
  }

  // -------------------------------------------------------------------
  // Question 5.1 - Stockage
  // -------------------------------------------------------------------

  /**
   * cache() pour un DataFrame réutilisé plusieurs fois.
   * Pour un DataFrame, cache() = persist(MEMORY_AND_DISK) : mémoire,
   * débordement sur disque si nécessaire, données non sérialisées.
   * Le cache est paresseux : il se remplit à la première action.
   */
  def cacheIfEnabled[T](ds: Dataset[T], name: String): Dataset[T] =
    if (cacheEnabled) register(name, ds.cache()) else ds

  /**
   * persist(MEMORY_AND_DISK_SER) pour le DataFrame le plus volumineux
   * (transactions enrichies, ~60 colonnes) : stockage sérialisé, plus compact
   * en mémoire, et débordement sur disque au lieu d'un recalcul.
   */
  def persistLarge[T](ds: Dataset[T], name: String): Dataset[T] =
    if (cacheEnabled) register(name, ds.persist(StorageLevel.MEMORY_AND_DISK_SER)) else ds

  private def register[T](name: String, ds: Dataset[T]): Dataset[T] = {
    persisted.put(name, ds)
    ds
  }

  /**
   * unpersist() des DataFrame dont le nom commence par `prefix`.
   * Attention : libérer un DataFrame dont dépend un autre DataFrame en cache
   * fait perdre aussi le contenu de ce dernier (Spark le recalculera).
   * On ne libère donc en cours de route que des "feuilles" ; le reste est
   * libéré à la fin du pipeline.
   */
  def release(prefix: String): Unit =
    persisted.keys.filter(_.startsWith(prefix)).toList.foreach { name =>
      persisted.remove(name).foreach(_.unpersist())
      println(s"[Optimisation] cache libéré : $name")
    }

  def releaseAll(): Unit = release("")

  // -------------------------------------------------------------------
  // Question 5.2 - Jointures
  // -------------------------------------------------------------------

  /**
   * broadcast() : la petite table est copiée sur chaque exécuteur, la grande
   * table (transactions) n'a plus besoin d'être redistribuée (pas de shuffle).
   */
  def broadcastIfEnabled(df: DataFrame): DataFrame =
    if (broadcastEnabled) broadcast(df) else df
}

/**
 * Question 5.3 (BONUS) - Chronométrage des grandes étapes.
 * Spark étant paresseux, chaque étape doit se terminer par des actions
 * (count, show, write) pour que sa durée corresponde au travail réel.
 */
class StageTimer {

  private val durations = mutable.LinkedHashMap.empty[String, Double]
  private val clock     = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** Exécute une étape en journalisant son heure de début, de fin et sa durée (5.3 et 6.2). */
  def time[T](stage: String)(block: => T): T = {
    println(s"\n[Étape] $stage | début : ${LocalDateTime.now().format(clock)}")
    val start = System.nanoTime()
    try block
    finally {
      val seconds = (System.nanoTime() - start) / 1e9
      durations.update(stage, seconds)
      println(f"[Étape] $stage | fin : ${LocalDateTime.now().format(clock)} | durée : $seconds%.2f s")
    }
  }

  private def rows: Seq[(String, Double)] =
    durations.toSeq :+ ("Total" -> durations.values.sum)

  def printSummary(mode: String): Unit = {
    println(s"\n===== DURÉES ($mode) =====")
    rows.foreach { case (stage, s) => println(f"$stage%-28s $s%8.2f s") }
  }

  /** Sauvegarde dans <output>/performance/<mode> pour remplir le tableau du README. */
  def save(spark: SparkSession, mode: String): Unit = {
    import spark.implicits._
    val root = ConfigLoader.getString("app.data.output.path", "output/").stripSuffix("/")
    try {
      rows.map { case (stage, s) => (mode, stage, BigDecimal(s).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) }
        .toDF("mode", "etape", "duree_s")
        .coalesce(1).write.mode("overwrite").option("header", "true")
        .csv(s"$root/performance/$mode")
    } catch {
      case NonFatal(e) => System.err.println(s"[Temps] ERREUR d'écriture des durées : ${e.getMessage}")
    }
  }
}

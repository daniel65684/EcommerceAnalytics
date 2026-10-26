package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Question 7.1 - Accès centralisé à la configuration.
 *
 * Ordre de priorité (du plus fort au plus faible) :
 *   1. propriétés système      : -Dapp.spark.shuffle.partitions=16
 *   2. fichier externe         : -Dconfig.file=cluster.conf
 *   3. application.conf        : src/main/resources (embarqué dans le JAR)
 *   4. valeur par défaut       : second argument de chaque getter ci-dessous
 *
 * Mécanisme de valeur par défaut : si la clé est absente, OU si sa valeur a
 * un type incorrect (ex : "huit" au lieu de 8), la valeur par défaut est
 * utilisée et un avertissement est affiché une seule fois par clé. Le job
 * ne s'arrête pas pour un paramètre mal saisi.
 *
 * Responsable : Membre A
 */
object ConfigLoader {

  /** ConfigFactory.load() applique déjà les priorités 1 à 3. */
  lazy val config: Config = ConfigFactory.load()

  private val warned = mutable.Set.empty[String]

  def getString(path: String, default: String): String   = read(path, default)(config.getString)
  def getInt(path: String, default: Int): Int             = read(path, default)(config.getInt)
  def getLong(path: String, default: Long): Long          = read(path, default)(config.getLong)
  def getDouble(path: String, default: Double): Double    = read(path, default)(config.getDouble)
  def getBoolean(path: String, default: Boolean): Boolean = read(path, default)(config.getBoolean)

  /**
   * Toutes les paires clé -> valeur d'une section (ex : app.spark.conf),
   * ou une Map vide si la section est absente. Les clés entre guillemets
   * ("spark.sql.xxx") sont restituées sans les guillemets.
   */
  def getStringMap(path: String): Map[String, String] =
    try {
      if (!config.hasPath(path)) Map.empty
      else config.getConfig(path).entrySet().asScala.map { entry =>
        ConfigUtil.splitPath(entry.getKey).asScala.mkString(".") -> entry.getValue.unwrapped().toString
      }.toMap
    } catch {
      case e: ConfigException =>
        warn(path, s"section illisible (${e.getMessage}), ignorée")
        Map.empty
    }

  private def read[T](path: String, default: T)(get: String => T): T =
    try {
      if (config.hasPath(path)) get(path)
      else { warn(path, s"clé absente, valeur par défaut utilisée : $default"); default }
    } catch {
      case e: ConfigException =>
        warn(path, s"valeur invalide (${e.getMessage}), valeur par défaut utilisée : $default")
        default
    }

  private def warn(path: String, message: String): Unit = synchronized {
    if (warned.add(path)) System.err.println(s"[Config] $path : $message")
  }
}

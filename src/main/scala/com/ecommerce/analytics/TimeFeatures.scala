package com.ecommerce.analytics

import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

/**
 * Caractéristiques temporelles extraites d'un timestamp (résultat de l'UDF).
 * Dans Spark, cette case class devient une colonne de type struct.
 */
case class TimeFeatures(
                         hour:             Int,     // 0 à 23
                         day_of_week:      String,  // Monday ... Sunday
                         month:            String,  // January ... December
                         is_weekend:       Int,     // 1 si samedi ou dimanche
                         day_period:       String,  // Morning / Afternoon / Evening / Night
                         is_working_hours: Int      // 1 si 9h <= heure < 17h
                       )

/**
 * Question 3.1 - UDF extractTimeFeatures.
 * Responsable : Membre B
 */
object TimeFeatures {

  /**
   * Format yyyyMMddHHmmss en mode STRICT : une date impossible
   * (ex : 20241345...) est refusée au lieu d'être "corrigée".
   * "uuuu" (année) est obligatoire avec ResolverStyle.STRICT.
   * lazy val : DateTimeFormatter n'est pas sérialisable, il est donc créé
   * une fois par JVM (driver ou exécuteur) au lieu d'être envoyé par le réseau.
   */
  private lazy val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  /**
   * Logique pure, testable sans Spark.
   * Renvoie None pour une chaîne nulle, vide ou mal formée : la colonne vaut
   * alors null et le job continue (exigence de robustesse).
   */
  def extract(timestamp: String): Option[TimeFeatures] =
    Option(timestamp)
      .map(_.trim)
      .filter(_.matches("\\d{14}"))
      .flatMap(s => Try(LocalDateTime.parse(s, formatter)).toOption)
      .map { dt =>
        val hour      = dt.getHour
        val dayOfWeek = dt.getDayOfWeek
        TimeFeatures(
          hour             = hour,
          day_of_week      = dayOfWeek.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend       = if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) 1 else 0,
          day_period       = dayPeriod(hour),
          is_working_hours = if (hour >= 9 && hour < 17) 1 else 0
        )
      }

  /**
   * Morning [6h-12h[, Afternoon [12h-18h[, Evening [18h-22h[,
   * Night pour 22h-23h ET 0h-5h (le sujet ne définit pas 0h-6h :
   * nous le rattachons à la nuit, choix documenté dans CONTRIBUTIONS.md).
   */
  def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"

  /** L'UDF utilisable sur une colonne : extractTimeFeatures(col("timestamp")). */
  val extractTimeFeatures: UserDefinedFunction = udf((timestamp: String) => extract(timestamp))
}

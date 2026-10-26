package com.ecommerce.utils


import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {

  /** Charge application.conf, ou le fichier indiqué par -Dconfig.file. */
  lazy val config: Config = ConfigFactory.load()

  def getString(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  def getInt(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  def getDouble(path: String, default: Double): Double =
    if (config.hasPath(path)) config.getDouble(path) else default

  def getBoolean(path: String, default: Boolean): Boolean =
    if (config.hasPath(path)) config.getBoolean(path) else default
}
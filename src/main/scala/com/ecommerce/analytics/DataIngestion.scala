package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.{col, lit, split, trim}
import org.apache.spark.sql.types._
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoder, SparkSession}

import scala.util.control.NonFatal

/**
 * Résultat d'un chargement réussi (Question 2.3) : les données typées
 * et le nombre de lignes lues avant validation.
 */
case class LoadedDataset[T](name: String, data: Dataset[T], rowCount: Long)

/**
 * Question 2.1 - Ingestion multi-format.
 *
 * Centralise la lecture des quatre sources et renvoie des Dataset typés.
 * Aucun chemin n'est codé en dur : tous proviennent de application.conf
 * (la valeur par défaut n'est utilisée que si la clé est absente).
 *
 * Responsable : Membre A
 */
class DataIngestion(spark: SparkSession) {

  import spark.implicits._

  // -------------------------------------------------------------------
  // Chemins lus dans application.conf
  // -------------------------------------------------------------------
  val transactionsPath: String =
    ConfigLoader.getString("app.data.input.transactions", "src/main/resources/data/transactions.csv")
  val usersPath: String =
    ConfigLoader.getString("app.data.input.users", "src/main/resources/data/users.json")
  val productsPath: String =
    ConfigLoader.getString("app.data.input.products", "src/main/resources/data/products.parquet")
  val merchantsPath: String =
    ConfigLoader.getString("app.data.input.merchants", "src/main/resources/data/merchants.csv")

  // -------------------------------------------------------------------
  // transactions.csv : schéma défini explicitement
  // -------------------------------------------------------------------
  /**
   * Schéma explicite. L'ordre des colonnes doit être celui du fichier :
   * avec un schéma fourni, Spark associe les colonnes CSV par position.
   * Tout est nullable : une valeur illisible devient null au lieu de faire
   * échouer la lecture, et sera rejetée par la validation (Partie 2.2).
   */
  val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("user_id",        StringType, nullable = true),
    StructField("product_id",     StringType, nullable = true),
    StructField("merchant_id",    StringType, nullable = true),
    StructField("amount",         DoubleType, nullable = true),
    StructField("timestamp",      StringType, nullable = true), // String : garde le format yyyyMMddHHmmss
    StructField("location",       StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category",       StringType, nullable = true)
  ))

  def readTransactions(): Dataset[Transaction] =
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE") // ligne mal formée -> valeurs null, pas d'échec
      .schema(transactionSchema)
      .csv(transactionsPath)
      .as[Transaction]

  // -------------------------------------------------------------------
  // users.json : champs potentiellement imbriqués
  // -------------------------------------------------------------------
  /**
   * Le schéma est déduit du JSON (une ligne = un objet), puis :
   *  1. les objets imbriqués sont aplatis (ex : address.city -> city) ;
   *  2. preferred_categories est garanti en tableau, même si le fichier
   *     le fournit sous forme de texte "Books,Electronics" ;
   *  3. les types sont alignés sur la case class (age : bigint -> int).
   */
  def readUsers(): Dataset[User] = {
    val raw  = spark.read.json(usersPath)
    val flat = flattenStructs(raw)

    val withCategories = flat.schema.find(_.name == "preferred_categories").map(_.dataType) match {
      case Some(StringType) =>
        flat.withColumn("preferred_categories", split(trim(col("preferred_categories")), "\\s*,\\s*"))
      case _ => flat
    }

    alignTo[User](withCategories)
  }

  // -------------------------------------------------------------------
  // products.parquet : format colonnaire optimisé
  // -------------------------------------------------------------------
  /**
   * Le chemin désigne un répertoire contenant 12 fichiers Parquet : Spark les
   * lit tous. Le schéma est stocké dans les fichiers ; mergeSchema réconcilie
   * d'éventuelles différences entre fichiers.
   */
  def readProducts(): Dataset[Product] = {
    val raw = spark.read
      .option("mergeSchema", "true")
      .parquet(productsPath)
    alignTo[Product](raw)
  }

  // -------------------------------------------------------------------
  // merchants.csv : schéma inféré par Spark
  // -------------------------------------------------------------------
  /**
   * inferSchema déduit les types en parcourant le fichier. Il lit
   * establishment_date (20220918) comme un entier : alignTo la reconvertit
   * en String, conformément à la case class.
   */
  def readMerchants(): Dataset[Merchant] = {
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(merchantsPath)
    alignTo[Merchant](raw)
  }

  // -------------------------------------------------------------------
  // Question 2.3 - Chargement sécurisé (try-catch) et comptage
  // -------------------------------------------------------------------
  def loadTransactions(): Option[LoadedDataset[Transaction]] =
    load("transactions", transactionsPath)(readTransactions())

  def loadUsers(): Option[LoadedDataset[User]] =
    load("users", usersPath)(readUsers())

  def loadProducts(): Option[LoadedDataset[Product]] =
    load("products", productsPath)(readProducts())

  def loadMerchants(): Option[LoadedDataset[Merchant]] =
    load("merchants", merchantsPath)(readMerchants())

  /**
   * Exécute une lecture dans un bloc try-catch.
   * Le count() force Spark à lire réellement le fichier : sans lui, certaines
   * erreurs n'apparaîtraient que plus tard, loin de leur cause.
   * En cas d'erreur, le message est affiché et None est renvoyé : les autres
   * datasets peuvent continuer à être traités.
   */
  def load[T](name: String, path: String)(read: => Dataset[T]): Option[LoadedDataset[T]] =
    try {
      val ds = read
      val n  = ds.count()
      println(f"[Ingestion] $name%-12s : $n%,d lignes lues avant validation ($path)")
      Some(LoadedDataset(name, ds, n))
    } catch {
      case e: AnalysisException if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
        System.err.println(s"[Ingestion] ERREUR $name : fichier introuvable -> $path")
        None
      case e: AnalysisException =>
        System.err.println(s"[Ingestion] ERREUR $name : structure incorrecte -> ${e.getMessage}")
        None
      case e: SparkException =>
        System.err.println(s"[Ingestion] ERREUR $name : lecture impossible -> ${e.getMessage}")
        None
      case NonFatal(e) =>
        System.err.println(s"[Ingestion] ERREUR $name : ${e.getClass.getSimpleName} -> ${e.getMessage}")
        None
    }

  // -------------------------------------------------------------------
  // Utilitaires
  // -------------------------------------------------------------------

  /**
   * Sélectionne les colonnes de la case class T, dans son ordre, en les
   * convertissant au type attendu. Une colonne absente devient null au lieu
   * de faire échouer le .as[T]. Les colonnes en trop sont ignorées.
   * Une valeur non convertible (ex : "abc" en Double) devient null.
   */
  private def alignTo[T: Encoder](df: DataFrame): Dataset[T] = {
    val target = implicitly[Encoder[T]].schema
    val columns: Seq[Column] = target.fields.toSeq.map { field =>
      if (df.columns.contains(field.name)) col(field.name).cast(field.dataType).as(field.name)
      else lit(null).cast(field.dataType).as(field.name)
    }
    df.select(columns: _*).as[T]
  }

  /**
   * Aplatit récursivement les colonnes de type struct.
   * Une feuille garde son nom simple s'il est unique (address.city -> city),
   * sinon elle prend le chemin complet (address_city).
   * Les tableaux (preferred_categories) sont conservés tels quels.
   */
  private def flattenStructs(df: DataFrame): DataFrame = {
    def leaves(schema: StructType, prefix: Seq[String]): Seq[Seq[String]] =
      schema.fields.toSeq.flatMap { f =>
        f.dataType match {
          case nested: StructType => leaves(nested, prefix :+ f.name)
          case _                  => Seq(prefix :+ f.name)
        }
      }

    val paths      = leaves(df.schema, Seq.empty)
    val nameCounts = paths.groupBy(_.last).map { case (name, ps) => name -> ps.size }

    val columns = paths.map { path =>
      val alias = if (nameCounts(path.last) == 1) path.last else path.mkString("_")
      col(path.map(p => s"`$p`").mkString(".")).as(alias)
    }
    df.select(columns: _*)
  }
}
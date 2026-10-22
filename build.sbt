// =====================================================================
//  EcommerceAnalytics - build.sbt
//  Projet final Spark & Scala - Groupe 3
// =====================================================================

ThisBuild / organization := "com.ecommerce"
ThisBuild / version      := "1.0.0"

// Spark 3.5.x est compilé par défaut pour Scala 2.12 : les deux versions
// doivent correspondre, sinon erreurs NoSuchMethodError à l'exécution.
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    // ---------------------------------------------------------------
    // Dépendances
    // ---------------------------------------------------------------
    libraryDependencies ++= Seq(
      // "provided" : Spark est déjà présent sur le cluster (spark-submit),
      // on ne l'embarque donc pas dans le JAR.
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",

      // Configuration externalisée (application.conf) - embarquée dans le JAR
      "com.typesafe" % "config" % "1.4.3",

      // Tests unitaires
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    // ---------------------------------------------------------------
    // Exécution locale avec `sbt run`
    // ---------------------------------------------------------------
    // Remet les dépendances "provided" dans le classpath de `sbt run`,
    // sinon Spark est introuvable en local.
    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated,

    Compile / mainClass := Some("com.ecommerce.analytics.MainApp"),

    // JVM séparée : évite les fuites mémoire de Spark dans sbt et permet
    // de passer les options JVM ci-dessous.
    run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false, // une seule SparkSession à la fois

    // Options nécessaires pour faire tourner Spark sous Java 17
    javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    ),

    // Le répertoire courant de `sbt run` reste la racine du projet,
    // pour que les chemins relatifs de application.conf fonctionnent.
    run / baseDirectory := (ThisBuild / baseDirectory).value,

    // ---------------------------------------------------------------
    // Génération du JAR exécutable (sbt-assembly)
    // ---------------------------------------------------------------
    assembly / mainClass       := Some("com.ecommerce.analytics.MainApp"),
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",

    // Scala est fourni par Spark sur le cluster : inutile de l'embarquer.
    assembly / assemblyOption ~= { _.withIncludeScala(false) },

    // Résolution des fichiers en double lors de la fusion des JAR
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case "application.conf"                   => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )

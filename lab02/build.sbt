name := "lab02"

version := "0.1"

scalaVersion := "2.13.5"

val circeVersion = "0.13.0"

libraryDependencies ++= Seq(
  "io.circe"  %% "circe-core"     % circeVersion,
  "io.circe"  %% "circe-generic"  % circeVersion,
  "io.circe" %% "circe-generic-extras"  % circeVersion,
  "io.circe"  %% "circe-parser"   % circeVersion,
  "io.circe"  %% "circe-literal"  % circeVersion

)

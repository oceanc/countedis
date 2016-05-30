def projectCfg(id: String, path: String): Project = {
  Project(id, file(path)).
    settings(
      organization := "com.github.oceanc",
      version := "0.1.0.alpha",
      scalaVersion := "2.11.7",
      autoScalaLibrary := false,
      scalacOptions ++= Seq(
        "-target:jvm-1.7",
        "-encoding", "UTF-8",
        "-deprecation",
        "-feature",
        "-language:higherKinds",
        "-Ywarn-dead-code",
        "-Xlint"
      ),
      parallelExecution in Test := false,
      fork in Test := true,
      logBuffered := false,
      libraryDependencies ++= Seq(
        "org.log4s"           %% "log4s"           % "1.3.0"   exclude("org.scala-lang", "scala-library"),
        "ch.qos.logback"      % "logback-classic"  % "1.1.6"   % Provided,
        "org.scalatest"       % "scalatest_2.11"   % "2.2.6"   % Test,
        "com.storm-enroute"   %% "scalameter"      % "0.7"     % Test  exclude(
          "com.fasterxml.jackson.module", "jackson-module-scala_2.11")
      ),
      dependencyOverrides ++= Set(
        "org.scala-lang.modules"   %%  "scala-parser-combinators"  % "1.0.4",
        "org.scala-lang.modules"   %%  "scala-xml"                 % "1.0.4",
        "org.slf4j"                %   "slf4j-api"                 % "1.7.21"
      )
      //testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
      //testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "html", "pandoc", "false")
    )
}


lazy val core = projectCfg("core", ".").aggregate(net).dependsOn(net).settings(
  name := "countedis-core",
  libraryDependencies ++= Seq(
    "net.debasishg"       %% "redisclient"     % "3.1"    exclude("org.slf4j", "slf4j-api"),
    "org.apache.curator"  % "curator-recipes"  % "2.9.1"  exclude("log4j", "log4j")
  )
)


lazy val net = projectCfg("net", "net").settings(
  name := "countedis-net",
  libraryDependencies ++= Seq(
    "org.scala-lang"        % "scala-reflect"      % scalaVersion.value,
    "io.netty"              % "netty-all"          % "4.0.33.Final",
    "com.google.guava"      % "guava"              % "16.0.1",
    "net.jpountz.lz4"       % "lz4"                % "1.3.0",
    "org.xerial.snappy"     % "snappy-java"        % "1.1.2.1",
    "org.json4s"            %% "json4s-native"     % "3.3.0",
    "com.alibaba"           % "fastjson"           % "1.2.8",
    "com.twitter"           %% "chill"             % "0.8.0",
    //"de.ruedigermoeller"    % "fst"                % "2.44",
    "de.javakaffee"         % "kryo-serializers"   % "0.37" excludeAll(
      ExclusionRule("com.esotericsoftware", "kryo"),
      ExclusionRule("com.google.protobuf", "protobuf-java"))
  )
)

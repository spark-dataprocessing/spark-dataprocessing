/*
Copyright 2019 Kaspar Mösinger

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


package io.github.sparkdataprocessing

import java.util.NoSuchElementException

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object DataProcessingApp {

  def apply(appName: String, configurations: Set[DataProcessingConf], steps: BaseStep*):DataProcessingApp = {
    new DataProcessingApp(appName, configurations, steps:_*)
  }

  def apply(appName: String, configuration: DataProcessingConf, steps: BaseStep*):DataProcessingApp = {
    new DataProcessingApp(appName, configuration, steps:_*)
  }
}


/**
  * A DataPrepApp brings together the steps and the configurations.
  *
  * @param appName        Name of the Application.
  * @param configurations Set of configurations
  * @param mainStep
  */
case class DataProcessingApp(appName: String, configurations: Set[DataProcessingConf], mainStep: BaseStep) {

  /**
    * Alternative Constructor accepting a variable list of steps
    */
  def this(appName: String, configurations: Set[DataProcessingConf], steps: BaseStep*) =
    this(appName, configurations, new StepGroup("Main", steps: _*))

  /**
    * Alternative Constructor accepting one configuration
    */
  def this(appName: String, configuration: DataProcessingConf, mainStep: BaseStep) =
    this(appName, Set(configuration), mainStep)

  /**
    * Alternative Constructor accepting one configuration and a variable list of steps
    */
  def this(appName: String, configuration: DataProcessingConf, steps: BaseStep*) =
    this(appName, Set(configuration), new StepGroup("Main", steps: _*))

  /**
    * Get a configuration
    *
    * @param name of the configuration
    */
  def getConfiguration(name: String): DataProcessingConf = configurations.filter(c => c.name == name).last

  /**
    * Create the SparkSession
    *
    * @param conf Configuration used to set up the SparkSession
    */
  private def createSparkSession(conf: DataProcessingConf): SparkSession = {

    Logger.getLogger("org").setLevel(conf.sparkLogLevel)
    Logger.getLogger("akka").setLevel(conf.sparkLogLevel)
    Logger.getLogger("hive.metastore").setLevel(conf.sparkLogLevel)

    val sparkConf = conf.sparkConf
    val sparkAppName = s"$appName (${conf.name})"

    SparkSession.builder
      .config(conf = sparkConf)
      .appName(sparkAppName)
      .getOrCreate()
  }

  /**
    * Run the Application
    *
    * @param conf Configuration
    */
  def run(conf: DataProcessingConf): State = {
    println(conf.toString) // Log Info

    val spark = createSparkSession(conf)

    val state = mainStep.execute(conf, spark)

    state.show()
    println(s"--- DataPrepApp finished ---") // Log Info

    state
  }

  /**
    * Run the Application
    *
    * @param config Name of the Configuration
    */
  def run(config: String): State = this.run(getConfiguration(config))

  /**
    * Run the Application. Can only be used when exactly one configuration is available.
    * @return
    */
  def run(): State = {
    if(configurations.size != 1)
      throw new NoSuchElementException(s"DataPrepApp.run() without specifying the configuration requires that there is exactly 1 configuration available.")
    this.run(configurations.head)
  }

  /**
    * Run the Application
    *
    * @param args Arguments containing the name of the Configuration to execute
    */
  def run(args: Array[String]): State = {


    def nextOption(map : Map[String, String], list: List[String]) : Map[String, String] = {
      list match {
        case Nil => map
        case key :: value :: tail =>
          if(key.substring(0,2) != "--") {
            throw new IllegalArgumentException(s"Illegal argument $key")
          }
          nextOption(map ++ Map(StringUtils.substring(key,2) -> value), tail)
        case key :: Nil => throw new IllegalArgumentException(s"Missing value for option $key")
      }
    }
    val options = nextOption(Map(), args.toList)


    var conf =
      if(options.contains("configuration"))
        getConfiguration(options("configuration"))
      else if(configurations.size == 1)
        configurations.last
      else {
        throw new IllegalArgumentException("Missing configuration argument")
      }

    conf = options.toSeq.foldLeft(conf){case(c, (key,value)) => c.set(key,value)}

    conf =
      if(options.contains("targets"))
        conf.setTargets(options("targets"))
      else
        conf

    run(conf)
  }

  /**
    * Analyze the dependencies of this application
    *
    * @param state Assumes that for each step the state is cached
    */
  def getDependencies(state: State): Dependencies = {
    mainStep.getDependencies(state)
  }

  override def toString: String = {
    s"$appName ${configurations.toString()}"
  }

}

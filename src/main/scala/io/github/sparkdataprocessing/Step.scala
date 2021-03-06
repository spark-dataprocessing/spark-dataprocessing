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

import org.apache.spark.sql.{DataFrame, SparkSession}


object Step {

  def apply(name:String, f:(State, SparkSession, DataProcessingConf) => DataFrame):Step = {
    val g = (state:State) => f(state, state.spark, state.settings)
    new StepFunctional(name, g)
  }

  def apply(name:String, f: ()=> DataFrame):Step = {
    new StepFunctional(name, (_:State) => f())
  }

  def apply(name:String, f: SparkSession=> DataFrame):Step = {
    new StepFunctional(name, (state :State) => f(state.spark))
  }
}

abstract class Step(val target:String) extends BaseStep {

  import org.apache.spark.sql.{DataFrame, SparkSession}

  import scala.collection.immutable._



  override def targets:Set[String] = Set(target)



  override def toString: String = {
    s"Step($target, $id, ${this.getClass().getSimpleName})"
  }


  def loadFromCache(state:State):State = {
    val t0 = System.nanoTime()
    val tableName = state.settings.hivePrefix + state.uniqueTableName(target)

    println(s"  Use cached data")
    println(s"  Temporary-Table:   $tableName")

    val df = state.spark.table(tableName)
    state.add(StateRecord(this, df, tableName, "loaded from cache", System.nanoTime() - t0))
  }

  def writeToCache(state:State):State = {
    val spark = state.spark

    val tableName = state.settings.hivePrefix + state.uniqueTableName(target)

    //get definition
    val t0 = System.nanoTime()
    val df = getDefinition(state)

    // save table to disk
    println(s"  Write step $target to $tableName")

    spark.sparkContext.setLocalProperty("callSite.short", target)
    df.write.
      format("parquet").
      mode(org.apache.spark.sql.SaveMode.Overwrite).
      saveAsTable(tableName)

    // load the saved table from cache
    val df2 = spark.table(tableName)
    state.add(StateRecord(this, df2, tableName, "written to cache", System.nanoTime() - t0))
  }

  def getDefinition(state:State):DataFrame = {
    // Register all the available tables as TempViews
    state.show()
    state.activate()

    // Get the
    state.spark.sparkContext.setLocalProperty("callSite.short", target + " (def)")
    val tableDef:DataFrame = definition(state, state.spark)


    state.spark.sparkContext.setLocalProperty("callSite.short", target + " (post)")

    tableDef
  }

  def passDefinition(state:State):State = {
    val t0 = System.nanoTime()
    val df = getDefinition(state)
    state.add(StateRecord(this, df, "", "lazy", System.nanoTime() - t0))
  }


  final val Execute:String = "Execute"
  final val LoadFromCache:String = "LoadFromCache"
  final val PassDefinition:String = "PassDefinition"


  def getAction(state:State) = {
    // default: PassDefinition
    val stepsToWrite = state.settings.targets
    val stepsToLoadFromCache = state.settings.targetsLoadCache

    if(stepsToWrite.map(s => target.matches(s)).reduceOption(_||_).getOrElse(false))
      Execute
    else if (stepsToLoadFromCache.map(s => target.matches(s)).reduceOption(_||_).getOrElse(false))
      LoadFromCache
    else
      PassDefinition
  }

  def run(state: State):State = {
    val spark = state.spark

    println(s"--- Step ${target} ---")
    spark.sparkContext.setLocalProperty("callSite.short", target + " (pre)")
    spark.sparkContext.setLocalProperty("callSite.long", "Class: " + getClass.getName)

    val action = getAction(state)
    println(s"Action: $action")

    action match {
      case LoadFromCache => loadFromCache(state)
      case Execute => writeToCache(state)
      case PassDefinition => passDefinition(state)
    }
  }





  def getDependency(_state:State):Dependency = {
    var dependencies = Seq[(Step, Seq[String])]()

    println(s"Checking dependencies of $target")

    /**
      * Helper function that checks if this step can be compiled given the State s.
      * @param s State to consider
      */
    def checkDependency(s:State): Boolean = {
      s.activate(cleanTempViews = true)
      try {
        val tableDef = definition(s)
        val _ = tableDef.schema
        false
      } catch {
        case scala.util.control.NonFatal(_) => true
      }
    }


    // Get the State for this step:
    val state = _state
      .getStateForStep(this)
      .setSettings(_state.settings.setTargets().setTargetsLoadCache(Set()))

    // Check the dependency on each previous step
    for (stepNameToAnalyze <- state.tableNames) {
      val stepToAnalyse = state.getStep(stepNameToAnalyze)

      // removes ALL previous steps given the name. This is desired in the case a step overwrites a previous step
      if (checkDependency(state.remove(stepNameToAnalyze))) {
        // In case this step depends on a previous step, check which columns are required
        var dependendColumns = Seq[String]()

        val dataFrame = state(stepNameToAnalyze)
        for(colName <- dataFrame.columns.toSeq) {
          val df = dataFrame.drop(colName)
          val s = state.add(StateRecord(stepToAnalyse, df, hiveTableName= "", status="dependency analysis", time=0))

          if (checkDependency(s)) {
            dependendColumns = dependendColumns :+ colName
          }
        }

        // Make sure that the dependency is correctly captured even in case where no column is explicitely given
        if (dependendColumns.isEmpty) {
          dependendColumns = dependendColumns :+ "<no explicit column>"
        }

        dependencies = dependencies :+ (stepToAnalyse, dependendColumns)
      }
    }
    Dependency(this, dependencies, state)
  }


  override def getDependencies(state: State): Dependencies = new Dependencies(Seq(getDependency(state)))



  // Funktion, welche die Subklasse überschreibt
  def definition(state: State, spark: SparkSession):DataFrame

  def definition(state:State):DataFrame = definition(state, state.spark)
}
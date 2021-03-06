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

import scala.collection.immutable.Map
import org.apache.commons.lang.StringUtils


object State {

  /**
    * Auxiliary constructor
    * @param settings settings for the state
    * @param spark SparkSession
    * @return
    */


  def createForInputTables(inputTables: Map[String, String], st:State = State(settings = DataProcessingConf().setCleanTempViews(false))): State = {

    var state = st.setSettings(st.settings.setInputTables(st.settings.inputTables ++ inputTables))

    val inputSteps = inputTables.map{ case (name, table) => new StepHiveSource(name, table)}

    inputSteps.foldLeft(state)((st, s) => {
      val t0 = System.nanoTime()
      val tableDef = s.definition(st)
      st.add(StateRecord(s, tableDef, s.tableName, "input", System.nanoTime() - t0))
    })
  }


  def createForHivePrefix(hivePrefix:String, inputTables: Map[String, String] = Map(), spark:SparkSession = SparkSession.active): State = {

    val settings = DataProcessingConf()
      .setHivePrefix(hivePrefix)
      .setCleanTempViews(false)

    val steps =
      listTables(hivePrefix, spark)
        .collect()
        .map(row => {
          val tableName = row.getAs[String]("tableName")
          val database = row.getAs[String]("database")

          val target = StringUtils.substring(tableName, hivePrefix.length - database.length - 1)
          new StepHiveSource(target, database + "." + tableName)
        })

    val state = State.createForInputTables(inputTables, State(settings=settings, spark=spark))

    steps.foldLeft(state)((s, step) => {
      val t0 = System.nanoTime()
      val tableDef = step.definition(s)
      s.add(StateRecord(step, tableDef, step.tableName, "hivePrefix", System.nanoTime() - t0))
    })
  }


  def dropTablesWithPrefix(hivePrefix:String, spark:SparkSession = SparkSession.active): Unit = {
    listTables(hivePrefix,spark)
      .collect()
      .foreach(t => {
        val q = s"drop table ${t.getAs[String]("database")}.${t.getAs[String]("tableName")}"
        println(q)
        spark.sql(q)
      })
  }

  /** INTERNAL
    */
  def listTables(hivePrefix:String, spark:SparkSession): DataFrame = {
    import spark.implicits._

    val Array(database, prefix) = hivePrefix.split("\\.")

    spark
      .sqlContext
      .tables(database)
      .filter($"tableName".startsWith(prefix))
  }

}


/**
  *
  * @param steps
  * @param dataFramesById
  * @param hiveTableById
  */
case class State(steps: Seq[Step] = Seq(),
                 settings: DataProcessingConf = DataProcessingConf(),
                 stateRecordById: Map[String, StateRecord] = Map(),
                 previousStates: Map[String, State] = Map(),
                 spark: SparkSession = SparkSession.active) {


  def writeDataFrame(target:String, df:DataFrame, activate:Boolean = true, show:Boolean = true):State = {
    val step = new StepFunctional(target, _ => df)
    val state = step.writeToCache(this)
    if(activate)
      state.activate(false)
    if(show)
      state.show()
    state
  }


  def add(record:StateRecord):State = copy(
    steps = this.steps :+ record.step,
    stateRecordById = stateRecordById + (record.id -> record)
  )

  def remove(name:String):State = copy(
    steps = steps.filterNot(p => p.target == name),
    stateRecordById = stateRecordById.filterNot(t => t._2.step.target == name)
  )

  def setSettings(a: DataProcessingConf):State = copy(settings = a)


  def snapshotState(a:BaseStep):State = copy(
    previousStates = this.previousStates + (a.id -> this)
  )

  def getState(tableName: String):State = previousStates(getStep(tableName).id)

  def getStateForStep(s: Step):State = previousStates(s.id)

  // gibt den letzten Step mit dem Namen name zurück
  def getStep(tableName: String):Step = steps.filter(_.target == tableName).last
  def getDF(tableName: String):DataFrame = stateRecordById(getStep(tableName).id).dataFrame


  def apply(tableName: String):DataFrame = getDF(tableName)

  /**
    * List all the available tables
    * @return
    */
  def tableNames:Seq[String] = steps.map(step => step.target).distinct


  // ...byId --> benötigt für Dependencies
  def getStepById(id: String):Step = steps.filter(_.id == id).last
  def getDFById(id: String):DataFrame = stateRecordById(id).dataFrame

  def getStateRecord(step: Step):StateRecord = stateRecordById(step.id)

  /**
    * Activate the state:
    * - Cleanup (unregister) all existing temp-views
    * - register the the tables of this state
    */
  def activate(cleanTempViews: Boolean = settings.cleanTempViews):Unit = {
    if(cleanTempViews) {
      unregisterAllTempViews()
    }
    registerTempViews()
  }

  /**
    * Unregister all temp-views
    */
  def unregisterAllTempViews():Unit = {
    import spark.implicits._

    spark
      .sqlContext
      .tables()
      .filter($"isTemporary")
      .select("tableName")
      .collect()
      .foreach(row => spark.catalog.dropTempView(row.getString(0)))
  }

  /**
    * Register the latest Tables of this state as TempViews
    */
  def registerTempViews():Unit = tableNames.foreach(name => getDF(name).createOrReplaceTempView(name))


  /**
    * Returns an unique table name, that can be used to spill the results of a step to hdfs.
    * Multiple Steps can have the same name. If step foo does not exist yet, it returns 'foo',
    * otherwise it returns 'foo_2', 'foo_3',....
    *
    * @param name
    * @return unique table name
    */
  def uniqueTableName(name: String):String = {
    val n = steps.count(_.target == name)

    if(n==0)
      name
    else
      s"${name}_${n+1}"
  }

  /**
    * Return the content of th
    */
  def content: DataFrame = {
    import spark.implicits._

    steps.map(step => {
      val stateRecord = stateRecordById(step.id)

      // Reduce clutter
      val source = step match {
        case _: StepFunctional => "Step(name, ...)"
        case _: StepHiveSource => ""
        case s => s.getClass.getSimpleName
      }

      // Mark the steps that are overwritten by a subsequent step
      val stepVisible = step.id == getStep(step.target).id
      val name = if(stepVisible) step.target else s"*${step.target}*"
      val status = if(stepVisible) stateRecord.status else s"${stateRecord.status}-overwritten"


      (name, status, stateRecord.hiveTableName, (stateRecord.time * 1e-9).toInt, source)
    }
    ).toDF("Name", "Status", "Hive Table", "Processing Time [sec]", "Defined in")
  }

  def show(numRows:Int = 100): Unit = content.show(numRows, truncate = false)

  def show:Unit = this.show()
}

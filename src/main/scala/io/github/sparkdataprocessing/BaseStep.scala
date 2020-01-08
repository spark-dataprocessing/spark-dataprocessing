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

object BaseStep {
  private var current = 0

  def inc: Int = {
    current += 1
    current
  }
}


trait BaseStep {
  import org.apache.spark.sql.SparkSession

  import scala.collection.immutable._


  /**
    *
    * @return
    */
  def target:String


  def targets:Set[String]

  /**
    * Each instance gets an unique ID
    */
  private val _id = BaseStep.inc

  /**
    * Human readable unique ID
    */
  def id = target + "_" + _id


  /**
    * Excecutes this step
    * @param state
    * @return
    */
  def run(state: State):State

  def apply(state: State):State = run(state)


  def execute(settings:DataProcessingConf,
              spark:SparkSession):State = {

    // Create the fist state object. All subsequent states are derived from this object.
    var state = State(settings = settings, spark = spark)
    state = State.createForInputTables(settings.inputTables, state)

    state.show(50)
    println("--- Finished input data ---")

    // Run this step
    state = state.snapshotState(this)
    try {
      run(state)
    } catch {
      case c:CatchAllException => {
        if(state.settings.returnStateOnException) {
          c.getCause.printStackTrace()
          println(s"Caught exception... return last State. You can re-run on the failing step using step(state)")
          c.lastState
        } else
          throw  c.getCause
      }
    }
  }

  def getDependencies(state:State):Dependencies




  override def equals(obj: Any): Boolean = obj match {
    case obj: BaseStep => obj.id == this.id
    case _ => false
  }

 // override def hashCode(): Int = super.hashCode()
}

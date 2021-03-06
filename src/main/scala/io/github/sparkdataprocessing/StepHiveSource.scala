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

// TODO: protected
class StepHiveSource(target:String, val tableName:String) extends Step(target) {


  override def getDependency(_state:State):Dependency = Dependency(this, Seq(), _state)

  // Funktion, welche die Subklasse überschreibt
  override def definition(inputDataFrames: State, spark: SparkSession):DataFrame = {
    println(s"StepHiveSource.definition(...)  $tableName")
    spark.table(tableName)
  }

  override def toString: String = {
    f"name: $target%-25s   id: $id%-30s   table: $tableName%-40s   ${getClass.getName}"
  }


}

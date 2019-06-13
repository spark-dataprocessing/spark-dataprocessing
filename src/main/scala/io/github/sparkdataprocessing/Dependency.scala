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

import org.apache.spark.sql.DataFrame

case class Dependency(step: Step,
                      dependsOn: Seq[(Step, Seq[String])],
                      state:State
                     ) {


  def content: DataFrame = {
    val spark = state.spark
    import spark.implicits._

    if(dependsOn.isEmpty) {
      Seq.empty[(String, String, String)].toDF("Step", "Depends on", "Column", "Defined in")
    }else {
      dependsOn.map(x => {
        val stepDepends = x._1
        val columns = x._2

        // Reduce clutter
        val source = stepDepends match {
          case _: StepFunctional => "Step(name, ...)"
          case _: StepHiveSource => ""
          case s => s.getClass.getSimpleName
        }

        columns.map(c => (step.target, stepDepends.target, c, source))
      }).reduce(_ ++ _).toDF("Step", "Depends on", "Column", "Defined in")
    }
  }

  def show(numRows:Int = 100): Unit = {
    println(s"Dependencies for ${step.target}:")
    content.show(numRows, truncate = false)
  }

  def show:Unit = this.show()

}

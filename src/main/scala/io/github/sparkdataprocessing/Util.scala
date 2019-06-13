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

object Util {

  def renderGraphviz(deps: Seq[Dependency]): String = {
    var output = ""
    output += s"digraph D {\r\n"

    output += """  node [shape = box, fontname = Helvetica]\r\n"""


    // Todo: Alle Nodes zusammensuchen (auch die Input-Tables)
    //val nodes = deps.map()

    output += deps.map(d => {
      val step = d.step
      s"""  ${step.id}  [label = \"${step.target}\"];\r\n"""
    }).reduce(_+_)
    output += "\r\n"

    output += deps.map(d => {
      if(d.dependsOn.isEmpty){
        ""
      }else {
        d.dependsOn.map(dependsOn => {
          val source = dependsOn._1

          s"""  ${source.id} -> ${d.step.id};\r\n"""
        }).reduce(_ + _)
      }
    }).reduce(_+_)

    output + "}"
  }
}

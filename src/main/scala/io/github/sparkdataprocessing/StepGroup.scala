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

class StepGroup(val groupName:String, val childSteps:BaseStep*) extends BaseStep {

  import scala.collection.immutable._



  override def target:String = groupName

  override def run(_state: State):State = {

    println("")
    println(s"--- StepGroup $groupName ---")

    var state = _state

    // If the group is in the target, add all steps of the group to the target
    if(state.settings.targets contains target) {
      println(s"Add targets: $targets")
      state = state.setSettings(state.settings.addTargets(this.targets))
    }

    // If no target is defined, execute all
    if(state.settings.targets.isEmpty) {
      state = state.setSettings(state.settings.addTargets(this.targets))
    }


    println(s"  Targets:  ${targets.mkString(", ")}")
    println(state.settings)
    val t0 = System.nanoTime()

    childSteps.foreach( s => {
      state = state.snapshotState(s)

      try {
        state = s.run(state)
      } catch {
        case c: CatchAllException => throw c // in case of nested StepGroup, don't nest the Exception
        case c: Throwable => throw new CatchAllException(c, state, s)
      }

    })
    val t1 = System.nanoTime()
    println(s"--- End StepGroup $groupName ---")
    println(s"  Elapsed time $groupName: " + ((t1 - t0)*1e-9).round + " sec")
    state
  }


  override def getDependencies(state: State): Dependencies = {
    childSteps.map(_.getDependencies(state)).reduce(_++_)
  }


  override def targets:Set[String] = {
    childSteps.map(_.targets).reduceLeft(_++_)
  }
}
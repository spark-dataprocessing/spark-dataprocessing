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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.{Map, Set}

case class DataProcessingConf(
                               name:String = "<unnamed configuration>",
                               hivePrefix:String = "",
                               targets:Set[String] = Set(".*"),
                               targetsLoadCache:Set[String] = Set(),
                               preprocess: SparkSession => Unit = (x:SparkSession) => Unit,
                               inputTables: Map[String, String] = Map(),
                               sparkConf:SparkConf = new SparkConf(),
                               cleanTempViews:Boolean = true,
                               returnStateOnException:Boolean = false,
                               sparkLogLevel:org.apache.log4j.Level = org.apache.log4j.Level.ERROR,
                               keyValue:Map[String, Any] = Map()
                       ) {

  import org.apache.spark.sql.SparkSession

  def setReturnStateOnException(a: Boolean): DataProcessingConf = copy(returnStateOnException = a)

  def setSparkConf(spark:SparkConf):DataProcessingConf = this.copy(sparkConf=spark)

  def setName(aName:String):DataProcessingConf = this.copy(name=aName)


  def set(key:String, value:Any):DataProcessingConf =
    this.copy(keyValue = this.keyValue + (key -> value))

  def get(key:String):Any = keyValue(key)
  def getString(key:String):String = keyValue(key).asInstanceOf[String]



  def setHivePrefix(p:String):DataProcessingConf = this.copy(hivePrefix=p)
  def setCleanTempViews(b:Boolean):DataProcessingConf = this.copy(cleanTempViews=b)

  def setPreprocess(f:SparkSession => Unit):DataProcessingConf = this.copy(preprocess=f)

  def setInputTables(t: Map[String, String]):DataProcessingConf = this.copy(inputTables=t)

  def setTargetsLoadCache(t:Set[String]):DataProcessingConf = this.copy(targetsLoadCache=t)


  def addTarget(t:String):DataProcessingConf = this.copy(targets = this.targets + t)

  def addTargets(t:Set[String] ):DataProcessingConf = this.copy(targets = this.targets ++ t)
  def addTargets(t:String*):DataProcessingConf = this.copy(targets = this.targets ++ t)
  def setTargets(t:Set[String] ):DataProcessingConf = this.copy(targets = t)
  def setTargets(t:String*): DataProcessingConf = copy(targets = t.toSet)
  def setTargets(t:String): DataProcessingConf = copy(targets = Set(t))
  def setTargets(): DataProcessingConf = copy(targets = Set())


  /**
    * check the validity of this configuration
    * @return
    */
  def valid = true


  /**
    * Checks the validation and notifies if not correct
    */
  def validate() = {
    if(!this.valid) {
      println("Error: Configuration is not valid. Please add...")
    }
  }


  override def toString: String = {
    s""" DataPrepConf
       |   name:                    $name
       |   targets:                 ${targets.mkString(", ")}
       |   cacheTables:             ${targetsLoadCache.mkString(", ")}
       |   cleanTempViews:          $cleanTempViews
       |   returnStateOnException:  $returnStateOnException
       |   hivePrefix:              $hivePrefix
       |   keyValue:                ${keyValue.map{case(k,v) => s"$k -> $v"}.mkString(", ")}
     """.stripMargin

   // |   sparkConf:         ${sparkConf.toDebugString}
  }


}

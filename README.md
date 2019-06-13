# Spark-DataProcessing

The requirements on a data processing pipeline are inherently different during model development and production.
In the development-phase we want:
* to be able to deploy fast
* being able to easily modify the pipeline
* and to work interactively.

In production, on the other hand, we want a pipeline that is fast, stable and works unattended.


The Spark-DataProcessing framework aims to make this process easier.
It manages configurations and and it gives a structure on how to organize the code.


The framework has the following three building blocks:

* *Step*
* *Configuration* 
* *App*



## Example

**Warning:** This example writes into the hdfs directory `/sandbox/spark-dataprocessing/example1` and adds hive tables in `default.tmp_sdp_ex1_*`.

### Create some dummy data

```scala 
Seq(
    (8, "Smith", "James", "m"),
    (64, "Jones", "George", "m"),
    (12, "Miller", "Maria", "f")
  ).toDF("cust_id", "surname", "name", "gender").
  write.
  format("parquet").
  option("path", "/sandbox/spark-dataprocessing/example1/customer").
  saveAsTable("default.tmp_sdp_ex1_customer")
```

### Define the steps

The first Step filters on male customers.
The customer-table is called by its alias `customer`. 
The mapping `"default.tmp_sdp_ex1_customer" -> "customer"` is defined in the configuration.

```scala
import io.github.sparkdataprocessing.{Step, DataProcessingConf, State}
import org.apache.spark.sql.{DataFrame, SparkSession}

// The name of the step is given in the constructor:
class CustomerMale extends Step("customer_male") {

  // Function containing the logic
  override def definition(state:State, spark: SparkSession, settings: DataProcessingConf): DataFrame = {
    spark.sql("select cust_id, surname, name from customer where gender = \"m\"")
    
    /*
    Alternatively, we can access the input tables and DataFrames from previous steps through the state object:
    
    import spark.implicits._
    state("customer").filter($"gender" === "m").select("cust_id", "surname", "name")
    */
  }
}
```
The second Step counts the number of male customers.
It uses the output of the step `customer`.

```scala
class CountMale extends Step("count_male") {
 
  override def definition(state:State, spark: SparkSession, settings: DataProcessingConf): DataFrame = {
    spark.sql("select count(*) as n from customer_male")
  }
}
```

### Define the configurations

A configuration has a name and it defines the prefix of the location where the intermediate results should be written. 
The mapping for the input tables is also defined here.

We have two configurations for two developers and one productive configuration.
The intermediate results won't interfere, since both developers have different locations for their intermediate results.

```scala
import io.github.sparkdataprocessing._
 
val dev1Conf = DataProcessingConf("dev1").
  setHdfsDirectory("/sandbox/spark-dataprocessing/example1/dev1/").
  setHivePrefix("default.tmp_sdp_ex1_dev1_").
  setInputTables(Map("customer" -> "default.tmp_sdp_ex1_customer"))
 
val dev2Conf =dev1Conf.
  setName("dev2").
  setHdfsDirectory("/sandbox/spark-dataprocessing/example1/dev2/").
  setHivePrefix("default.tmp_sdp_ex1_dev2_")
  
val prodConf = DataProcessingConf("prod").
  setCacheTables(false).
  setTargets("").
  setInputTables(Map("customer" -> "default.tmp_sdp_ex1_customer"))
```

### Define the pipeline

Multiple configurations can be registered:

```scala
val app = DataProcessingApp("Example 1", Set(dev1Conf, dev2Conf, prodConf), new CustomerMale, new CountMale)
```


### Run the pipeline (development)

On executing the pipeline, the configuration has to be specified:

```scala
val state = app.run("dev2") 
```

Inspect the available objects after the execution:

```scala
state.show
```
```
+-------------+----------------+--------------------------------------+---------------------+------------+
|Name         |Status          |Hive Table                            |Processing Time [sec]|Defined in  |
+-------------+----------------+--------------------------------------+---------------------+------------+
|customer     |input           |default.tmp_sdp_ex1_customer          |2                    |            |
|customer_male|written to cache|default.tmp_sdp_ex1_dev2_customer_male|5                    |CustomerMale|
|count_male   |written to cache|default.tmp_sdp_ex1_dev2_count_male   |6                    |CountMale   |
+-------------+----------------+--------------------------------------+---------------------+------------+
```

Access the intermediate results

```scala
state("customer_male").show
```
```
+-------+-------+------+
|cust_id|surname|  name|
+-------+-------+------+
|     64|  Jones|George|
|      8|  Smith| James|
+-------+-------+------+
```
 
```scala
state("count_male").show
```
```
+---+
|  n|
+---+
|  2|
+---+
```

### Run the pipeline (production)

```scala
val stateProd = app.run("prod") 
```

In production, the intermediate results are not cached - they are lazy coupled together:

```scala
stateProd.show
```
```
+-------------+------+----------------------------+---------------------+------------+
|Name         |Status|Hive Table                  |Processing Time [sec]|Defined in  |
+-------------+------+----------------------------+---------------------+------------+
|customer     |input |default.tmp_sdp_ex1_customer|0                    |            |
|customer_male|lazy  |                            |0                    |CustomerMale|
|count_male   |lazy  |                            |0                    |CountMale   |
+-------------+------+----------------------------+---------------------+------------+
```

The state object returns a DataFrame that is not yet evaluated.

```scala
stateProd("customer_male").show
```

```
+-------+-------+------+
|cust_id|surname|  name|
+-------+-------+------+
|     64|  Jones|George|
|      8|  Smith| James|
+-------+-------+------+
```




### Show Dependencies


```scala
app.getDependencies(state).show
```

```
+-------------+-------------+--------------------+------------+
|Step         |Depends on   |Column              |Defined in  |
+-------------+-------------+--------------------+------------+
|customer_male|customer     |cust_id             |            |
|customer_male|customer     |surname             |            |
|customer_male|customer     |name                |            |
|customer_male|customer     |gender              |            |
|count_male   |customer_male|<no explicit column>|CustomerMale|
+-------------+-------------+--------------------+------------+
```



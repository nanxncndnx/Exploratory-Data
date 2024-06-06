# Exploratory-Data
### Analyse your Data With **PySpark**

 - [Detect Missing Values](./DetectMissingValues/DMV.py)
 - [PySpark Lazy Evaluation](./PysparkLazyEvaluation/PLE.py)
 - [PySpark UDF Registring](./PysparkUdfRegistering/PUR.py)
 - [Unstack PySpark DataFrame](./UnstackPysparkDataframe/UPD.py)
 - [Convert row objects to Spark Resilient Distributed Dataset (RDD)](./RDD/RDD.py)

## Detect Missing Values

 - Detect **Abnormal Zeroes**
 - For string columns, we check for **None** and **Null**
 - For numeric columns, we check for **Zeroes** and **NaN**
 - For array type columns, we check if the array contain **Zeroes** or **NaN**

#### Calculating Total of the Workout Records and Gender Segregation and Counting Their Activity :

<p align="center" width="100%">
  <br>
  <img width = "68%" src="./DetectMissingValues/result.png">
  <br>
  <br>
</p>
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
  <img width = "84%" src="./DetectMissingValues/result.png">
  <br>
  <br>
</p>

## PySpark Lazy Evaluation

 - Finding Top 5 **Sports** that have the Most **Users**
 - Checking **Activities** by **Gender**

<p align="center" width="100%">
  <br>
  <img width = "75%" src="./PysparkLazyEvaluation/result.png">
  <br>
  <br>
</p>

## UNSTACK PYSPARK DATAFRAME

 - Top **Activities** By Gender
 - How many **People** **Participated** in more than 1 **Sport ?**
 - Number of **Users** having more than **10 Workouts**

<p align="center" width="100%">
  <br>
  <img width = "75%" src="./UnstackPysparkDataframe/result.png">
  <br>
  <br>
</p>  
## 1.Extract the zip file and store in a folder.
## 2.Keep the python scripts also in the same directory
## 3.First run simulate.py which creates queue.json 
    python3 simulate.py
## 4.Then run round_robin.py which creates distribution
    python3 round_robin.py
## 5.Distibution should have two folders 
     (i) Spark_Module_A 
     (ii) Spark_Module_B
## 6.Submit the spark job 
    spark-submit --master local[*] round_robin_distribute.py
## 7.Spark_Modules folders would contain
    For example:
    distribution/Spark_Module_A/part-00000-xxxx.json
    distribution/Spark_Module_B/part-00000-xxxx.json

%SPARK_HOME%\bin\spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin\Debug\netcoreapp3.1\microsoft-spark-2-4_2.11-1.0.0.jar dotnet bin\Debug\netcoreapp3.1\BigDataApp.dll

nc -vv -l -p 9999
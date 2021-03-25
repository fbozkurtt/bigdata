using System;
using Microsoft.Spark.Sql;
using BigDataAppML.Model;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark;

namespace BigDataApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = "localhost";
            var port = 9999;
            
            SparkSession spark = SparkSession
                .Builder()
                .AppName("Emotion_Prediction")
                .GetOrCreate();
            DataFrame lines = spark
                .ReadStream()
                .Format("socket")
                .Option("host", host)
                .Option("port", port)
                .Load();

            Func<Column, Column> udfArray =
                Udf<string, string[]>((str) => new string[] { str, " => " + Predict(str) });

            DataFrame arrayDf = lines.Select(Explode(udfArray(lines["value"])));

            StreamingQuery query = arrayDf
                .WriteStream()
                .Format("console")
                .Start();

            query.AwaitTermination();
        }

        private static string Predict(string text)
        {
            ModelInput sampleData = new ModelInput()
            {
                Col0 = text,
            };
            var predictionResult = ConsumeModel.Predict(sampleData);
            return predictionResult.Prediction;
        }
    }
}

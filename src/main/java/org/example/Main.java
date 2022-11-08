package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.event.Level;

import javax.xml.crypto.Data;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

//        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("aa").master("local[*]")
                .getOrCreate();

        Dataset<Row> df_calendar = spark.read()
                .option("header", true)
                .csv("/home/rohitg/Downloads/java_work/sparkJava/src/main/resources/Data/sample/calendar/Calendar_Base.csv");

       df_calendar = df_calendar
                .withColumn("schoolReference",to_json(struct("schoolId")))
                .withColumn("schoolYearTypeReference", to_json(struct("schoolYear")))
               .withColumn("payload",
                       to_json(struct("schoolReference",
                               "schoolYearTypeReference"
                               )));

       df_calendar = df_calendar
               .select("Operation","calendarCode","schoolId","payload");
//                       (String) to_json(struct("schoolYear")).alias("payload"));



//                                "calendarCode","calendarTypeDescriptor")).alias("payload")
//                                );
//                ));

        df_calendar.show(false);
//        df_calendar.createOrReplaceTempView("my_students_table");

        Dataset<Row> results = spark.sql("SELECT * FROM my_students_table");

        results.show();
        spark.close();


//

    }
}
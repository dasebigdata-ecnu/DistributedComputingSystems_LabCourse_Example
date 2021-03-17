package cn.edu.ecnu.sparkstreaming.example.java.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1 创建 JavaStreamingContext
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountJava");
//        sparkConf.setMaster("local[2]"); // 仅用于本地调试, 放集群中运行时删除本行
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Durations.seconds(10)); // 处理间隔为 10s
        sc.sparkContext().setLogLevel("ERROR");

        // 2 处理数据
        JavaPairDStream<String, Integer> wordCount = sc
                // 接收 socket 流数据, 其中 args[0] 为 IP 地址, args[1] 为端口号
                .socketTextStream(args[0], Integer.parseInt(args[1]))
                // 将接收到的字符串按空格分割
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator())
                // 映射单词为 (word, 1)
                .mapToPair((String word) -> new Tuple2<>(word, 1))
                // 窗口操作, 窗口长度是 20s, 滑动时间间隔是 10s, 即每隔 10s 统计前 20s 的单词
                .reduceByKeyAndWindow(
                        (Integer v1, Integer v2) -> v1 + v2,
                        Durations.seconds(20),
                        Durations.seconds(10)
                );

        // 3 显示结果
        wordCount.print();

        // 4 将结果输出到文件中, 其中 args[2] 为输出路径
        wordCount.saveAsNewAPIHadoopFiles(
                args[2] + "/stream", "output", Text.class, IntWritable.class, TextOutputFormat.class
        );

        // 5 开启StreamingContext
        sc.start();
        sc.awaitTermination();
    }

}

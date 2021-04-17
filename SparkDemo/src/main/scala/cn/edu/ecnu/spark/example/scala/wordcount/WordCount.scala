package cn.edu.ecnu.spark.example.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def run(args: Array[String]): Unit = {
    /* 步骤1：通过SparkConf设置配置信息，并创建SparkContext */
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local") // 仅用于本地进行调试，如在集群中运行则删除本行
    val sc = new SparkContext(conf)

    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    // 读入文本数据，创建名为lines的RDD
    val lines = sc.textFile(args(0))
    // 将lines中的每一个文本行按空格分割成单个单词
    val words = lines.flatMap { line => line.split(" ") }
    // 将每个单词的频数设置为1，即将每个单词映射为[单词, 1]
    val pairs = words.map { word => (word, 1) }

    // 按单词聚合，并对相同单词的频数使用sum进行累计
    val wordCounts = pairs.groupByKey().map(t => (t._1, t._2.sum))
    // 如需使用合并机制则将第上一行替换为下行
    // val wordCounts = pairs.reduceByKey(_+_)

    // 输出词频统计结果到文件
    wordCounts.saveAsTextFile(args(1))

    /* 步骤3：关闭SparkContext */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}

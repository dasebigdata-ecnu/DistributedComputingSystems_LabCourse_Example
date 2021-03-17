package cn.edu.ecnu.storm.example.java.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(-1);
            return;
        }
        /* 步骤1：构建拓扑 */
        TopologyBuilder builder = new TopologyBuilder();
        // 设置 Spout 的名称为 "SPOUT"， executor 数量为 1，任务数量为 1
        builder.setSpout("Spout", new SocketSpout(args[1], args[2]), 1);
        // 设置 Bolt 的名称为 "SPLIT"， executor 数量为 2，任务数量为 2，与 "SPOUT" 之间的流分组策略为随机分组
        builder.setBolt("split", new SplitBolt(), 2).setNumTasks(2).shuffleGrouping("Spout");
        // 设置 Bolt 取名 "COUNT"，executor 数量为 2，任务数量为 2，订阅策略为 fieldsGrouping
        builder.setBolt("count", new CountBolt(), 2).fieldsGrouping("split", new Fields("word"));

        /* 步骤2：设置配置信息 */
        Config conf = new Config();
        conf.setDebug(false); // 关闭调试模式
        conf.setNumWorkers(2); // 设置 Worker 数量为 2
        conf.setNumAckers(0); // 设置 Acker 数量为 0

        /* 步骤3：指定程序运行的方式 */
        if (args[0].equals("cluster")) { // 在集群运行程序，拓扑的名称为 WORDCOUNT
            StormSubmitter.submitTopology("WORDCOUNT", conf, builder.createTopology());
        } else if (args[0].equals("local")) { // 在本地 IDE 调试程序，拓扑的名称为 WORDCOUNT
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
        } else {
            System.exit(-2);
        }
    }
}
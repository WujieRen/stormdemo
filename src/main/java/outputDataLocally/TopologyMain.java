package outputDataLocally;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import outputDataLocally.bolt.SuffixBolt;
import outputDataLocally.bolt.UpperBolt;
import outputDataLocally.spout.RandomWordSpout;

/**
 * Created by renwujie on 2017/12/28 at 20:02
 */
public class TopologyMain {

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        //parallelism_hint ：4  表示用4个excutor来执行这个组件
        //setNumTasks(8) 设置的是该组件执行时的并发task数量，也就意味着1个excutor会运行2个task
        builder.setSpout("wordRecieverSpout", new RandomWordSpout(), 4).setNumTasks(8);
        builder.setBolt("toUpperBolt", new UpperBolt(), 4).shuffleGrouping("wordRecieverSpout");
        builder.setBolt("addSuffix", new SuffixBolt()).shuffleGrouping("toUpperBolt");
        //builder.setBolt("addSuffix", new SuffixBolt(), 4).shuffleGrouping("toUpperBolt");

        Config config = new Config();
        config.setNumWorkers(4);
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("outputWordLocal", config, builder.createTopology());
        //cluster.shutdown();
    }
}

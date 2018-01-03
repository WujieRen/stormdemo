package w3c.grouping.countword3.anotherExamSearch.firstExam;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by renwujie on 2018/01/03 at 15:37
 */
public class Topology {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sendMessage", new WordReader(), 2);
        builder.setBolt("wordCount", new WordCounter(), 3).customGrouping("sendMessage", new MyFirstStreamGrouping());

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testSelfPartitionGrouping", config, builder.createTopology());

    }

}

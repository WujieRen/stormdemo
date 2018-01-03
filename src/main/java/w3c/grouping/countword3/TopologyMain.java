package w3c.grouping.countword3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import w3c.grouping.countword3.bolts.WordCounter;
import w3c.grouping.countword3.bolts.WordNormalizer;
import w3c.grouping.countword3.selfGroup.CustomShuffleGrouping;
import w3c.grouping.countword3.spouts.WordReader;

/**
 * 自定义数据流组：
 * 	CustomShuffleGrouping
 *
 * 	测试结果来看啥都没有，好像不太对，暂时没弄明白哪里有问题
 *
 * 	这个例子不好
 */
public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
        
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("word-reader",new WordReader());

		builder.setBolt("word-normalizer", new WordNormalizer()).customGrouping("word-reader", new CustomShuffleGrouping());

		builder.setBolt("word-counter", new WordCounter(),2)
			.fieldsGrouping("word-normalizer",new Fields("word"));


        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", "data\\w3c\\getstart\\input\\word.txt");
		conf.setDebug(true);

        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
	}
}

package w3c.grouping.countword1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import w3c.grouping.countword1.bolts.WordCounter;
import w3c.grouping.countword1.bolts.WordNormalizer;
import w3c.grouping.countword1.bolts.WordWriter;
import w3c.grouping.countword1.spouts.WordReader;

/**
 * 域数据流组
 */
public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {

        /**
         * 再builder中如果设置为2个 task 来 execute 该Bolt，最终结果会翻倍
         */
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter())
			.fieldsGrouping("word-normalizer",new Fields("word"));
		builder.setBolt("word-writer", new WordWriter()).globalGrouping("word-counter");

        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", "data\\w3c\\getstart\\input\\word.txt");
		conf.setDebug(true);
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        //Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		//cluster.shutdown();
	}
}

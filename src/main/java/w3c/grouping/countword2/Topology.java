package w3c.grouping.countword2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import w3c.grouping.countword2.bolts.WordCounter;
import w3c.grouping.countword2.bolts.WordNormalizer;
import w3c.grouping.countword2.spouts.SignalsSpout;
import w3c.grouping.countword2.spouts.WordReader;

/**
 * Created by renwujie on 2017/12/08 at 17:56
 *
 * 全局数据流组,为每个接收数据的实例复制一份元组副本。
 * 这种分组方式用于向 bolts 发送信号。
 *      比如，要刷新缓存，可以向所有的 bolts 发送一个刷新缓存信号。
 *          在单词计数器的例子里，可以使用一个全部数据流组，添加清除计数器缓存的功能.
 */
public class Topology {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-reader",new WordReader());
        builder.setSpout("signals-spout", new SignalsSpout());

        builder.setBolt("word-normalizer", new WordNormalizer(), 2).shuffleGrouping("word-reader");
        /**
         * 将sifnalsSpout发射到WordCounterBolt下的所有task。
         */
        builder.setBolt("word-counter", new WordCounter(),2)
                .fieldsGrouping("word-normalizer",new Fields("word"))
                .allGrouping("signals-spout","signals");

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", "data\\w3c\\getstart\\input\\word.txt");
        conf.setDebug(true);

        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}

package w3c.getstart;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import w3c.getstart.bolt.WordCounter_version2;
import w3c.getstart.bolt.WordNormalizer;
import w3c.getstart.bolt.WordWriter;
import w3c.getstart.spout.WordReader;

/**
 * Created by renwujie on 2017/12/28 at 19:08
 *
 * 加了一个词后有9个词就成了9次，问题还是很大。
 *  最后发现是WordCounter emit() 时，for()循环本身已经能够解决问题了，我又在外面多加了一层for循环，导致有几个不同的词就发送了几次。
 */
public class WordCountTopology1 {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordReader", new WordReader());
        //builder.setBolt("wordFormat", new WordNormalizer()).shuffleGrouping("wordReader");
        //TODO：以为生成8变收到这里影响，于是用了域数据流组。结果还是8个。
        builder.setBolt("wordFormat", new WordNormalizer()).fieldsGrouping("wordReader", new Fields("line"));
        //builder.setBolt("wordCount", new WordCounter(), 3).shuffleGrouping("wordFormat");
        //builder.setBolt("wordCount", new WordCounter()).fieldsGrouping("wordFormat", new Fields("word"));
        //builder.setBolt("wordCount", new WordCounter_version2()).fieldsGrouping("wordFormat", new Fields("word"));
        builder.setBolt("wordCount", new WordCounter_version2()).shuffleGrouping("wordFormat");
        builder.setBolt("wordWriter", new WordWriter()).globalGrouping("wordCount");

        Config conf = new Config();
        conf.put("wordFile", "data\\getstart\\input\\word.txt");
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        Thread.sleep(1000);
        //cluster.shutdown();
    }
}

package w3c.topN;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import w3c.topN.Spout.WordReader;
import w3c.topN.bolt.WordCounter;
import w3c.topN.bolt.WordWriter;

/**
 * Created by renwujie on 2018/01/02 at 10:52
 *
 * Reference:
 *  http://blog.itpub.net/28912557/viewspace-1579860/
 *
 *
 * 新东西：
 *  AtomicInteger:
 *      http://blog.csdn.net/u012734441/article/details/51619751
 *  ConcurrentHashMap:
 *      http://www.importnew.com/18126.html
 *
 *  volatile 关键字:
 *      http://www.importnew.com/18126.html
 *
 *  Strom中IBasicBolt与IRichBolt的区别:
 *      http://blog.csdn.net/ch717828/article/details/52561904
 *      http://blog.csdn.net/c_sdnq2451q/article/details/51452366
 *      http://blog.sina.com.cn/s/blog_40d46ec20102xbpl.html
 *
 */
public class WordTopN {
    public static void main(String[] args){
        if(args == null || args.length == 1){
            System.err.println("Usage: N");
            System.err.println("such as : 10");
            System.exit(-1);
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordreader", new WordReader(), 2);
        //fieldsGrouping()——按照字段分组，这一点很重要，它能保证相同的word被分发到同一个bolt上。像做wordcount、TopN之类的应用就要使用这种分组策略
        builder.setBolt("wordcounter", new WordCounter(), 2).fieldsGrouping("wordreader", new Fields("word"));
        //全局分组，tuple会被分配到一个bolt用来汇总。
        builder.setBolt("wordwriter", new WordWriter()).globalGrouping("wordcounter");


        Config config = new Config();
        config.put("N", "20");
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCountTopN", config, builder.createTopology());


    }
}

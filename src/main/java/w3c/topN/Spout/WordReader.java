package w3c.topN.Spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by renwujie on 2018/01/02 at 11:02
 */
public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private static String[] words = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    //spout的作用是随机发送word，发送100次，由于并行度是2，将产生2个spout实例，所以这里的计数器使用了static的AtomicInteger来保证线程安全。
    private static AtomicInteger i = new AtomicInteger();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if(i.intValue() < 100){
            Random random = new Random();
            String word = words[random.nextInt(words.length)];
            collector.emit(new Values(word));
            i.incrementAndGet();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


}

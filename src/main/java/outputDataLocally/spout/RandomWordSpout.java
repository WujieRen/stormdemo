package outputDataLocally.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by renwujie on 2017/12/28 at 20:03
 */
public class RandomWordSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    //模拟一些数据
    String[] words = {"iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        Random random = new Random();
        int index = random.nextInt(words.length);
        String goodName = words[index];
        collector.emit(new Values(goodName));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orignname"));
    }
}

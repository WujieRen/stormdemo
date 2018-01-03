package w3c.grouping.countword2.spouts;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by renwujie on 2017/12/08 at 17:59
 */
public class SignalsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        /**
         * 我什么要sleep???
         */
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        collector.emit("signals", new Values("refreshCache"));
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("signals", new Fields("action"));
    }
}

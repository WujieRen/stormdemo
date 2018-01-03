package w3c.grouping.countword3.anotherExamSearch;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by renwujie on 2018/01/03 at 15:21
 */
public class ModGroupingTest {
    public static class TestUidSpout extends BaseRichSpout {
        boolean _isDistributed;
        SpoutOutputCollector _collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        public void close() {

        }

        public void nextTuple() {
            Utils.sleep(100);

            final Random rand = new Random();
            final int uid =rand.nextInt(100000000);

            _collector.emit(new Values(uid));

        }

        public void ack(Object msgId) {

        }

        public void fail(Object msgId) {

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("uid"));
        }


    }

    public static class modGroupBolt extends BaseRichBolt {
        OutputCollector _collector;
        String _ComponentId;
        int _TaskId;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

            _collector = collector;
            _ComponentId = context.getThisComponentId();
            _TaskId = context.getThisTaskId();
        }

        @Override
        public void execute(Tuple tuple) {
//            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            System.out.println(_ComponentId+":"+_TaskId +"recevie :" + tuple.getInteger(0));

            _collector.emit(new Values(tuple));
            _collector.ack(tuple);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("uid"));
        }


    }

    public static void main(String args[]){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("uid", new TestUidSpout());
        builder.setBolt("process", new modGroupBolt(), 10).customGrouping("uid", new ModStreamGrouping());

        Config config = new Config();
        config.setDebug(true);

        config.setNumWorkers(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
    }
}

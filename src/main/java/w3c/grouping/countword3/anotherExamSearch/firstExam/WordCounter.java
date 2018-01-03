package w3c.grouping.countword3.anotherExamSearch.firstExam;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renwujie on 2018/01/02 at 11:01
 */
public class WordCounter implements IRichBolt {

    private static Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        if (counters.containsKey(word)) {
            Integer c = counters.get(word) + 1;
            counters.put(word, c);
        } else {
            counters.put(word, 1);
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
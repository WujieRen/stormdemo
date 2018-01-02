package w3c.topN.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renwujie on 2018/01/02 at 11:01
 */
public class WordCounter implements IRichBolt {

    private static Map<String, Integer> counters = new ConcurrentHashMap<String, Integer>();
    private volatile boolean edit = true;
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    //5秒后counter不再变化，可以认为spout已经发送完
                    if (!edit) {
                        if (counters.size() > 0) {
                            java.util.List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>();
                            list.addAll(counters.entrySet());
                            Collections.sort(list, new ValueComparator());

                            //向下个Bolt发送前N个word
                            for (int i = 0; i < list.size(); i++) {
                                if (i < Integer.valueOf(stormConf.get("N").toString())) {
                                    collector.emit(new Values(list.get(i).getKey() + " : " + list.get(i).getValue()));
                                }
                            }
                        }
                        //发送之后，清空counters，以防spout再次发送word过来
                        counters.clear();
                    }

                    edit = false;
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
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

        edit = true;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word_count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static class ValueComparator implements Comparator<Map.Entry<String, Integer>> {

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue() - o2.getValue();
        }
    }

}
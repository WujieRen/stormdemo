package w3c.grouping.countword1.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class WordCounter extends BaseRichBolt {

    Map<String, Integer> counters;
    private OutputCollector collector;
    private boolean edit = true;

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;

        new Thread(new Runnable() {
            @Override
            public void run() {

                while(true){
                    if(!edit){
                        if(counters.size() > 0){
                            java.util.List<Map.Entry<String, Integer>> list = new ArrayList<>();
                            list.addAll(counters.entrySet());
                            Collections.sort(list, new ValuesComparator());

                            for(int i = 0; i < list.size(); i++){
                                collector.emit(new Values(list.get(i).getKey() + " : " + list.get(i).getValue()));
                            }

                            //counters.clear();
                        }
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

    /**
     * On each word We will count
     */
    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        edit = true;

        //Set the tuple as Acknowledge
        collector.ack(input);
    }


    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     */
    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordCount"));
    }

    private static class ValuesComparator implements Comparator<Map.Entry<String, Integer>>{
        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue() - o2.getValue();
        }
    }
}

package w3c.getstart.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renwujie on 2018/01/02 at 17:37
 * <p>
 * 通过  if (mapCounters.size > 0) {执行}  来控制不要让Storm重复发送，但是因为
 */
public class WordCounter_version2 implements IRichBolt {

    //TODO：其实没有必要担心线程安全的问题，记得官网好像又说。storm内部实现已经确保了线程安全
    //private static Map<String, Integer> mapCounter = new tHashMap<String, Integer>();

    private static Map<String, Integer> mapCounter = new ConcurrentHashMap<String, Integer>();
    private volatile boolean edit = true;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
       /* try {
            this.fileWriter = new FileWriter(new File("data\\getstart\\output\\wordcount.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        this.collector = collector;
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    if (!edit) {
                        if (mapCounter.size() > 0) {
                            //排序
                            Map<String, Integer> tmpMap = new TreeMap<>(mapCounter);

                            //发送
                            for (String key : tmpMap.keySet()) {
                                collector.emit(new Values(key + " : " + tmpMap.get(key)));
                            }
                        }
                        mapCounter.clear();
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
        String str = input.getString(0);
        if (!mapCounter.containsKey(str)) {
            mapCounter.put(str, 1);
        } else {
            Integer c = mapCounter.get(str) + 1;
            mapCounter.put(str, c);
        }

        //写到本地
        //TODO:这个有问题，重复写了，而且不能正确统计单词数目
       /* for(Map.Entry<String, Integer> entry : mapCounter.entrySet()){
            try {
                fileWriter.write(entry.getKey() + " : " + entry.getValue() + "\n");
                fileWriter.flush();
            } catch (IOException e) {
                System.out.println("写出数据到本地出错！");
            }
        }*/

        //对元组作为应答
        collector.ack(input);

        edit = true;
    }

    /**
     * 进群关闭时现实单词数量
     */
    @Override
    public void cleanup() {
/*
        if(fileWriter != null){
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordCounter"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static class ValuesComparator implements Comparator<Map.Entry<String, Integer>> {

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue() - o2.getValue();
        }
    }
}

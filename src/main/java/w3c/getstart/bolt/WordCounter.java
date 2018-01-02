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
 * Created by renwujie on 2018/01/02 at 10:18
 */
public class WordCounter implements IRichBolt{

    //private FileWriter fileWriter;
    /**
     * 线程安全的容器ConcurrentHashMap，来存储word以及对应的次数。在prepare方法里启动一个线程，长期监听edit的状态，监听间隔是5秒，
     当edit为false，即execute方法不再执行、容器不再变化，可以认为spout已经发送完毕了，可以开始排序取TopN了。这里使用了一个volatile edit（回忆一下volatile的使用场景：
     对变量的修改不依赖变量当前的值，这里设置true or false，显然不相互依赖）
     */
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
               while(true){

                   if(!edit){
                       Map<String, Integer> tmpMap = new TreeMap<>(mapCounter);

                       if(tmpMap.size() > 0){
                           for(int i= 0; i < tmpMap.size(); i++){
                               for(String key : tmpMap.keySet()){
                                   collector.emit(new Values(key + " : " + tmpMap.get(key)));
                               }
                           }
                       }
                       tmpMap.clear();
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
        if(!mapCounter.containsKey(str)){
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

    private static class ValuesComparator implements Comparator<Map.Entry<String, Integer>>{

        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue() - o2.getValue();
        }
    }

}

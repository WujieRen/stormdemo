package w3c.getstart.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by renwujie on 2017/12/28 at 18:04
 */
public class WordReader implements IRichSpout{

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;

    public boolean isDistributed(){
        return completed;
    }

    //创建一个文件并维持一个collector对象
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;

        try {
            this.fileReader = new FileReader(conf.get("wordFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
    }


    /**
     * nextTuple() 会在同一个循环内被 ack() 和 fail() 周期性的调用。没有任务时它必须释放对线程的控制，其它方法才有机会得以执行。因此 nextTuple 的第一行就要检查是否已处理完成。如果完成了，为了降低处理器负载，会在返回前休眠一毫秒。如果任务完成了，文件中的每一行都已被读出并分发了。
     */
    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        String str;

        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while((str = reader.readLine()) != null){
                this.collector.emit(new Values(str), str);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading tuple",e);
        } finally {
            completed = true;
        }

    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

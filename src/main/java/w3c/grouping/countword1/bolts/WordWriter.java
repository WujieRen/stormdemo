package w3c.grouping.countword1.bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by renwujie on 2018/01/03 at 10:33
 */
public class WordWriter extends BaseBasicBolt {

    private FileWriter fileWriter;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter = new FileWriter(new File("data/w3c/grouping/countword1/output/" + this));
        } catch (IOException e) {
            System.out.println("写出到本地错误!!!");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String s = input.getString(0);
        try {
            fileWriter.write(s);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //writer不能close，因为execute需要一直运行
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

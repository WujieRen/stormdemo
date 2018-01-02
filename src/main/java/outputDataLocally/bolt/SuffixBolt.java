package outputDataLocally.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by renwujie on 2017/12/28 at 20:14
 */
public class SuffixBolt extends BaseBasicBolt {

    private FileWriter fileWriter = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            this.fileWriter = new FileWriter(new File("W:\\output" + UUID.randomUUID()) + ".txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String upperName = input.getString(0);
        String suffix_name = upperName + "lalala";
        try {
            fileWriter.write(suffix_name + "\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        if(fileWriter == null){
            try {
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

package w3c.grouping.countword3.selfGroup;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2017/12/08 at 18:43
 */
public class CustomShuffleGrouping implements CustomStreamGrouping,Serializable {

    int numTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        numTasks = targetTasks.size();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        if(values.size() > 0){
            String str = values.get(0).toString();
            if(str.isEmpty()){
                boltIds.add(0);
            } else {
                boltIds.add(str.charAt(0) % 1);
            }
        }
        return boltIds;
    }
}

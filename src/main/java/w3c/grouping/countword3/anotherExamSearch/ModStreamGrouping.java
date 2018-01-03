package w3c.grouping.countword3.anotherExamSearch;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by renwujie on 2018/01/03 at 15:15
 *
 * 我们现在业务中遇到一个问题想让用户的uid按照分段的规则grouping到对应的task上面，于是采用uid%k的方法将相同模值的记录在一个task进行业务处理，自己实现了ModStreamingGrouping，代码如下：
 */
public class ModStreamGrouping implements CustomStreamGrouping {

    private Map _map;
    private List<Integer> _targetTasks;

    public ModStreamGrouping(){

    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        // TODO Auto-generated method stub
        Long groupingKey = Long.valueOf( values.get(0).toString());
        int index = (int) (groupingKey%(_targetTasks.size()));
        return Arrays.asList(_targetTasks.get(index));
    }
}

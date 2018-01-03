package w3c.grouping.countword3.anotherExamSearch.firstExam;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Arrays;

/**
 * Created by renwujie on 2018/01/03 at 15:29
 */
public class MyFirstStreamGrouping implements CustomStreamGrouping {

    private java.util.List<Integer> tasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, java.util.List<Integer> targetTasks) {
        this.tasks = targetTasks;
    }
    @Override
    public java.util.List<Integer> chooseTasks(int taskId, java.util.List<Object> values) {
        return Arrays.asList(tasks.get(0));
    }
}

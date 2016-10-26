package com.mesosphere.dcos.kafka.offer;

import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.state.StateStore;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Strategy that separates the tasks to separate nodes.
 */
public class NodePlacementStrategy {
    public static List<String> getAgentsToAvoid(StateStore stateStore, TaskInfo thisTaskInfo) {
        return stateStore.fetchTasks().stream()
                .filter(taskInfo -> !taskInfo.getName().equals(thisTaskInfo.getName()))
                .map(taskInfo -> taskInfo.getSlaveId().getValue())
                .collect(Collectors.toList());
    }
}

package com.mesosphere.dcos.kafka.offer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.constrain.AgentRule;
import org.apache.mesos.offer.constrain.PlacementRule;
import org.apache.mesos.offer.constrain.PlacementRuleGenerator;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PlacementStrategyManager {
    private static final Log log = LogFactory.getLog(PlacementStrategyManager.class);

    public static Optional<PlacementRuleGenerator> getPlacementStrategy(
            String placementStrategy,
            Protos.TaskInfo taskInfo) {
        log.info("Using placement strategy: " + placementStrategy);

        switch (placementStrategy) {
            case "ANY":
                log.info("Returning ANY strategy");
                return Optional.empty();
            case "NODE":
                log.info("Returning NODE strategy");
                return Optional.of(new AvoidAgentPlacementRuleGenerator(taskInfo));
            default:
                log.info("Returning DEFAULT strategy");
                return Optional.of(new AvoidAgentPlacementRuleGenerator(taskInfo));
        }
    }

    public static class AvoidAgentPlacementRuleGenerator implements PlacementRuleGenerator {

        private final Protos.TaskInfo taskInfo;

        public AvoidAgentPlacementRuleGenerator(Protos.TaskInfo taskInfo) {
            this.taskInfo = taskInfo;
        }

        @Override
        public PlacementRule generate(Collection<Protos.TaskInfo> tasks) {
             List<String> avoidAgents = tasks.stream()
                    .filter(taskInfo -> !taskInfo.getName().equals(this.taskInfo.getName()))
                    .map(taskInfo -> taskInfo.getSlaveId().getValue())
                    .collect(Collectors.toList());

            log.info(String.format("For '%s' avoiding agents: %s", taskInfo.getName(), avoidAgents));
            return new AgentRule.AvoidAgentsGenerator(avoidAgents).generate(tasks);
        }
    }
}

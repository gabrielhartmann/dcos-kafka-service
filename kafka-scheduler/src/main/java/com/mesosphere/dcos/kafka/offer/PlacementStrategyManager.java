package com.mesosphere.dcos.kafka.offer;

import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.constrain.PlacementRuleGenerator;
import org.apache.mesos.offer.constrain.PlacementUtils;
import org.apache.mesos.state.StateStore;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class PlacementStrategyManager {
    private static final Log log = LogFactory.getLog(PlacementStrategyManager.class);
    private final StateStore stateStore;

    PlacementStrategyManager(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public Optional<PlacementRuleGenerator> getPlacementStrategy(
            KafkaSchedulerConfiguration config,
            Protos.TaskInfo taskInfo) {
        String placementStrategy = config.getServiceConfiguration().getPlacementStrategy();

        log.info("Using placement strategy: " + placementStrategy);

        switch (placementStrategy) {
            case "ANY":
                log.info("Returning ANY strategy");
                return Optional.empty();
            case "NODE":
                log.info("Returning NODE strategy");
                log.info(String.format("Current tasks for Strategy: %s",
                        stateStore.fetchTasks().stream()
                                .map(ti -> ti.getName()).collect(Collectors.toList())));
                List<String> avoidAgents = NodePlacementStrategy.getAgentsToAvoid(stateStore, taskInfo);
                log.info(String.format("For '%s' avoiding agents: %s", taskInfo.getName(), avoidAgents));

                return PlacementUtils.getAgentPlacementRule(
                        avoidAgents,
                        Collections.emptyList());
            default:
                log.info("Returning DEFAULT strategy");
                return Optional.empty();
        }
    }
}

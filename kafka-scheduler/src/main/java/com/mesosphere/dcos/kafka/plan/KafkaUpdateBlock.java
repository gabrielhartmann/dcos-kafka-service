package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.DefaultBlock;
import org.apache.mesos.scheduler.plan.Status;
import org.apache.mesos.state.StateStoreException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KafkaUpdateBlock extends DefaultBlock {
    private static final Log LOGGER = LogFactory.getLog(KafkaUpdateBlock.class);
    private final FrameworkState state;
    private final KafkaOfferRequirementProvider offerRequirementProvider;
    private final String targetConfigName;
    private final int brokerId;

    public static KafkaUpdateBlock create(
            FrameworkState state,
            KafkaOfferRequirementProvider offerRequirementProvider,
            String targetConfigName,
            int brokerId) throws InvalidRequirementException, IOException, URISyntaxException {

        String brokerName = OfferUtils.brokerIdToTaskName(brokerId);

        TaskInfo taskInfo = fetchTaskInfo(state, brokerId);
        Optional<OfferRequirement> offerRequirementOptional = Optional.of(
                getOfferRequirement(
                        offerRequirementProvider,
                        taskInfo,
                        targetConfigName,
                        brokerId));

        return new KafkaUpdateBlock(
                brokerName,
                offerRequirementOptional,
                initializeStatus(brokerName, taskInfo, targetConfigName),
                Collections.emptyList(),
                state,
                offerRequirementProvider,
                targetConfigName,
                brokerId);
    }

    private KafkaUpdateBlock(
            String name,
            Optional<OfferRequirement> offerRequirementOptional,
            Status status,
            List<String> errors,
            FrameworkState state,
            KafkaOfferRequirementProvider offerRequirementProvider,
            String targetConfigName,
            int brokerId) {
        super(name, offerRequirementOptional, status, errors);
        this.state = state;
        this.offerRequirementProvider = offerRequirementProvider;
        this.targetConfigName = targetConfigName;
        this.brokerId = brokerId;
    }

    @Override
    public Optional<OfferRequirement> start() {
        TaskInfo taskInfo = fetchTaskInfo(state, brokerId);
        try {
            return Optional.of(
                    getOfferRequirement(
                            offerRequirementProvider,
                            taskInfo,
                            targetConfigName,
                            brokerId));
        } catch (IOException | InvalidRequirementException | URISyntaxException e) {
            LOGGER.error("Failed to generate OfferRequirement", e);
            return Optional.empty();
        }
    }

    private static Status initializeStatus(String brokerName, TaskInfo taskInfo, String targetConfigName) {
        LOGGER.info("Setting initial status for: " + brokerName);
        Status status = Status.PENDING;

        if (taskInfo != null) {
            String configName = OfferUtils.getConfigName(taskInfo);
            LOGGER.info("TargetConfigName: " + targetConfigName + " currentConfigName: " + configName);
            if (configName.equals(targetConfigName)) {
                status = Status.COMPLETE;
            }
        }

        LOGGER.info("Status initialized as " + status + " for block: " + brokerName);
        return status;
    }

    private static OfferRequirement getOfferRequirement(
            KafkaOfferRequirementProvider offerReqProvider,
            TaskInfo taskInfo,
            String targetConfigName,
            int brokerId) throws IOException, InvalidRequirementException, URISyntaxException {

        if (taskInfo == null) {
            return offerReqProvider.getNewOfferRequirement(targetConfigName, brokerId);
        } else {
            return offerReqProvider.getUpdateOfferRequirement(targetConfigName, taskInfo);
        }
    }

    private static TaskInfo fetchTaskInfo(FrameworkState state, int brokerId) {
        try {
            Optional<TaskInfo> taskInfoOptional = Optional.empty();
            try {
                taskInfoOptional = state.getStateStore().fetchTask(OfferUtils.brokerIdToTaskName(brokerId));
            } catch (StateStoreException e) {
                LOGGER.warn(String.format(
                        "Failed to get TaskInfo for broker %d. This is expected when the service is "
                                + "starting for the first time.", brokerId), e);
            }

            if (taskInfoOptional.isPresent()) {
                return taskInfoOptional.get();
            } else {
                LOGGER.warn("TaskInfo not present for broker: " + brokerId);
                return null;
            }
        } catch (Exception ex) {
            LOGGER.error(String.format("Failed to retrieve TaskInfo for broker %d", brokerId), ex);
            return null;
        }
    }
}

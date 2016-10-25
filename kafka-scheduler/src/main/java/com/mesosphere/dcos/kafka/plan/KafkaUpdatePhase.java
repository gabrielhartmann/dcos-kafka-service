package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.scheduler.plan.Block;

import java.util.ArrayList;
import java.util.List;

public class KafkaUpdatePhase {
    private final List<Block> blocks;
    private final String configName;
    private final KafkaSchedulerConfiguration config;

    public KafkaUpdatePhase(
            String targetConfigName,
            KafkaSchedulerConfiguration targetConfig,
            FrameworkState frameworkState,
            KafkaOfferRequirementProvider offerReqProvider) {
        this.configName = targetConfigName;
        this.config = targetConfig;
        this.blocks = createBlocks(configName, config.getServiceConfiguration().getCount(), frameworkState, offerReqProvider);
    }

    public List<Block> getBlocks() {
        return blocks;
    }

    private List<Block> createBlocks(
            String configName,
            int brokerCount,
            FrameworkState frameworkState,
            KafkaOfferRequirementProvider offerReqProvider) {

        List<Block> blocks = new ArrayList<Block>();

        for (int i=0; i<brokerCount; i++) {
            blocks.add(KafkaUpdateBlock.create(frameworkState, offerReqProvider, configName, i));
        }

        return blocks;
    }
}

package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import com.mesosphere.dcos.kafka.config.ServiceConfiguration;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * This tests the construction of Kafka Stages.
 */
public class KafkaStageTest {
    @Mock KafkaSchedulerConfiguration schedulerConfiguration;
    @Mock ServiceConfiguration serviceConfiguration;
    @Mock FrameworkState frameworkState;
    @Mock PersistentOfferRequirementProvider offerRequirementProvider;
    @Mock OfferRequirement offerRequirement;
    @Mock Reconciler reconciler;

    private Plan plan;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(serviceConfiguration.getCount()).thenReturn(3);
        when(schedulerConfiguration.getServiceConfiguration()).thenReturn(serviceConfiguration);
        when(offerRequirementProvider.getNewOfferRequirement(anyString(), anyInt())).thenReturn(offerRequirement);
        when(frameworkState.getTaskInfoForBroker(anyInt())).thenReturn(Optional.empty());
        plan = getTestPlan();
    }

    @Test
    public void testStageConstruction() {
        Assert.assertEquals(2, plan.getChildren().size());
        Assert.assertEquals(1, plan.getChildren().get(0).getChildren().size());
        Assert.assertEquals(3, plan.getChildren().get(1).getChildren().size());
    }

    private Plan getTestPlan() {
        KafkaUpdatePhase updatePhase = new KafkaUpdatePhase(
                "target-config-name",
                schedulerConfiguration,
                frameworkState,
                offerRequirementProvider);
        List<Phase> phases = Arrays.asList(
                ReconciliationPhase.create(reconciler),
                new DefaultPhase("update", updatePhase.getBlocks(), new SerialStrategy<>(), Collections.emptyList()));

        return new DefaultPlan("deploy", phases);
    }
}

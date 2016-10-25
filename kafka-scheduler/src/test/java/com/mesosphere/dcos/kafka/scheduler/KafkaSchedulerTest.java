package com.mesosphere.dcos.kafka.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.kafka.config.DropwizardConfiguration;
import com.mesosphere.dcos.kafka.test.KafkaDropwizardAppRule;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.ResourceHelpers;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.scheduler.SchedulerErrorCode;
import org.junit.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.*;

/**
 * This class tests the KafkaScheduler class.
 */
public class KafkaSchedulerTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    private static DropwizardConfiguration dropwizardConfiguration;
    private static Environment environment;

    private static final String testTaskName = "test-task-name";
    private static final String testFrameworkId = "test-framework-id";

    private static final Protos.TaskID testTaskId = Protos.TaskID.newBuilder()
            .setValue(testTaskName + "__" + UUID.randomUUID())
            .build();

    private static final Protos.TaskInfo testTaskInfo = Protos.TaskInfo.newBuilder()
            .setName(testTaskName)
            .setTaskId(testTaskId)
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("test-agent-id"))
            .build();

    private static final Protos.TaskStatus testTaskStatus = Protos.TaskStatus.newBuilder()
            .setTaskId(testTaskId)
            .setState(Protos.TaskState.TASK_RUNNING)
            .build();

    private static final Protos.MasterInfo masterInfo = Protos.MasterInfo.newBuilder()
            .setId("test-master-id")
            .setIp(100)
            .setPort(0)
            .build();

    private KafkaScheduler kafkaScheduler;
    public static Injector injector;

    @Mock private SchedulerDriver driver;

    @ClassRule
    public static KafkaDropwizardAppRule<DropwizardConfiguration> RULE =
            new KafkaDropwizardAppRule<>(Main.class, ResourceHelpers.resourceFilePath("scheduler.yml"));

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        kafkaScheduler = getTestKafkaScheduler();
        kafkaScheduler.registered(driver, getTestFrameworkId(), masterInfo);
        Assert.assertEquals(
                getTestFrameworkId(),
                kafkaScheduler.getFrameworkState().getStateStore().fetchFrameworkId().get());
    }

    @BeforeClass
    public static void before() {
        final Main main = (Main) RULE.getApplication();
        dropwizardConfiguration = main.getDropwizardConfiguration();
        environment = main.getEnvironment();
        injector = Guice.createInjector(
                new TestModule(
                        dropwizardConfiguration.getSchedulerConfiguration(),
                        environment));
    }

    @Test
    public void testResourceOffersEmpty() {
        kafkaScheduler.resourceOffers(driver, Collections.emptyList());
        verify(driver, times(1)).reconcileTasks(anyObject());
    }

    @Test
    public void testStatusUpdateRevive() throws Exception {
        kafkaScheduler.statusUpdate(driver, getTestTaskStatus());
        verify(driver, atLeastOnce()).reviveOffers();
    }

    @Test
    public void testStatusUpdate() {
        kafkaScheduler.statusUpdate(driver, getTestTaskStatus());
    }

    @Test
    public void testReregistered() {
        exit.expectSystemExitWithStatus(SchedulerErrorCode.RE_REGISTRATION.ordinal());
        kafkaScheduler.reregistered(driver, null);
    }

    private KafkaScheduler getTestKafkaScheduler() throws Exception {
        return new KafkaScheduler(dropwizardConfiguration.getSchedulerConfiguration(), environment);
    }

    private Protos.FrameworkID getTestFrameworkId() {
        return Protos.FrameworkID.newBuilder()
                .setValue(testFrameworkId)
                .build();
    }

    private Protos.TaskStatus getTestTaskStatus() {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(testTaskInfo.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING)
                .build();
    }
}

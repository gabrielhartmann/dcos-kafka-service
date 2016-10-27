package com.mesosphere.dcos.kafka.scheduler;

import com.mesosphere.dcos.kafka.cmd.CmdExecutor;
import com.mesosphere.dcos.kafka.commons.state.KafkaState;
import com.mesosphere.dcos.kafka.config.ConfigStateUpdater;
import com.mesosphere.dcos.kafka.config.ConfigStateValidator.ValidationError;
import com.mesosphere.dcos.kafka.config.ConfigStateValidator.ValidationException;
import com.mesosphere.dcos.kafka.config.KafkaConfigState;
import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.kafka.plan.KafkaUpdatePhase;
import com.mesosphere.dcos.kafka.state.ClusterState;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import com.mesosphere.dcos.kafka.web.BrokerController;
import com.mesosphere.dcos.kafka.web.ConnectionController;
import com.mesosphere.dcos.kafka.web.TopicController;
import io.dropwizard.setup.Environment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.api.JettyApiServer;
import org.apache.mesos.dcos.DcosCluster;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.DefaultScheduler;
import org.apache.mesos.scheduler.SchedulerDriverFactory;
import org.apache.mesos.scheduler.SchedulerUtils;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.scheduler.recovery.RecoveryRequirementProvider;
import org.apache.mesos.state.StateStore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Kafka Framework Scheduler.
 */
public class KafkaScheduler extends DefaultScheduler implements Runnable {
    private static final Log LOGGER = LogFactory.getLog(KafkaScheduler.class);

    private static final int TWO_WEEK_SEC = 2 * 7 * 24 * 60 * 60;

    private final KafkaConfigState configState;
    private final KafkaSchedulerConfiguration envConfig;
    private final FrameworkState frameworkState;
    private final KafkaState kafkaState;
    private final KafkaSchedulerConfiguration kafkaSchedulerConfiguration;
    private SchedulerDriver driver;
    private boolean registered = false;


    public static KafkaScheduler create(KafkaSchedulerConfiguration configuration, Environment environment)
            throws IOException, URISyntaxException, InvalidRequirementException {

        ConfigStateUpdater configStateUpdater = new ConfigStateUpdater(configuration);
        List<String> stageErrors = new ArrayList<>();
        KafkaSchedulerConfiguration targetConfigToUse;

        try {
            targetConfigToUse = configStateUpdater.getTargetConfig();
        } catch (ValidationException e) {
            // New target config failed to validate and was not used. Fall back to previous target config.
            LOGGER.error("Got " + e.getValidationErrors().size() +
                    " errors from new config. Falling back to last valid config.");
            targetConfigToUse = configStateUpdater.getConfigState().getTargetConfig();
            for (ValidationError err : e.getValidationErrors()) {
                stageErrors.add(err.toString());
            }
        }

        KafkaConfigState configState = configStateUpdater.getConfigState();
        FrameworkState frameworkState = configStateUpdater.getFrameworkState();
        KafkaState kafkaState = configStateUpdater.getKafkaState();

        KafkaSchedulerConfiguration envConfig = targetConfigToUse;
        Reconciler reconciler = new DefaultReconciler(frameworkState.getStateStore());
        ClusterState clusterState = new ClusterState();

        OfferAccepter offerAccepter =
                new OfferAccepter(Arrays.asList(new PersistentOperationRecorder(frameworkState)));

        PersistentOfferRequirementProvider offerRequirementProvider =
                new PersistentOfferRequirementProvider(configState, clusterState);

        KafkaUpdatePhase updatePhase = new KafkaUpdatePhase(
                configState.getTargetName().toString(),
                envConfig,
                frameworkState,
                offerRequirementProvider);

        List<Phase> phases = Arrays.asList(
                ReconciliationPhase.create(reconciler),
                new DefaultPhase("update", updatePhase.getBlocks(), new SerialStrategy<>(), Collections.emptyList()));

        // If config validation had errors, expose them via the Stage.
        Plan deploymentPlan = stageErrors.isEmpty()
                ? new DefaultPlan("deployment", phases)
                : new DefaultPlan("deployment", phases, new SerialStrategy<>(), stageErrors);

        Optional<Integer> gracePeriodSecs = Optional.empty();

        if (configuration.getRecoveryConfiguration().isReplacementEnabled()) {
            gracePeriodSecs = Optional.of(configuration.getRecoveryConfiguration().getGracePeriodSecs());
        }

        return new KafkaScheduler(
                configuration.getServiceConfiguration().getName(),
                deploymentPlan,
                configuration.getKafkaConfiguration().getMesosZkUri(),
                gracePeriodSecs,
                configuration.getRecoveryConfiguration().getRecoveryDelaySecs(),
                configuration,
                envConfig,
                configState,
                frameworkState,
                kafkaState,
                offerAccepter,
                reconciler,
                offerRequirementProvider);
    }

    protected KafkaScheduler(
            String frameworkName,
            Plan deploymentPlan,
            String zkConnectionString,
            Optional<Integer> permanentFailureTimeoutSec,
            Integer destructiveRecoveryDelaySec,
            KafkaSchedulerConfiguration kafkaSchedulerConfiguration,
            KafkaSchedulerConfiguration envConfig,
            KafkaConfigState configState,
            FrameworkState frameworkState,
            KafkaState kafkaState,
            OfferAccepter offerAccepter,
            Reconciler reconciler,
            RecoveryRequirementProvider offerRequirementProvider) {

        super(
                frameworkName,
                new DefaultPlanManager(deploymentPlan),
                offerRequirementProvider,
                zkConnectionString,
                permanentFailureTimeoutSec,
                destructiveRecoveryDelaySec);

        this.kafkaSchedulerConfiguration = kafkaSchedulerConfiguration;
        this.envConfig = envConfig;
        this.configState = configState;
        this.frameworkState = frameworkState;
        this.kafkaState = kafkaState;
        this.offerAccepter = offerAccepter;
        this.reconciler = reconciler;
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        super.registered(driver, frameworkId, masterInfo);
        registered = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        super.reregistered(driver, masterInfo);
        registered = true;
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        super.disconnected(driver);
        registered = false;
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        super.error(driver, message);
        registered = false;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("KafkaScheduler");
        Thread.currentThread().setUncaughtExceptionHandler(getUncaughtExceptionHandler());

        String zkPath = "zk://" + envConfig.getKafkaConfiguration().getMesosZkUri() + "/mesos";
        FrameworkInfo fwkInfo = getFrameworkInfo(
                kafkaSchedulerConfiguration.getServiceConfiguration().getName(),
                kafkaSchedulerConfiguration.getServiceConfiguration().getUser(),
                frameworkState.getStateStore());
        startApiServer(this, Integer.valueOf(System.getenv("PORT1")), kafkaSchedulerConfiguration);
        LOGGER.info("Registering framework with: " + fwkInfo);
        registerFramework(this, fwkInfo, zkPath);
    }

    private void startApiServer(
            DefaultScheduler defaultScheduler,
            int apiPort,
            KafkaSchedulerConfiguration kafkaSchedulerConfiguration) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                JettyApiServer apiServer = null;
                try {
                    LOGGER.info("Starting API server thread");
                    Collection<Object> resources = defaultScheduler.getResources();
                    resources.add(new ConnectionController(
                            kafkaSchedulerConfiguration.getFullKafkaZookeeperPath(),
                            getConfigState(),
                            getKafkaState(),
                            new ClusterState(new DcosCluster()),
                            kafkaSchedulerConfiguration.getZookeeperConfig().getFrameworkName()));
                    resources.add(new TopicController(
                            new CmdExecutor(kafkaSchedulerConfiguration, getKafkaState()),
                            getKafkaState()));
                    resources.add(new BrokerController(kafkaState, taskKiller));
                    apiServer = new JettyApiServer(apiPort, defaultScheduler.getResources());
                    apiServer.start();
                } catch (Exception e) {
                    LOGGER.error("API Server failed with exception: ", e);
                } finally {
                    LOGGER.info("API Server exiting.");
                    try {
                        if (apiServer != null) {
                            apiServer.stop();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed to stop API server with exception: ", e);
                    }
                }
            }
        }).start();
    }

    private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                final String msg = "Scheduler exiting due to uncaught exception";
                LOGGER.error(msg, e);
                LOGGER.fatal(msg, e);
                System.exit(2);
            }
        };
    }

    private Protos.FrameworkInfo getFrameworkInfo(String serviceName, String user, StateStore stateStore) {
        Protos.FrameworkInfo.Builder fwkInfoBuilder = Protos.FrameworkInfo.newBuilder()
                .setName(serviceName)
                .setFailoverTimeout(TWO_WEEK_SEC)
                .setUser(user)
                .setRole(SchedulerUtils.nameToRole(serviceName))
                .setPrincipal(SchedulerUtils.nameToPrincipal(serviceName))
                .setCheckpoint(true);

        // The framework ID is not available when we're being started for the first time.
        Optional<Protos.FrameworkID> optionalFrameworkId = stateStore.fetchFrameworkId();
        if (optionalFrameworkId.isPresent()) {
            fwkInfoBuilder.setId(optionalFrameworkId.get());
        }

        return fwkInfoBuilder.build();
    }

    private void registerFramework(KafkaScheduler sched, FrameworkInfo frameworkInfo, String masterUri) {
        LOGGER.info("Registering without authentication");
        driver = new SchedulerDriverFactory().create(sched, frameworkInfo, masterUri);
        driver.run();
    }

    public KafkaState getKafkaState() {
        return kafkaState;
    }

    public KafkaConfigState getConfigState() {
        return configState;
    }

    public FrameworkState getFrameworkState() {
        return frameworkState;
    }

    public PlanManager getPlanManager() {
        return deployPlanManager;
    }

    public boolean isRegistered() {
        return registered;
    }
}

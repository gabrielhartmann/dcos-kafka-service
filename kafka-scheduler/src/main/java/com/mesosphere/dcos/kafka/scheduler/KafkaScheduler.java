package com.mesosphere.dcos.kafka.scheduler;

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
import io.dropwizard.setup.Environment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.api.JettyApiServer;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.DefaultScheduler;
import org.apache.mesos.scheduler.SchedulerDriverFactory;
import org.apache.mesos.scheduler.SchedulerUtils;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.state.StateStore;

import java.net.URISyntaxException;
import java.util.*;

/**
 * Kafka Framework Scheduler.
 */
public class KafkaScheduler implements Scheduler, Runnable {
    private static final Log LOGGER = LogFactory.getLog(KafkaScheduler.class);

    private static final int TWO_WEEK_SEC = 2 * 7 * 24 * 60 * 60;

    private final KafkaConfigState configState;
    private final KafkaSchedulerConfiguration envConfig;
    private final FrameworkState frameworkState;
    private final KafkaState kafkaState;
    private final ClusterState clusterState;


    private final OfferAccepter offerAccepter;
    private final Reconciler reconciler;
    private final DefaultPlan installPlan;
    private final PersistentOfferRequirementProvider offerRequirementProvider;
    private final KafkaSchedulerConfiguration kafkaSchedulerConfiguration;
    private final DefaultScheduler defaultScheduler;
    private final DefaultPlanManager defaultPlanManager;
    private SchedulerDriver driver;
    private boolean registered;


    public KafkaScheduler(KafkaSchedulerConfiguration configuration, Environment environment)
            throws ConfigStoreException, URISyntaxException {

        this.kafkaSchedulerConfiguration = configuration;
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

        configState = configStateUpdater.getConfigState();
        frameworkState = configStateUpdater.getFrameworkState();
        kafkaState = configStateUpdater.getKafkaState();

        envConfig = targetConfigToUse;
        reconciler = new DefaultReconciler(frameworkState.getStateStore());
        clusterState = new ClusterState();

        offerAccepter =
                new OfferAccepter(Arrays.asList(new PersistentOperationRecorder(frameworkState)));

        this.offerRequirementProvider =
                new PersistentOfferRequirementProvider(frameworkState, configState, clusterState);

        KafkaUpdatePhase updatePhase = new KafkaUpdatePhase(
                configState.getTargetName().toString(),
                envConfig,
                frameworkState,
                offerRequirementProvider);

        List<Phase> phases = Arrays.asList(
                ReconciliationPhase.create(reconciler),
                new DefaultPhase("update", updatePhase.getBlocks(), new SerialStrategy<>(), Collections.emptyList()));

        // If config validation had errors, expose them via the Stage.
        this.installPlan = stageErrors.isEmpty()
                ? new DefaultPlan("deployment", phases)
                : new DefaultPlan("deployment", phases, new SerialStrategy<>(), stageErrors);

        defaultPlanManager = new DefaultPlanManager(installPlan);

        defaultScheduler = DefaultScheduler.create(
                configuration.getServiceConfiguration().getName(),
                defaultPlanManager,
                configuration.getKafkaConfiguration().getMesosZkUri());
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
        LOGGER.info("Registering framework with: " + fwkInfo);
        registerFramework(this, fwkInfo, zkPath);
        startApiServer(defaultScheduler, Integer.valueOf(System.getenv("PORT1")));
    }

    private void startApiServer(DefaultScheduler defaultScheduler, int apiPort) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                JettyApiServer apiServer = null;
                try {
                    LOGGER.info("Starting API server.");
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

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
        defaultScheduler.registered(driver, frameworkId, masterInfo);
        registered = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
        defaultScheduler.reregistered(driver, masterInfo);
        registered = true;
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        defaultScheduler.resourceOffers(driver, offers);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
        defaultScheduler.offerRescinded(driver, offerId);
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
        defaultScheduler.statusUpdate(driver, status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        defaultScheduler.frameworkMessage(driver, executorId, slaveId, data);
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        defaultScheduler.disconnected(driver);
        registered = false;
    }

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
        defaultScheduler.slaveLost(driver, slaveId);
    }

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {
        defaultScheduler.executorLost(driver, executorId, slaveId, status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        defaultScheduler.error(driver, message);
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

    public FrameworkState getFrameworkState() {
        return frameworkState;
    }

    public PlanManager getPlanManager() {
        return defaultPlanManager;
    }

    public boolean isRegistered() {
        return registered;
    }
}

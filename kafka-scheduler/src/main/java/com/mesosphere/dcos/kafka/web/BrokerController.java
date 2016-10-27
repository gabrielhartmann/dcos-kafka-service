package com.mesosphere.dcos.kafka.web;

import com.mesosphere.dcos.kafka.commons.state.KafkaState;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.scheduler.TaskKiller;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("/v1/brokers")
@Produces("application/json")
public class BrokerController {
    private final Log log = LogFactory.getLog(BrokerController.class);
    private final KafkaState kafkaState;
    private final TaskKiller taskKiller;

    public BrokerController(
            KafkaState kafkaState,
            TaskKiller taskKiller) {
        this.kafkaState = kafkaState;
        this.taskKiller = taskKiller;
    }

    @GET
    public Response listBrokers() {
        try {
            return Response.ok(kafkaState.getBrokerIds(), MediaType.APPLICATION_JSON).build();
        } catch (Exception ex) {
            log.error("Failed to fetch broker ids", ex);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/{id}")
    public Response getBroker(@PathParam("id") String id) {
        try {
            Optional<JSONObject> brokerObj = kafkaState.getBroker(id);
            if (brokerObj.isPresent()) {
                return Response.ok(brokerObj, MediaType.APPLICATION_JSON).build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        } catch (Exception ex) {
            log.error("Failed to fetch broker id: " + id, ex);
            return Response.serverError().build();
        }
    }

    @PUT
    @Path("/{id}")
    public Response killBrokers(
            @PathParam("id") String id,
            @QueryParam("replace") String replace) {

        String name = OfferUtils.brokerIdToTaskName(Integer.valueOf(id));
        boolean taskExists = taskKiller.killTask(name, Boolean.parseBoolean(replace));

        if (taskExists) {
            return Response.accepted().build();
        } else {
            log.error("User requested to kill non-existent task: " + name);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
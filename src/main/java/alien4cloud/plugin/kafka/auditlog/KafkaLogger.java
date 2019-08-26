package alien4cloud.plugin.kafka.auditlog;

import alien4cloud.application.ApplicationEnvironmentService;
import alien4cloud.deployment.DeploymentRuntimeStateService;
import alien4cloud.deployment.DeploymentService;

import alien4cloud.model.common.Tag;
import alien4cloud.model.deployment.Deployment;
import alien4cloud.model.deployment.DeploymentTopology;
import alien4cloud.paas.IPaasEventListener;
import alien4cloud.paas.IPaasEventService;
import alien4cloud.paas.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.common.collect.Lists;
import org.springframework.expression.Expression;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component("kafka-logger")
public class KafkaLogger {

    @Inject
    private IPaasEventService eventService;

    @Inject
    private DeploymentService deploymentService;

    @Inject
    private ApplicationEnvironmentService environmentService;

    @Inject
    private DeploymentRuntimeStateService deploymentRuntimeStateService;

    @Inject
    private KafkaConfiguration configuration;

    private Map<String,Expression> bindings = Maps.newHashMap();

    private ObjectMapper mapper = new ObjectMapper();

    private String hostname;

    Producer<String,String> producer;

    IPaasEventListener listener = new IPaasEventListener() {
        @Override
        public void eventHappened(AbstractMonitorEvent event) {
            if (event instanceof PaaSDeploymentStatusMonitorEvent) {
                handleEvent((PaaSDeploymentStatusMonitorEvent) event);
            } else if ((event instanceof PaaSWorkflowStartedEvent)
                   || (event instanceof PaaSWorkflowFinishedEvent)) {
                handleWorkflowEvent((AbstractPaaSWorkflowMonitorEvent) event);
            }
        }

        @Override
        public boolean canHandle(AbstractMonitorEvent event) {
            return     (event instanceof PaaSDeploymentStatusMonitorEvent)
                    || (event instanceof PaaSWorkflowStartedEvent)
                    || (event instanceof PaaSWorkflowFinishedEvent);
        }
    };

    private void handleWorkflowEvent(AbstractPaaSWorkflowMonitorEvent inputEvent) {
        OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());

        Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());

        publish(
                stamp,
                deployment,
                buildId(deployment),
                inputEvent instanceof PaaSWorkflowStartedEvent ? "MODULE_START" : "MODULE_END",
                String.format("Execution Job %s",deployment.getSourceName())
            );
    }


    private void handleEvent(PaaSDeploymentStatusMonitorEvent inputEvent) {
        String eventName;

        switch(inputEvent.getDeploymentStatus()) {
            case DEPLOYMENT_IN_PROGRESS:
                eventName = "DEPLOY_BEGIN";
                break;
            case DEPLOYED:
                eventName = "DEPLOY_SUCCESS";
                break;
            case FAILURE:
                eventName = "DEPLOY_ERROR";
                break;
            case UNDEPLOYMENT_IN_PROGRESS:
                eventName = "UNDEPLOY_BEGIN";
                break;
            case UNDEPLOYED:
                eventName = "UNDEPLOY_SUCCESS";
                break;
            default:
                return;
        }

        OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());

        Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());

        if (inputEvent.getDeploymentStatus().equals(DeploymentStatus.DEPLOYED) || inputEvent.getDeploymentStatus().equals(DeploymentStatus.FAILURE) || inputEvent.getDeploymentStatus().equals(DeploymentStatus.UNDEPLOYED)) {
            DeploymentTopology toplogy = deploymentRuntimeStateService.getRuntimeTopology(deployment.getId());

            for (NodeTemplate node : toplogy.getUnprocessedNodeTemplates().values()) {
                if (node.getTags() != null && node.getTags().stream().anyMatch(this::filterTag)) {
                    publish(stamp,deployment,buildId(deployment,node),eventName,String.format("Deploys node %s",node.getName()));
                }
            }
        }

        publish(stamp,deployment,buildId(deployment),eventName,String.format("Deploys the application %s",deployment.getSourceName()));
    }

    private void publish(OffsetDateTime stamp, Deployment deployment, List<Object> id, String event, String message) {
        Map<String,Object> outputEvent = Maps.newLinkedHashMap();

        outputEvent.put("timestamp",stamp.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        outputEvent.put("hostname",hostname);
        outputEvent.put("user",deployment.getDeployerUsername());
        outputEvent.put("source",String.format("Log Audit Deploiement %s",deployment.getSourceName()));
        outputEvent.put("domaine","Socle/Service applicatif");
        outputEvent.put("composant","A4C");
        outputEvent.put("site",configuration.getSite());
        outputEvent.put("processus","java");
        outputEvent.put("ids_technique",id);
        outputEvent.put("event",event);
        outputEvent.put("message",message);

        try {
            doPublish(mapper.writeValueAsString(outputEvent));
        } catch(JsonProcessingException e) {
            log.error("Cant send kafka event: {}",e);
        }
    }

    private List<Object> buildId(Deployment deployment) {
        return Lists.newArrayList(buildIdElement("id_A4C",deployment.getId()));
    }

    private List<Object> buildId(Deployment deployment,NodeTemplate node) {
        List result = buildId(deployment);
        result.add(buildIdElement("nom",node.getName()));

        for (Tag tag : node.getTags()) {
            result.add(buildIdElement(tag.getName(),tag.getValue()));
        }
        return result;
    }

    private Map<String,Object> buildIdElement(String name,String value) {
        Map<String,Object> result = Maps.newHashMap();
        result.put(name,value);
        return result;
    }
    @PostConstruct
    public void init() {
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            hostname = "N/A";
        }

        if (configuration.getBootstrapServers() == null || configuration.getSite() == null || configuration.getTopic() == null) {
            log.error("Kafka Logger is not configured.");
        } else {
            Properties props = new Properties();
            props.put("bootstrap.servers", configuration.getBootstrapServers());

            producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

            eventService.addListener(listener);
            log.info("Kafka Logger registered");
        }
    }

    @PreDestroy
    public void term() {
        if (producer != null) {
            eventService.removeListener(listener);

            // Close the kafka producer
            producer.close();

            log.info("Kafka Logger unregistered");
        }
    }

    /**
     * Tag Predictate for nodes filtering
     * @param tag
     * @return
     */
    private boolean filterTag(Tag tag) {
        return tag.getName().equals(configuration.getModuleTagName()) && tag.getValue().equals(configuration.getModuleTagValue());
    }

    private void doPublish(String json) {
        producer.send(new ProducerRecord<>(configuration.getTopic(),null,json));
        log.debug("=> KAFKA[{}] : {}",configuration.getTopic(),json);
    }
}

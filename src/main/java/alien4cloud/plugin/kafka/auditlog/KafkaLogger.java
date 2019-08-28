package alien4cloud.plugin.kafka.auditlog;

import alien4cloud.application.ApplicationEnvironmentService;
import alien4cloud.common.MetaPropertiesService;
import alien4cloud.deployment.DeploymentRuntimeStateService;
import alien4cloud.deployment.DeploymentService;

import alien4cloud.model.common.MetaPropertyTarget;
import alien4cloud.model.common.Tag;
import alien4cloud.model.deployment.Deployment;
import alien4cloud.model.deployment.DeploymentTopology;
import alien4cloud.paas.IPaasEventListener;
import alien4cloud.paas.IPaasEventService;
import alien4cloud.paas.model.*;
import alien4cloud.tosca.context.ToscaContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.types.NodeType;
import org.alien4cloud.tosca.normative.constants.NormativeTypesConstant;
import org.alien4cloud.tosca.utils.NodeTypeUtils;
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
import java.util.function.Predicate;

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

    @Inject
    private MetaPropertiesService metaPropertiesService;

    private Map<String,Expression> bindings = Maps.newHashMap();

    private ObjectMapper mapper = new ObjectMapper();

    private String hostname;

    Producer<String,String> producer;

    IPaasEventListener listener = new IPaasEventListener() {
        @Override
        public void eventHappened(AbstractMonitorEvent event) {
            if (event instanceof PaaSDeploymentStatusMonitorEvent) {
                handleEvent((PaaSDeploymentStatusMonitorEvent) event);
            } else if (event instanceof PaaSWorkflowStartedEvent) {
                handleWorkflowEvent((PaaSWorkflowStartedEvent) event);
            } else if (event instanceof WorkflowStepStartedEvent) {
                handleWorkflowStepEvent((WorkflowStepStartedEvent) event);
            }
        }

        @Override
        public boolean canHandle(AbstractMonitorEvent event) {
            return (event instanceof PaaSDeploymentStatusMonitorEvent)
                    || (event instanceof PaaSWorkflowStartedEvent)
                    || (event instanceof WorkflowStepStartedEvent);
        }
    };

    private void handleWorkflowStepEvent(WorkflowStepStartedEvent inputEvent) {
        if (inputEvent.getOperationName().equals("tosca.interfaces.node.lifecycle.runnable.submit")) {
            Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());
            DeploymentTopology toplogy = deploymentRuntimeStateService.getRuntimeTopology(deployment.getId());

            try {
                ToscaContext.init(toplogy.getDependencies());

                NodeTemplate node = toplogy.getUnprocessedNodeTemplates().get(inputEvent.getNodeId());
                NodeType type = ToscaContext.getOrFail(NodeType.class, node.getType());

                if (type.getDerivedFrom().contains("org.alien4cloud.nodes.Job")) {
                    OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());
                    publish(
                            stamp,
                            deployment,
                            buildId(deployment),
                            "JOB_SUBMIT",
                            String.format("Job started on application %s / node %s",deployment.getSourceName(),inputEvent.getNodeId())
                    );
                }
            } finally {
                ToscaContext.destroy();
            }
            log.info("WORKFLOWSTEP: {}",inputEvent);
        }
    }

    private void handleWorkflowEvent(PaaSWorkflowStartedEvent inputEvent) {
        String phaseName;
        String eventName;

        OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());
        Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());

        if (inputEvent.getWorkflowName() == null) {
            return;
        }
        if (inputEvent.getWorkflowName().equals("install")) {
            eventName="DEPLOY_BEGIN";
            phaseName="Deploys";
        } else if (inputEvent.getWorkflowName().equals("uninstall")) {
            eventName="UNDEPLOY_BEGIN";
            phaseName="Undeploys";
        } else {
            return;
        }

        if (inputEvent.getWorkflowName().equals("uninstall") || inputEvent.getWorkflowName().equals("install")) {
            publish(
                    stamp,
                    deployment,
                    buildId(deployment),
                    eventName,
                    String.format("%s the application %s",phaseName,deployment.getSourceName())
            );
        }
    }


    private void handleEvent(PaaSDeploymentStatusMonitorEvent inputEvent) {
        String eventName;
        String phaseName;

        switch(inputEvent.getDeploymentStatus()) {
            case DEPLOYED:
                eventName = "DEPLOY_SUCCESS";
                phaseName = "Deploys";
                break;
            case FAILURE:
                eventName = "DEPLOY_ERROR";
                phaseName = "Deploys";
                break;
            case UNDEPLOYED:
                eventName = "UNDEPLOY_SUCCESS";
                phaseName = "Undeploys";
                break;
            default:
                return;
        }

        Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());
        OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());

        // We must send an event per module
        DeploymentTopology toplogy = deploymentRuntimeStateService.getRuntimeTopology(deployment.getId());

        if (inputEvent.getDeploymentStatus().equals(DeploymentStatus.DEPLOYED) || inputEvent.getDeploymentStatus().equals(DeploymentStatus.FAILURE)) {
            publish(stamp,deployment,buildId(deployment),eventName,String.format("%s the application %s",phaseName,deployment.getSourceName()));
        }

        String metaId = metaPropertiesService.getMetapropertykeyByName(configuration.getModuleTagName(),MetaPropertyTarget.COMPONENT);
        if (metaId != null) {
            try {
                ToscaContext.init(toplogy.getDependencies());

                for (NodeTemplate node : toplogy.getUnprocessedNodeTemplates().values()) {
                    NodeType type = ToscaContext.getOrFail(NodeType.class, node.getType());
                    if (type.getMetaProperties() != null && configuration.getModuleTagValue().equals(type.getMetaProperties().get(metaId))) {
                        publish(stamp, deployment, buildId(deployment, node), eventName, String.format("%s the module %s",phaseName,node.getName()));
                    }
                }
            } finally {
                ToscaContext.destroy();
            }
        }

        if (inputEvent.getDeploymentStatus().equals(DeploymentStatus.UNDEPLOYED)) {
            publish(stamp,deployment,buildId(deployment),eventName,String.format("%s the application %s",phaseName,deployment.getSourceName()));
        }
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

    private void doPublish(String json) {
        producer.send(new ProducerRecord<>(configuration.getTopic(),null,json));
        log.debug("=> KAFKA[{}] : {}",configuration.getTopic(),json);
    }
}

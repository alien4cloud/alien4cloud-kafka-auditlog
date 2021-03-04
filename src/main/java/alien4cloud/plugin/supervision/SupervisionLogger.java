package alien4cloud.plugin.supervision;

import alien4cloud.application.ApplicationEnvironmentService;
import alien4cloud.common.MetaPropertiesService;
import alien4cloud.deployment.DeploymentRuntimeStateService;
import alien4cloud.deployment.DeploymentService;
import alien4cloud.model.common.MetaPropertyTarget;
import alien4cloud.model.deployment.Deployment;
import alien4cloud.model.deployment.DeploymentTopology;
import alien4cloud.paas.IPaasEventListener;
import alien4cloud.paas.IPaasEventService;
import alien4cloud.paas.model.*;
import alien4cloud.topology.TopologyServiceCore;
import alien4cloud.tosca.context.ToscaContext;
import alien4cloud.utils.PropertyUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import alien4cloud.model.common.Tag;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.model.types.NodeType;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.expression.Expression;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;


@Slf4j
@Component("kafka-logger")
public class SupervisionLogger {

    private static final String OPERATION_SUBMIT = "tosca.interfaces.node.lifecycle.runnable.submit";
    private static final String OPERATION_RUN = "tosca.interfaces.node.lifecycle.runnable.run";

    @Inject
    private IPaasEventService eventService;

    @Inject
    private DeploymentService deploymentService;

    @Inject
    private TopologyServiceCore topologyServiceCore;

    @Inject
    private ApplicationEnvironmentService environmentService;

    @Inject
    private DeploymentRuntimeStateService deploymentRuntimeStateService;

    @Inject
    private SupervisionConfiguration configuration;

    @Inject
    private MetaPropertiesService metaPropertiesService;

    private Map<String,Expression> bindings = Maps.newHashMap();

    private ObjectMapper mapper = new ObjectMapper();

    private String hostname;

    private List<String> undeploysInProgress = Collections.synchronizedList(new ArrayList<String>());

    Producer<String,String> producer;

    // A4C K8S resource types defined in org.alien4cloud.plugin.kubernetes.modifier
    public static final String K8S_TYPES_DEPLOYMENT_RESOURCE = "org.alien4cloud.kubernetes.api.types.DeploymentResource";
    public static final String K8S_TYPES_SIMPLE_RESOURCE = "org.alien4cloud.kubernetes.api.types.SimpleResource";

    IPaasEventListener listener = new IPaasEventListener() {
        @Override
        public void eventHappened(AbstractMonitorEvent event) {
            try {
                if (event instanceof PaaSDeploymentStatusMonitorEvent) {
                    handleEvent((PaaSDeploymentStatusMonitorEvent) event);
                } else if (event instanceof PaaSWorkflowStartedEvent) {
                    handleWorkflowEvent((PaaSWorkflowStartedEvent) event);
                } else if (event instanceof WorkflowStepStartedEvent) {
                    handleWorkflowStepEvent((WorkflowStepStartedEvent) event);
                } else if (event instanceof AbstractTaskEndedEvent) {
                    handleTaskEndedEvent((AbstractTaskEndedEvent) event);
                }
            } catch(RuntimeException e) {
                log.error("Exception in event handler",e);
            }
        }

        @Override
        public boolean canHandle(AbstractMonitorEvent event) {
            return (event instanceof PaaSDeploymentStatusMonitorEvent)
                    || (event instanceof PaaSWorkflowStartedEvent)
                    || (event instanceof WorkflowStepStartedEvent)
                    || (event instanceof AbstractTaskEndedEvent);
        }
    };

    private void handleTaskEndedEvent(AbstractTaskEndedEvent event) {
        String kafkaEvent = "JOB_END_SUCCESS";
        String jobStatus = "success";

        if (event.getOperationName().equals(OPERATION_RUN)) {
            Deployment deployment = deploymentService.get(event.getDeploymentId());
            Topology initialTopology = deploymentRuntimeStateService.getUnprocessedTopology(event.getDeploymentId());

            try {
                ToscaContext.init(initialTopology.getDependencies());

                NodeTemplate node = initialTopology.getNodeTemplates().get(event.getNodeId());
                NodeType type = ToscaContext.getOrFail(NodeType.class, node.getType());

                if (event instanceof TaskFailedEvent) {
                    kafkaEvent = "JOB_END_ERROR";
                    jobStatus = "error";
                }

                if (type.getDerivedFrom().contains("org.alien4cloud.nodes.Job")) {
                    OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(event.getDate()), ZoneId.systemDefault());
                    publish(
                            stamp,
                            deployment,
                            buildId(deployment),
                            kafkaEvent,
                            String.format("Job ended on %s on application %s / node %s",jobStatus,deployment.getSourceName(),event.getNodeId())
                    );
                }
            } finally {
                ToscaContext.destroy();
            }
        }
    }

    private void handleWorkflowStepEvent(WorkflowStepStartedEvent inputEvent) {
        if (inputEvent.getOperationName().equals(OPERATION_SUBMIT)) {
            Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());
            Topology initialTopology = deploymentRuntimeStateService.getUnprocessedTopology(inputEvent.getDeploymentId());

            try {
                ToscaContext.init(initialTopology.getDependencies());

                NodeTemplate node = initialTopology.getNodeTemplates().get(inputEvent.getNodeId());
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
            synchronized(undeploysInProgress) {
               if (undeploysInProgress.contains(inputEvent.getDeploymentId())) {
                  return;
               } else {
                  undeploysInProgress.add(inputEvent.getDeploymentId());
               }
            }
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
                synchronized(undeploysInProgress) {
                   undeploysInProgress.remove (inputEvent.getDeploymentId());
                }
                break;
            case UNDEPLOYED:
                eventName = "UNDEPLOY_SUCCESS";
                phaseName = "Undeploys";
                synchronized(undeploysInProgress) {
                   undeploysInProgress.remove (inputEvent.getDeploymentId());
                }
                break;
            default:
                return;
        }

        Deployment deployment = deploymentService.get(inputEvent.getDeploymentId());
        OffsetDateTime stamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(inputEvent.getDate()), ZoneId.systemDefault());

        // We must send an event per module
        //DeploymentTopology topology = deploymentRuntimeStateService.getRuntimeTopology(deployment.getId());
        //Topology initialTopology = topologyServiceCore.getOrFail(topology.getInitialTopologyId());
        Topology initialTopology = deploymentRuntimeStateService.getUnprocessedTopology(deployment.getId());
        if (initialTopology == null) {
            log.warn("Unprocessed Topology not found for deployment <{}>",deployment.getId());
            return;
        }

        if (inputEvent.getDeploymentStatus().equals(DeploymentStatus.DEPLOYED) || inputEvent.getDeploymentStatus().equals(DeploymentStatus.FAILURE)) {
            publish(stamp,deployment,buildId(deployment),eventName,String.format("%s the application %s",phaseName,deployment.getSourceName()));
        }

        DeploymentTopology deployedTopology = deploymentRuntimeStateService.getRuntimeTopology(deployment.getId());
        if (deployedTopology != null) {
            String metaId = metaPropertiesService.getMetapropertykeyByName(configuration.getModuleTagName(),MetaPropertyTarget.COMPONENT);
            if (metaId != null) {
                try {
                    ToscaContext.init(initialTopology.getDependencies());
                    for (NodeTemplate node : initialTopology.getNodeTemplates().values()) {
                        NodeType type = ToscaContext.getOrFail(NodeType.class, node.getType());
                        if (type.getMetaProperties() != null && configuration.getModuleTagValue().equals(type.getMetaProperties().get(metaId))) {
                            // Module found
                            String deploymentName = findDeploymentName(node, initialTopology, deployedTopology);
                            String namespaceName = findNamespaceName(deployedTopology);

                            publish(stamp, deployment, buildId(deployment, node, deploymentName, namespaceName), "MODULE_" + eventName, String.format("%s the module %s", phaseName, node.getName()));
                        }
                    }
                } finally {
                    ToscaContext.destroy();
                }
            }
        } else {
            log.error("Deployed topology is no longer available. Cannot send module events for deployment {}",deployment.getId());
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

    private List<Object> buildId(Deployment deployment,NodeTemplate node, String deploymentName, String namespace) {
        List result = buildId(deployment);

        result.add(buildIdElement("nom",node.getName()));
        if (deploymentName != null) {
            result.add(buildIdElement("KubeDeployment",deploymentName));
        }
        if (namespace != null) {
            result.add(buildIdElement("KubeNamespace",namespace));
        }
        if (deploymentName != null) {
            result.add(buildIdElement("Moteur d'exécution","Kubernetes"));
        }
        return result;
    }

    private String findDeploymentName(NodeTemplate nodeContainer,Topology initialToplogy,Topology deployedTopology) {
        NodeTemplate nodeDeployment = TopologyNavigationUtil.getImmediateHostTemplate(initialToplogy,nodeContainer);
        if (nodeDeployment != null) {
            for (NodeTemplate node : TopologyNavigationUtil.getNodesOfType(deployedTopology, K8S_TYPES_DEPLOYMENT_RESOURCE, true)) {
                List<Tag> tags = node.getTags();
                if (tags != null && tags.stream().anyMatch(buildTagPredicate(nodeDeployment))) {
                    return PropertyUtil.getScalarValue(node.getProperties().get("resource_id"));
                }
            }
        }
        return null;
    }

    private Predicate<Tag> buildTagPredicate(NodeTemplate node) {
        final String nodeName = node.getName();
        return tag -> tag.getName().equals("a4c_kubernetes-adapter-modifier_ReplacementNodeFor") && tag.getValue().equals(nodeName);
    }

    private String findNamespaceName(Topology deployedToplogy) {
        // find Namespace related  SimpleResource
        for (NodeTemplate node : TopologyNavigationUtil.getNodesOfType(deployedToplogy,K8S_TYPES_SIMPLE_RESOURCE,true)) {
            if (PropertyUtil.getScalarValue(node.getProperties().get("resource_type")).equals("namespaces")) {
                return PropertyUtil.getScalarValue(node.getProperties().get("resource_id"));
            }
        }
        return null;
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
            props.putAll (configuration.getProducerProperties());

            try {
               producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());
            } catch (Exception e) {
               log.error ("Cannot connect to Kafka ({})", e.getMessage());
            }

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
        if (producer != null) {
            producer.send(new ProducerRecord<>(configuration.getTopic(), null, json));
            log.debug("=> KAFKA[{}] : {}", configuration.getTopic(), json);
        }
    }
}

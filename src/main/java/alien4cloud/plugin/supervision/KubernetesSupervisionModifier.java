package alien4cloud.plugin.supervision;

import alien4cloud.common.MetaPropertiesService;
import alien4cloud.model.application.Application;
import alien4cloud.model.application.ApplicationEnvironment;
import alien4cloud.model.common.MetaPropertyTarget;
import alien4cloud.model.common.Tag;
import alien4cloud.tosca.context.ToscaContext;
import alien4cloud.utils.PropertyUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.alien4cloud.alm.deployment.configuration.flow.FlowExecutionContext;
import org.alien4cloud.alm.deployment.configuration.flow.TopologyModifierSupport;
import org.alien4cloud.tosca.model.definitions.ComplexPropertyValue;
import org.alien4cloud.tosca.model.definitions.ScalarPropertyValue;
import org.alien4cloud.tosca.model.templates.NodeTemplate;
import org.alien4cloud.tosca.model.templates.Topology;
import org.alien4cloud.tosca.model.types.NodeType;
import org.alien4cloud.tosca.normative.constants.NormativeRelationshipConstants;
import org.alien4cloud.tosca.utils.TopologyNavigationUtil;
import org.apache.commons.lang3.ClassUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component(value = "kubernetes-supervision-modifier")
public class KubernetesSupervisionModifier extends TopologyModifierSupport {

    private final static String K8S_DEPLOYMENT_RESOURCE = "org.alien4cloud.kubernetes.api.types.DeploymentResource";

    private final static String K8S_SPARK_JOBS = "org.alien4cloud.k8s.spark.jobs.AbstractSparkJob";

    private final static String CAS_USAGE = "Cas d'usage";

    private final static String TEMPLATE_NAME = "A4C_META_TemplateName";

    private final static String TAG_CONTAINER = "a4c_kubernetes-adapter-modifier_Container_";

    private final static String TAG_REPLACEMENT = "a4c_kubernetes-adapter-modifier_ReplacementNodeFor";

    private final static String MLS_TOKEN = "MLS_tokenid";

    private final ObjectMapper mapper = new ObjectMapper();

    private String metaAppId;

    @Inject
    private MetaPropertiesService metaPropertiesService;

    @Inject
    private SupervisionConfiguration configuration;

    @Override
    public void process(Topology deployedTopology, FlowExecutionContext context) {
        Topology initialTopology = (Topology) context.getExecutionCache().get("INITIAL_TOPOLOGY");

        if (initialTopology == null) {
            log.error("Can't find initial topology");
            return;
        }

        for (NodeTemplate node : TopologyNavigationUtil.getNodesOfType(deployedTopology,K8S_DEPLOYMENT_RESOURCE,true)) {
            ScalarPropertyValue specProp = (ScalarPropertyValue) node.getProperties().get("resource_spec");

            try {
                ObjectNode spec = (ObjectNode) mapper.readTree(PropertyUtil.getScalarValue(specProp));

                annotateDeployment(initialTopology,deployedTopology, node, context,spec);

                specProp.setValue(mapper.writeValueAsString(spec));
            } catch(IOException e) {
                log.error("Can't parse json: {}",e);
            }
        }

        for (NodeTemplate node : TopologyNavigationUtil.getNodesOfType(deployedTopology,K8S_SPARK_JOBS,true)) {
            annotateSparkJob(initialTopology,deployedTopology,node,context);
        }
    }

    private void annotateSparkJob(Topology initialTopology, Topology deployedTopology, NodeTemplate deploymentNode, FlowExecutionContext context) {
        Application application = context.getEnvironmentContext().get().getApplication();
        ApplicationEnvironment environment = context.getEnvironmentContext().get().getEnvironment();

        NodeTemplate initialDeployment = getInitialDeployment(deploymentNode,initialTopology);

        // Create context by hand because it needs to be done on initialTopology
        ToscaContext.Context toscaContext = new ToscaContext.Context(initialTopology.getDependencies());

        // Domaine
        if (metaAppId != null && application.getMetaProperties() != null) {
            String value = application.getMetaProperties().get(metaAppId);
            if (value != null) {
                log.info("Must Add annotation {}",value);
                addAnnotationOnSparkJob(deploymentNode,String.format("%s.domaine", configuration.getMetaPrefix()),String.format("%s/%s", CAS_USAGE, value));
            }
        }

        // Site
        addAnnotationOnSparkJob(deploymentNode, String.format("%s.site", configuration.getMetaPrefix()), configuration.getSite());

        // Application
        if (deployedTopology.getTags()!=null) {
            Optional<Tag> tag = deployedTopology.getTags().stream().filter(t -> t.getName().equals(TEMPLATE_NAME)).findFirst();
            if (tag.isPresent()) {
                addAnnotationOnSparkJob(deploymentNode, String.format("%s.application", configuration.getMetaPrefix()), tag.get().getValue());
            }
        }

        // Domaine
        /*
        if (initialDeployment != null) {
            for (NodeTemplate containerNode : TopologyNavigationUtil.getSourceNodesByRelationshipType(initialTopology, initialDeployment, NormativeRelationshipConstants.HOSTED_ON)) {
                NodeType containerType = toscaContext.getElement(NodeType.class,containerNode.getType(),true);

                String type = ClassUtils.getShortClassName(containerNode.getType()).toLowerCase();

                addLabel(spec, String.format("%s.module.%s", configuration.getMetaPrefix(), map.get(containerNode.getName())), type);

                if (containerNode.getTags()!=null) {
                    Optional<Tag> tokenTag = containerNode.getTags().stream().filter(t -> t.getName().equals(MLS_TOKEN)).findFirst();
                    if (tokenTag.isPresent()) {
                        addLabel(spec, String.format("%s.tokenid.%s", configuration.getMetaPrefix(), map.get(containerNode.getName())), tokenTag.get().getValue());
                    }
                }
            }
        }*/

        addAnnotationOnSparkJob(deploymentNode, String.format("%s.app_instance", configuration.getMetaPrefix()), String.format("%s-%s", environment.getApplicationId(), environment.getName()));
    }

    private void addAnnotationOnSparkJob(NodeTemplate deploymentNode,String key,String value) {
        ComplexPropertyValue prop = (ComplexPropertyValue) deploymentNode.getProperties().get("annotations");
        prop.getValue().put(key,value);
    }

    private void addLabelOnSparkJob(NodeTemplate deploymentNode,String key,String value) {
        ComplexPropertyValue prop = (ComplexPropertyValue) deploymentNode.getProperties().get("labels");
        prop.getValue().put(key,value);
    }

    private void annotateDeployment(Topology initialTopology, Topology deployedTopology, NodeTemplate deploymentNode, FlowExecutionContext context, ObjectNode spec) {
        Application application = context.getEnvironmentContext().get().getApplication();
        ApplicationEnvironment environment = context.getEnvironmentContext().get().getEnvironment();

        Map<String,String> map = buildContainerMap(deploymentNode);

        NodeTemplate initialDeployment = getInitialDeployment(deploymentNode,initialTopology);

        // Create context by hand because it needs to be done on initialTopology
        ToscaContext.Context toscaContext = new ToscaContext.Context(initialTopology.getDependencies());

        // Domaine
        if (metaAppId != null && application.getMetaProperties() != null) {
            String value = application.getMetaProperties().get(metaAppId);
            if (value != null) {
                addAnnotation(spec, String.format("%s.domaine", configuration.getMetaPrefix()), String.format("%s/%s", CAS_USAGE, value));
            }
        }

        // Site
        addAnnotation(spec, String.format("%s.site", configuration.getMetaPrefix()), configuration.getSite());

        // Application
        if (deployedTopology.getTags()!=null) {
            Optional<Tag> tag = deployedTopology.getTags().stream().filter(t -> t.getName().equals(TEMPLATE_NAME)).findFirst();
            if (tag.isPresent()) {
                addAnnotation(spec, String.format("%s.application", configuration.getMetaPrefix()), tag.get().getValue());
            }
        }

        // Domaine
        if (initialDeployment != null) {
            for (NodeTemplate containerNode : TopologyNavigationUtil.getSourceNodesByRelationshipType(initialTopology, initialDeployment, NormativeRelationshipConstants.HOSTED_ON)) {
                NodeType containerType = toscaContext.getElement(NodeType.class,containerNode.getType(),true);

                String type = ClassUtils.getShortClassName(containerNode.getType()).toLowerCase();

                addLabel(spec, String.format("%s.module.%s", configuration.getMetaPrefix(), map.get(containerNode.getName())), type);

                if (containerNode.getTags()!=null) {
                    Optional<Tag> tokenTag = containerNode.getTags().stream().filter(t -> t.getName().equals(MLS_TOKEN)).findFirst();
                    if (tokenTag.isPresent()) {
                        addLabel(spec, String.format("%s.tokenid.%s", configuration.getMetaPrefix(), map.get(containerNode.getName())), tokenTag.get().getValue());
                    }
                }
            }
        }

        addAnnotation(spec, String.format("%s.app_instance", configuration.getMetaPrefix()), String.format("%s-%s", environment.getApplicationId(), environment.getName()));
    }

    /**
     * Build Node name to k8s name map
     * @param node
     * @return
     */
    private Map<String,String> buildContainerMap(NodeTemplate node) {
        return node.getTags().stream()
                .filter(tag -> tag.getName().startsWith(TAG_CONTAINER))
                .collect(Collectors.toMap(tag ->  tag.getName().substring(TAG_CONTAINER.length()), Tag::getValue));
    }

    private NodeTemplate getInitialDeployment(NodeTemplate deployment,Topology initialTopology) {
        Optional<Tag> tag = deployment.getTags().stream().filter(t -> t.getName().equals(TAG_REPLACEMENT)).findFirst();
        if (tag.isPresent()) {
            return initialTopology.getNodeTemplates().get(tag.get().getValue());
        }
        return null;
    }

    private void addLabel(ObjectNode spec,String key,String value) {
        spec.with("spec").with("template").with("metadata").with("labels").put(key,value);
    }

    private void addAnnotation(ObjectNode spec,String key,String value) {
        spec.with("spec").with("template").with("metadata").with("annotations").put(key,value);
    }

    @PostConstruct
    private void init() {
        metaAppId = metaPropertiesService.getMetapropertykeyByName(CAS_USAGE, MetaPropertyTarget.APPLICATION);
    }
}

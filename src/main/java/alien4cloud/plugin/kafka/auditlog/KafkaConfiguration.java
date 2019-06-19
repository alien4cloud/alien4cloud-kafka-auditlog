package alien4cloud.plugin.kafka.auditlog;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Getter
@Setter
@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfiguration {

    private String bootstrapServers;

    private String site;

    private String topic;
}

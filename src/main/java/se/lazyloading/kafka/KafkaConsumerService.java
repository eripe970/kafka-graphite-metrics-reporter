package se.lazyloading.kafka;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Properties;
import kafka.admin.AdminClient;
import kafka.coordinator.GroupOverview;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import se.lazyloading.common.Configuration;
import static scala.collection.JavaConversions.asJavaIterable;

public class KafkaConsumerService {

    private final AdminClient adminClient;
    private final KafkaConsumer consumer;

    public KafkaConsumerService(Configuration configuration) {

        adminClient = createAdminClient(configuration);
        consumer = createNewConsumer(configuration);
    }

    private List<String> getConsumerGroups() {

        List<String> groups = Lists.newArrayList();

        for (GroupOverview group : asJavaIterable(adminClient.listAllConsumerGroupsFlattened())) {
            groups.add(group.groupId());
        }

        return groups;
    }

    private List<Metric> getGroupMetrics(String group) {

        List<Metric> metrics = Lists.newArrayList();

        Iterable<AdminClient.ConsumerSummary> summaries = asJavaIterable(adminClient.describeConsumerGroup(group));

        for (AdminClient.ConsumerSummary summary : summaries) {

            Iterable<TopicPartition> topicPartitions = asJavaIterable(summary.assignment());

            for (TopicPartition topicPartition : topicPartitions) {

                long currentOffset = getCurrentOffset(topicPartition.topic(), topicPartition.partition());
                long endOffset = getEndOffset(topicPartition.topic(), topicPartition.partition());

                Metric metric = new Metric(group, topicPartition.topic(), topicPartition.partition(), currentOffset,
                        endOffset);

                metrics.add(metric);

            }
        }

        return metrics;
    }

    public List<Metric> getMetrics() {
        List<Metric> metrics = Lists.newArrayList();

        for (String group : getConsumerGroups()) {
            metrics.addAll(getGroupMetrics(group));
        }

        return metrics;
    }

    private long getEndOffset(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        consumer.assign(Lists.newArrayList(topicPartition));
        consumer.seekToEnd(topicPartition);

        return consumer.position(topicPartition);
    }

    private long getCurrentOffset(String topic, int partition) {

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return consumer.committed(topicPartition).offset();
    }

    private KafkaConsumer createNewConsumer(Configuration configuration) {
        Properties properties = new Properties();

        StringDeserializer deserializer = new StringDeserializer();

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getKafkaBootstrapServer());

        return new KafkaConsumer(properties);
    }

    private AdminClient createAdminClient(Configuration configuration) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getKafkaBootstrapServer());

        return AdminClient.create(properties);
    }

}

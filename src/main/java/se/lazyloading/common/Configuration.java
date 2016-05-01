package se.lazyloading.common;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;
import kafka.utils.VerifiableProperties;

public class Configuration {

    private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";

    private final int graphitePort;
    private final String graphitePrefix;
    private final boolean enabled;
    private String graphiteHost;

    private final String kafkaBootstrapServer;

    public Configuration(VerifiableProperties props) {

        graphiteHost = props.getString("kafka.metrics.graphite.host");
        graphitePort = props.getInt("kafka.metrics.graphite.port");
        graphitePrefix = props.getString("kafka.metrics.graphite.prefix");
        enabled = props.getBoolean("kafka.metrics.graphite.enabled", true);

        kafkaBootstrapServer = props.getString("kafka.metrics.bootstrap.server", KAFKA_BOOTSTRAP_SERVER);

        Preconditions.checkArgument(graphiteHost != null, "Specify graphite host");
        Preconditions.checkArgument(graphitePort > 0, "Specify graphite port");
    }


    public InetSocketAddress getGraphiteAddress(){
        return new InetSocketAddress(graphiteHost, graphitePort);
    }

    public String getGraphitePrefix() {
        return graphitePrefix;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("graphitePort", graphitePort)
                .add("graphitePrefix", graphitePrefix)
                .add("enabled", enabled)
                .add("graphiteHost", graphiteHost)
                .toString();
    }

    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }
}

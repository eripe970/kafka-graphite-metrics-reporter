package se.lazyloading.graphite;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import java.util.SortedMap;
import javax.net.SocketFactory;
import se.lazyloading.common.Configuration;
import se.lazyloading.kafka.KafkaConsumerService;
import se.lazyloading.kafka.Metric;

public class GraphiteService {

    private final Configuration configuration;
    private final GraphiteReporter graphiteReporter;
    private final MetricRegistry metricRegistry;

    public GraphiteService(Configuration configuration) {

        this.metricRegistry = new MetricRegistry();
        this.configuration = configuration;

        Graphite graphite = new Graphite(configuration.getGraphiteAddress(), SocketFactory.getDefault());

        graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith(configuration.getGraphitePrefix())
                .build(graphite);
    }

    public void run() {

        collectLagMetrics();

        // Collect other interesting metrics

        graphiteReporter.report();
    }

    private void collectLagMetrics() {

        KafkaConsumerService service = new KafkaConsumerService(configuration);

        for (Metric metric : service.getMetrics()) {

            SortedMap<String, Histogram> metrics = metricRegistry.getHistograms();

            Histogram histogram;

            if (!metrics.containsKey(metric.getLagKey())) {
                histogram = metricRegistry.histogram(metric.getLagKey());
            } else {
                histogram = metrics.get(metric.getLagKey());
            }

            histogram.update(metric.getLag());
        }
    }
}

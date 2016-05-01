package se.lazyloading;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import se.lazyloading.common.Configuration;
import se.lazyloading.graphite.GraphiteService;

public class KafkaGraphiteMetricsReporter implements KafkaMetricsReporter {

    private static Logger log = Logger.getLogger(KafkaGraphiteMetricsReporter.class);
    private boolean initialized;

    public void init(VerifiableProperties props) {

        if (initialized) {
            return;
        }

        Configuration configuration = new Configuration(props);

        log.info(String.format("Parsed metric context %s", configuration));

        GraphiteService graphiteService = new GraphiteService(configuration);

        graphiteService.run();

        initialized = true;
    }

}

# WIP WIP WIP kafka-graphite-metrics-reporter
A kafka metric reporter for kafka `0.9.0.*` that will send consumer lag metrics to a configured Graphite server.


Configuration
---
Drop the jar in the `kafka/lib/` or make it available in the classpath. Below is the minimum configuration that is required

    kafka.metrics.reporters=se.lazyloading.KafkaGraphiteMetricsReporter
    kafka.metrics.graphite.host=localhost
    kafka.metrics.graphite.host=2003


Build
---
Build using 

    mvn clean package



Credit
---
Code is inspired by https://github.com/krux/kafka-metrics-reporter/ a kafka metric reporter for kafka for the old consumer api.

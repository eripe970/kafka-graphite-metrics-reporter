package se.lazyloading.kafka;

public class Metric {

    private String topic;
    private int partition;
    private String group;
    private long currentOffset;
    private long endOffset;

    public Metric(String group, String topic, int partition, long currentOffset, long endOffset) {

        this.group = group;
        this.currentOffset = currentOffset;
        this.endOffset = endOffset;
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String getGroup() {
        return group;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getLag() {
        return endOffset - currentOffset;
    }

    public String getLagKey() {
        return String.format("%s.%s.%s.lag", group, topic, partition);
    }
}

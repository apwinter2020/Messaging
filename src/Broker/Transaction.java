package Broker;

import java.util.Queue;

public class Transaction {

    private TopicWriter topicWriter;
    private String producerName;
    private Queue<Integer> values;

    Transaction(TopicWriter topicWriter, String producerName) {
        this.topicWriter = topicWriter;
        this.producerName = producerName;
    }

    void put(int value) {
        values.add(value);
    }

    void commit() {
        while(!values.isEmpty()) {
            topicWriter.writeValue(values.remove());
        }
    }
}

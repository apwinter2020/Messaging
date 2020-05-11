package Broker;

import java.util.LinkedList;
import java.util.Queue;

public class Transaction {

    private TopicWriter topicWriter;
    private String producerName;
    private Queue<Integer> values;

    Transaction(TopicWriter topicWriter, String producerName) {
        this.topicWriter = topicWriter;
        this.producerName = producerName;
        values = new LinkedList<>();
    }

    void put(int value) {
        values.add(value);
    }

    void commit() {
        topicWriter.writeValue(0);
        while(!values.isEmpty()) {
            topicWriter.writeValue(values.remove());
        }
        topicWriter.writeValue(-1);
    }
}

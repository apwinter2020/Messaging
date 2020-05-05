package Consumer;

import Broker.MessageBroker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class ConsumerGroup extends Thread{
    private ArrayList<Consumer> consumers;
    private File consumerGroupFile;
    private MessageBroker messageBroker;
    private String topicName;
    private String groupName;
    private PrintWriter printWriter;

    public ConsumerGroup(MessageBroker messageBroker, String topicName, String groupName, File consumerGroupFile, int numberOfConsumers) {
        this.messageBroker = messageBroker;
        this.consumerGroupFile = consumerGroupFile;
        this.topicName = topicName;
        this.groupName = groupName;
        consumers = new ArrayList<>();
    }

    private void initialize(int numberOfConsumers) {
        for(int i=0;i<numberOfConsumers;i++) {
            String consumerName = groupName + "_" + i;
            consumers.add(new Consumer(messageBroker, topicName, this, consumerName));
        }
    }

    public void run() {
        try {
            printWriter = new PrintWriter(consumerGroupFile);

            for(Consumer consumer: consumers) {
                consumer.start();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        printWriter.close();
    }

    public void performAction(Consumer consumer, int value) {
        printWriter.println("Consumer with name " + consumer.getConsumerName() + " read the value " + value);
        printWriter.flush();
    }

    public String getGroupName() {
        return groupName;
    }
}


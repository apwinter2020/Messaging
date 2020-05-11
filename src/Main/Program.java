package Main;

import Broker.MessageBroker;
import Consumer.ConsumerGroup;
import Producer.ProducerGroup;

import java.io.File;

public class Program {

    private String[] args;
    private MessageBroker messageBroker;

    Program(String args[]) {
        this.args = args;
        messageBroker = new MessageBroker();
    }

    private File getProducerGroupDirectory() {
        File producerDirectory = new File("data/");
        if(args.length>0) {
            producerDirectory = new File(args[0]);
        }

        return producerDirectory;
    }

    void run() {
        File producerGroupDirectory = getProducerGroupDirectory();
        String topicName = producerGroupDirectory.getName();

        File consumerGroupFile = new File(topicName+".txt");
        String consumerGroupName = topicName + "Readers";
        int numberOfConsumers = 10;

        ProducerGroup producerGroup = new ProducerGroup(messageBroker, producerGroupDirectory, topicName);
        ConsumerGroup consumerGroup = new ConsumerGroup(messageBroker, topicName, consumerGroupName, consumerGroupFile, numberOfConsumers);

        producerGroup.start();
        consumerGroup.start();

        while(producerGroup.isAlive() || consumerGroup.isAlive()) {
            try {
                producerGroup.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


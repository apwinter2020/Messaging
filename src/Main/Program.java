package Main;

import Broker.MessageBroker;
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
        ProducerGroup producerGroup = new ProducerGroup(messageBroker, producerGroupDirectory, producerGroupDirectory.getName());
        producerGroup.start();

        while(producerGroup.isAlive()) {
            try {
                producerGroup.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


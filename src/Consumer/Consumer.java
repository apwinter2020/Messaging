package Consumer;

import Broker.MessageBroker;
import Broker.NoSuchTopicException;

public class Consumer extends Thread {

    private MessageBroker messageBroker;
    private ConsumerGroup consumerGroup;
    private String topicName;
    private String consumerName;

    Consumer(MessageBroker messageBroker, String topicName, ConsumerGroup consumerGroup, String consumerName) {
        this.messageBroker = messageBroker;
        this.topicName = topicName;
        this.consumerGroup = consumerGroup;
        this.consumerName = consumerName;
    }

    public int get() throws NoSuchTopicException {
        return messageBroker.get(topicName, consumerGroup.getGroupName(), consumerName);
    }

    public void run() {
        while(true) {
            try {
                consumerGroup.performAction(this, get());

            } catch (NoSuchTopicException e) {
                e.printStackTrace();
            }
        }
    }

    public String getConsumerName() {
        return consumerName;
    }
}

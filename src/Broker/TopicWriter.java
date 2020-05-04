package Broker;

import java.io.RandomAccessFile;
import java.util.HashMap;

public class TopicWriter {
    RandomAccessFile buffer;

    private Topic topic;
    private HashMap<String, Transaction> transactions;

    TopicWriter(Topic topic) {
        this.topic=topic;
        transactions = new HashMap<>();
    }

    public void put(String producerName, int value) {
        if(value <= 0) {
            handleTransactionOperation(producerName, value);
        }
        else {
            handleInsertOperation(producerName, value);
        }
    }

    private void handleTransactionOperation(String producerName, int value) {
        switch (value) {
            case 0:
                startTransaction(producerName);
                break;
            case -1:
                commitTransaction(producerName);
                break;
            case -2:
                cancelTransaction(producerName);
        }
    }

    private void handleInsertOperation(String producerName, int value) {
        if(transactions.containsKey(producerName)) {
            transactions.get(producerName).put(value);
        }
        else {
            writeValue(value);
        }
    }

    private void addTransaction(String producerName) {
        transactions.put(producerName, new Transaction(this, producerName));
    }

    /**
     * This method is used to start a transaction for putting a transaction of values inside the buffer.
     * @return Nothing.
     */
    private void startTransaction(String producerName) {
        if(transactions.containsKey(producerName)) {
            //To Do - Log the problem in finalizing previous transaction.
            commitTransaction(producerName);
            transactions.remove(producerName);
        }
        addTransaction(producerName);
    }

    /**
     * This method is used to end the transaction for putting a its values inside the file.
     * @return Nothing.
     */
    private void commitTransaction(String producerName) {
        if(transactions.containsKey(producerName)) {
            transactions.get(producerName).commit();
        }
        else {
            //To Do - Log the problem in committing a non-existing transaction.
        }
    }

    /**
     * This method is used to cancel a transaction.
     * @return Nothing.
     */
    private void cancelTransaction(String producerName) {
        if(transactions.containsKey(producerName)) {
            transactions.remove(producerName);
        }
        else {
            //To Do - Log the problem in canceling a non-existing transaction.
        }
    }

    public void writeValue(int value) {
        //To Do - Put the given value at the end of the topicFile
    }
}

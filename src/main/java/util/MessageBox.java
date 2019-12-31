package util;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A MessageBox is used by worker threads to receive messages and process messages
 * There are two methods:
 * 1) addMessage(String msg), which adds a message to this box to be processed
 * 2) pollMessage(), which removes the first message of this box in FIFO order
 *
 * NOTE: pollMessage() should only be called by subclasses that extend this interface
 */

public class MessageBox extends Thread {

    private Queue<String> messages = new LinkedList<>();
    private ReentrantLock messageLock = new ReentrantLock();
    private Condition waitForMessage = messageLock.newCondition();

    /**
     * Adds a message to message queue for this worker to process
     * @param msg
     */

    public void addMessage(String msg) {
        try {
            messageLock.lock();
            messages.add(msg);
            waitForMessage.signal();

        } finally {
            messageLock.unlock();
        }
    }

    /**
     * Polls the first message of this worker's message queue
     * @return
     */

    protected String pollMessage() {
        String msg = "";

        /**
         * BEGIN: Atomically read the first element of the queue
         */

        try {
            messageLock.lock();

            while (messages.size() == 0) { // while there is no message, go to sleep
                waitForMessage.await();
            }

            msg = messages.poll(); // poll the first message

        } catch (InterruptedException e) {
            e.printStackTrace();

        } finally {
            messageLock.unlock();
        }

        /**
         * END: Atomically read the first element of the queue
         */

        return msg;
    }
}

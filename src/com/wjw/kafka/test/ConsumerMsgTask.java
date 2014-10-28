package com.wjw.kafka.test;

/**
 * Created by wjw on 14-10-19.
 */
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerMsgTask implements Runnable {

    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            String tmp = new String(it.next().message());
            if((tmp.split(",").length) != 9) {
                System.out.println(tmp);
            } else {
                System.out.println("N");
            }
        }

        //System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
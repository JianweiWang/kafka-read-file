/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.kafka.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import reader.MyFileReader;
import reader.MyReader;

import java.io.IOException;
import java.util.*;

public class ProducerInfo {
    static ProducerConfig config = null;

    static Producer<String, String> producer = null;
    static Properties props = new Properties();
    public static void init() {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","192.168.0.8:9092");
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }
    public static void main(String[] args) {
        //long events = Long.parseLong(args[0]);


        // props.put("zookeeper.connect", "127.0.0.1:2181");

//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
//        props.put("request.required.acks", "1");


//        String[] sentences = {
//                "the cow jumped over the moon",
//                "an apple a day keeps the doctor away",
//                "four score and seven years ago",
//                "snow white and the seven dwarfs",
//                "i am at two with nature"
//        };
//        for (long nEvents = 0; nEvents < 10; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//        }
//        producer.close();

//        int length = sentences.length;
//        int i = 1000000;
//        while(i > 1) {
//            try {
//                Thread.sleep(10L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            Random random = new Random();
//            String msg = sentences[random.nextInt(length)];
//            //long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//            i--;
//        }
//        producer.close();

//        String filePath2 = "/home/wjw/storm/experiment/data/uncompressed/2";
//        String filePath3 = "/home/wjw/storm/experiment/data/uncompressed/3";
//        MyRunnable runnable1 = new MyRunnable(filePath1);
//        Thread thread1 = new Thread(runnable1);
//        MyRunnable runnable2 = new MyRunnable(filePath2);
//        Thread thread2 = new Thread(runnable2);
//        MyRunnable runnable3 = new MyRunnable(filePath3);
//        Thread thread3 = new Thread(runnable3);
//        thread3.start();
//        thread1.start();
//        thread2.start();
        sendMsgMultipleThreads(8);

    }

    public static void sendMsgMultipleThreads(int threadNum) {
        String filePath1 = "/home/wjw/storm/experiment/data/id/info";
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(new MyRunnable(filePath1 + i + ".txt")));
        }

        for (int i = 0; i < threadNum; i++) {
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            threads.get(i).start();
        }

    }

    public static void readFiles(String path) {
        long start = System.currentTimeMillis();
        MyFileReader myFileReader = new MyFileReader();
        try {
            myFileReader.findFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Iterator<String> iterator = myFileReader.getFileList().iterator();
        while(iterator.hasNext()) {
            String file = iterator.next();
            Iterator<String> content = MyReader.readFileByLines(file).iterator();
             while(content.hasNext()) {
                 long finish = System.currentTimeMillis();
                 long delta = finish -start;

                 if(delta < (20 * 60 * 1000)) {
                     sleep(1L);

                 } else if (delta >= (20 * 60 * 1000) && delta < (40 * 60 * 1000)) {
                     sleep(3L);
                 } else if(delta >= ( 40* 60 * 1000) && delta < (60 * 60 * 1000)) {
                     sleep(5L);
                 } else {
                     start = System.currentTimeMillis();
                 }

                 if(content.next().split(",").length == 9)
                 sendMsg(content.next(),"info");
                 //System.out.println(content.next());
             }
        }

    }
    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static void sendMsg(String msg) {
        //long runtime = new Date().getTime();
//        Random rnd = new Random();
//        String ip = "192.168.2." + rnd.nextInt(255);
//        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//        producer.send(data);
        sendMsg(msg,"page_visits");
    }
    public static void sendMsg(String msg, String topic) {
        Random rnd = new Random();
        String ip = "192.168.2." + rnd.nextInt(255);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
        producer.send(data);
    }
    static class MyRunnable implements Runnable {
        String filePath;
        MyRunnable(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void run() {
            init();
            readFiles(filePath);
            producer.close();
        }
    }

}



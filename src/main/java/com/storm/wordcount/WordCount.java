package com.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import java.util.Map;

public class WordCount {

    public static class Generator extends ShellSpout implements IRichSpout {

        public Generator() {
            super("cmd", "/k", "CALL", "StormSimple.exe", "generator");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Splitter extends ShellBolt implements IRichBolt {

        public Splitter() {
            super("cmd", "/k", "CALL", "StormSimple.exe", "splitter");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Counter extends ShellBolt implements IRichBolt {

        public Counter(){
            super("cmd", "/k", "CALL", "StormSimple.exe", "counter");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("generator", new Generator(), 1);

        builder.setBolt("splitter", new Splitter(), 1).fieldsGrouping("generator", new Fields("word"));
        builder.setBolt("counter", new Counter(), 1).fieldsGrouping("splitter", new Fields("word", "count"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCount", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}

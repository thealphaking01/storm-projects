package com.word_count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws InterruptedException {
        SentenceSpout sentenceSpout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout);
        topologyBuilder.setBolt(SPLIT_BOLT_ID, splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLT_ID, wordCountBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        topologyBuilder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        config.setDebug(false);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
        TimeUnit.SECONDS.sleep(10);
        localCluster.killTopology(TOPOLOGY_NAME);
        localCluster.shutdown();
    }
}

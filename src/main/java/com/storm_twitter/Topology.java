package com.storm_twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class Topology {
    static final String TOPOLOGY_NAME = "apache-storm-twitter-sentiment-analysis";

    public static void main(String[] args) {
        Set<String> languages = new HashSet<String>(Arrays.asList(new String[] {"en"}));
        Set<String> hashtags = new HashSet<String>(Arrays.asList(new String[] {
                "brexit", "barackobama", "hillaryclinton", "donaldtrump"
        }));
        Set<String> mentions = new HashSet<String>(Arrays.asList(new String[] {
                "barackobama", "hillaryclinton", "realdonaldtrump"
        }));

        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("MentionBolt", new MentionBolt(languages, hashtags, mentions)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("TweetWordSplitterBolt", new TwitterWordSplitter(3)).shuffleGrouping("MentionBolt");
        b.setBolt("SentimentAnalysisBolt", new TwitterSentimentAnalysis(10, 10 * 60)).shuffleGrouping("TweetWordSplitterBolt");

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });

    }
}

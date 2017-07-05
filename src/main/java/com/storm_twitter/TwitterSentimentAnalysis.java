package com.storm_twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class TwitterSentimentAnalysis extends BaseRichBolt {

    public TwitterSentimentAnalysis(long clearIntervalSec, long logIntervalSec) {
        this.clearIntervalSec = clearIntervalSec;
        this.logIntervalSec = logIntervalSec;
    }

    private final long clearIntervalSec;
    private final long logIntervalSec;
    private static final Logger logger = LoggerFactory.getLogger(TwitterSentimentAnalysis.class);
    private Map<String, Long> sentimentScoreCounter;
    private long lastLogTime;
    private long lastClearTime;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.sentimentScoreCounter = new HashMap<String, Long>();
        this.lastLogTime = System.currentTimeMillis();
        this.lastClearTime = System.currentTimeMillis();
    }

    public void execute(Tuple tuple) {
        String mention = (String) tuple.getValueByField("mention");
        String word = (String) tuple.getValueByField("word");

        Long sentimentScore = sentimentScoreCounter.get(mention);
        if (sentimentScore == null) {
            sentimentScore = 0L;
        }

        if (DictonaryWords.POSITIVE_WORDS.contains(word)) {
            sentimentScore += 1;
        } else if (DictonaryWords.NEGATIVE_WORDS.contains(word)) {
            sentimentScore -= 1;
        }

        sentimentScoreCounter.put(mention, sentimentScore);

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            System.out.println("\n\n");
            publishList();
            lastLogTime = now;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void publishList() {
        // Output list:
        for (Map.Entry<String, Long> entry : sentimentScoreCounter.entrySet()) {
            logger.info(new StringBuilder("mention - ").append(entry.getKey()).append(" :: score - ").append(entry.getValue()).toString());
        }

        // Clear list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            sentimentScoreCounter.clear();
            lastClearTime = now;
        }
    }
}

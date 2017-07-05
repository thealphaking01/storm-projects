package com.storm_twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class TwitterWordSplitter extends BaseRichBolt{
    private OutputCollector outputCollector;
    private final int minWordLength;

    public TwitterWordSplitter(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String mention = (String) tuple.getValueByField("mention");
        String text = (String) tuple.getValueByField("text");

        String[] words = text.split(" ");
        for(String word: words) {
            if(word.length() > minWordLength) {
                outputCollector.emit(new Values(mention, word));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mention", "word"));
    }
}

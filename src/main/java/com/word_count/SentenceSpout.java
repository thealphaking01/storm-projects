package com.word_count;
import com.sun.javafx.util.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.util.Map;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private String[] sentences = {
		 "my dog has fleas",
		 "i like cold beverages",
		 "the dog ate my homework",
		 "don't have a cow man",
		 "i don't think i like fleas"
    };
    private int index = 0;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        this.spoutOutputCollector.emit(new Values(sentences[index]));
        index = (index + 1)%(sentences.length);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
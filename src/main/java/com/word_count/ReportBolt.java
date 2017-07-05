package com.word_count;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanup() {
        System.out.println("----Final Counts----");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key: keys) {
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("------------");
    }
}

package com.storm_twitter;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class TwitterSampleSpout extends BaseRichSpout{
    private SpoutOutputCollector spoutOutputCollector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.queue = new LinkedBlockingQueue<Status>(1000);

        StatusListener statusListener = new StatusListener() {
            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception e) {

            }
        };

        TwitterStreamFactory factory = new TwitterStreamFactory();
        twitterStream = factory.getInstance();
        twitterStream.addListener(statusListener);
        twitterStream.sample();
    }

    public void nextTuple() {
        Status status = this.queue.poll();
        if(status == null) {
            Utils.sleep(50);
        } else {
            spoutOutputCollector.emit(new Values(status));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}

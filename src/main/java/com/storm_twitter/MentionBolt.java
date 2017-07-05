package com.storm_twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import java.util.Map;
import java.util.Set;

/**
 * Created by abdur.rahman on 05/07/17.
 */
public class MentionBolt extends BaseRichBolt{
    private OutputCollector outputCollector;
    private Set<String> languages;
    private Set<String> hashtags;
    private Set<String> mentions;

    public MentionBolt(Set<String> languages, Set<String> hashtags, Set<String> mentions) {
        this.languages = languages;
        this.hashtags = hashtags;
        this.mentions = hashtags;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();

        if(!this.languages.contains(lang)) {
            return;
        }

        UserMentionEntity mentions[] = tweet.getUserMentionEntities();
        for(UserMentionEntity mention: mentions) {
            String mentionUserText = mention.getName().toLowerCase();

            if (!this.mentions.contains(mentionUserText)) {
                continue;
            }

            outputCollector.emit(new Values(mentionUserText, text));
        }

        HashtagEntity hashtags[] = tweet.getHashtagEntities();
        for (HashtagEntity hashtag : hashtags) {
            String hashtagText = hashtag.getText().toLowerCase();

            if (!this.hashtags.contains(hashtagText)) {
                continue;
            }

            outputCollector.emit(new Values(hashtagText, text));
        }
     }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mention", "text"));
    }
}

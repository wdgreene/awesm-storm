package awesm.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class RandomClickEventSpout implements IRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;    
    
    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[] {
	            "{'created_at': '2011-01-20 10:00:00', " + 
	            "'account_id': 7, " + 
	            "'http_user_agent': 'Mozilla/5.0 (compatible; MSIE 6.0; Windows NT 5.1)', " +
	            "'remote_addr': '151.38.39.114', " +
	            "'bot_flag': 0, " + 
	            "'referrer': 'http://twitter.com/123456'}",
                "{'created_at': '2011-01-20 10:00:00', " +
	            "'account_id': 7, " +
	            "'http_user_agent': 'AppEngine-Google', " +
	            "'remote_addr': '64.81.104.131', " +
	            "'bot_flag': 0, " +
	            "'referrer': 'http://twitter.com/123456'}"
            };
        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence));
    }
    
    @Override
    public void close() {        
    }


    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }
    
}
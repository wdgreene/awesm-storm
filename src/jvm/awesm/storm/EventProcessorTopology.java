package awesm.storm;

import awesm.storm.bolt.BotFlagBolt;
import awesm.storm.bolt.GeoBolt;
import awesm.storm.bolt.ThrottlingGeoCountryBolt;
import awesm.storm.scheme.StringScheme;
import awesm.storm.bolt.ReferrerBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.spout.AMQPSpout;

public class EventProcessorTopology {
    
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new AMQPSpout("127.0.0.1", 5672, "guest", "guest", "/", new SharedQueueWithBinding("fan_2", "test_fanout", "#"), new StringScheme()));
        builder.setBolt("bot", new BotFlagBolt())
        		.shuffleGrouping("spout");
        builder.setBolt("referrer", new ReferrerBolt())
        		.shuffleGrouping("bot");
        builder.setBolt("geo", new GeoBolt())
        		.shuffleGrouping("referrer");
        builder.setBolt("geo-throttle", new ThrottlingGeoCountryBolt())
        		.shuffleGrouping("geo");

        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(1);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("event-processor", conf, builder.createTopology());
            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}

package awesm.storm;

import awesm.storm.bolt.BotFlagBolt;
import awesm.storm.bolt.GeoBolt;
import awesm.storm.bolt.ThrottlingGeoCountryBolt;
import awesm.storm.spout.RandomClickEventSpout;
import awesm.storm.bolt.ReferrerBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class EventProcessorTopology {
    
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new RandomClickEventSpout(), 1);

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

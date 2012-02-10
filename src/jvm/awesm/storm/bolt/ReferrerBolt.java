package awesm.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import backtype.storm.tuple.Values;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.net.URL;
import java.util.Map;

public class ReferrerBolt implements IRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("ReferrerBolt: " + tuple);

        String jsonText = (String)tuple.getValue(0);
        jsonText = jsonText.replaceAll("'", "\"");

        ObjectMapper m = new ObjectMapper();
        String newJsonText = "";

		try {
			JsonNode rootNode = m.readValue(jsonText, JsonNode.class);

			JsonNode referrerNode = rootNode.path("referrer");
	        String referrer = referrerNode.getTextValue();

	        System.out.println("Referrer is " + referrer);

            URL referrerURL = new URL(referrer);

            String hostname =  referrerURL.getHost();
            String protocol = referrerURL.getProtocol();
            String referrerDomain = protocol + "://" + hostname;

            System.out.println("Referrer Domain: " + referrerDomain);

	        ((ObjectNode)rootNode).put("referrer_domain", referrerDomain);

	        newJsonText = m.writeValueAsString(rootNode);

	        System.out.println("Revised json is " + newJsonText);

		} catch (Exception e) {
			System.out.println("Exception is " + e.getMessage());
		}


        _collector.emit(tuple, new Values(newJsonText));

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("referrer-processed-event"));
    }

}

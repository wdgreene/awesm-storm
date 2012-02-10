package awesm.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;

import backtype.storm.tuple.Values;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class BotFlagBolt implements IRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("BotFlagBolt: " + tuple);
        
        String jsonText = (String)tuple.getValue(0);     
        jsonText = jsonText.replaceAll("'", "\"");      
        
        ObjectMapper m = new ObjectMapper();
        String newJsonText = "";

		try {
			JsonNode rootNode = m.readValue(jsonText, JsonNode.class);
			
			JsonNode createdAtNode = rootNode.path("created_at");
	        JsonNode botFlagNode = rootNode.path("bot_flag");
	        
	        String createdAt = createdAtNode.getTextValue();
	        int botFlag = botFlagNode.getIntValue();
	        
	        System.out.println("CreatedAt is " + createdAt);
	        System.out.println("BotFlag is " + botFlag);
	        
	        ((ObjectNode)rootNode).put("new_bot_flag", 1);
	    
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
        declarer.declare(new Fields("bot-processed-event"));
    }

}

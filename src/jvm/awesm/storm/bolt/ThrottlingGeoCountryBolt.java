package awesm.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import backtype.storm.tuple.Values;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Region;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class ThrottlingGeoCountryBolt implements IRichBolt {

    OutputCollector _collector;
    Map<String, Integer> _counts = new HashMap<String, Integer>();
    long _lastInsertTime = System.currentTimeMillis();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("ThrottlingGeoCountryBolt: " + tuple);

        String jsonText = (String)tuple.getValue(0);
        jsonText = jsonText.replaceAll("'", "\"");

        ObjectMapper m = new ObjectMapper();
        String newJsonText = "";

		try {
			JsonNode rootNode = m.readValue(jsonText, JsonNode.class);

            // ignore values with new_bot_flag != 0
            JsonNode botFlagNode = rootNode.path("new_bot_flag");
            int botFlag = botFlagNode.getIntValue();
            if (botFlag != 0)
            {
                System.out.println("Skipping increment because it's a bot");
                _collector.ack(tuple);
                return;

            }

            // create key
            JsonNode accountIDNode = rootNode.path("account_id");
	        int accountID = accountIDNode.getIntValue();
            JsonNode redirectionIDNode = rootNode.path("redirection_id");
            long redirectionID = redirectionIDNode.getLongValue();
            JsonNode countryCodeNode = rootNode.path("geo_country_code");
            String countryCode = countryCodeNode.getTextValue();

            String key = accountID + ":" + redirectionID + ":" + countryCode;

	        System.out.println("Key is " + key);

            // increment keys
            Integer count = _counts.get(key);
            if(count==null) count = 0;
            count++;
            _counts.put(key, count);

            // occasionally insert
            if (isTimeToInsert())
            {
                System.out.println("Time to insert data");
                System.out.println("Counts are " + _counts);
                _lastInsertTime = System.currentTimeMillis();                
            }




		} catch (Exception e) {
			System.out.println("Exception is " + e.getMessage());
		}

        _collector.ack(tuple);

    }


    private boolean isTimeToInsert()
    {
        return (System.currentTimeMillis() - _lastInsertTime) > 2000;            
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}

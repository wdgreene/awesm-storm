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

import java.io.IOException;
import java.util.Map;

public class GeoBolt implements IRichBolt {

    OutputCollector _collector;
    LookupService _geoLookup;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        try {
            _geoLookup = new LookupService("/home/wdgreene/git_repo/awesm-storm/src/jvm/resources/GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException e)
        {
            System.out.println("TROUBLE STARTING GEO LOOKUP SERVICE");
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("GeoBolt: " + tuple);

        String jsonText = (String)tuple.getValue(0);
        jsonText = jsonText.replaceAll("'", "\"");

        ObjectMapper m = new ObjectMapper();
        String newJsonText = "";

		try {
			JsonNode rootNode = m.readValue(jsonText, JsonNode.class);

			JsonNode ipAddressNode = rootNode.path("remote_addr");
	        String ipAddress = ipAddressNode.getTextValue();

	        System.out.println("IP Address is " + ipAddress);

            //Country country = _geoLookup.getCountry(ipAddress);
            //System.out.println("Country is: " + country + " " + country.getCode() + " " + country.getName());
	        //((ObjectNode)rootNode).put("geo_country_code", country.getCode());

            Location location = _geoLookup.getLocation(ipAddress);
            
            ((ObjectNode)rootNode).put("geo_country_code", location.countryCode);
            ((ObjectNode)rootNode).put("geo_country", location.countryName);
            ((ObjectNode)rootNode).put("geo_region", location.region);
            ((ObjectNode)rootNode).put("geo_city", location.city);

	        newJsonText = m.writeValueAsString(rootNode);

	        System.out.println("Revised json is " + newJsonText);

		} catch (Exception e) {
			System.out.println("Exception is " + e.getMessage());
		}


        _collector.emit(tuple, new Values(newJsonText));

    }

    @Override
    public void cleanup() {
        _geoLookup.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("geo-processed-event"));
    }

}

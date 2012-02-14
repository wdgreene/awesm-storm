package awesm.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

public class ThrottlingGeoCountryBolt implements IRichBolt {

    OutputCollector _collector;
    SimpleJdbcTemplate _simpleJdbc;
    Map<String, Integer> _counts = new HashMap<String, Integer>();
    long _lastInsertTime = System.currentTimeMillis();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        ApplicationContext springContext = new ClassPathXmlApplicationContext("beans.xml");
        DataSource dataSource = (DataSource) springContext.getBean("dataSource");
        _simpleJdbc = new SimpleJdbcTemplate(dataSource);
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

            String key = toKey(accountID, redirectionID, countryCode);
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


                // pull everything out of the incrementors
                Set<String> countKeys = _counts.keySet();
                for (String countKey : countKeys)
                {
                    int incValue = _counts.get(countKey);
                    Key keyValues = fromKey(countKey);

                    String query = "INSERT INTO geo_country_totals (account_id, redirection_id, country_code, clicks)" +
                            "VALUES (?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE clicks = clicks + ?";
                    _simpleJdbc.update(query, keyValues.getAccountID(), keyValues.getRedirectionID(), keyValues.getCountryCode(), incValue, incValue);

                    _counts.remove(countKey);
                }
            }

		} catch (Exception e) {
			System.out.println("Exception is " + e.getMessage());
		}

        _collector.ack(tuple);

    }

    public static class Key {

        private int _accountID;
        private long _redirectionID;
        private String _countryCode;

        public Key(int accountID, long redirectionID, String countryCode)
        {
            _accountID = accountID;
            _redirectionID = redirectionID;
            _countryCode = countryCode;
        }

        public int getAccountID()
        {
            return _accountID;
        }

        public long getRedirectionID()
        {
            return _redirectionID;
        }

        public String getCountryCode()
        {
            return _countryCode;
        }
    }

    public static Key fromKey(String strKey)
    {
        String[] pieces = strKey.split(":");
        return new Key(Integer.parseInt(pieces[0]), Long.parseLong(pieces[1]), pieces[2]);
    }

    public static String toKey(int accountID, long redirectionID, String countryCode)
    {
        return accountID + ":" + redirectionID + ":" + countryCode;
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

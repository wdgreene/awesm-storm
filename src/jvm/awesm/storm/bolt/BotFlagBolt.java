package awesm.storm.bolt;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class BotFlagBolt extends ShellBolt implements IRichBolt {

    public BotFlagBolt() {
        super("php", "botflag.php");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("bot-processed-event"));
    }
}

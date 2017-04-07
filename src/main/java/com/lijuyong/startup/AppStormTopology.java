package com.lijuyong.startup;

import com.lijuyong.startup.WordCount.bolt.WordCountBolt;
import com.lijuyong.startup.WordCount.bolt.WordNormBolt;
import com.lijuyong.startup.WordCount.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class AppStormTopology
{
    private static TopologyBuilder builder = new TopologyBuilder();
    public static void main( String[] args )
    {
        System.out.println( "这是一个伟大的开端!" );
        Config config = new Config();

        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("WordNormalizer", new WordNormBolt(), 10).shuffleGrouping(
                "RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(), 20).fieldsGrouping("WordNormalizer",
                new Fields("word"));

        config.setDebug(true);
        try{
            StormSubmitter.submitTopology("wordcount",config,builder.createTopology());

        }
        catch (Exception exc)
        {
            System.out.println(exc.getMessage());
        }


    }
}

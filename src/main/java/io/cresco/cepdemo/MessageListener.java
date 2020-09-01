package io.cresco.cepdemo;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Random;

public class MessageListener implements Runnable  {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;

    public MessageListener(PluginBuilder plugin) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        gson = new Gson();
        logger.info("Mode=1");

    }

    public void run() {

        try {
            while (!plugin.isActive()) {
                logger.error("Listener: Wait until plugin is active...");
                Thread.sleep(1000);
            }

            //setup stuffs
            createIt();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void createIt() {

        String inputStreamName = "UserStream";
        String inputStreamDescription = "source string, urn string, metric string, ts long, value double";
        //"source":"mysource","urn":"myurn","metric":"mymetric","ts":1576355714623,"value":0.3142739729174573

        String outputStreamName = "BarStream";
        String outputStreamDescription = "source string, avgValue double";

        javax.jms.MessageListener ml = new javax.jms.MessageListener() {
            public void onMessage(Message msg) {
                try {


                    if (msg instanceof TextMessage) {

                        System.out.println(" SEND MESSAGE:" + ((TextMessage) msg).getText());

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"stream_name='" + inputStreamName + "'");

        javax.jms.MessageListener ml2 = new javax.jms.MessageListener() {
            public void onMessage(Message msg) {
                try {


                    if (msg instanceof TextMessage) {

                        System.out.println(" OUTPUT EVENT:" + ((TextMessage) msg).getText());

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml2,"stream_name='" + outputStreamName + "'");

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from UserStream#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into BarStream; ";
        //public String createCEP(String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {
        String cepid = plugin.getAgentService().getDataPlaneService().createCEP(inputStreamName, inputStreamDescription, outputStreamName,outputStreamDescription, queryString);
        logger.error("CEPID: " + cepid);

    }


}

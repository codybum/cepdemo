package io.cresco.cepdemo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MessageListener implements Runnable  {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private Type listMaps;
    private List<Map<String, Object>> edgeMapList;
    private List<String> CEPList;

    public MessageListener(PluginBuilder plugin) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        CEPList = new ArrayList<>();
        gson = new Gson();
        listMaps = new TypeToken<List<Map<String, Object>>>() {
        }.getType();
        edgeMapList = gson.fromJson(plugin.getConfig().getStringParam("edges"), listMaps);
        logger.info("Mode=1");
    }


    public void run() {

        try {
            while (!plugin.isActive()) {
                logger.error("Listener: Wait until plugin is active...");
                Thread.sleep(1000);
            }

            for(Map<String,Object> edgeMap : edgeMapList) {
                createListners(edgeMap);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void stopListners() {

        try {

            //remove listners
            for(Map<String,Object> edgeMap : edgeMapList) {
                String node_from = edgeMap.get("node_from").toString().replace("-","");
                String node_to = edgeMap.get("node_to").toString().replace("-","");
                plugin.getAgentService().getDataPlaneService().removeMessageListener("stream_name='" + node_from + "'");
                plugin.getAgentService().getDataPlaneService().removeMessageListener("stream_name='" + node_to + "'");
            }

            //remove cep
            for (String cepid : CEPList) {
                plugin.getAgentService().getDataPlaneService().removeCEP(cepid);
            }

        } catch(Exception ex) {

            ex.printStackTrace();
        }
    }

    private void createListners(Map<String,Object> edgeMap) {

        String node_from = edgeMap.get("node_from").toString().replace("-","");
        String node_to = edgeMap.get("node_to").toString().replace("-","");

        String inputStreamDescription = "source string, urn string, metric string, ts long, value double";
        //"source":"mysource","urn":"myurn","metric":"mymetric","ts":1576355714623,"value":0.3142739729174573

        String outputStreamDescription = "source string, avgValue double";

        javax.jms.MessageListener ml = new javax.jms.MessageListener() {
            public void onMessage(Message msg) {
                try {


                    if (msg instanceof TextMessage) {

                        System.out.println(" SEND MESSAGE:" + ((TextMessage) msg).getText() + " from:" + node_from);

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,"stream_name='" + node_from + "'");

        javax.jms.MessageListener ml2 = new javax.jms.MessageListener() {
            public void onMessage(Message msg) {
                try {


                    if (msg instanceof TextMessage) {

                        System.out.println(" OUTPUT EVENT:" + ((TextMessage) msg).getText() + " from:" + node_to);

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml2,"stream_name='" + node_to + "'");

        String queryString = " " +
                //from TempStream#window.timeBatch(10 min)
                //"from UserStream#window.time(5 sec) " +
                "from " + node_from + "#window.timeBatch(5 sec) " +
                "select source, avg(value) as avgValue " +
                "  group by source " +
                "insert into " + node_to + "; ";
        logger.error(queryString);
        //public String createCEP(String inputStreamName, String inputStreamDefinition, String outputStreamName, String outputStreamDefinition, String queryString) {
        String cepid = plugin.getAgentService().getDataPlaneService().createCEP(node_from, inputStreamDescription, node_to,outputStreamDescription, queryString);
        logger.error("CEPID: " + cepid);
        CEPList.add(cepid);

    }

}

package io.cresco.cepdemo;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class
ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private MeasurementEngine me;
    private Gson gson;

    public ExecutorImpl(PluginBuilder pluginBuilder, MeasurementEngine me) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(),CLogger.Level.Info);
        this.me = me;
        gson = new Gson();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        //logger.info("INCOMING INFO MESSAGE : " + incoming.getParams());
        //System.out.println("INCOMING INFO MESSAGE FOR PLUGIN");
        return null;
    }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        logger.debug("Processing Exec message : " + incoming.getParams());

        if(incoming.getParams().containsKey("action")) {
            switch (incoming.getParam("action")) {

                case "pluginkpi":
                    return getCEPInfoMap(incoming);

            }
        }
        return null;
    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }

    public MsgEvent getCEPInfoMap(MsgEvent incoming) {

        try {

            Map<String, List<Map<String,String>>> info = new HashMap<>();
            info.put("cep", me.getMetricGroupList("cep"));
            info.put("jvm", me.getMetricGroupList("jvm"));

            Map<String,String> metricsMap = new HashMap<>();
            metricsMap.put("name","cep_group");
            metricsMap.put("metrics",gson.toJson(info));

            List<Map<String,String>> metricsList = new ArrayList<>();
            metricsList.add(metricsMap);

            incoming.setCompressedParam("pluginkpi",gson.toJson(metricsList));

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return incoming;
    }

}
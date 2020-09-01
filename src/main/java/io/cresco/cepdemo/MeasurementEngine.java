package io.cresco.cepdemo;

import com.google.gson.Gson;
import io.cresco.library.metrics.CrescoMeterRegistry;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MeasurementEngine {

    private PluginBuilder plugin;
    private CLogger logger;
    private CrescoMeterRegistry crescoMeterRegistry;

    private Gson gson;
    private Map<String,CMetric> metricMap;

    public MeasurementEngine(PluginBuilder plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(MeasurementEngine.class.getName(), CLogger.Level.Info);

        this.metricMap = new ConcurrentHashMap<>();

        gson = new Gson();

        crescoMeterRegistry = plugin.getCrescoMeterRegistry();

        metricInit();
    }

    public void shutdown() {
        try {
            crescoMeterRegistry.close();
            crescoMeterRegistry = null;
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    public List<Map<String,String>> getMetricGroupList(String group) {
        List<Map<String,String>> returnList = null;
        try {
            returnList = new ArrayList<>();

            for (Map.Entry<String, CMetric> entry : metricMap.entrySet()) {
                CMetric metric = entry.getValue();
                if(metric.group.equals(group)) {
                    returnList.add(writeMetricMap(metric));
                }
            }
        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return returnList;
    }

    public Map<String,String> writeMetricMap(CMetric metric) {

        Map<String,String> metricValueMap = null;

        try {
            metricValueMap = new HashMap<>();

            if (Meter.Type.valueOf(metric.className) == Meter.Type.GAUGE) {

                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("value",String.valueOf(crescoMeterRegistry.get(metric.name).gauge().value()));

            } else if (Meter.Type.valueOf(metric.className) == Meter.Type.TIMER) {
                TimeUnit timeUnit = crescoMeterRegistry.get(metric.name).timer().baseTimeUnit();
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("mean",String.valueOf(crescoMeterRegistry.get(metric.name).timer().mean(timeUnit)));
                metricValueMap.put("max",String.valueOf(crescoMeterRegistry.get(metric.name).timer().max(timeUnit)));
                metricValueMap.put("totaltime",String.valueOf(crescoMeterRegistry.get(metric.name).timer().totalTime(timeUnit)));
                metricValueMap.put("count",String.valueOf(crescoMeterRegistry.get(metric.name).timer().count()));
                }

            else if (Meter.Type.valueOf(metric.className) == Meter.Type.DISTRIBUTION_SUMMARY) {
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                metricValueMap.put("mean",String.valueOf(crescoMeterRegistry.get(metric.name).summary().mean()));
                metricValueMap.put("max",String.valueOf(crescoMeterRegistry.get(metric.name).summary().max()));
                metricValueMap.put("totaltime",String.valueOf(crescoMeterRegistry.get(metric.name).summary().totalAmount()));
                metricValueMap.put("count",String.valueOf(crescoMeterRegistry.get(metric.name).summary().count()));
            }

            else  if (Meter.Type.valueOf(metric.className) == Meter.Type.COUNTER) {
                metricValueMap.put("name",metric.name);
                metricValueMap.put("class",metric.className);
                metricValueMap.put("type",metric.type.toString());
                try {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).functionCounter().count()));
                } catch (Exception ex) {
                    metricValueMap.put("count", String.valueOf(crescoMeterRegistry.get(metric.name).counter().count()));
                }

            } else {
                logger.error("NO WRITER FOUND " + metric.className);
            }

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return metricValueMap;
    }

    public Boolean setTimer(String name, String description, String group) {

        if(metricMap.containsKey(name)) {
            return false;
        } else {
            //this.crescoMeterRegistry.timer()

            Timer timer = Timer.builder(name).description(description).register(crescoMeterRegistry);
            //this.crescoMeterRegistry.
            //Timer timer = this.crescoMeterRegistry.timer(agentcontroller.getPluginID() + "_" + name);
            //timer.takeSnapshot().
            metricMap.put(name,new CMetric(name,description,group,"TIMER"));
            return true;
        }
    }

    public Timer getTimer(String name) {
        if(metricMap != null) {
            if (metricMap.containsKey(name)) {
                if(this.crescoMeterRegistry != null) {
                    return this.crescoMeterRegistry.timer(name);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public void updateTimer(String name, long timeStamp) {

        Timer tmpTimer = getTimer(name);
        if(tmpTimer != null) {
            tmpTimer.record(System.nanoTime() - timeStamp, TimeUnit.NANOSECONDS);
        }
    }

    public Gauge getGauge(String name) {
        if(metricMap.containsKey(name)) {
            return this.crescoMeterRegistry.get(name).gauge();
        } else {
            return null;
        }
    }

    public Gauge getGaugeRaw(String name) {
            return this.crescoMeterRegistry.get(name).gauge();
    }

    private void metricInit() {

        setTimer("cep.transaction.time", "The timer for cep messages", "cep");


    }


}

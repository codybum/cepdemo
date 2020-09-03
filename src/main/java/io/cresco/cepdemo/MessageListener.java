package io.cresco.cepdemo;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.metrics.CMetric;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MessageListener implements Runnable  {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private MeasurementEngine me;

    public MessageListener(PluginBuilder plugin, MeasurementEngine me) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.me = me;
        gson = new Gson();
        logger.info("Mode=1");
        metricInit();
        initJVMMetrics();
    }

    private void metricInit() {

        me.setTimer("cep.transfer.time", "The timer for cep messages", "cep");
        me.setGauge("cep.payload.value", "The timer for cep messages", "cep", CMetric.MeasureClass.GAUGE_DOUBLE);

    }

    private void initJVMMetrics() {

        new ClassLoaderMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmMemoryMetrics().bindTo(me.getCrescoMeterRegistry());
        //not sure why this is disabled, perhaps not useful
        //new JvmGcMetrics().bindTo(me.getCrescoMeterRegistry());
        new ProcessorMetrics().bindTo(me.getCrescoMeterRegistry());
        new JvmThreadMetrics().bindTo(me.getCrescoMeterRegistry());

        Map<String,String> internalMap = new HashMap<>();

        internalMap.put("jvm.memory.max", "jvm");
        internalMap.put("jvm.memory.used", "jvm");
        //internalMap.put("jvm.memory.committed", "jvm");
        //internalMap.put("jvm.buffer.memory.used", "jvm");
        //internalMap.put("jvm.threads.daemon", "jvm");
        internalMap.put("jvm.threads.live", "jvm");
        internalMap.put("jvm.threads.peak", "jvm");
        //internalMap.put("jvm.classes.loaded", "jvm");
        //internalMap.put("jvm.classes.unloaded", "jvm");
        //internalMap.put("jvm.buffer.total.capacity", "jvm");
        //internalMap.put("jvm.buffer.count", "jvm");
        internalMap.put("system.load.average.1m", "jvm");
        internalMap.put("system.cpu.count", "jvm");
        internalMap.put("system.cpu.usage", "jvm");
        internalMap.put("process.cpu.usage", "jvm");

        for (Map.Entry<String, String> entry : internalMap.entrySet()) {
            String name = entry.getKey();
            String group = entry.getValue();
            me.setExisting(name,group);
        }

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
                        String payload = ((TextMessage) msg).getText();
                        System.out.println(" SEND MESSAGE:" + payload);
                        Ticker ticker = gson.fromJson(payload,Ticker.class);
                        //System.out.println("TS0: " + ticker.ts);
                        //System.out.println("TS1: " + System.nanoTime());
                        //System.out.println("diff: " + (System.nanoTime() - ticker.ts));

                        me.updateTimer("cep.transfer.time", ticker.ts);
                        me.updateDoubleGauge("cep.payload.value", ticker.value);
                        //nano-time


                        /*
            me.updateTimer("cep.transaction.time", diff);
            me.updateIntGauge("cep.transaction.time.g.i", 123);
            me.updateLongGauge("cep.transaction.time.g.l", 1234567890123456788l);
            me.updateDoubleGauge("cep.transaction.time.g.d", 12345.6789);
            me.updateDistributionSummary("cep.transaction.time.ds",t0);
             */

                        //me.setTimer("cep.transfer.time", "The timer for cep messages", "cep_receive");
                        //me.setTimer("cep.transaction.time", "The timer for cep messages", "cep_receive");
                        //me.setGauge("cep.payload.value", "The timer for cep messages", "cep", CMetric.MeasureClass.GAUGE_DOUBLE);

                        //me.setTimer("message.send.time", "The timer for cep messages", "cep_send");
                        //me.setGauge("message.send.value", "The timer for cep messages", "cep_send", CMetric.MeasureClass.GAUGE_DOUBLE);
                        //me.setGauge("cep.transaction.time.g.i", "The timer for cep messages", "cep", CMetric.MeasureClass.GAUGE_INT);
                        //me.setGauge("cep.transaction.time.g.l", "The timer for cep messages", "cep", CMetric.MeasureClass.GAUGE_LONG);
                        //me.setGauge("cep.transaction.time.g.d", "The timer for cep messages", "cep", CMetric.MeasureClass.GAUGE_DOUBLE);
                        //me.setDistributionSummary("cep.transaction.time.ds", "The timer for cep messages", "cep");


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

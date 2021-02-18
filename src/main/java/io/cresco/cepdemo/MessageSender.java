package io.cresco.cepdemo;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.metrics.CrescoMeterRegistry;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Random;

public class MessageSender implements Runnable  {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private MeasurementEngine me;

    public MessageSender(PluginBuilder plugin, MeasurementEngine me) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        gson = new Gson();
        this.me = me;
        logger.info("Mode=0");
    }

    public void run() {

        try {
            while (!plugin.isActive()) {
                logger.error("Sender: Wait until plugin is active...");
                Thread.sleep(1000);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        while(plugin.isActive()) {
            try {
                //send a message once a second
                sendIt();
                Thread.sleep(1000);
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void sendIt() {

        try {
            long t0 = System.currentTimeMillis();

            //String inputStreamName = "UserStream";
            String inputStreamName = plugin.getConfig().getStringParam("pluginID").replace("-","");

            TextMessage tickle = plugin.getAgentService().getDataPlaneService().createTextMessage();
            tickle.setText(getStringPayload());
            tickle.setStringProperty("stream_name",inputStreamName);

            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,tickle);
            long diff = System.currentTimeMillis() - t0;
            me.updateTimer("cep.transaction.time", diff);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public String getStringPayload() {

        String rec = null;

        try{

            String source = "mysource";
            String urn = "myurn";
            String metric = "mymetric";
            long ts = System.currentTimeMillis();

            Random r = new Random();
            double value = r.nextDouble();

            Ticker tick = new Ticker(source, urn, metric, ts, value);

            rec = gson.toJson(tick);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return rec;
    }



}

package org.apache.jmeter.protocol;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
@Slf4j
public class MongodbQuerySampler implements JavaSamplerClient {


    private static String host = null;
    private static int port = 0;
    private String collection = "";
    private String db = "";
    private String key = "";
    private String keyValue = "";

    private MongodbClient mc = null;
    private MongoCursor<Document> mongoCursor = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongodbQuerySampler.class);

    //拿到用户gui输入参数值
    @Override
    public void setupTest(JavaSamplerContext context) {
        host = context.getParameter("host");
        port = context.getIntParameter("port");
        db = context.getParameter("db");
        collection = context.getParameter("collection");
        key = context.getParameter("key");
        keyValue = context.getParameter("keyValue");
        mc = new MongodbClient();
        mc.ConnectionMongoClient(host, port, db);

        log.info("setupTest query");
    }

    //核心处理逻辑
    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        log.info("runTest query");
        SampleResult result = new SampleResult();
        result.setSampleLabel("MongodbQuerySampler");
        result.sampleStart();
        mongoCursor = mc.query(collection, key, keyValue);

        //判断游标有没有数据
        while (mongoCursor.hasNext()) {
            collection = collection + mongoCursor.next().toString() + "\n";
            LOGGER.info("Collection: " + collection);
        }

        if (collection != "" || !collection.equals("")) {
            result.setResponseData(collection.getBytes());
        } else {
            result.setResponseData("return data is null".getBytes());
        }


        result.setSuccessful(true);
        result.sampleEnd();

        return result;

    }

    @Override
    public void teardownTest(JavaSamplerContext javaSamplerContext) {
        log.info("teardownTest query");
        mc.closeConnection();
    }

    //参数默认值展示在gui
    @Override
    public Arguments getDefaultParameters() {

        Arguments params = new Arguments();
        params.addArgument("host", "localhost");
        params.addArgument("port", "27017");
        params.addArgument("db", "myDB");
        params.addArgument("collection", "test");
        params.addArgument("key", "key");
        params.addArgument("keyValue", "");

        return params;
    }
}

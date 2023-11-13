package org.apache.jmeter.protocol;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbClient<Document,findIterable> {

    private MongoClient mongoClient = null;
    private MongoDatabase database = null;
    private MongoCollection<org.bson.Document> collection = null;
    private FindIterable<Document> findIterable = null; //查找迭代器
    private MongoCursor<Document> mongoCursor = null; //mongo 游标

    private static final Logger LOGGER = LoggerFactory.getLogger(MongodbClient.class);

    //建立连接
    public void ConnectionMongoClient(String host, int port, String databaseName) {
        try {
            mongoClient = new MongoClient(host, port);
            database = mongoClient.getDatabase(databaseName);
            LOGGER.info("Connection Successfully!");
        } catch (Exception e) {
            LOGGER.info("Mongodb Connection Exception!");
            e.printStackTrace();
        }
    }

    //查询数据
    public  MongoCursor query(String collectionName,String key,String queryStr){
        collection=database.getCollection(collectionName);

        if("".equals(queryStr)){
            findIterable=(FindIterable<Document>) collection.find();
        }else {
            findIterable=(FindIterable<Document>) collection.find(Filters.eq(key,queryStr));
        }

        mongoCursor = findIterable.iterator();
        LOGGER.info("query successfully!");

        return  mongoCursor;
    }

    //插入数据
    public  boolean insert(String collectionName,Document doc){
        try {
            collection= database.getCollection(collectionName);
            collection.insertOne((org.bson.Document) doc);
        } catch (Exception e) {
            e.printStackTrace();
            return  false;
        }
        return true;

    }

    //关闭连接
    public  void  closeConnection(){
        if(mongoClient!=null){
            mongoClient.close();
        }
    }


}

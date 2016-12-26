package edu.indiana.d2i.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bson.BSON;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongodbManager {
	private static final ServerAddress[] SHARDS = { new ServerAddress("crow.soic.indiana.edu", 27017),
			new ServerAddress("pipit.soic.indiana.edu", 27017), new ServerAddress("vireo.soic.indiana.edu", 27017) };
	private static final String DATA_BASE = "volumeContents";
	public static final String VOLUME_COLLECTION = "volumeInfo0";
	public static final String PAGE_COLLECTION = "pageInfo0";
	
	public static final String VOLUME_COLLECTION_2 = "volumeInfo1";
	//private static final MongoClient MONGO_CLIENT; //new MongoClient(Arrays.asList(SHARDS));
/*	static {
		MONGO_CLIENT = new MongoClient(Arrays.asList(SHARDS));
	}*/
	public static final List<MongoClient> clients = new LinkedList<MongoClient>();
	/*public static MongoClient getMongoClient() {
		return MONGO_CLIENT;
	}*/
	
	public static void shutdown() {
		//if(MONGO_CLIENT != null) MONGO_CLIENT.close();
		if(clients.size() > 0) {
			for(MongoClient client: clients) {
				client.close();
			}
		}
	}

	public static MongoCollection<Document> getCollection(String collectionName) {
	//	if(MONGO_CLIENT == null) return null;
		MongoClient client = new MongoClient(Arrays.asList(SHARDS));
		clients.add(client);
		MongoDatabase db = client.getDatabase(DATA_BASE);
		MongoCollection<Document> collection = db.getCollection(collectionName);
		return collection;
	}
	
	
	
	public static void main(String[] args) {

		MongoClient mongoClient = new MongoClient(Arrays.asList(SHARDS));
		MongoDatabase db = mongoClient.getDatabase("volumeContents");
		MongoCollection<Document> collection = db.getCollection("test");

		ArrayList<DBObject> list = new ArrayList<DBObject>();
		list.add(new BasicDBObject("pageSequence", "00000001").append("pageContent", "xyz lalala").append("byteCount", 10));
		list.add(new BasicDBObject("pageSequence", "00000002").append("pageContent", "abc wuwuwu").append("byteCount", 11));
		Document doc0 = new Document("_id", "mdp.123").append("type", "database").append("count", 1).append("info",
				new Document("x", 203).append("y", 202)).append("pages", list);
		
		Document doc1 = new Document("_id", "inu.459").append("type", "database").append("count", 1).append("info",
				new Document("x", 203).append("y", 202)).append("pages", list);
	/*	
		Document doc = new Document("name", "MongoDB")
	               .append("type", "database")
	               .append("count", 1)
	               .append("info", new Document("x", 203).append("y", 102));*/
		
		collection.insertOne(doc0);
		collection.insertOne(doc1);
		System.out.println(collection.count());
		mongoClient.close();
	}

}
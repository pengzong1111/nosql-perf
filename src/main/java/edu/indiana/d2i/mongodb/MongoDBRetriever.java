package edu.indiana.d2i.mongodb;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.datastax.driver.core.Row;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import edu.indiana.d2i.nosql.Retriever;
import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.Tools;

import static com.mongodb.client.model.Filters.*;

public class MongoDBRetriever extends Retriever {

	private MongoCollection<Document> pageCollection = MongodbManager.getCollection(MongodbManager.PAGE_COLLECTION);
	private PrintWriter statsWriter;
	private int label;

	public MongoDBRetriever(int i) {
		this.label = i;
		try {
			statsWriter = new PrintWriter(new FileOutputStream("mongo-stats_" + label + ".txt", true));
			// statsWriter.println("volumes" + '\t' + "bytes" + '\t' + "time");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void releaseResource() {
		statsWriter.flush();
		statsWriter.close();
	}

	public static void main(String[] args) {
		/*Block<Document> printBlock = new Block<Document>() {
			public void apply(final Document document) {
				System.out.println(document.toJson());
			}
		};*/
		MongoCollection<Document> pageCollection = MongodbManager.getCollection(MongodbManager.PAGE_COLLECTION);

		List<String> volumeList = new LinkedList<String>();
		volumeList.add("inu.30000121140325");
		volumeList.add("mdp.39015023920666");

		List<String> pageSequences = Arrays.asList(Configuration.getProperty("PAGE_SEQUENCES").split(","));

		
		Bson bson = and(in("volumeId", volumeList), in("sequence", pageSequences));
		MongoCursor<Document> cursor = pageCollection
				.find(bson).batchSize(4000).projection(new BasicDBObject("volumeId", 1).append("sequence", 1)
						.append("contents", 1).append("byteCount", 1).append("_id", 0))
				.sort(new BasicDBObject("volumeId", 1)).iterator();
		long size = 0;
		while (cursor.hasNext()) {
			Document doc = cursor.next();
			System.out.println(doc.get("volumeId") + "  " + doc.get("sequence"));
			size += doc.getLong("byteCount");
		}

		System.out.println("overall size is " + size);
		cursor.close();
	}

	@Override
	public void retrieve(List<String> volumesToRetrieve) {

		long bytes = 0;
		long pages = 0;
		long volumes = 0;
		String prevVolumeId = "";

		Bson bson = null;

		if (Configuration.getProperty("ACCESS_PATTERN").equals("VOLUME")) {
			bson = in("volumeId", volumesToRetrieve);
		} else {
			//List<String> pageSequences = Arrays.asList(Configuration.getProperty("PAGE_SEQUENCES").split(","));
			List<String> pageSequences = Tools.generateSequences(10);
			if (volumesToRetrieve == null || volumesToRetrieve.isEmpty()) {
				System.out.println("volume list is empty or null");
				return;
			}
			bson = and(in("volumeId", volumesToRetrieve), in("sequence", pageSequences));
			
		}
		System.out.println("========start========");
		long t0 = System.currentTimeMillis();
		MongoCursor<Document> cursor = pageCollection.find(bson).batchSize(1000).projection(new BasicDBObject("volumeId", 1)
				.append("sequence", 1).append("contents", 1).append("byteCount", 1).append("_id", 0))
				.sort(new BasicDBObject("volumeId", 1)).iterator();
		while (cursor.hasNext()) {
			// System.out.println("========");
			Document doc = cursor.next();
		//	System.out.println(doc.get("volumeId") + "  " + doc.get("sequence"));
			bytes += doc.getLong("byteCount");
			byteCount.addAndGet(doc.getLong("byteCount"));
			pages++;
			pageCount.incrementAndGet();
			if (!prevVolumeId.equals(doc.getString("volumeId"))) {
				// System.out.println(row.getString("volumeId"));
				volumes++;
				volumeCount.incrementAndGet();
				prevVolumeId = doc.getString("volumeId");
			}
		}

		System.out.println("========end========");
		// System.out.println(resultSet.toString());
		long t1 = System.currentTimeMillis();
		System.out.println("retrieving time: " + (t1 - t0) + " for " + volumes + " volumes, " + pages + " pages,"
				+ bytes + " bytes.");
		if (Configuration.getProperty("ACCESS_PATTERN").equals("VOLUME")) {
			statsWriter.println(volumesToRetrieve.size() + "\t" + bytes + "\t" + (t1 - t0) + "\t" + false);
		} else {
			statsWriter.println("--------");
			statsWriter.println(volumesToRetrieve.size() + "\t" + pages + "\t" + bytes + "\t" + (t1 - t0) + "\t" + false);
		}

	}

}

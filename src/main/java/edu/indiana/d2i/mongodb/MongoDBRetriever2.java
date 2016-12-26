package edu.indiana.d2i.mongodb;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import com.datastax.driver.core.Row;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import edu.indiana.d2i.nosql.Retriever;
import edu.indiana.d2i.tools.Configuration;

import static com.mongodb.client.model.Filters.*;

public class MongoDBRetriever2 extends Retriever {

	private MongoCollection<Document> volumeCollection = MongodbManager.getCollection(MongodbManager.VOLUME_COLLECTION_2);
	private PrintWriter statsWriter;
	private int label;

	public MongoDBRetriever2(int i) {
		this.label = i;
		try {
			statsWriter = new PrintWriter(new FileOutputStream("mongo2-stats_" + label + ".txt", true));
			// statsWriter.println("volumes" + '\t' + "bytes" + '\t' + "time");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void releaseResource() {
		statsWriter.flush();
		statsWriter.close();
	}

	public static void main(String[] args) throws IOException {
		MongoCollection<Document> volumeCollection = MongodbManager.getCollection(MongodbManager.VOLUME_COLLECTION_2);

		List<String> volumeList = new LinkedList<String>();
		volumeList.add("inu.30000121140325");
		volumeList.add("mdp.39015023920666");

		
		Bson bson = in("_id", volumeList);
		
		MongoCursor<Document> cursor = volumeCollection.find(bson).batchSize(25).projection(new BasicDBObject("_id", 1).append("zippedContent", 1).append("byteCount", 1)).iterator();
		long size = 0;
		while (cursor.hasNext()) {
			
			Document doc = cursor.next();
			String volumeId = doc.getString("_id");
			System.out.println(volumeId + " found !!!!!!!!!!!!!!!!!!~~~~~~~~~~~~~~~~~~~~");
			Binary binary = ((Binary)doc.get("zippedContent"));
			byte[] bytes = binary.getData();
			FileOutputStream fos = new FileOutputStream(volumeId + ".zip");
			fos.write(bytes);
			fos.flush();fos.close();
			size += doc.getLong("byteCount");
		}

		System.out.println("overall size is " + size);
		cursor.close();
	}

	@Override
	public void retrieve(List<String> volumesToRetrieve) {

		long bytes = 0;
		long volumes = 0;

		Bson bson = in("_id", volumesToRetrieve);
		System.out.println("========start========");
		long t0 = System.currentTimeMillis();
		MongoCursor<Document> cursor = volumeCollection.find(bson).projection(new BasicDBObject("_id", 1).append("zippedContent", 1).append("byteCount", 1)).iterator();
		while (cursor.hasNext()) {
			// System.out.println("========");
			Document doc = cursor.next();
		//	System.out.println(doc.get("volumeId") + "  " + doc.get("sequence"));
			bytes += doc.getLong("byteCount");
			byteCount.addAndGet(doc.getLong("byteCount"));
				// System.out.println(row.getString("volumeId"));
				volumes++;
				volumeCount.incrementAndGet();
		}

		System.out.println("========end========");
		// System.out.println(resultSet.toString());
		long t1 = System.currentTimeMillis();
		System.out.println("retrieving time: " + (t1 - t0) + " for " + volumes + " volumes, " + bytes + " bytes.");
		statsWriter.println(volumesToRetrieve.size() + "\t" + bytes + "\t" + (t1 - t0) + "\t" + false);

	}

}

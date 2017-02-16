package edu.indiana.d2i.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;


public class MongoBulkReader {

	/**
	 * args0 is the path of the list of volumes to load ids
	 * args1 is the batch size
	 * args2 is the size of request
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		MongoCollection<Document> volumeCollection = MongodbManager.getCollection(MongodbManager.VOLUME_COLLECTION);

		List<String> volumeList = new LinkedList<String>();
		loadVolumeIds(volumeList, args[0], Integer.valueOf(args[2]));

	//	List<String> pageSequences = Arrays.asList(Configuration.getProperty("PAGE_SEQUENCES").split(","));

		
		//Bson bson = and(in("volumeId", volumeList), in("sequence", pageSequences));
		Bson bson = in("_id", volumeList);
		
		FindIterable<Document> plans = volumeCollection
		.find(bson).modifiers(new Document("$explain", true));
		for(Document doc : plans) {
			System.out.println(doc);
		}
		Thread.sleep(20000);
		long t0 = System.currentTimeMillis();
		MongoCursor<Document> cursor = volumeCollection
				.find(bson).batchSize(Integer.valueOf(args[1])).iterator();
		long size = 0;
		int i=0;
		while (cursor.hasNext()) {
			i++;
			Document queryResult = cursor.next();
			System.out.println(queryResult.get("_id"));
			if (queryResult != null) {
		    	  Set<Entry<String, Object>> entries = queryResult.entrySet();
		    	  
		    	  for(Entry<String, Object> entry: entries) {
		    		  Object value = entry.getValue();
		    		  if(value instanceof Document) {
		    			  Document doc = (Document) value;
		    		//	  System.out.println(doc.getString("contents"));
		    		//	  System.out.println(doc.getLong("byteCount"));
		    			  size += doc.getLong("byteCount");
		    		  }
		    	  }
		    	}
		}
		long t1 = System.currentTimeMillis();
		System.out.printf("overall volume read is %d \n", i);
		System.out.printf("overall size is %d bytes \n", size);
		long time = (t1-t0)/1000;
		System.out.printf("time elapsed: %d seconds \n", time);
		System.out.printf("throughput is %d MB/s \n",  size/1024/1024/time);
		cursor.close();
	

	}
	
	public static void loadVolumeIds(List<String> volumeIdList, String volumeIdsPath, int size) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(volumeIdsPath), "UTF-8"));
			String volumeId = null;
			int i=0;
			while((volumeId = br.readLine()) != null) {
				if(i == size) break;
				volumeIdList.add(volumeId);
				i++;
			}
			br.close();
			Collections.shuffle(volumeIdList);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

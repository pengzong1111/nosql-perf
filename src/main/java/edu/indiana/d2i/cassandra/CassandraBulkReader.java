package edu.indiana.d2i.cassandra;

import static com.mongodb.client.model.Filters.in;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import edu.indiana.d2i.mongodb.MongodbManager;
import edu.indiana.d2i.tools.Configuration;

public class CassandraBulkReader {


	/**
	 * args0 is the path of the list of volumes to load ids
	 * args1 is the batch size
	 * args2 is the size of request
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {

		List<String> volumeList = new LinkedList<String>();
		loadVolumeIds(volumeList, args[0], Integer.valueOf(args[2]));
		CassandraManager cassandraManager = CassandraManager.getInstance();
		String columnFamilyName = Configuration.getProperty("VOLUME_COLUMN_FAMILY");
	
		Statement select = QueryBuilder.select().column("volumeid")/*.column("sequence")*/.column("contents").column("bytecount").from(Configuration.getProperty("KEY_SPACE"), columnFamilyName).where(QueryBuilder.in("volumeid", volumeList));
		select.setFetchSize(Integer.valueOf(args[1]));
		System.out.printf("start reading %d volumes \n", volumeList.size());

		long t0 = System.currentTimeMillis();
		ResultSet resultSet = cassandraManager.execute(select);
		Iterator<Row> iter = resultSet.iterator();
		long bytes = 0;
		while (iter.hasNext()) {
			//	System.out.println("========");
				Row row = iter.next();
				bytes += row.getLong("bytecount");
		}
		long t1 = System.currentTimeMillis();
		System.out.printf("overall size is %d bytes \n", bytes);
		long time = (t1-t0)/1000;
		System.out.printf("time elapsed: %d seconds \n", time);
		System.out.printf("throughput is %d MB/s \n",  bytes/1024/1024/time);
		cassandraManager.shutdown();

	}
	
	public static void loadVolumeIds(List<String> volumeIdList, String volumeIdsPath, int size) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(volumeIdsPath), "UTF-8"));
			String volumeId = null;
			int i=0;
			List<String> tmp = new LinkedList<String>();
			while((volumeId = br.readLine()) != null) {
				tmp.add(volumeId);
				i++;
			}
			br.close();
			System.out.println("loaded ids, shuffling...");
			Collections.shuffle(tmp);
			System.out.println("shuffled and get top " + size);
			volumeIdList.addAll(tmp.subList(0, size));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}

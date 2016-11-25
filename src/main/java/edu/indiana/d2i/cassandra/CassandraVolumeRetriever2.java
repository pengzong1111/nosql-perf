package edu.indiana.d2i.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.cassandra.tools.Configuration;
import edu.indiana.d2i.cassandra.tools.Tools;

public class CassandraVolumeRetriever2 extends Retriever {
	private static Logger logger = LogManager.getLogger(CassandraVolumeRetriever2.class);
	private static CassandraManager cassandraManager;
//	private static PreparedStatement selectVolumeStatement;
//	private static PreparedStatement selectPageStatement;
	private static String columnFamilyName;
	private PrintWriter statsWriter;
	private int label;
	static {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty("VOLUME_COLUMN_FAMILY");
		
	//	selectVolumeStatement = cassandraManager.prepare("SELECT volumeid, sequence FROM " + columnFamilyName + " WHERE volumeid in (?)" );
		
	//	selectPageStatement = cassandraManager.prepare("SELECT " + "contents"  +" FROM " + columnFamilyName + " WHERE volumeid=\'?\' AND sequence=\'?\'");
		
	}
	
	public CassandraVolumeRetriever2(int i) {
		this.label = i;
		try {
			statsWriter = new PrintWriter(new FileOutputStream("stats_"+label+".txt", true));
		//	statsWriter.println("volumes" + '\t' + "bytes" + '\t' + "time");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void releaseResource() {
		statsWriter.flush();statsWriter.close();
	}

	public static void main(String[] args) {
	//	Tools.generateRandomInputVolumeList(new File("all.txt"), Integer.parseInt(Configuration.getProperty("INPUT_SIZE")));
		List<String> volumesToRetrieve = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		CassandraVolumeRetriever2 retriever = new CassandraVolumeRetriever2(0);
		retriever.retrieve(volumesToRetrieve);
		
		retriever.releaseResource();
		cassandraManager.shutdown();
	}

	public void retrieve(List<String> volumesToRetrieve) {
		
		
		if (volumesToRetrieve == null || volumesToRetrieve.isEmpty()) {
			System.out.println("volume list is empty or null");
			return;
		}
		
		Statement select = null;
		if(volumesToRetrieve.size() == 1) {
			select = QueryBuilder.select().column("volumeid")./*column("sequence").*//*column("mets").*/column("zippedcontent").column("bytecount").from(Configuration.getProperty("KEY_SPACE"), columnFamilyName).where(QueryBuilder.eq("volumeid", volumesToRetrieve.get(0)));
		} else {
			select = QueryBuilder.select().column("volumeid")/*.column("sequence")*//*.column("mets")*/.column("zippedcontent").column("bytecount").from(Configuration.getProperty("KEY_SPACE"), columnFamilyName).where(QueryBuilder.in("volumeid", volumesToRetrieve));
		}
	//	System.out.println(select.toString());
		System.out.printf("start reading %d volumes \n", volumesToRetrieve.size());
		select.setFetchSize(20);
		long t0 = System.currentTimeMillis();
		
		ResultSet resultSet = retrieveVolumes(select);
		
		boolean fetchInSingleBatch = Boolean.parseBoolean(Configuration.getProperty("FETCH_IN_SINGLE_BATCH"));
		Iterator<Row> iter = null;
		/*if(fetchInSingleBatch) {
			iter = resultSet.all().iterator();
		} else {*/
			iter = resultSet.iterator();
	//	}
	 
	//	resultSet.all()
		System.out.println("========start========");
		long bytes = 0;
		long volumes = 0;
		while (iter.hasNext()) {
		//	System.out.println("========");
			Row row = iter.next();
			bytes += row.getLong("bytecount");
			byteCount.addAndGet(row.getLong("bytecount"));
	//		String volId = row.getString("volumeid");
		//	System.out.println(volId);
			volumes++;
			volumeCount.incrementAndGet();
			
			/*ByteBuffer mets = row.getBytes("mets");
			ByteBuffer zip = row.getBytes("zippedcontent");
			writeToFile(mets, ".xml", volId);
			writeToFile(zip, ".zip", volId);*/
		}
		System.out.println("========end========");
		System.out.println(resultSet.toString());
		long t1 = System.currentTimeMillis();
		System.out.println("retrieving time: " + (t1 - t0) + " for " + volumes + " volumes, " + bytes + " bytes.");
		statsWriter.println(volumesToRetrieve.size() + "\t" + bytes + "\t" + (t1 - t0) + "\t" + fetchInSingleBatch);

	}
	private void writeToFile(ByteBuffer byteBuffer, String fileSuffix, String volId) {
		File outputFile = new File(volId + fileSuffix);
		try {
			FileOutputStream fos = new FileOutputStream(outputFile, false);
			FileChannel channel = fos.getChannel();
			channel.write(byteBuffer);
			channel.close();
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static ResultSet retrieveVolumes(Statement select) {
		ResultSet resultSet = cassandraManager.execute(select);
		return resultSet;
	}
}

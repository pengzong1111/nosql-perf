package edu.indiana.d2i.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.nosql.Retriever;
import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.Tools;

public class CassandraPageRetriever extends Retriever{
	private static Logger logger = LogManager.getLogger(CassandraVolumeRetriever.class);
	private static CassandraManager cassandraManager;
//	private static PreparedStatement selectVolumeStatement;
	private static PreparedStatement selectPageStatement;
	private static String columnFamilyName;
	private PrintWriter statsWriter;
	private int label;
	
	static {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty("VOLUME_COLUMN_FAMILY");
		
	//	selectVolumeStatement = cassandraManager.prepare("SELECT volumeid, sequence FROM " + columnFamilyName + " WHERE volumeid in (?)" );
		
    	selectPageStatement = cassandraManager.prepare("SELECT " + "volumeid, sequence, contents, bytecount"  +" FROM " + columnFamilyName + " WHERE volumeid=? AND sequence=?");
		
	}
	
	public CassandraPageRetriever(int i) {
		this.label = i;
		try {
			statsWriter = new PrintWriter(new FileOutputStream("page-stats_" + label + ".txt", true));
		//	statsWriter.println("volumes" + '\t' + "bytes" + '\t' + "time");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Retriever retriever = new CassandraPageRetriever(0);
		Tools.generateRandomInputVolumeList(new File("all.txt"), Integer.parseInt(Configuration.getProperty("INPUT_SIZE")));
		List<String> volumesToRetrieve = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		retriever.retrieve(volumesToRetrieve);
		cassandraManager.shutdown();
		retriever.releaseResource();
		
	}
	public void retrieve(List<String> volumesToRetrieve) {
		
		List<String> pageSequences = Tools.generateSequences(10); //Arrays.asList(Configuration.getProperty("PAGE_SEQUENCES").split(","));

		if (volumesToRetrieve == null || volumesToRetrieve.isEmpty()) {
			System.out.println("volume list is empty or null");
			return;
		}
		
		System.out.println("========start========");
		int volumes = 0;
		int pages = 0;
		int bytes = 0;
		int request = 0;
		long t0 = System.currentTimeMillis();
		for(String volumeId : volumesToRetrieve) {
			for(String pageSeq : pageSequences) {
				BoundStatement boundStatement = new BoundStatement(selectPageStatement);
				boundStatement.bind(volumeId, pageSeq);
				ResultSet resultSet = cassandraManager.execute(boundStatement);
				request++;
				List<Row> rows = resultSet.all();
				if(rows.size() == 0) {
				//	System.out.println("no result");
				} else if(rows.size() == 1) {
				//	System.out.println(rows.get(0).getString("sequence"));
					bytes += rows.get(0).getLong("bytecount");
					byteCount.addAndGet(bytes);
					pages++;
					pageCount.incrementAndGet();
				} else {
					System.out.println("rows: " + rows.size());
				}
			}
			volumes++;
			volumeCount.incrementAndGet();
		}
		long t1 = System.currentTimeMillis();
		System.out.println("========end========");
		
		System.out.println("retrieving time: " + (t1 - t0));
		statsWriter.println(volumesToRetrieve.size() + "\t" + request + '\t' + pages + '\t' + bytes + "\t" + (t1 - t0) + "\t");
		System.out.println(volumesToRetrieve.size() + "\t" + request + '\t' + pages + '\t' + bytes + "\t" + (t1 - t0) + "\t");
		
	}
	private ResultSet retrieveVolumes(Statement select) {
		ResultSet resultSet = cassandraManager.execute(select);
		return resultSet;
	}
	
	@Override
	public void releaseResource() {
		statsWriter.flush();statsWriter.close();
		
	}
}

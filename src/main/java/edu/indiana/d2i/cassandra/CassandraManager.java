package edu.indiana.d2i.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.METSParser.VolumeRecord.PageRecord;

public class CassandraManager {

	private static final CassandraManager manager = new CassandraManager();
	private Cluster cluster ;
	private Session session;
	private String[] contactPoints;
	private String volumeKeySpace;
	private String metadataKeySpace;
	private CassandraManager() {
		
		this.contactPoints = Configuration.getProperty("CONTACT_POINTS").split(",");
		Builder clusterBuilder = Cluster.builder(); //.addContactPoint("crow.soic.indiana.edu").build();
		for(String contactPoint : contactPoints) {
			clusterBuilder.addContactPoint(contactPoint);
		}
		this.cluster = clusterBuilder.build();
		this.volumeKeySpace = Configuration.getProperty("KEY_SPACE");
		this.session = this.cluster.connect(this.volumeKeySpace);
	}
	
	
	public boolean checkTableExist(String tableName) {
		KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(volumeKeySpace);
		TableMetadata table = keyspace.getTable(tableName);
		if(table == null) {
			return false;
		} else {
			return true;
		}
	}
	
	public ResultSet execute(String statement) {
		return session.execute(statement);
	}
	
	public ResultSet execute(Statement statement) {
		return session.execute(statement);
	}
	
	public static CassandraManager getInstance() {
			return manager;
	}
	
	public static void shutdown() {
		manager.cluster.close();
	}


	public PreparedStatement prepare(String statementTemplate) {
		return session.prepare(statementTemplate);
	}


	public ResultSet execute(BoundStatement boundStatement) {
		//boolean executed = false;
		int maxAttempts = 3;
		while(/*!executed &&*/ maxAttempts > 0) {
			try{
				boundStatement.setReadTimeoutMillis(20000);
				ResultSet results = session.execute(boundStatement);
				//executed = true;
				return results;
			} catch (Exception e) {
				
				if (maxAttempts > 0) {
					maxAttempts--;
					System.out.println("write time out error: " + e.getMessage());
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					System.out.println("execution failed: " + boundStatement.toString());
				}
			}
		}
		return null;
	}
	
}

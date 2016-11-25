package edu.indiana.d2i.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import edu.indiana.d2i.cassandra.tools.Configuration;
import edu.indiana.d2i.cassandra.tools.Constants;
import edu.indiana.d2i.cassandra.tools.CopyrightEnum;
import edu.indiana.d2i.cassandra.tools.METSParser;
import edu.indiana.d2i.cassandra.tools.METSParser.VolumeRecord;
import edu.indiana.d2i.cassandra.tools.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.cassandra.tools.Tools;

/**
 * this is for data model 2 that store pages as a zip file
 * @author Zong
 *
 */
public class CassandraIngester2 {
	private static PrintWriter pw;
	private static PrintWriter pw2;
	private static CassandraManager cassandraManager;
	private static PreparedStatement insertStatement;
	private static String columnFamilyName;

	static {
		try {
			pw = new PrintWriter("ingested.txt");
			pw2 = new PrintWriter("failed.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty("VOLUME_COLUMN_FAMILY");
		//create column family
				if(! checkTableExist(columnFamilyName)) {
					String createTableStr = "CREATE TABLE " + columnFamilyName + " ("
				    		+ "volumeID text PRIMARY KEY, "
				    		+ "zippedContent blob, "
				    		+ "mets blob, "
				    		+ "byteCount bigint) ";
					cassandraManager.execute(createTableStr);
				}
		insertStatement = cassandraManager.prepare("INSERT INTO " + columnFamilyName + " (volumeID, zippedContent, mets, byteCount)" + "VALUES(?,?,?,?);");
	}
	
	public static void main(String[] args) {
		CassandraIngester2 ingester = new CassandraIngester2();
		
		//add several volumes into VOLUME_COLUMN_FAMILY
		
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		if(volumesToIngest == null || volumesToIngest.isEmpty()) {
			System.out.println("volume list is empty or null");
			return;
		}
		long t0 = System.currentTimeMillis();
		for(String id: volumesToIngest) {
			boolean success = ingester.ingestZip(insertStatement, id);
			if(success) {
				System.out.println(id + " zip ingested");
				pw.println(id);pw.flush();
			} else {
				pw2.println(id);pw.flush();
			}
		}
		ingester.close();
		long t1 = System.currentTimeMillis();
		pw.flush();pw.close(); pw2.flush();pw2.close();
		System.out.println("done");
		System.out.println("time elapsed in millisecond: " + (t1 - t0));
		CassandraManager.shutdown();
	}


	private void close() {}

	private static boolean checkTableExist(String tableName) {
		return cassandraManager.checkTableExist(tableName);
	}
	
	private boolean ingestZip(PreparedStatement insertStatement, String id) {
	//	BoundStatement bs = new BoundStatement(insertStatement);
		String cleanId = Tools.cleanId(id);
		String pairtreePath = Tools.getPairtreePath(id);
		
		String cleanIdPart = cleanId.split("\\.", 2)[1];
		String zipFileName = cleanIdPart  + Constants.VOLUME_ZIP_SUFFIX; // e.g.: ark+=13960=t02z18p54.zip
		String metsFileName = cleanIdPart + Constants.METS_XML_SUFFIX; // e.g.: ark+=13960=t02z18p54.mets.xml
		
		/*
		 *  get the zip file and mets file for this volume id based on relative path(leafPath) and zipFileName or metsFileName
		 *  e.g.: /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.zip
		 *  /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.mets.xml
		 */
		File volumeZipFile = Tools.getFileFromPairtree(pairtreePath, zipFileName);
		File volumeMetsFile = Tools.getFileFromPairtree(pairtreePath, metsFileName);
		if(volumeZipFile == null || volumeMetsFile == null || !volumeZipFile.exists() || !volumeMetsFile.exists()) {
			System.out.println("zip file or mets file does not exist for " + id);
			return false;
		}
		
		boolean volumeAdded = updateVolume(id, volumeZipFile, volumeMetsFile);
		
		return volumeAdded;
	}


	private boolean updateVolume(String volumeId, File volumeZipFile, File metsXmlFile) {
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		ByteBuffer zipBinaryContent = getByteBuffer(volumeZipFile);
		ByteBuffer metsBinaryContent = getByteBuffer(metsXmlFile);
		if(zipBinaryContent != null && metsBinaryContent != null) {
			boundStatement.bind(volumeId, zipBinaryContent, metsBinaryContent, volumeZipFile.length());
		} else {
			return false;
		}
		
		ResultSet result = cassandraManager.execute(boundStatement);
		return result != null;
	}

	private ByteBuffer getByteBuffer(File file) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buffer = new byte[32767];
		
		try {
			InputStream is = new FileInputStream(file);
			int read = -1;
			while((read = is.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
			is.close();
			
			byte[] bytes = bos.toByteArray();
			ByteBuffer bf = ByteBuffer.wrap(bytes);
			return bf;
		} catch (FileNotFoundException e) {
			System.out.println(file.getAbsolutePath() + " is not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOException while attempting to read " + file.getName());
			e.printStackTrace();
		}
		return null;
	}

	/**
     * Method to extract the filename from a ZipEntry name
     * @param entryName name of a ZipEntry
     * @return extracted filename
     */
    protected String extractEntryFilename(String entryName) {
        int lastIndex = entryName.lastIndexOf('/');
        return entryName.substring(lastIndex + 1);
    }
    
    static final int SEQUENCE_LENGTH = 8;
    /**
     * Method to generate a fixed-length zero-padded page sequence number
     * @param order the ordering of a page
     * @return a fixed-length zero-padded page sequence number based on the ordering
     */
    protected String generateSequence(int order) {
        String orderString = Integer.toString(order);
        
        StringBuilder sequenceBuilder = new StringBuilder();
        
        int digitCount = orderString.length();
        for (int i = digitCount; i < SEQUENCE_LENGTH; i++) {
            sequenceBuilder.append('0');
        }
        sequenceBuilder.append(orderString);
        return sequenceBuilder.toString();
        
    }
 }

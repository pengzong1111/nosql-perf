package edu.indiana.d2i.mongodb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;

import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.Constants;
import edu.indiana.d2i.tools.Tools;
import edu.indiana.d2i.tools.METSParser.VolumeRecord;
import edu.indiana.d2i.tools.METSParser.VolumeRecord.PageRecord;

public class MongodbIngester2 {
	private static PrintWriter pw;
	private static PrintWriter pw2;
	private static MongoCollection<Document> volumeCollection;
	private static List<Document> docs;
	private static int DOCS_SIZE = 10;
	static {
		try {
			pw = new PrintWriter("ingested.txt");
			pw2 = new PrintWriter("failed.txt");
			docs = new LinkedList<Document>();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		volumeCollection = MongodbManager.getCollection(MongodbManager.VOLUME_COLLECTION_2);
	}
	
	public boolean ingestZip(String id) {
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
	//	VolumeRecord volumeRecord = Tools.getVolumeRecord(id, volumeMetsFile);
		
		
		boolean volumeAdded = updateVolume(volumeZipFile, volumeMetsFile, id);
		
		return volumeAdded;
	}
	
	private boolean updateVolume(File volumeZipFile, File volumeMetsFile, String volumeId) {
		Document volumeDoc = new Document("_id", volumeId).append("accessLevel", 1).append("language", "English");
		
		Binary volumeZip = getBinary(volumeZipFile);
		Binary metsXML = getBinary(volumeMetsFile);
		
		long byteCount = volumeZip.length();
		
		volumeDoc.append("zippedContent", volumeZip).append("mets", metsXML).append("byteCount", byteCount);
		
		boolean volumeAdded = false;
		docs.add(volumeDoc);
		
		if(docs.size() >= DOCS_SIZE) {
			volumeCollection.insertMany(docs);
			docs.clear();
		}
		
		
		System.out.println("Successfully pushed all pages for volume " + volumeId);
		 
		volumeAdded = true;
		return volumeAdded;
	}

	private Binary getBinary(File file) {
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
			Binary data = new Binary(bytes);
			bos.close();
			return data;
		} catch (FileNotFoundException e) {
			System.out.println(file.getAbsolutePath() + " is not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOException while attempting to read " + file.getName());
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		MongodbIngester2 ingester = new MongodbIngester2();
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		if(volumesToIngest == null || volumesToIngest.isEmpty()) {
			System.out.println("volume list is empty or null");
			return;
		}
		long t0 = System.currentTimeMillis();
		for(String id: volumesToIngest) {
			boolean success = ingester.ingestZip(id);
			System.out.println(id + " zip ingested");
			if(success) {
				pw.println(id);pw.flush();
				ingester.ingestMetadata(id);
				System.out.println(id + " metadata ingested");
			} else {
				pw2.println(id);pw2.flush();
			}
		}
		ingester.close();
		long t1 = System.currentTimeMillis();
		pw.flush();pw.close(); pw2.flush();pw2.close();
		System.out.println("done");
		System.out.println("time elapsed in millisecond: " + (t1 - t0));

	}
	 
	private void close() {
		if(docs.size() > 0) {
			volumeCollection.insertMany(docs);
			docs.clear();
		}
		MongodbManager.shutdown();
		
	}

	private void ingestMetadata(String id) {
		// TODO Auto-generated method stub
		
	}
}

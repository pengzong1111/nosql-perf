package edu.indiana.d2i.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.tools.Configuration;
import edu.indiana.d2i.tools.Constants;
import edu.indiana.d2i.tools.Tools;
import edu.indiana.d2i.tools.METSParser.VolumeRecord;
import edu.indiana.d2i.tools.METSParser.VolumeRecord.PageRecord;


public class CassandraIngester {
	private static PrintWriter pw;
	private static PrintWriter pw2;
	private static CassandraManager cassandraManager;
//	private static PreparedStatement insertStatement;
	private static String columnFamilyName;
//	private static BatchUpdater batchUpdater;
	private static boolean batchUpdate;
	static {
		try {
			pw = new PrintWriter("ingested.txt");
			pw2 = new PrintWriter("failed.txt");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty("VOLUME_COLUMN_FAMILY");
		//create column family
				if(! checkTableExist(columnFamilyName)) {
					String createTableStr = "CREATE TABLE " + columnFamilyName + " ("
				    		+ "volumeID text, "
							+ "accessLevel int static, "
				    		+ "language text static, "
							//+ "mets text static,"
				    		+ "volumeByteCount bigint static, "
							+ "volumeCharacterCount int static, "
				    		+ "sequence text, "
				    		+ "byteCount bigint, "
				    		+ "characterCount int, "
				    		+ "contents text, "
				    		+ "checksum text, "
				    		+ "checksumType text, "
				    		+ "pageNumberLabel text, "
				    		+ "PRIMARY KEY (volumeID, sequence))";
					cassandraManager.execute(createTableStr);
				}
	//	insertStatement = cassandraManager.prepare("INSERT INTO " + columnFamilyName + " (volumeID, sequence, byteCount, characterCount, contents, checksum, checksumType, pageNumberLabel)" + "VALUES(?,?,?,?,?,?,?,?);");
		
		/*batchUpdate = Boolean.parseBoolean(Configuration.getProperty("BATCH_UPDATE"));
		if(batchUpdate) {
			batchUpdater = new BatchUpdater();
		}*/
	}
	
	public static void main(String[] args) {
		CassandraIngester ingester = new CassandraIngester();
		
		//add several volumes into VOLUME_COLUMN_FAMILY
		
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		if(volumesToIngest == null || volumesToIngest.isEmpty()) {
			System.out.println("volume list is empty or null");
			return;
		}
		long t0 = System.currentTimeMillis();
		for(String id: volumesToIngest) {
			boolean success = ingester.ingestPages(id);
		//	System.out.println(id + " pages ingested");
			if(success) {
				pw.println(id);pw.flush();
				ingester.ingestMetadata(id);
			//	System.out.println(id + " metadata ingested");
			}
		}
		ingester.close();
		long t1 = System.currentTimeMillis();
		pw.flush();pw.close(); pw2.flush();pw2.close();
		System.out.println("done");
		System.out.println("time elapsed in millisecond: " + (t1 - t0));
		cassandraManager.shutdown();
	}

//	private void ingest() {}

	private void close() {
		/*if(batchUpdater != null) {
			batchUpdater.close();
		}*/
		
	}

	private static boolean checkTableExist(String tableName) {
		return cassandraManager.checkTableExist(tableName);
	}
	
	private void ingestMetadata(String id) {
		// TODO Auto-generated method stub
		
	}

	private boolean ingestPages(String id) {
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
		File volumeZipFile = null;
		File volumeMetsFile = null;
		if(Configuration.getProperty("SOURCE").equalsIgnoreCase("pairtree")) {
			volumeZipFile = Tools.getFileFromPairtree(pairtreePath, zipFileName);
			volumeMetsFile = Tools.getFileFromPairtree(pairtreePath, metsFileName);
		} else {
			String commonPath = "/home/zongpeng/data";
			for(int i = 0; i < 6; i ++) {
				File volumeDir = new File(commonPath + i + "/" + cleanIdPart);
				if(volumeDir.exists()) {
					volumeZipFile = new File(volumeDir, zipFileName);
					volumeMetsFile = new File(volumeDir, metsFileName);
					break;
				}
			}
		}
		
		if(volumeZipFile == null || volumeMetsFile == null || !volumeZipFile.exists() || !volumeMetsFile.exists()) {
			System.out.println("zip file or mets file does not exist for " + id);
			return false;
		}
		VolumeRecord volumeRecord = Tools.getVolumeRecord(id, volumeMetsFile);
		
		
		boolean volumeAdded = updatePages(volumeZipFile, volumeRecord);
		
		return volumeAdded;
	}

	private boolean updatePages(File volumeZipFile, VolumeRecord volumeRecord) {
		String volumeId = volumeRecord.getVolumeID();
		HashMap<String, List<String>> featuredPagesMap = new HashMap<String, List<String>>(); // feature maps to a list of page seqs
		
		boolean volumeAdded = false;
		
		boolean hasValidPage = false;
		BatchStatement batchStmt = new BatchStatement();
		try {
			long volumeByteCount = 0;
			long volumeCharacterCount = 0;
			int i=0;
			Insert firstPageInsert = null;
			ZipInputStream zis = new ZipInputStream(new FileInputStream(volumeZipFile));
			ZipEntry zipEntry = null;
			while((zipEntry = zis.getNextEntry()) != null) {
				String entryName = zipEntry.getName();
				String entryFilename = extractEntryFilename(entryName);
				PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
				if(pageRecord == null) {
					System.out.println("No PageRecord found by " + entryFilename + " in volume zip " + volumeZipFile.getAbsolutePath());
					continue;
				}
				if(entryFilename != null && !"".equals(entryFilename)) {
					//1. read page contents in bytes
					byte[] pageContents = readPagecontentsFromInputStream(zis);
					if(pageContents == null) {
						pw2.println("failed reading page contents for " + entryName + " of " + volumeId);pw2.flush();
						continue;
					}
					//2. verify byte count of this page
					if(pageContents.length != pageRecord.getByteCount() ) {
					//	System.out.println("Actual byte count and byte count from METS mismatch for entry " + entryName + " for volume " + volumeId + ". Actual: " + pageContents.length + " from METS: " + pageRecord.getByteCount());
					//	System.out.println("Recording actual byte count");
						pageRecord.setByteCount(pageContents.length);
						volumeByteCount += pageContents.length;
					} else {
						volumeByteCount += pageRecord.getByteCount();
						//System.out.println("verified page content for page " + entryFilename + " of " + volumeId);
					}
					//3. check against checksum of this page declared in METS
					String checksum = pageRecord.getChecksum();
					String checksumType = pageRecord.getChecksumType();
					try {
						String calculatedChecksum = Tools.calculateChecksum(pageContents, checksumType);
						if (!checksum.equals(calculatedChecksum)) {
					//		System.out.println("Actual checksum and checksum from METS mismatch for entry " + entryName + " for volume: " + volumeId + ". Actual: " + calculatedChecksum
					//				+ " from METS: " + checksum);
						//	System.out.println("Recording actual checksum");
							pageRecord.setChecksum(calculatedChecksum, checksumType);
						} else {
						//	System.out.println("verified checksum for page " + entryFilename + " of " + volumeId);
						}
					} catch (NoSuchAlgorithmException e) {
                        System.out.println("NoSuchAlgorithmException for checksum algorithm " + checksumType);
                        System.out.println("Using checksum found in METS with a leap of faith");
                    }
					//4. get 8-digit sequence for this page
					int order = pageRecord.getOrder();
					String sequence = generateSequence(order);
					pageRecord.setSequence(sequence);
					
					 //5 - convert to string and count character count -- NOTE: some pages are not encoded in utf-8, but there is no charset indicator, so assume utf-8 for all for now
                    String pageContentsString = new String(pageContents, "utf-8");
                    pageRecord.setCharacterCount(pageContentsString.length());
                    volumeCharacterCount += pageContentsString.length();
					//6. push page content into cassandra
               //     updatePage(volumeId, pageRecord, pageContentsString);
                    /*if(batchUpdate) {
                    	batchUpdater.updatePage(volumeId, pageRecord, pageContentsString);
                    } else {
                    	updatePage(volumeId, pageRecord, pageContentsString);
                    }
                    
					hasValidPage = true;*/
                    
              /*  	+ "sequence text, "
		    		+ "byteCount bigint, "
		    		+ "characterCount int, "
		    		+ "contents text, "
		    		+ "checksum text, "
		    		+ "checksumType text, "
		    		+ "pageNumberLabel text, "
		    		+ "PRIMARY KEY (volumeID, sequence))";*/
					Insert insertStmt = QueryBuilder
							.insertInto(columnFamilyName);
					insertStmt
							.value("volumeID", volumeId)
							.value("sequence", pageRecord.getSequence())
							.value("byteCount", pageRecord.getByteCount())
							.value("characterCount",
									pageRecord.getCharacterCount())
							.value("contents", pageContentsString)
							.value("checksum", pageRecord.getChecksum())
							.value("checksumType", pageRecord.getChecksumType())
							.value("pageNumberLabel", pageRecord.getLabel());
					batchStmt.add(insertStmt);
					if(i == 0) {
						firstPageInsert = insertStmt;
					}
					i++;
				}
			}
			zis.close();
			if (firstPageInsert != null) {
				firstPageInsert.value("accessLevel", i % 4)
						.value("language", languages[i % languages.length])
						.value("volumeByteCount", volumeByteCount)
						.value("volumeCharacterCount", volumeCharacterCount);
			}
			cassandraManager.execute(batchStmt);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			//log.error("IOException getting entry from ZIP " + volumeZipPath, e);
			System.out.println("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath());
		} catch (WriteFailureException e) {
			System.out.println("write failure for " + volumeId + ": " + e.getMessage());
		}
		 System.out.println("Successfully pushed all pages for volume " + volumeId);
		 
		 volumeAdded = true;
		return volumeAdded;
	}
	private static String[] languages = {"Chinese", "English", "Spanish", "German", "Japanese", "Korean"};
	/*private void updatePage(String volumeId, PageRecord pageRecord, String pageContentsString) {
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		boundStatement.bind(volumeId, pageRecord.getSequence(), pageRecord.getByteCount(), pageRecord.getCharacterCount(), pageContentsString, pageRecord.getChecksum(), pageRecord.getChecksumType(),
				pageRecord.getLabel());
		cassandraManager.execute(boundStatement);
	}*/

	private byte[] readPagecontentsFromInputStream(ZipInputStream zis) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int read = -1;
		byte[] buffer = new byte[32767];
		try {
			while((read = zis.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
		} catch (IOException e) {
			System.out.println("error reading zip stream" + e.getMessage());
		//	e.printStackTrace();
		}
		try {
			bos.close();
		} catch (IOException e) {
			System.out.println("IOException while attempting to close ByteArrayOutputStream()" + e.getMessage());
		}
		return bos.toByteArray();
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

    /*public static class BatchUpdater {

		int batchSize = Integer.parseInt(Configuration.getProperty("BATCH_SIZE"));
		int batch = batchSize;
		BatchStatement batchStatement = new BatchStatement();

		public void updatePage(String volumeId, PageRecord pageRecord, String pageContentsString) {
			BoundStatement boundStatement = new BoundStatement(insertStatement);
			if (batch > 0) {
				batchStatement.add(boundStatement.bind(volumeId, pageRecord.getSequence(), pageRecord.getByteCount(), pageRecord.getCharacterCount(), pageContentsString, pageRecord.getChecksum(),
						pageRecord.getChecksumType(), pageRecord.getLabel()));
				batch--;
			} else {
				System.out.println("batch execution size " + batchStatement.size());
				cassandraManager.execute(batchStatement);
				batchStatement.clear();
				batch = batchSize;
			}

		}
		
		public void close() {
			if(batch < batchSize) {
				cassandraManager.execute(batchStatement);
			}
		}
	}*/
 }

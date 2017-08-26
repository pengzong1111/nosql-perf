package edu.indiana.d2i.hbase;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.indiana.d2i.tools.Constants;
import edu.indiana.d2i.tools.Tools;
import edu.indiana.d2i.tools.METSParser.VolumeRecord;
import edu.indiana.d2i.tools.METSParser.VolumeRecord.PageRecord;


public class HBaseIngester {
	private static PrintWriter pw;
	private static PrintWriter pw2;
	
	private static Configuration conf = null;
	private HBaseAdmin hbaseAdmin = null;
	private HTable table = null;
	private static String tableName = edu.indiana.d2i.tools.Configuration.getProperty("HBASE_TABLE_NAME");
	private static String ZOOKEEPER_QUORUM = edu.indiana.d2i.tools.Configuration.getProperty("ZOOKEEPER_QUORUM");
	static {
		try {
			pw = new PrintWriter("ingested-hbase.txt");
			pw2 = new PrintWriter("failed-hbase.txt");
			conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM/*"crow.soic.indiana.edu,vireo.soic.indiana.edu,pipit.soic.indiana.edu"*/);
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
	}
	
	public HBaseIngester() {
		try {
			 this.hbaseAdmin = new HBaseAdmin(conf);
			 this.table = new HTable(conf, tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HBaseIngester ingester = new HBaseIngester();
		if(!ingester.checkTableExist(tableName)) {
			HTableDescriptor tableDesc = new HTableDescriptor(edu.indiana.d2i.tools.Configuration.getProperty("HBASE_TABLE_NAME"));
            for (int i = 0; i < columnFamilies.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamilies[i]));
            }
            try {
				ingester.createTable(tableDesc);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("error creating table" + tableName + " : " + e.getMessage());
				return;
			}
            System.out.println("create table " + tableName + " ok.");
		} else {
			System.out.println(edu.indiana.d2i.tools.Configuration.getProperty("HBASE_TABLE_NAME") + " already exists");
		}
		//add several volumes into VOLUME_COLUMN_FAMILY
		
		List<String> volumesToIngest = Tools.getVolumeIds(new File(edu.indiana.d2i.tools.Configuration.getProperty("VOLUME_ID_LIST")));
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
			} else {
				pw2.println(id); pw2.flush();
			}
		}
		ingester.close();
		long t1 = System.currentTimeMillis();
		pw.flush();pw.close(); pw2.flush();pw2.close();
		System.out.println("done");
		System.out.println("time elapsed in millisecond: " + (t1 - t0));
		ingester.close();
	}

//	private void ingest() {}

	private void createTable(HTableDescriptor tableDesc) throws IOException {
		hbaseAdmin.createTable(tableDesc);
	}

	private void close() {
		/*if(batchUpdater != null) {
			batchUpdater.close();
		}*/
		
	}

	private boolean checkTableExist(String tableName) {
		try {
			return hbaseAdmin.tableExists(tableName);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
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
		if(edu.indiana.d2i.tools.Configuration.getProperty("SOURCE").equalsIgnoreCase("pairtree")) {
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

	//language, accessLevel, etc for volumeMetadata
	private static String[] columnFamilies = {"contents", "byteCount", "characterCount", "checksum", "pagenumberlabel", "volumeMetadata"};
	private boolean updatePages(File volumeZipFile, VolumeRecord volumeRecord) {
		String volumeId = volumeRecord.getVolumeID();
		HashMap<String, List<String>> featuredPagesMap = new HashMap<String, List<String>>(); // feature maps to a list of page seqs
		//List<Put> puts = new LinkedList<Put>();
		Put put = new Put(Bytes.toBytes(volumeId));
		boolean volumeAdded = false;
		boolean hasValidPage = false;
		try {
			long volumeByteCount = 0;
			long volumeCharacterCount = 0;
			int i=0;
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
				//		System.out.println("Actual byte count and byte count from METS mismatch for entry " + entryName + " for volume " + volumeId + ". Actual: " + pageContents.length + " from METS: " + pageRecord.getByteCount());
				//		System.out.println("Recording actual byte count");
						pageRecord.setByteCount(pageContents.length);
						volumeByteCount += pageContents.length;
					} else {
						volumeByteCount += pageRecord.getByteCount();
				//		System.out.println("verified page content for page " + entryFilename + " of " + volumeId);
					}
					//3. check against checksum of this page declared in METS
					String checksum = pageRecord.getChecksum();
					String checksumType = pageRecord.getChecksumType();
					try {
						String calculatedChecksum = Tools.calculateChecksum(pageContents, checksumType);
						if (!checksum.equals(calculatedChecksum)) {
					//		System.out.println("Actual checksum and checksum from METS mismatch for entry " + entryName + " for volume: " + volumeId + ". Actual: " + calculatedChecksum
					//				+ " from METS: " + checksum);
					//		System.out.println("Recording actual checksum");
							pageRecord.setChecksum(calculatedChecksum, checksumType);
						} else {
					//		System.out.println("verified checksum for page " + entryFilename + " of " + volumeId);
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
                    
                    try{
                    	put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes(pageRecord.getSequence()), Bytes
                                .toBytes(pageContentsString));
                    } catch(NullPointerException e) {
                    	System.out.println("null contents");
                    }
                    
                    put.addColumn(Bytes.toBytes("byteCount"), Bytes.toBytes(pageRecord.getSequence()), Bytes
                            .toBytes(pageRecord.getByteCount()));
                    put.addColumn(Bytes.toBytes("characterCount"), Bytes.toBytes(pageRecord.getSequence()), Bytes
                            .toBytes(pageRecord.getCharacterCount()));
                    try {
                    	put.addColumn(Bytes.toBytes("checksum"), Bytes.toBytes(pageRecord.getSequence()), Bytes
                                .toBytes(pageRecord.getChecksum()));
                    } catch (NullPointerException e) {
                    	System.out.println("null checksum");
                    }
                    try {
                    	put.addColumn(Bytes.toBytes("pagenumberlabel"), Bytes.toBytes(pageRecord.getSequence()), Bytes
                                .toBytes(pageRecord.getLabel()));
                    } catch (NullPointerException e) {
                    //	System.out.println("null pagenumberlabel");
                    }
                    
					if(i == 0) {
						try {
							put.addColumn(Bytes.toBytes("checksum"), Bytes.toBytes("type"), Bytes
		                            .toBytes(pageRecord.getChecksumType()));
						} catch(NullPointerException e) {
							System.out.println("null checksumtype");
						}
						
					}
					i++;
				}
			}
			zis.close();
			
			put.addColumn(Bytes.toBytes("volumeMetadata"), Bytes.toBytes("language"), Bytes
                    .toBytes(languages[i % languages.length]));
			put.addColumn(Bytes.toBytes("volumeMetadata"), Bytes.toBytes("accessLevel"), Bytes
                    .toBytes(i%4));
			put.addColumn(Bytes.toBytes("volumeMetadata"), Bytes.toBytes("volumeByteCount"), Bytes
                    .toBytes(volumeByteCount));
			put.addColumn(Bytes.toBytes("volumeMetadata"), Bytes.toBytes("volumeCharacterCount"), Bytes
                    .toBytes(volumeByteCount));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			//log.error("IOException getting entry from ZIP " + volumeZipPath, e);
			System.out.println("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath());
		} 
		
		try {
			table.put(put);
		} catch (IOException e) {
			System.out.println("error adding volume " + volumeId + ", " + e.getMessage());
			return false;
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

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 }

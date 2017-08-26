package edu.indiana.d2i.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import edu.indiana.d2i.mongodb.MongoBulkReader;

public class HBaseBulkReader {
	/* args0 is the path of the list of volumes to load ids
	 * args1 is the size of request
	 */
	public static void main(String[] args) throws IOException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", edu.indiana.d2i.tools.Configuration.getProperty("ZOOKEEPER_QUORUM"));
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    	String tableName = edu.indiana.d2i.tools.Configuration.getProperty("HBASE_TABLE_NAME");

        HTable table = new HTable(conf, "default:" + tableName);
        List<String> volumeList = new LinkedList<String>();
        MongoBulkReader.loadVolumeIds(volumeList, args[0], Integer.valueOf(args[1]));
      //  String rowKey = "mdp.39015077053844";
        List<Get> getList = new LinkedList<Get>();
        for(String volumeId : volumeList) {
        	Get get = new Get(volumeId.getBytes());
        	get.addFamily("contents".getBytes());
        	getList.add(get);
        }
        
        
      //  get.addFamily("contents".getBytes());
       /* get.addColumn("contents".getBytes(), "00000006".getBytes());
        get.addColumn("contents".getBytes(), "00000009".getBytes());
        get.addColumn("contents".getBytes(), "00000019".getBytes());*/
        /*get.addColumn("contents".getBytes(), "00000009".getBytes());
        Result rs = table.get(get);
        Cell cell = rs.getColumnLatestCell("contents".getBytes(), "00000009".getBytes());
        System.out.println(new String(CellUtil.cloneValue(cell)));*/
        long t0 = System.currentTimeMillis();
        Result[] results = null;
        if(getList.size() == 1) {
        	Result tmp = table.get(getList.get(0));
        	results = new Result[1];
        	results[0] = tmp;
        } else {
        	results = table.get(getList);
        }
        
        long t1 = System.currentTimeMillis();
        System.out.println("read time is " + (t1-t0));
        long size = 0;
        for(Result result : results ) {
        	NavigableMap<byte[], byte[]> map = result.getFamilyMap("contents".getBytes());
        	if(map == null) {
        		System.out.println(" null result ...");
        		continue;
        	}
            for(Entry<byte[], byte[]> entry : map.entrySet()) {
            	size += entry.getValue().length;
            	//System.out.println(new String(entry.getKey()));
            	//System.out.println(new String(entry.getValue()));
            	//System.out.println("===========");
            	//Thread.sleep(2000);
            }
        }
        System.out.printf("overall volume read is %d \n", results.length);
		System.out.printf("overall size is %d bytes \n", size);
		long time = (t1-t0)/1000;
		System.out.printf("time elapsed: %d seconds \n", time);
		System.out.printf("throughput is %d MB/s \n",  size/1024/1024/time);
        /*Result rs = results[0];
        NavigableMap<byte[], byte[]> map = rs.getFamilyMap("contents".getBytes());
        for(Entry<byte[], byte[]> entry : map.entrySet()) {
        	System.out.println(new String(entry.getKey()));
        	System.out.println(new String(entry.getValue()));
        	System.out.println("===========");
        	Thread.sleep(2000);
        }*/
       
	

	}

}

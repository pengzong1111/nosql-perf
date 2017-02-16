package edu.indiana.d2i.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HBaseUtil {

	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "crow.soic.indiana.edu,vireo.soic.indiana.edu,pipit.soic.indiana.edu");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    	String tableName = edu.indiana.d2i.tools.Configuration.getProperty("HBASE_TABLE_NAME");

        HTable table = new HTable(conf, "default:" + tableName);
        String rowKey = "mdp.39015077053844";
        Get get = new Get(rowKey.getBytes());
      //  get.addFamily("contents".getBytes());
        get.addColumn("contents".getBytes(), "00000006".getBytes());
        get.addColumn("contents".getBytes(), "00000009".getBytes());
        get.addColumn("contents".getBytes(), "00000019".getBytes());
        /*get.addColumn("contents".getBytes(), "00000009".getBytes());
        Result rs = table.get(get);
        Cell cell = rs.getColumnLatestCell("contents".getBytes(), "00000009".getBytes());
        System.out.println(new String(CellUtil.cloneValue(cell)));*/
        long t0 = System.currentTimeMillis();
        Result rs = table.get(get);
        long t1 = System.currentTimeMillis();
        NavigableMap<byte[], byte[]> map = rs.getFamilyMap("contents".getBytes());
       
        System.out.println("read time is " + (t1-t0));
        for(Entry<byte[], byte[]> entry : map.entrySet()) {
        	System.out.println(new String(entry.getKey()));
        	System.out.println(new String(entry.getValue()));
        	System.out.println("===========");
        	Thread.sleep(2000);
        }
       
	}
}

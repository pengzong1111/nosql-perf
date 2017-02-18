package edu.indiana.d2i.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class YcsbResultParser {

	public static void main(String[] args) throws IOException {
		File result1 = new File("/home/zong/failover-test/shard-failure/2-node-down/1");
		File result2 = new File("/home/zong/failover-test/shard-failure/2-node-down/2");
		File result3 = new File("/home/zong/failover-test/shard-failure/2-node-down/3");
		
		LinkedHashMap<String, String> map1 = parseYcsbResults(result1);
		LinkedHashMap<String, String> map2 = parseYcsbResults(result2);
		LinkedHashMap<String, String> map3 = parseYcsbResults(result3);
		
		List<Iterator<Entry<String, String>>> list = new LinkedList<Iterator<Entry<String, String>>>();
		list.add(map1.entrySet().iterator());
		list.add(map2.entrySet().iterator());
		list.add(map3.entrySet().iterator());
		
		Map<String, Double> map = merge(list);
		PrintWriter pw = new PrintWriter("merged.txt");
		pw.println("second" + '\t' + "throughput");
		for(Entry<String, Double> entry : map.entrySet()) {
			pw.println(entry.getKey() + '\t' + entry.getValue());
			pw.flush();
		}
		pw.flush();pw.close();
	}
	
	
	
	private static Map<String, Double> merge(List<Iterator<Entry<String, String>>> iterList) {
		Map<String, Double> mergedMap = new LinkedHashMap<String, Double>();
		while(true) {
			String key = null;
			double value = 0;
			for(Iterator<Entry<String, String>> iterator: iterList) {
				if(iterator.hasNext()) {
					Entry<String, String> entry = iterator.next();
					key = entry.getKey();
					value += Double.parseDouble(entry.getValue());
				} else {
					return mergedMap;
				}
			}
			mergedMap.put(key, value);
		}
	}



	public static LinkedHashMap<String, String> parseYcsbResults(File input) throws IOException {
		
	//	File input = new File("/home/zong/failover-test/shard-failure/3");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		String line = null;
		while((line = br.readLine()) != null) {
			if(line.contains("; ")) {
				String[] splits = line.split("; ");
				print(splits);
				String seconds = splits[0].split(" ")[2];
				String opsPerSec = "0";
				if(splits[1].contains("current ops/sec")) {
					opsPerSec = splits[1].split(" ")[0];
				}
				map.put(seconds, opsPerSec);
			}
		}
		br.close();
		return map;
	}

	private static void print(String[] splits) {
		for(String s : splits) {
			System.out.println(s);
		}
		
	}

}

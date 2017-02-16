package edu.indiana.d2i.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class YcsbResultParser {

	public static void main(String[] args) throws IOException {
	}
	
	public static Map<String, String> parseYcsbResults(File input) throws IOException {
		
	//	File input = new File("/home/zong/failover-test/shard-failure/3");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
		Map<String, String> map = new HashMap<String, String>();
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

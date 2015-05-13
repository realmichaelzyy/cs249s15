package edu.ucla.cs249;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	static public void main(String[] args) {
		System.out.println("--- HDFS Test ---");
		
		try {
			URI uri = URI.create ("hdfs://54.88.56.9:8020/abc.txt");
			Configuration conf = new Configuration ();
			FileSystem file = FileSystem.get (uri, conf);
			FSDataInputStream in = file.open(new Path(uri));
			String content = in.toString();
			System.out.println(content);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

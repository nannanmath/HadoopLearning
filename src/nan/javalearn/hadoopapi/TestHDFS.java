package nan.javalearn.hadoopapi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestHDFS {
	/**
	 * Read file from hdfs by URL.
	 */
	@Test
	public void readFileBuyUrl() throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL("hdfs://192.168.137.201:8020/usr/centos/hadoop/a.txt");
		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		byte[] buf = new byte[is.available()];
		is.read(buf);
		is.close();
		String str = new String(buf);
		System.out.println(str);
	}
	
	/**
	 * Read file from hdfs by API.
	 */
	@Test
	public void readFileByApi() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020");
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/usr/centos/hadoop/a.txt");
		FSDataInputStream fis = fs.open(p);
		byte[] buf = new byte[1024];
		int len = -1;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while((len = fis.read(buf)) != -1) {
			baos.write(buf, 0, len);
		}
		fis.close();
		baos.close();
		System.out.println(new String(baos.toByteArray()));
	}
	
	/**
	 * Read file from hdfs by API 2.
	 */
	@Test
	public void readFileByApi2() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020");
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/usr/centos/hadoop/a.txt");
		FSDataInputStream fis = fs.open(p);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		IOUtils.copyBytes(fis, baos, 1024);
		baos.close();
		System.out.println(new String(baos.toByteArray()));
	}
	
	/**
	 * mkdir.
	 * NOTE: $> hdfs dfs -chmod 777 /usr
	 * @throws Exception 
	 */
	@Test
	public void mkdirByApi() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020");
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/usr/win7admin");
		fs.mkdirs(p);
	}
	
	/**
	 * putFile.
	 */
	@Test
	public void putFileByApi() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020");
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/usr/win7admin/win7.txt");
		FSDataOutputStream fsdos = fs.create(p);
		fsdos.writeBytes("Hello, Windows7!");
		fsdos.close();
	}
	
	/**
	 * removeFile.
	 */
	@Test
	public void removeFileByApi() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020");
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/usr/win7admin");
		fs.delete(p, true);
	}
	
	
}

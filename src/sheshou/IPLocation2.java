package sheshou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by IntelliJ IDEA.
 * Time:   9/7/15 3:05 AM
 *
 * @author jianghe.cao
 */
public class IPLocation2 {
  private static Logger logger = LoggerFactory.getLogger(IPLocation2.class);
  private ByteBuffer dataBuffer = null;
  private FSDataInputStream fin = null;
  private ByteBuffer indexBuffer = null;
  private int offset;
  private int[] index = new int[256];

  public IPLocation2(String fileName) {
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      this.dataBuffer = ByteBuffer.allocate(((int) fs.getFileStatus(new Path(fileName)).getLen()));
      this.fin = fs.open(new Path(fileName));

      int readBytesLength;
      byte[] chunk = new byte[4096];
      while (this.fin.available() > 0) {
        readBytesLength = this.fin.read(chunk);
        this.dataBuffer.put(chunk, 0, readBytesLength);
      }
      this.dataBuffer.position(0);
      int indexLength = this.dataBuffer.getInt();
      byte[] indexBytes = new byte[indexLength];
      this.dataBuffer.get(indexBytes, 0, indexLength - 4);
      this.indexBuffer = ByteBuffer.wrap(indexBytes);
      this.indexBuffer.order(ByteOrder.LITTLE_ENDIAN);
      this.offset = indexLength;

      int loop = 0;
      while (loop++ < 256) {
        this.index[loop - 1] = this.indexBuffer.getInt();
      }
      this.indexBuffer.order(ByteOrder.BIG_ENDIAN);
    } catch (IOException ioe) {
      logger.info("Error in IpLocation: " + ioe.getMessage());
    } finally {
      try {
        this.fin.close();
      } catch (IOException e) {
        logger.info("Error in IpLocation: " + e.getMessage());
      }
    }
  }

  public String[] find(String ip) {
    int ip_prefix_value = new Integer(ip.substring(0, ip.indexOf(".")));
    long ip2long_value = ip2long(ip);
    int start = this.index[ip_prefix_value];
    int max_comp_len = this.offset - 1028;
    long tmpInt;
    long index_offset = -1;
    int index_length = -1;
    byte b = 0;
    for (start = start * 8 + 1024; start < max_comp_len; start += 8) {
      tmpInt = int2long(this.indexBuffer.getInt(start));
      if (tmpInt >= ip2long_value) {
        index_offset =
          bytesToLong(b, this.indexBuffer.get(start + 6), this.indexBuffer.get(start + 5),
            this.indexBuffer.get(start + 4));
        index_length = 0xFF & this.indexBuffer.get(start + 7);
        break;
      }
    }

    this.dataBuffer.position(this.offset + (int) index_offset - 1024);
    byte[] areaBytes = new byte[index_length];
    this.dataBuffer.get(areaBytes, 0, index_length);

    String region = "";
    try {
      region = new String(areaBytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      region =new String(areaBytes);
    }

    return region.split("\t");
  }

  private long bytesToLong(byte a, byte b, byte c, byte d) {
    return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
  }

  private int str2Ip(String ip) {
    String[] ss = ip.split("\\.");
    int a, b, c, d;
    a = Integer.parseInt(ss[0]);
    b = Integer.parseInt(ss[1]);
    c = Integer.parseInt(ss[2]);
    d = Integer.parseInt(ss[3]);
    return (a << 24) | (b << 16) | (c << 8) | d;
  }

  private long ip2long(String ip) {
    return int2long(str2Ip(ip));
  }

  private long int2long(int i) {
    long l = i & 0x7fffffffL;
    if (i < 0) {
      l |= 0x080000000L;
    }
    return l;
  }
  
}

package com.yahoo.ycsb.db;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.http.impl.cookie.PublicSuffixDomainFilter;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class AlluxioClient extends DB {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private FileSystemContext mFileSystemContext = null;
  private FileSystemMasterClient mFileSystemMasterClient = null;

  private AlluxioURI mMasterLocation = null;
  private AlluxioURI mTestDir = null;
  private String mTestPath = "/default_rpc_test_files";

  @Override
  public void init() throws DBException {

    //String masterAddress = null;
    String masterAddress = "alluxio://localhost:19998";      // default value of master address: localhost.

    // Load configuration from property files.
    // TODO: figure out how to read from property file.
    if (masterAddress == null) {
      try {
        InputStream propFile = AlluxioClient.class.getClassLoader()
                .getResourceAsStream("alluxio.properties");
        Properties props = new Properties(System.getProperties());
        props.load(propFile);
        masterAddress = props.getProperty("alluxio.master.address");
        if (masterAddress == null) {
          System.out.println("Can not load alluxio.master.address property from configuraiotn file");
        }
      } catch (Exception e) {
        System.err.println("The property file doesn't exist");
        e.printStackTrace();
      }
    }

    // Initialize class members.
    mFileSystemContext = FileSystemContext.INSTANCE;
    mFileSystemMasterClient = mFileSystemContext.acquireMasterClient();
    mMasterLocation = new AlluxioURI(masterAddress);
    mTestDir = new AlluxioURI(mTestPath);
    Configuration.set(PropertyKey.MASTER_HOSTNAME, mMasterLocation.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mMasterLocation.getPort()));
    ClientContext.init();

    System.out.println("Master hostname:" + Configuration.get(PropertyKey.MASTER_HOSTNAME));
    System.out.println("Master port:" + Configuration.get(PropertyKey.MASTER_RPC_PORT));
  }


  @Override
  public Status read(
          String table, String key, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(
          String table, String startkey, int recordcount, Set<String> fields,
          Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
          String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(
          String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

}

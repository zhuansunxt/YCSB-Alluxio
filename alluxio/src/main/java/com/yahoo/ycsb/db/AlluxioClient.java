package com.yahoo.ycsb.db;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.security.authentication.AuthType;
import alluxio.wire.WorkerInfo;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.http.impl.client.NullBackoffStrategy;
import org.apache.http.impl.cookie.PublicSuffixDomainFilter;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;

public class AlluxioClient extends DB {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterClient mBlockMasterClient = null;
  private FileSystemContext mFileSystemContext = null;
  private FileSystemMasterClient mFileSystemMasterClient = null;

  private AlluxioURI mMasterLocation = null;
  private AlluxioURI mTestDir = null;
  private String mTestPath = "/default_rpc_test_files";

  // only for getStatus() RPC call's test.
  private String getStatusTestDir = "/tmp_dir_ycsb_getstatus";
  private String getStatusTestFile = "/tmp_dir_ycsb_getstatus/tmp_file_ycsb_getstatus";

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


    mMasterLocation = new AlluxioURI(masterAddress);
    Configuration.set(PropertyKey.MASTER_HOSTNAME, mMasterLocation.getHost());
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mMasterLocation.getPort()));
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);

    mFileSystemContext = FileSystemContext.INSTANCE;
    mFileSystemMasterClient = mFileSystemContext.acquireMasterClient();
    mBlockMasterClient = new RetryHandlingBlockMasterClient(
            new InetSocketAddress(mMasterLocation.getHost(), mMasterLocation.getPort()));

    mTestDir = new AlluxioURI(mTestPath);
    ClientContext.init();

    // create a directory and file for getStatus() RPC call.
    createFileForReadonlyRPC(getStatusTestDir, getStatusTestFile);

    // console logging.
    System.out.println("Master hostname:" + Configuration.get(PropertyKey.MASTER_HOSTNAME));
    System.out.println("Master port:" + Configuration.get(PropertyKey.MASTER_RPC_PORT));

    checkRPCConnectivity();
  }

  @Override
  public Status read(
          String table, String key, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    try {
      long capacity = mBlockMasterClient.getCapacityBytes();
      System.out.println("Capacity:" + capacity);
    } catch (Exception e) {
      System.out.println("Not possible to issue read-only RPC");
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(
          String table, String startkey, int recordcount, Set<String> fields,
          Vector<HashMap<String, ByteIterator>> result) {
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
    try {
      long capacity = mBlockMasterClient.getUsedBytes();
      System.out.println("Used:" + capacity);
    } catch (Exception e) {
      System.out.println("Not possible to issue read-only RPC");
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private Status checkRPCConnectivity() {
    try {
      // Read-only RPCs
      long capacity = mBlockMasterClient.getCapacityBytes();
      long used = mBlockMasterClient.getUsedBytes();
      List<WorkerInfo> workerInfoList = mBlockMasterClient.getWorkerInfoList();
      AlluxioURI fileForRead = new AlluxioURI(getStatusTestFile);
      URIStatus status = mFileSystemMasterClient.getStatus(fileForRead);

      System.out.println("--- Read-only-RPC-Connectivity-Check ---");
      System.out.println("Read-only RPCs");
      System.out.println("Capacity: " + capacity);
      System.out.println("Used-bytes: " + used);
      System.out.println("Worker-info-list: ");
      for (WorkerInfo worker : workerInfoList) {
        System.out.println("[Worker" + worker.getId() + "] " + worker.getState());
      }
      System.out.println("Status for test file " + getStatusTestFile + ":");
      System.out.println("[Group] " + status.getGroup());
      System.out.println("[Name] " + status.getName());
      System.out.println("[Owner] " + status.getOwner());
      System.out.println("[UFS-path] " + status.getUfsPath());
      System.out.println("[Creation-time] " + status.getCreationTimeMs());
      System.out.println("------------------------------------------");
    } catch (Exception e) {
      System.out.println("Unable to issue all types of RPCs to Alluxio Master");
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void createFileForReadonlyRPC(String dir, String file) {
    AlluxioURI dirPath = new AlluxioURI(dir);
    AlluxioURI filePath = new AlluxioURI(file);

    try {
      mFileSystemMasterClient.delete(dirPath, DeleteOptions.defaults());
      mFileSystemMasterClient.createDirectory(dirPath, CreateDirectoryOptions.defaults());
      mFileSystemMasterClient.createFile(filePath, CreateFileOptions.defaults());
    } catch (Exception e) {
      System.out.println("Unable to create directory and file for read-only RPC");
      e.printStackTrace();
    }
  }
}

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
import alluxio.client.file.options.*;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.security.authentication.AuthType;
import alluxio.wire.WorkerInfo;
import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;

public class AlluxioClient extends DB {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterClient mBlockMasterClient = null;
  private FileSystemContext mFileSystemContext = null;
  private FileSystemMasterClient mFileSystemMasterClient = null;

  private AlluxioURI mMasterLocation = null;
  private String defaultDir = null;
  private AlluxioURI mTestDir = null;
  private String mTestPath = "/default_rpc_test_files";

  // only for checking connectivity.
  private String getStatusTestDir = "/tmp_dir_ycsb_getstatus";
  private String getStatusTestFile = "/tmp_dir_ycsb_getstatus/tmp_file_ycsb_getstatus";

  /**
   * Cleanup client resources.
   */
  @Override
  public void cleanup() throws DBException {
    System.out.println("[Alluxio-YCSB] cleanup called");

    try {
      DeleteOptions deleteOption = DeleteOptions.defaults();
      deleteOption.setRecursive(true);
      mFileSystemMasterClient.delete(new AlluxioURI(defaultDir), deleteOption);
    } catch (Exception e){
      System.err.println("Could not delete the default directory "+ defaultDir);
    }

    try {
      mBlockMasterClient.close();
      System.out.println("Allxio Block Master client is shut down successfully");
    } catch (Exception e) {
      System.err.println("Could not shut down Alluxio Block Master Client");
    } finally {
      if (mBlockMasterClient != null)
        mBlockMasterClient = null;
    }

    try {
      mFileSystemMasterClient.close();
      System.out.println("Allxio FS Master client is shut down successfully");
    } catch (Exception e) {
      System.err.println("Could not shut down Alluxio FS Master client");
    } finally {
      if (mFileSystemMasterClient != null)
        mFileSystemMasterClient = null;
    }
  }

  // TODO(Xiaotong): make the initialization configurable by setting configuration file.
  @Override
  public void init() throws DBException {
    System.out.println("[Alluxio-YCSB] init called");
    //String masterAddress = null;
    String masterAddress = "alluxio://localhost:19998";      // default value of master address: localhost.

    // Load configuration from property files.
    // TODO(Xiaotong): figure out how to read from property file.
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

    // console logging.
    System.out.println("Master hostname:" + Configuration.get(PropertyKey.MASTER_HOSTNAME));
    System.out.println("Master port:" + Configuration.get(PropertyKey.MASTER_RPC_PORT));

    // create default directory for insertion.
    try {
      // TODO(Xiaotong): figure out how to configure the table(directory) to use.
      defaultDir = "/usertable";
      AlluxioURI alluxioDefaultDir = new AlluxioURI(defaultDir);
      mFileSystemMasterClient.createDirectory(alluxioDefaultDir, CreateDirectoryOptions.defaults());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Create files under a certain directory.
   * RPC invoked: {@link alluxio.thrift.FileSystemMasterClientService.Iface}.createFile(path, option)
   *
   * @param dir name of the file's parent directory.
   * @param file name of the file.
   * @param values Ignored.
   * @return OK on success, ERROR otherwise. See the
   *         {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status insert(
          String dir, String file, HashMap<String, ByteIterator> values) {
    System.out.println("[Alluxio-YCSB] insert called");

    try {
      // "/foo/bar" = "/" + "foo" + "/" + "bar".
      AlluxioURI alluxioFile = new AlluxioURI("/"+dir+"/"+file);
      mFileSystemMasterClient.createFile(alluxioFile, CreateFileOptions.defaults());
    } catch (Exception e) {
      System.err.println("Could not create the file"+"/"+dir+"/"+file);
      if (e instanceof FileAlreadyExistsException) return Status.OK;
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Delete a file from Alluxio.
   * RPC invoked: {@link alluxio.thrift.FileSystemMasterClientService.Iface}.remove(path, option)
   *
   * @param dir name of the file's parent directory.
   * @param file name of the file.
   * @return OK on success. Otherwise return ERROR. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String dir, String file) {
    System.out.println("[Alluxio-YCSB] delete called");
    try {
      // "/foo/bar" = "/foo" + "/" + "bar".
      AlluxioURI fullpath = new AlluxioURI("/"+dir+"/"+file);
      mFileSystemMasterClient.delete(fullpath, DeleteOptions.defaults());
    } catch (Exception e) {
      System.err.println("Could not delete the file "+"/"+dir+"/"+file);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }


  /**
   * Get status of a file.
   * RPC invoked: {@link alluxio.thrift.FileSystemMasterClientService.Iface}.getStatus
   *
   * @param dir name of the file's parent directory.
   * @param file name of the file.
   * @param fields Ignored.
   * @param result Store file's status.
   * @return OK on success. Otherwise return ERROR. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status read(
          String dir, String file, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    System.out.println("[Alluxio-YCSB] read called");
    try {
      AlluxioURI alluxioFile = new AlluxioURI("/"+dir+"/"+file);
      URIStatus alluxioFileStatus = mFileSystemMasterClient.getStatus(alluxioFile);

      byte[] statusToStream = alluxioFileStatus.toString().getBytes();
      result.put(alluxioFile.toString(), new ByteArrayByteIterator(statusToStream));
    } catch (Exception e) {
      System.err.println("Could not get status of file "+"/"+dir+"/"+file);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Set attribute of a file on Alluxio.
   * RPC invoked: {@link alluxio.thrift.FileSystemMasterClientService.Iface}.setAttribute(path, option)
   *
   * @param dir name of the file's parent directory.
   * @param file name of the file.
   * @param values Ignored.
   * @return OK on success. Otherwise return ERROR. See the
   * {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status update(
          String dir, String file, HashMap<String, ByteIterator> values) {
    System.out.println("[Alluxio-YCSB] update called");
    try {
      AlluxioURI alluxioFile = new AlluxioURI("/"+dir+"/"+file);
      mFileSystemMasterClient.setAttribute(alluxioFile, SetAttributeOptions.defaults());
    } catch (Exception e) {
      System.err.println("Could not set attribute to the file "+"/"+dir+"/"+file);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Leave this unimplemented. There's no equivalent RPC operation on
   * Alluxio master metadata server.
   * Please make sure than there's no scan operation in your workload.
   */
  @Override
  public Status scan(
          String table, String startkey, int recordcount, Set<String> fields,
          Vector<HashMap<String, ByteIterator>> result) {
    System.out.println("[Alluxio-YCSB] scan called");
    return Status.NOT_IMPLEMENTED;
  }




  /**
   * For debugging usage. Check all RPCs' connectivity with Alluxio master server.
   * @return YCSB status.
   */
  private Status checkRPCConnectivity() {
    try {
      // Read-only RPCs.
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

      // Update RPCs.
    } catch (Exception e) {
      System.out.println("Unable to issue all types of RPCs to Alluxio Master");
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Creating a file for read-only RPC getStatus() invocation.
   * @param dir directory containing that file.
   * @param file file path for getStatus().
   */
  private void createFileForReadonlyRPC(String dir, String file) {
    AlluxioURI dirPath = new AlluxioURI(dir);
    AlluxioURI filePath = new AlluxioURI(file);

    // Delete dir and file if exists.
    try {
      mFileSystemMasterClient.delete(dirPath, DeleteOptions.defaults());
      mFileSystemMasterClient.delete(filePath, DeleteOptions.defaults());
    } catch (Exception e) {
      // Ignore Alluxio exception.
      if (!(e instanceof AlluxioException)) e.printStackTrace();
    }

    // Create dir and file.
    try {
      mFileSystemMasterClient.createDirectory(dirPath, CreateDirectoryOptions.defaults());
      mFileSystemMasterClient.createFile(filePath, CreateFileOptions.defaults());
    } catch (Exception e) {
      System.out.println("Unable to create directory and file for read-only RPC");
      e.printStackTrace();
    }
  }
}

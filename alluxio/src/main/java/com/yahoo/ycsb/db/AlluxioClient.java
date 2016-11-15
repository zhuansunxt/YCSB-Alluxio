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
  private String mDefaultDir = null;

  /**
   * Cleanup client resources.
   */
  @Override
  public void cleanup() throws DBException {
    System.out.println("[Alluxio-YCSB] cleanup called");

    try {
      DeleteOptions deleteOption = DeleteOptions.defaults();
      deleteOption.setRecursive(true);
      mFileSystemMasterClient.delete(new AlluxioURI(mDefaultDir), deleteOption);
    } catch (Exception e){
      System.err.println("Could not delete the default directory "+ mDefaultDir);
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

  @Override
  public void init() throws DBException {
    System.out.println("[Alluxio-YCSB] init called");

    // TODO: master address should be initialized by reading config file.
    String masterAddress = "alluxio://localhost:19998";

    // TODO: Load configuration from property files.
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
    // Authentication configuration: make client compatible with all types of master RPC server.
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);

    mFileSystemContext = FileSystemContext.INSTANCE;
    mFileSystemMasterClient = mFileSystemContext.acquireMasterClient();
    mBlockMasterClient = new RetryHandlingBlockMasterClient(
            new InetSocketAddress(mMasterLocation.getHost(), mMasterLocation.getPort()));

    ClientContext.init();

    // Console logging metadata master server's address.
    System.out.println("Master hostname:" + Configuration.get(PropertyKey.MASTER_HOSTNAME));
    System.out.println("Master port:" + Configuration.get(PropertyKey.MASTER_RPC_PORT));

    // Create default directory for insertion.
    try {
      mDefaultDir = "/usertable";
      AlluxioURI alluxioDefaultDir = new AlluxioURI(mDefaultDir);
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
}

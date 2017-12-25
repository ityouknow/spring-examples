package com.neo.fastdfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.FileInfo;
import org.csource.fastdfs.ServerInfo;
import org.csource.fastdfs.StorageClient;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;

public class FileManager implements FileManagerConfig {

	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(FileManager.class);

	private static TrackerClient trackerClient;
	private static TrackerServer trackerServer;
	private static StorageClient storageClient;
	private static StorageServer storageServer;

	static {
			try {
				//String classPath = FileManager.class.getClassLoader().getResource("/").toURI().getPath();   //本地测试使用
				String  classPath =FileManager.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			     logger.info("Fast DFS test classPath:" + classPath+"***********");
				String path = FileManager.class.getClassLoader().getResource("/").toURI().getPath();
				 logger.info("Fast DFS test path:" + path+"***********");
			     //classPath =  classPath.substring(0,classPath.indexOf("com/neo"));
				if("\\".equals(File.separator)){
					classPath = path.replace("/", "\\");
				}
			String fdfsClientConfigFilePath = classPath +File.separator+ CLIENT_CONFIG_FILE;
			logger.info("Fast DFS configuration file path:" + fdfsClientConfigFilePath);
			ClientGlobal.init(fdfsClientConfigFilePath);

			trackerClient = new TrackerClient();
			trackerServer = trackerClient.getConnection();
			//在这里初始化复用，会有并发问题
			//storageClient = new StorageClient(trackerServer, storageServer);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public static String[] upload(FastDFSFile file) {
		logger.info("File Name: " + file.getName() + "File Length:" + file.getContent().length);

		NameValuePair[] meta_list = new NameValuePair[3];
		meta_list[0] = new NameValuePair("width", "120");
		meta_list[1] = new NameValuePair("heigth", "120");
		meta_list[2] = new NameValuePair("author", "Diandi");

		long startTime = System.currentTimeMillis();
		String[] uploadResults = null;
		try {
			storageClient = new StorageClient(trackerServer, storageServer);
			uploadResults = storageClient.upload_file(file.getContent(), file.getExt(), meta_list);
		} catch (IOException e) {
			logger.error("IO Exception when uploadind the file:" + file.getName(), e);
		} catch (Exception e) {
			logger.error("Non IO Exception when uploadind the file:" + file.getName(), e);
		}
		logger.info("upload_file time used:" + (System.currentTimeMillis() - startTime) + " ms");

		if (uploadResults == null) {
			logger.error("upload file fail, error code:" + storageClient.getErrorCode());
		}

		String groupName = uploadResults[0];
		String remoteFileName = uploadResults[1];

		//String fileAbsolutePath = PROTOCOL + trackerServer.getInetSocketAddress().getHostName() + TRACKER_NGNIX_PORT + SEPARATOR + groupName + SEPARATOR + remoteFileName;

		logger.info("upload file successfully!!!" + "group_name:" + groupName + ", remoteFileName:" + " " + remoteFileName);
		System.out.println("upload file successfully!!!" + "group_name:" + groupName + ", remoteFileName:" + " " + remoteFileName);

		//return fileAbsolutePath;
		return uploadResults;
	}

	public static FileInfo getFile(String groupName, String remoteFileName) {
		try {
			storageClient = new StorageClient(trackerServer, storageServer);
			return storageClient.get_file_info(groupName, remoteFileName);
		} catch (IOException e) {
			logger.error("IO Exception: Get File from Fast DFS failed", e);
		} catch (Exception e) {
			logger.error("Non IO Exception: Get File from Fast DFS failed", e);
		}
		return null;
	}
	
	public static InputStream downFile(String groupName, String remoteFileName) {
		try {
			storageClient = new StorageClient(trackerServer, storageServer);
			byte[] fileByte = storageClient.download_file(groupName, remoteFileName);
			InputStream ins = new ByteArrayInputStream(fileByte); 
			return ins;
		} catch (IOException e) {
			logger.error("IO Exception: Get File from Fast DFS failed", e);
		} catch (Exception e) {
			logger.error("Non IO Exception: Get File from Fast DFS failed", e);
		}
		return null;
	}

	public static void deleteFile(String groupName, String remoteFileName)
			throws Exception {
		storageClient = new StorageClient(trackerServer, storageServer);
		int i = storageClient.delete_file(groupName, remoteFileName);
		System.out.println("delete file successfully!!!" + i);
	}

	public static StorageServer[] getStoreStorages(String groupName)
			throws IOException {
		storageClient = new StorageClient(trackerServer, storageServer);
		return trackerClient.getStoreStorages(trackerServer, groupName);
	}

	public static ServerInfo[] getFetchStorages(String groupName,
			String remoteFileName) throws IOException {
		storageClient = new StorageClient(trackerServer, storageServer);
		return trackerClient.getFetchStorages(trackerServer, groupName, remoteFileName);
	}
}
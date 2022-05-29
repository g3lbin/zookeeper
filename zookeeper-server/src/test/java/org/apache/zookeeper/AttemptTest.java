package org.apache.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.Mockito;

public class AttemptTest {

	final int CLIENT_PORT = 12345;
	final int SESSION_TIMEOUT = 30;
	
	// client
	private Watcher wc;
	
	// ZooKeeperServer parameters
	private FileTxnSnapLog txnLogFactory;
	private ServerConfig sConfig;
	
	// ZKDatabase parameter
	private FileTxnSnapLog snapLog;
	private QuorumPeerConfig qConfig;
	
	private int port;
	private ServerCnxnFactory serverFactory;
	private ZooKeeperServer zkServer;
	
	public AttemptTest() throws Exception {
		// Mock all units adjacent to ZooKeeperServer and ZKDatabase
		txnLogFactory = Mockito.mock(FileTxnSnapLog.class);
		snapLog = Mockito.mock(FileTxnSnapLog.class);
		sConfig = new ServerConfig();
		qConfig = Mockito.mock(QuorumPeerConfig.class);
		wc = Mockito.mock(Watcher.class);
	
		int numConnections = 5000;
		int tickTime = 2000;

		File dir = Files.createTempDirectory("zkTest").toFile();

		zkServer = new ZooKeeperServer(dir, dir, tickTime);
		serverFactory = NettyServerCnxnFactory.createFactory(new InetSocketAddress(CLIENT_PORT), tickTime);
		serverFactory.setZooKeeperServer(zkServer);
	}
	
	@Test
	public void serverAndDbIT() throws Exception {
		new Thread() {
		    public void run() {
		    	try {
		    		serverFactory.startup(zkServer, true);
				} catch (Exception e) {

				}
		    }
		}.start();
		ZooKeeper zk = new ZooKeeper("127.0.0.1:" + CLIENT_PORT, SESSION_TIMEOUT, wc);
		Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> zk.cnxn.getState() == ZooKeeper.States.CONNECTED);
		zk.create("/test", "znode data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		assertEquals("znode data", new String(zk.getData("/test", false, new Stat())));

		zk.close();
		serverFactory.shutdown();
	}
}

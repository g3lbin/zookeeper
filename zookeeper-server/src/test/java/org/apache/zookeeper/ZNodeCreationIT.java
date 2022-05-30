package org.apache.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith (value=Parameterized.class)
public class ZNodeCreationIT {
	
	enum TopDownPhase {FIRST, SECOND, THIRD, FOURTH}

	final int SESSION_TIMEOUT = 30;
	
	// ZooKeeperServer parameters
	private FileTxnSnapLog txnLogFactory;
	private ServerConfig sConfig;
	private QuorumPeerConfig qConfig;
	
	// ZKDatabase parameter
	private FileTxnSnapLog snapLog;
	@Mock
	FileTxnSnapLog mockedSnapLog;
	@InjectMocks
	ZKDatabase mockedZKDb;
	
	private File snapDir;
	private TopDownPhase phase;
	private int port;
	private long sessionId = -1;

	// server connection
	private ServerCnxnFactory serverFactory;
	// server
	private ZooKeeperServer zkServer;
	// db
	private ZKDatabase zkDb;
	// client
	private ZooKeeper zk;
	// watcher 
	private Watcher wc;
	
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{TopDownPhase.FIRST},
			{TopDownPhase.SECOND},
			{TopDownPhase.THIRD},
			{TopDownPhase.FOURTH},
		});
	}
	
	public ZNodeCreationIT(TopDownPhase phase) throws Exception {
		switch (phase) {
		case FIRST:
			configureFirstPhase();
			break;
		case SECOND:
			configureSecondPhase();
			break;
		case THIRD:
			configureThirdPhase();
		break;

		default:
			configureFourthPhase();
			break;
		}
	}
	
	public void configureFirstPhase() throws Exception {
		this.phase = TopDownPhase.FIRST;

		txnLogFactory = Mockito.mock(FileTxnSnapLog.class);
		wc = Mockito.mock(Watcher.class);
		
		port = 12345;
	}
	
	public void configureSecondPhase() throws Exception {
		configureFirstPhase();
		this.phase = TopDownPhase.SECOND;

		sConfig = new ServerConfig();
		qConfig = new QuorumPeerConfig();
		zkServer = new ZooKeeperServer(txnLogFactory, sConfig.getTickTime(), sConfig.getMinSessionTimeout(), sConfig.getMaxSessionTimeout(), 
				sConfig.getClientPortListenBacklog(), mockedZKDb, qConfig.getInitialConfig(), QuorumPeerConfig.isReconfigEnabled());
	}
	
	public void configureThirdPhase() throws Exception {
		configureFirstPhase();
		this.phase = TopDownPhase.THIRD;
		
		sConfig = new ServerConfig();
		qConfig = new QuorumPeerConfig();
		zkServer = new ZooKeeperServer(txnLogFactory, sConfig.getTickTime(), sConfig.getMinSessionTimeout(), sConfig.getMaxSessionTimeout(), 
				sConfig.getClientPortListenBacklog(), mockedZKDb, qConfig.getInitialConfig(), QuorumPeerConfig.isReconfigEnabled());
		
		serverFactory = ServerCnxnFactory.createFactory(new InetSocketAddress(0), zkServer.getTickTime());
		serverFactory.setZooKeeperServer(zkServer);
		port = serverFactory.getLocalPort();
	}
	
	public void configureFourthPhase() throws Exception {
		configureFirstPhase();
		this.phase = TopDownPhase.FOURTH;

		snapDir = Files.createTempDirectory("zkTest").toFile();
		snapLog = new FileTxnSnapLog(snapDir, snapDir);
		zkDb = new ZKDatabase(snapLog);

		sConfig = new ServerConfig();
		qConfig = new QuorumPeerConfig();
		zkServer = new ZooKeeperServer(txnLogFactory, sConfig.getTickTime(), sConfig.getMinSessionTimeout(), sConfig.getMaxSessionTimeout(), 
				sConfig.getClientPortListenBacklog(), zkDb, qConfig.getInitialConfig(), QuorumPeerConfig.isReconfigEnabled());

		serverFactory = ServerCnxnFactory.createFactory(new InetSocketAddress(0), zkServer.getTickTime());
		serverFactory.setZooKeeperServer(zkServer);
		port = serverFactory.getLocalPort();
	}
	
	@Test
	public void tryToConnectToTheServerIT() throws Exception {
		assumeTrue(phase == TopDownPhase.FIRST);

		zk = new ZooKeeper("127.0.0.1:" + port, SESSION_TIMEOUT, wc);
		assertEquals(ZooKeeper.States.CONNECTING, zk.cnxn.getState());
	}
	
	@Test
	public void checkClientAndServerStatesIT() throws Exception {
		assumeTrue(phase == TopDownPhase.SECOND);

		assertEquals("standalone", zkServer.getState());
		assertFalse(zkServer.isRunning());

		zk = new ZooKeeper("127.0.0.1:" + port, SESSION_TIMEOUT, wc);
		assertEquals(ZooKeeper.States.CONNECTING, zk.cnxn.getState());
	}
	
	@Test
	public void interactionClientAndServerIT() throws Exception {
		assumeTrue(phase == TopDownPhase.THIRD);

		new Thread() {
		    public void run() {
		    	try {
		    		serverFactory.startup(zkServer, true);
				} catch (Exception e) {

				}
		    }
		}.start();
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> zkServer.isRunning());
		assertTrue(zkServer.isRunning());
		
		zk = new ZooKeeper("127.0.0.1:" + port, SESSION_TIMEOUT, wc);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> zk.cnxn.getState() == ZooKeeper.States.CONNECTED);
		zk.create("/test", "znode data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		sessionId = zk.getSessionId();
		assertEquals("znode data", new String(zk.getData("/test", false, new Stat())));
	}
	
	@Test
	public void interactionClientAndServerAndPersistenceIT() throws Exception {
		assumeTrue(phase == TopDownPhase.FOURTH);

		new Thread() {
		    public void run() {
		    	try {
		    		serverFactory.startup(zkServer, true);
				} catch (Exception e) {

				}
		    }
		}.start();
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> zkServer.isRunning());
		assertTrue(zkServer.isRunning());
		
		zk = new ZooKeeper("127.0.0.1:" + port, SESSION_TIMEOUT, wc);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> zk.cnxn.getState() == ZooKeeper.States.CONNECTED);
		zk.create("/test", "znode data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		sessionId = zk.getSessionId();
		assertEquals("znode data", new String(zkServer.getZKDatabase().getData("/test", new Stat(), wc)));
	}
	
	@After
    public void cleanUp() {
		try {
			if (zk != null)
				zk.close();
			if (sessionId != -1)
				zkServer.closeSession(sessionId);
			if (serverFactory != null)
				serverFactory.shutdown();
			if (snapLog != null)
				snapLog.close();
			if (zkDb != null)
				zkDb.close();
	        FileUtils.deleteDirectory(snapDir);
		} catch(Exception e) {
			// do nothing
		}
	}
}

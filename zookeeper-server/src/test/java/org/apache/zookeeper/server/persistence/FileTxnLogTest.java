package org.apache.zookeeper.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith (value=Parameterized.class)
public class FileTxnLogTest 
{
	enum Type {APPEND, GET_LFILES};
	
	enum LogDirList {EMPTY, NULL, NON_EMPTY}
	
	private File logDir;
	private FileTxnLog fileTxnLog;
	private Type type;
	private boolean expected;
	
	// appendTest parameters
	private TxnHeader hdr;
	private Record txn;
	private TxnDigest digest;
	
	// getLogFiles parameters
	private LogDirList list;
	private File[] logDirList;
	private long snapshotZxid;
	private long lastZxid;
	
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			// type, expected, hdr, txn, digest, null, null, null
			{Type.APPEND, true, TxnBuilder.buildTxnHeader(), TxnBuilder.mockRecord(), TxnBuilder.buildTxnDigest(), null, null, null},
			{Type.APPEND, false, null, TxnBuilder.mockRecord(), null, null, null, null},
			{Type.APPEND, true, TxnBuilder.buildTxnHeader(), null, TxnBuilder.buildTxnDigest(), null, null, null},
			{Type.APPEND, true, TxnBuilder.spyTxnHeader(), null, null, null, null, null},
			// type, expected, null, null, null, logDirList, snapshotZxid, lastZxid
			{Type.GET_LFILES, true, null, null, null, LogDirList.NON_EMPTY, Long.valueOf(3), Long.valueOf(4)},
			{Type.GET_LFILES, true, null, null, null, LogDirList.EMPTY, Long.valueOf(-1), Long.valueOf(0)},
			{Type.GET_LFILES, true, null, null, null, LogDirList.NULL, Long.valueOf(1), Long.valueOf(2)},
			{Type.GET_LFILES, true, null, null, null, LogDirList.NON_EMPTY, Long.valueOf(4), Long.valueOf(2)},
		});
	}
	
	public FileTxnLogTest(Type type, boolean expected, TxnHeader hdr, Record txn,
			TxnDigest digest, LogDirList list, Long snapshotZxid, Long lastZxid) throws Exception {
		if (type == Type.APPEND)
			configure(type, expected, hdr, txn, digest);
		else
			configure(type, expected, list, snapshotZxid, lastZxid);
	}
	
	public void configure(Type type, boolean expected, TxnHeader hdr, Record txn, TxnDigest digest) throws Exception {
		this.type = type;
		this.expected = expected;
		this.hdr = hdr;
		this.txn = txn;
		this.digest = digest;
	
		logDir = Files.createTempDirectory("zkTest").toFile();
    	fileTxnLog = new FileTxnLog(logDir);
	}
	
	public void configure(Type type, boolean expected, LogDirList list, Long snapshotZxid, Long lastZxid) throws Exception {
		this.type = type;
		this.expected = expected;
		this.list = list;
		this.snapshotZxid = snapshotZxid.longValue();
		this.lastZxid = lastZxid.longValue();
		
		logDir = Files.createTempDirectory("zkTest").toFile();
    	fileTxnLog = new FileTxnLog(logDir);
    	for (long i = 1; i <= this.lastZxid; i++) {
    		fileTxnLog.logStream = null;
	    	fileTxnLog.append(TxnBuilder.buildTxnHeader(1L, 1, i, 0),
	    					  TxnBuilder.buildCreateTxn("/log" + i, ("testTxn" + i).getBytes()));
	    	fileTxnLog.commit();
    	}
		
		switch (list) {
		case EMPTY:
			this.logDirList = new File[] {};
			break;
		case NULL:
			this.logDirList = null;
			break;
		case NON_EMPTY:
			// Create a FilenameFilter
            FilenameFilter filter = new FilenameFilter() {
  
                public boolean accept(File f, String name) {
                    return name.startsWith("log");
                }
            };
			this.logDirList = logDir.listFiles(filter);
			break;

		default:
			break;
		}
	}

    @Test
    public void appendTest() throws IOException {
    	assumeTrue(type == Type.APPEND);
    	if (MockUtil.isMock(hdr)) {
    		Exception e = assertThrows(IOException.class, () -> fileTxnLog.append(hdr, txn, digest));
    		assertEquals(expected, "Faulty serialization for header and txn".equals(e.getMessage()));
    	} else {
	    	assertEquals(expected, fileTxnLog.append(hdr, txn, digest));
    	}
    }
    
    @Test
    public void getLogFilesTest() {
    	assumeTrue(type == Type.GET_LFILES);
    	File[] files = FileTxnLog.getLogFiles(logDirList, snapshotZxid);
    	
    	switch (list) {
		case NON_EMPTY:
			long len = (this.snapshotZxid >= this.lastZxid) ? 1L : (this.lastZxid - this.snapshotZxid + 1);
			assertEquals(len, files.length);
			if (len == 1) {
				assertEquals("log." + this.lastZxid, files[0].getName());
			} else {
				for (long i = this.snapshotZxid; i <= this.lastZxid; i++)
					assertEquals("log." + i, files[(int) (i - this.snapshotZxid)].getName());
			}
			break;
		case EMPTY:
		case NULL:
			assertEquals(expected, files.length == 0);
			break;
		default:
			break;
		}
    }
    
    @After
    public void cleanUp() throws Exception {
		try {
			if (fileTxnLog != null)
				fileTxnLog.close();
	        FileUtils.deleteDirectory(logDir);
		} catch(Exception e) {
			// do nothing
		}
	}
    
    public static class TxnBuilder {
    	
    	public static TxnHeader buildTxnHeader() {
    		return buildTxnHeader(0L, 0, 0L, 0);
    	}
    	
    	public static TxnHeader buildTxnHeader(long clientId, int cxid, long zxid, int type) {
    		long millis = Instant.now().toEpochMilli();
    		return new TxnHeader(clientId, cxid, zxid, millis, type);
    	}
    	
    	public static Record mockRecord() {
    		return Mockito.mock(Record.class);
    	}
    	
    	public static TxnHeader spyTxnHeader() {
    		try {
	    		TxnHeader hdr = Mockito.spy(new TxnHeader());
	    		Mockito.doAnswer(new Answer<Void>() {
					@Override
					public Void answer(InvocationOnMock i) throws Throwable {
						OutputArchive a_ = i.getArgument(0);
						String tag = i.getArgument(1);
						// build an empty record
						a_.startRecord(hdr,tag);
			    	    a_.endRecord(hdr,tag);
						return null;
					} 
				}).when(hdr).serialize(Mockito.any(), Mockito.any());
	    		return hdr;
    		} catch (Exception e) {
    			return null;
    		}
    	}

    	public static CreateTxn buildCreateTxn(String path, byte[] data) {
    		// acl without restrictions for tests scope
    		List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    		// not important parameters
    		boolean ephemeral = false;
    		int parentCVersion = 0;

    		return new CreateTxn(path, data, acl, ephemeral, parentCVersion);
    	}
    	
    	public static TxnDigest buildTxnDigest() {
    		return buildTxnDigest(0, 0L);
    	}

    	public static TxnDigest buildTxnDigest(int version, long treeDigest) {
    		return new TxnDigest(version, treeDigest);
    	}
    }
}

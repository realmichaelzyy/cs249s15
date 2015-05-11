package edu.ucla.cs249;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class LockTest {
	static public void main(String[] args) {
		System.out.println("--- Test 0 ---");
        try {
			ZooKeeper zk = new ZooKeeper("54.88.56.9:2181", 5000, new Watcher() {
			    @Override
			    public void process(WatchedEvent event) {
			      System.out.println("Watcher called");
			    }
			  });
			//String lockBasePath = zk.create("/lockdev", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//String lockName = zk.create("/lockdev/lock0_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			//System.out.println("After create");
			
			List<String> children = zk.getChildren("/lockdev", null);
			for (String child: children) {
				System.out.println(child);
				//zk.delete("/lockdev/" + child, -1);
			}
			
			DistributedLock mutex = new DistributedLock(zk, "/lockdev", "lock0_");
			System.out.println("Before lock");
			mutex.lock();
			System.out.println("After lock");
			while (true) {
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

package cs451.UniformReliableBroadcast;

import cs451.Host;
import cs451.PerfectLink.PerfectLink;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class BestEffortBroadcast
{
	// all ExecutorService implementations should be thread-safe for task submission
	private final ExecutorService sendThreadPool;
	private final ExecutorService recvThreadPool;
	
	private final Consumer<String> deliverCallback;
	
	private final List<AbstractMap.SimpleEntry<InetAddress, Integer>> hostsInfo = new ArrayList<>();
	private final PerfectLink perfectLink;
	
	public BestEffortBroadcast(Host self,
	                           List<Host> targetHosts,
	                           Consumer<String> deliverCallback,
	                           int threadPoolSize) throws SocketException, UnknownHostException
	{
		this.deliverCallback = deliverCallback;
		sendThreadPool = Executors.newFixedThreadPool(threadPoolSize);
		recvThreadPool = Executors.newFixedThreadPool(threadPoolSize);
		
		for (Host host: targetHosts)
		{
			hostsInfo.add(new AbstractMap.SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
		}
		
		this.perfectLink = new PerfectLink(self.getPort(), deliverCallback, recvThreadPool, sendThreadPool);
	}
	
	public void broadcast(String msg)
	{
		recvThreadPool.submit(() -> deliverCallback.accept(msg));
		
		for (AbstractMap.SimpleEntry<InetAddress, Integer> hostInfo: hostsInfo)
		{
			perfectLink.send(hostInfo.getKey(), hostInfo.getValue(), msg);
		}
	}
	
	public void close()
	{
		perfectLink.close();
	}
}

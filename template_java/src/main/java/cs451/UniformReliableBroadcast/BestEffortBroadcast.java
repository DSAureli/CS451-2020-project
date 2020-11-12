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
import java.util.function.Consumer;

public class BestEffortBroadcast
{
	private final Consumer<String> deliverCallback;
	private final ExecutorService recvThreadPool;
	
	private final List<AbstractMap.SimpleEntry<InetAddress, Integer>> hostsInfo = new ArrayList<>();
	private final PerfectLink perfectLink;
	
	public BestEffortBroadcast(Host self,
	                           List<Host> targetHosts,
	                           Consumer<String> deliverCallback,
	                           ExecutorService recvThreadPool) throws SocketException, UnknownHostException
	{
		this.deliverCallback = deliverCallback;
		this.recvThreadPool = recvThreadPool;
		
		for (Host host: targetHosts)
		{
			hostsInfo.add(new AbstractMap.SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
		}
		
		this.perfectLink = new PerfectLink(self.getPort(), recvThreadPool);
		this.perfectLink.startReceiving(deliverCallback);
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

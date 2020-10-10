package cs451.UniformReliableBroadcast;

import cs451.Host;
import cs451.PerfectLink.PerfectLink;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BestEffortBroadcast
{
	List<AbstractMap.SimpleEntry<InetAddress, Integer>> hostsInfo = new ArrayList<>();
	PerfectLink perfectLink;
	
	public BestEffortBroadcast(List<Host> hosts, int id, Consumer<String> deliverCallback) throws SocketException, UnknownHostException
	{
		for (Host host: hosts)
		{
			hostsInfo.add(new AbstractMap.SimpleEntry<>(InetAddress.getByName(host.getIp()), host.getPort()));
		}
		
		this.perfectLink = new PerfectLink(hosts.stream().filter(host -> host.getId() == id).findFirst().get().getPort());
		this.perfectLink.startReceiving(deliverCallback);
	}
	
	public void broadcast(String msg)
	{
		for (AbstractMap.SimpleEntry<InetAddress, Integer> hostInfo: hostsInfo)
		{
			perfectLink.send(hostInfo.getKey(), hostInfo.getValue(), msg);
		}
	}
}

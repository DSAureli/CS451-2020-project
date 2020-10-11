package cs451.UniformReliableBroadcast;

import cs451.Host;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Consumer;

public class UniformReliableBroadcast
{
	// Majority-Ack Uniform Reliable Broadcast
	
	private final Set<URBMessage> deliveredSet = new HashSet<>();
	private final Set<URBMessage> pendingSet = new HashSet<>();
	// TODO Map<URBMessage, Set<Integer>>?
	private final Map<String, Set<Integer>> ackMap = new HashMap<>();
	
	private final BestEffortBroadcast bestEffortBroadcast;
	private int hostsCount;
	private final int id;
	Consumer<String> deliverCallback;
	
	public UniformReliableBroadcast(List<Host> hosts, int id, Consumer<String> deliverCallback) throws SocketException, UnknownHostException
	{
		bestEffortBroadcast = new BestEffortBroadcast(hosts, id, deliver);
		this.hostsCount = hosts.size();
		this.id = id;
		this.deliverCallback = deliverCallback;
	}
	
	public void broadcast(String msg)
	{
		URBMessage urbMessage = new URBMessage(id, msg);
		pendingSet.add(urbMessage);
		bestEffortBroadcast.broadcast(urbMessage.toString());
	}
	
	Consumer<String> deliver = (msg) ->
	{
		URBMessage urbMessage = URBMessage.fromString(msg);
		String str = urbMessage.getMessage();
		
		// TODO use urbMessage instead of its message string?
		
		if (!ackMap.containsKey(str))
			ackMap.put(str, new HashSet<>());
		
		ackMap.get(str).add(urbMessage.getSender());
		
		if (!pendingSet.contains(urbMessage))
		{
			pendingSet.add(urbMessage);
			deliverCallback.accept(urbMessage.getMessage());
		}
		
		if (ackMap.get(str).size() > hostsCount / 2 && !deliveredSet.contains(urbMessage))
		{
			deliveredSet.add(urbMessage);
			deliverCallback.accept(str);
		}
	};
}
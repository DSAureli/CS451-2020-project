package cs451.UniformReliableBroadcast;

import cs451.Host;

import java.io.NotSerializableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class UniformReliableBroadcast
{
	// Majority-Ack Uniform Reliable Broadcast
	
	private final Set<String> deliveredSet = ConcurrentHashMap.newKeySet();
	private final Set<String> pendingSet = ConcurrentHashMap.newKeySet();
	private final Map<String, Set<Integer>> ackMap = new ConcurrentHashMap<>();
	
	private BestEffortBroadcast bestEffortBroadcast;
	private int hostsCount;
	private int id;
	Consumer<String> deliverCallback;
	
	public UniformReliableBroadcast(Host self, List<Host> targetHosts, Consumer<String> deliverCallback) throws SocketException, UnknownHostException
	{
		this.bestEffortBroadcast = new BestEffortBroadcast(self, targetHosts, deliver);
		this.hostsCount = targetHosts.size() + 1;
		this.id = self.getId();
		this.deliverCallback = deliverCallback;
//		System.out.printf("Hosts count: %s%n", hostsCount);
	}
	
	public void broadcast(String msg)
	{
		pendingSet.add(msg);
		
		URBMessage urbMessage = new URBMessage(id, msg);
		bestEffortBroadcast.broadcast(urbMessage.toString());
	}
	
	Consumer<String> deliver = (msg) ->
	{
		// Deserialize message
		URBMessage urbMessage;
		try
		{
			urbMessage = URBMessage.fromString(msg);
		}
		catch (NotSerializableException e)
		{
			e.printStackTrace();
			return;
		}
		
		String msgString = urbMessage.getMessage();
		
		// Add sender to ack[msg]
		
		if (!ackMap.containsKey(msgString))
			ackMap.put(msgString, ConcurrentHashMap.newKeySet());
		
		ackMap.get(msgString).add(urbMessage.getSender());
		
		// Forward message
		if (!pendingSet.contains(msgString))
		{
//			System.out.printf("Forwarding: %s%n", urbMessage.toString());
			pendingSet.add(msgString);
			
			URBMessage fwdURBMessage = new URBMessage(id, msgString);
			bestEffortBroadcast.broadcast(fwdURBMessage.toString());
		}
		
		System.out.printf("Checking: %s %s%n", urbMessage, ackMap.get(msgString));
		
		// Check for delivery
		if (ackMap.get(msgString).size() > hostsCount / 2 && !deliveredSet.contains(msgString))
		{
//			System.out.printf("Good check%n");
			deliveredSet.add(msgString);
			deliverCallback.accept(msgString);
		}
	};
}
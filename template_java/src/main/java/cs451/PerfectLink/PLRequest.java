package cs451.PerfectLink;

import java.net.InetAddress;
import java.util.Objects;
import java.util.StringJoiner;

public class PLRequest
{
	private final long timestamp;
	
	private final long seqNum;
	private final PLMessage.PLMessageType type;
	private final InetAddress address;
	private final int port;
	
	private String data;
	
	public long getTimestamp()
	{
		return timestamp;
	}
	
	public long getSeqNum()
	{
		return seqNum;
	}
	
	public PLMessage.PLMessageType getType()
	{
		return type;
	}
	
	public InetAddress getAddress()
	{
		return address;
	}
	
	public int getPort()
	{
		return port;
	}
	
	public String getData()
	{
		return data;
	}
	
	private PLRequest(long timestamp, long seqNum, PLMessage.PLMessageType type, InetAddress address, int port, String data)
	{
		this.timestamp = timestamp;
		this.seqNum = seqNum;
		this.type = type;
		this.address = address;
		this.port = port;
		this.data = data;
	}
	
	public static PLRequest newNormalRequest(long timestamp, long seqNum, InetAddress address, int port, String data)
	{
		return new PLRequest(timestamp, seqNum, PLMessage.PLMessageType.Normal, address, port, data);
	}
	
	public static PLRequest newACKRequest(long timestamp, long seqNum, InetAddress address, int port)
	{
		return new PLRequest(timestamp, seqNum, PLMessage.PLMessageType.ACK, address, port, null);
	}

	public PLRequest(PLRequest request, long newTimestamp)
	{
		this.timestamp = newTimestamp;
		this.seqNum = request.seqNum;
		this.type = request.type;
		this.address = request.address;
		this.port = request.port;
		this.data = request.data;
	}
	
	public PLMessage toPLMessage(int recvPort)
	{
		return new PLMessage(type, seqNum, recvPort, data == null ? 0 : data.length(), data);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		PLRequest plRequest = (PLRequest) o;
		return timestamp == plRequest.timestamp &&
			seqNum == plRequest.seqNum &&
			port == plRequest.port &&
			type == plRequest.type &&
			Objects.equals(address, plRequest.address) &&
			Objects.equals(data, plRequest.data);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(timestamp, seqNum, type, address, port, data);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", PLRequest.class.getSimpleName() + "[", "]")
			.add("timestamp=" + timestamp)
			.add("seqNum=" + seqNum)
			.add("type=" + type)
			.add("address=" + address)
			.add("port=" + port)
			.add(data == null ? "data=null" : ("data='" + data + "'"))
			.toString();
	}
}

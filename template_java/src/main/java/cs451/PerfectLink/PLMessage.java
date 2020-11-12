package cs451.PerfectLink;

import cs451.Constants;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

class PLMessage implements Serializable
{
	public enum PLMessageType
	{
		Normal, ACK
	}
	
	private final PLMessageType messageType;
	private final long seqNum;
	private final int senderRecvPort;
	private final int dataSize;
	private final String data;
	
	public PLMessageType getMessageType()
	{
		return messageType;
	}
	
	public long getSeqNum()
	{
		return seqNum;
	}
	
	public int getSenderRecvPort()
	{
		return senderRecvPort;
	}
	
	public String getData()
	{
		return data;
	}
	
	public PLMessage(PLMessageType messageType, long seqNum, int senderRecvPort, int dataSize, String data)
	{
		this.messageType = messageType;
		this.seqNum = seqNum;
		this.senderRecvPort = senderRecvPort;
		this.dataSize = dataSize;
		this.data = data;
	}
	
	private static void error(String error) throws NotSerializableException
	{
		throw new NotSerializableException("[PLMessage] " + error);
	}
	
	public static PLMessage fromBytes(byte[] bytes) throws NotSerializableException
	{
		String string = new String(bytes, StandardCharsets.US_ASCII);
		
		String[] parts = string.split(String.valueOf(Constants.CC.STX), 2);
		if (parts.length < 2)
			error("missing STX");
		
		String[] headerParts = parts[0].split(String.valueOf(Constants.CC.RS));
		if (headerParts.length != 5 || !headerParts[0].equals(String.valueOf(Constants.CC.SOH)))
			error("malformed header");
		
		PLMessageType type = headerParts[1].equals(String.valueOf(Constants.CC.ENQ)) ? PLMessageType.Normal : PLMessageType.ACK;
		int dataSize = Integer.parseInt(headerParts[4]);
		
		return new PLMessage(type,
		                     Long.parseLong(headerParts[2]),
		                     Integer.parseInt(headerParts[3]),
		                     dataSize,
		                     parts[1] == null ? null : parts[1].substring(0, dataSize)); // \0 does not automatically end a string in Java
	}
	
	public byte[] getBytes()
	{
		char type = messageType == PLMessageType.Normal ? Constants.CC.ENQ : Constants.CC.ACK;
		int port = messageType == PLMessageType.Normal ? senderRecvPort : 0;
		
		return String.format("%c%c%c%c%d%c%d%c%d%c%s",
		                     Constants.CC.SOH,
		                     Constants.CC.RS,
		                     type,
		                     Constants.CC.RS,
		                     seqNum,
		                     Constants.CC.RS,
		                     port,
		                     Constants.CC.RS,
		                     dataSize,
		                     Constants.CC.STX,
		                     data)
				.getBytes(StandardCharsets.US_ASCII);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", PLMessage.class.getSimpleName() + "[", "]")
			.add("messageType=" + messageType)
			.add("seqNum=" + seqNum)
			.add("senderRecvPort=" + senderRecvPort)
			.add("dataSize=" + dataSize)
			.add("data='" + data + "'")
			.toString();
	}
}

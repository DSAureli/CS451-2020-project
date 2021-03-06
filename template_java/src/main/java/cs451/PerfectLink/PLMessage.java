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
		Data, Ack
	}
	
	private final PLMessageType messageType;
	private final long seqNum;
	private final long sendTimestamp;
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
	
	public long getSendTimestamp()
	{
		return sendTimestamp;
	}
	
	public int getSenderRecvPort()
	{
		return senderRecvPort;
	}
	
	public String getData()
	{
		return data;
	}
	
	public PLMessage(PLMessageType messageType, long seqNum, long timestamp, int senderRecvPort, int dataSize, String data)
	{
		this.messageType = messageType;
		this.seqNum = seqNum;
		this.sendTimestamp = timestamp;
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
		if (headerParts.length != 6 || !headerParts[0].equals(String.valueOf(Constants.CC.SOH)))
			error("malformed header");
		
		PLMessageType type = headerParts[1].equals(String.valueOf(Constants.CC.ENQ)) ? PLMessageType.Data : PLMessageType.Ack;
		int dataSize = Integer.parseInt(headerParts[5]);
		
		return new PLMessage(type,
		                     Long.parseLong(headerParts[2]),
		                     Long.parseLong(headerParts[3]),
		                     Integer.parseInt(headerParts[4]),
		                     dataSize,
		                     parts[1] == null ? null : parts[1].substring(0, dataSize)); // \0 does not automatically end a string in Java
	}
	
	public String getString()
	{
		char type = messageType == PLMessageType.Data ? Constants.CC.ENQ : Constants.CC.ACK;
		
		return String.format("%c%c%c%c%d%c%d%c%d%c%d%c%s",
		                     Constants.CC.SOH,
		                     Constants.CC.RS,
		                     type,
		                     Constants.CC.RS,
		                     seqNum,
		                     Constants.CC.RS,
		                     sendTimestamp,
		                     Constants.CC.RS,
		                     senderRecvPort,
		                     Constants.CC.RS,
		                     dataSize,
		                     Constants.CC.STX,
		                     data);
	}
	
	public byte[] getBytes()
	{
		return getString().getBytes(StandardCharsets.US_ASCII);
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

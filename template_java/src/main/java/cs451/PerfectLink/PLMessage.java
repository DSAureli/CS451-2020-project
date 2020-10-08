package cs451.PerfectLink;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

class PLMessage implements Serializable
{
	// ASCII Control Codes
	private static class CC
	{
		public static char SOH = (char) 1; // start of heading
		public static char STX = (char) 2; // start of text
		public static char ENQ = (char) 5; // enquiry
		public static char ACK = (char) 6; // acknowledge
		public static char RS = (char) 30; // record separator
	}
	
	public enum PLMessageType
	{
		Normal, ACK
	}
	
	private final PLMessageType messageType;
	private final long seqNum;
	private final int senderRecvPort;
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
	
	public PLMessage(PLMessageType messageType, long seqNum, int senderRecvPort, String data)
	{
		this.messageType = messageType;
		this.seqNum = seqNum;
		this.senderRecvPort = senderRecvPort;
		this.data = data;
	}
	
	private static void error(String error) throws RuntimeException
	{
		throw new RuntimeException("Byte array is not a serialization of a Message object: " + error);
	}
	
	public static PLMessage fromBytes(byte[] bytes) throws RuntimeException
	{
		String string = new String(bytes, StandardCharsets.US_ASCII);
		String[] parts = string.split(String.valueOf(CC.STX), 2);
		if (parts.length < 2)
			error("missing STX");
		
		String[] headerParts = parts[0].split(String.valueOf(CC.RS));
		if (headerParts.length != 4 || !headerParts[0].equals(String.valueOf(CC.SOH)))
			error("malformed header");
		
		PLMessageType type = headerParts[1].equals(String.valueOf(CC.ENQ)) ? PLMessageType.Normal : PLMessageType.ACK;
		return new PLMessage(type, Long.parseLong(headerParts[2]), Integer.parseInt(headerParts[3]), parts[1]);
	}
	
	public byte[] getBytes()
	{
		char type = messageType == PLMessageType.Normal ? CC.ENQ : CC.ACK;
		int port = messageType == PLMessageType.Normal ? senderRecvPort : 0;
		
		return String.format("%c%c%c%c%d%c%d%c%s", CC.SOH, CC.RS, type, CC.RS, seqNum, CC.RS, port, CC.STX, data)
				.getBytes(StandardCharsets.US_ASCII);
	}
}

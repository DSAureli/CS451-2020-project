package cs451.UniformReliableBroadcast;

import cs451.Constants;

import java.io.NotSerializableException;
import java.util.Objects;

public class URBMessage
{
	private final int sender;
	private final String message;
	
	public URBMessage(int sender, String message)
	{
		this.sender = sender;
		this.message = message;
	}
	
	public int getSender()
	{
		return sender;
	}
	
	public String getMessage()
	{
		return message;
	}
	
	private static void error(String error) throws NotSerializableException
	{
		throw new NotSerializableException("[URBMessage] " + error);
	}
	
	public static URBMessage fromString(String str) throws NotSerializableException
	{
		String[] parts = str.split(String.valueOf(Constants.CC.STX), 2);
		if (parts.length < 2)
			error("missing STX");
		
		String[] headerParts = parts[0].split(String.valueOf(Constants.CC.SOH), 2);
		if (headerParts.length != 2)
			error("malformed header");
		
		return new URBMessage(Integer.parseInt(headerParts[1]), parts[1]);
	}
	
	@Override
	public String toString()
	{
		return String.format("%c%d%c%s",
		                     Constants.CC.SOH,
		                     sender,
		                     Constants.CC.STX,
		                     message);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		URBMessage that = (URBMessage) o;
		return sender == that.sender &&
				Objects.equals(message, that.message);
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(sender, message);
	}
}

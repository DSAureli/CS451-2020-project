package cs451.PerfectLink;

import cs451.Constants;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

public class PLMessageBatch implements Serializable
{
	private final List<PLMessage> plMessageList;
	
	public List<PLMessage> getPLMessageList()
	{
		return plMessageList;
	}
	
	public PLMessageBatch(List<PLMessage> plMessageList)
	{
		this.plMessageList = plMessageList;
	}
	
	private static void error(String error) throws NotSerializableException
	{
		throw new NotSerializableException("[PLMessageBatch] " + error);
	}
	
	public static PLMessageBatch fromBytes(byte[] bytes) throws NotSerializableException
	{
		String string = new String(bytes, StandardCharsets.US_ASCII);
		
		String[] parts = string.split(String.valueOf(Constants.CC.STX), 2);
		if (parts.length < 2)
			error("malformed message (missing STX)");
		
		if (!parts[0].equals(String.valueOf(Constants.CC.SOH)))
			error("malformed header (missing SOH)");
		
		String[] bodyParts = parts[1].split(String.valueOf(Constants.CC.FS));
		
		if (bodyParts.length < 2)
			error("empty batch");
		
		if (bodyParts[bodyParts.length - 1].charAt(0) != Constants.CC.EOT) // account for final \0 characters
			error("incomplete batch (missing EOT)");
		
		List<PLMessage> newPLMessageList = new LinkedList<>();
		for (int it = 0; it < bodyParts.length - 1; it++)
		{
			newPLMessageList.add(PLMessage.fromBytes(bodyParts[it].getBytes(StandardCharsets.US_ASCII)));
		}
		
		return new PLMessageBatch(newPLMessageList);
	}
	
	public byte[] getBytes()
	{
		String ret = String.valueOf(Constants.CC.SOH)
			.concat(String.valueOf(Constants.CC.STX));
		
		for (PLMessage plMessage : plMessageList)
		{
			ret = ret
				.concat(plMessage.getString())
				.concat(String.valueOf(Constants.CC.FS));
		}
		
		ret = ret.concat(String.valueOf(Constants.CC.EOT));
		return ret.getBytes(StandardCharsets.US_ASCII);
	}
	
	@Override
	public String toString()
	{
		return new StringJoiner(", ", PLMessageBatch.class.getSimpleName() + "[", "]")
			.add("plMessageList=" + plMessageList)
			.toString();
	}
}

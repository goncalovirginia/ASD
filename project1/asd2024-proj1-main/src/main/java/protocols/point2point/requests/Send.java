package protocols.point2point.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import utils.HashProducer;

import java.math.BigInteger;
import java.util.UUID;

public class Send extends ProtoRequest {

	public final static short REQUEST_ID = 401;

	private final byte[] senderID;
	private final byte[] destinationID;
	private final UUID messageID;
	private final byte[] messagePayload;

	public Send(byte[] senderID, byte[] destID, UUID mid, byte[] mPayload) {
		super(REQUEST_ID);
		this.senderID = senderID.clone();
		this.destinationID = destID.clone();
		this.messageID = mid;
		this.messagePayload = mPayload.clone();
	}

	public byte[] getSenderPeerID() {
		return this.senderID.clone();
	}

	public byte[] getDestinationPeerID() {
		return this.destinationID.clone();
	}

	public BigInteger getSenderPeerIDNumerical() {
		return HashProducer.toNumberFormat(senderID);
	}

	public String getSenderPeerIDHex() {
		return HashProducer.toNumberFormat(senderID).toString(16);
	}

	public BigInteger getDestinationPeerIDNumerical() {
		return HashProducer.toNumberFormat(destinationID);
	}

	public String getDestinationPeerIDHex() {
		return HashProducer.toNumberFormat(destinationID).toString(16);
	}

	public byte[] getMessagePayload() {
		return this.messagePayload.clone();
	}

	public UUID getMessageID() {
		return this.messageID;
	}

	public String toString() {
		return "SendRequest from " + this.getSenderPeerIDHex() + " to " + this.getDestinationPeerIDHex() + " with message ID " + this.messageID + " payload of " + this.messagePayload.length + " bytes";
	}

	public String toStringLong() {
		String representation = this.toString();
		representation += "\nPayload:\n" + new String(this.messagePayload);
		return representation;
	}
}

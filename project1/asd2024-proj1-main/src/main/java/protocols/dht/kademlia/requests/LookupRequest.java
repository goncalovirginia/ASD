package protocols.dht.kademlia.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import utils.HashProducer;

import java.math.BigInteger;
import java.util.UUID;

public class LookupRequest extends ProtoRequest {

	public final static short REQUEST_ID = 501;

	private final byte[] peerID;
	private final UUID mid;

	public LookupRequest(byte[] peerID, UUID mid) {
		super(REQUEST_ID);
		this.peerID = peerID.clone();
		this.mid = mid;
	}


	public byte[] getPeerID() {
		return this.peerID.clone();
	}

	public BigInteger getPeerIDNumerical() {
		return HashProducer.toNumberFormat(peerID);
	}

	public String getPeerIDHex() {
		return HashProducer.toNumberFormat(peerID).toString(16);
	}

	public UUID getMid() {
		return this.mid;
	}

	public String toString() {
		return "Lookup Request for: " + this.getPeerIDHex();
	}

}

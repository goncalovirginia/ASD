package protocols.dht.chord.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import utils.HashProducer;

import java.math.BigInteger;

public class LookupRequest extends ProtoRequest {

	public final static short REQUEST_ID = 501;

	private final byte[] peerID;

	public LookupRequest(byte[] peerID) {
		super(REQUEST_ID);
		this.peerID = peerID.clone();
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

	public String toString() {
		return "Lookup Request for: " + this.getPeerIDHex();
	}

}

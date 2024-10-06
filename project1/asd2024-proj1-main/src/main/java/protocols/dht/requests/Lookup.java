package protocols.dht.requests;

import java.math.BigInteger;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import utils.HashProducer;

public class Lookup extends ProtoRequest {

	public final static short REQUEST_ID = 501;
	
	private final byte[] peerID;
	
	public Lookup(byte[] peerID) {
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

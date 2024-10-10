package protocols.dht.chord;

import java.math.BigInteger;

public class Finger {

	private final BigInteger start, end;
	private final ChordNode chordNode;

	public Finger(BigInteger start, BigInteger end, ChordNode chordNode) {
		this.start = start;
		this.end = end;
		this.chordNode = chordNode;
	}

	public BigInteger getStart() {
		return start;
	}

	public BigInteger getEnd() {
		return end;
	}

	public ChordNode getChordNode() {
		return chordNode;
	}
}

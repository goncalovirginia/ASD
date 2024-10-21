package protocols.dht.chord;

import java.math.BigInteger;

public class Finger {

	private final BigInteger start, end;
	private ChordNode chordNode;

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

	public void setChordNode(ChordNode chordNode) {
		this.chordNode = chordNode;
	}

	public boolean isInInterval(BigInteger key) {
		return (start.compareTo(key) <= 0 && end.compareTo(key) > 0) ||
				(start.compareTo(end) > 0 && (start.compareTo(key) <= 0 || end.compareTo(key) > 0));
	}

	public static boolean belongsToSuccessor(BigInteger thisID, BigInteger successorID, BigInteger key) {
		return (thisID.compareTo(key) < 0 && successorID.compareTo(key) >= 0) ||
				(thisID.compareTo(successorID) > 0 && (thisID.compareTo(key) < 0 || successorID.compareTo(key) >= 0));
	}

	public static boolean belongsToOpenInterval(BigInteger startID, BigInteger endID, BigInteger key) {
		return (startID.compareTo(key) < 0 && endID.compareTo(key) > 0) ||
				(startID.compareTo(endID) > 0 && (startID.compareTo(key) < 0 || endID.compareTo(key) > 0));
	}

}

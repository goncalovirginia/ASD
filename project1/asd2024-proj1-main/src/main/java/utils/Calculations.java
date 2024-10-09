package utils;

public class Calculations {

	public static int log2Ceil(int maxPeers) {
		--maxPeers;
		int numFingers = 0;
		while (maxPeers > 0) {
			maxPeers /= 2;
			++numFingers;
		}
		return numFingers;
	}

}

package protocols.abd.utils;

public class Tag {

	private final int opSeq, processId;

	public Tag(int opSeq, int processId) {
		this.opSeq = opSeq;
		this.processId = processId;
	}

	public int getOpSeq() {
		return opSeq;
	}

	public int getProcessId() {
		return processId;
	}

	public boolean greaterThan(Tag other) {
		return opSeq > other.opSeq || (opSeq == other.opSeq && processId > other.processId);
	}

	public boolean greaterOrEqualThan(Tag other) {
		return (opSeq == other.opSeq && processId == other.processId) || (opSeq > other.opSeq || (opSeq == other.opSeq && processId > other.processId));
	}

}

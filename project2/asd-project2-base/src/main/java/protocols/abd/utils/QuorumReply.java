package protocols.abd.utils;

public class QuorumReply {

	private final Tag tag;
	private final byte[] value;

	public QuorumReply(Tag tag, byte[] value) {
		this.tag = tag;
		this.value = value;
	}

	public Tag getTag() {
		return tag;
	}

	public byte[] getValue() {
		return value;
	}

}

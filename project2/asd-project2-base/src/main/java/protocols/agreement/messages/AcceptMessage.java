package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.utils.Tag;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

	public final static short MSG_ID = 198;

	private final UUID opId;
	private final int instance;
	private final byte[] op;
	private final Tag sequenceNumber;
	private final int lastChosen;

	public AcceptMessage(int instance, Tag highest_prepare, UUID opId, byte[] op, int last) {
		super(MSG_ID);
		this.instance = instance;
		this.op = op;
		this.opId = opId;
		this.sequenceNumber = highest_prepare;
		this.lastChosen = last;
	}

	public int getInstance() {
		return instance;
	}

	public Tag getSeqNumber() {
		return sequenceNumber;
	}

	public int getLastChosen() {
		return lastChosen;
	}

	public UUID getOpId() {
		return opId;
	}

	public byte[] getOp() {
		return op;
	}


	@Override
	public String toString() {
		return "AcceptAddRemoveMessage{" +
				", instance=" + instance +
				", sequenceNumber=" + sequenceNumber +
				'}';

	}

	public static ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
		@Override
		public void serialize(AcceptMessage msg, ByteBuf out) throws IOException {
			out.writeInt(msg.instance);
			out.writeInt(msg.sequenceNumber.getOpSeq());
			out.writeInt(msg.sequenceNumber.getProcessId());

			out.writeLong(msg.opId.getMostSignificantBits());
			out.writeLong(msg.opId.getLeastSignificantBits());
			out.writeInt(msg.op.length);
			out.writeBytes(msg.op);
			out.writeInt(msg.lastChosen);

		}

		@Override
		public AcceptMessage deserialize(ByteBuf in) throws IOException {
			int instance = in.readInt();
			int opSeq = in.readInt();
			int processId = in.readInt();
			Tag sequenceNumber = new Tag(opSeq, processId);


			long highBytes = in.readLong();
			long lowBytes = in.readLong();
			UUID opId = new UUID(highBytes, lowBytes);
			byte[] op = new byte[in.readInt()];
			in.readBytes(op);
			int last = in.readInt();
			return new AcceptMessage(instance, sequenceNumber, opId, op, last);

		}
	};

}

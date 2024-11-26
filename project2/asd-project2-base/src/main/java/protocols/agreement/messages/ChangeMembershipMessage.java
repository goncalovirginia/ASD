package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.abd.utils.Tag;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ChangeMembershipMessage extends ProtoMessage {

	public final static short MSG_ID = 192;

	private final Host replica;
	private final int instance;
	private final Tag sequenceNumber;
	private final boolean adding;
	private final boolean ok;

	public ChangeMembershipMessage(Host newReplica, int instance, Tag sn, boolean ok, boolean adding) {
		super(MSG_ID);
		this.replica = newReplica;
		this.instance = instance;
		this.ok = ok;
		this.adding = adding;
		this.sequenceNumber = sn;
	}

	public Host getReplica() {
		return replica;
	}

	public int getInstance() {
		return instance;
	}

	public boolean isOK() {
		return ok;
	}

	public boolean isAdding() {
		return adding;
	}

	public Tag getSeqNumber() {
		return sequenceNumber;
	}

	@Override
	public String toString() {
		return "ChangeMembershipMessage{" +
				"replica=" + replica +
				", instance=" + instance +
				'}';
	}

	public static ISerializer<ChangeMembershipMessage> serializer = new ISerializer<ChangeMembershipMessage>() {
		@Override
		public void serialize(ChangeMembershipMessage msg, ByteBuf out) throws IOException {
			Host.serializer.serialize(msg.replica, out);
			out.writeInt(msg.instance);
			out.writeInt(msg.sequenceNumber.getOpSeq());
			out.writeInt(msg.sequenceNumber.getProcessId());

			out.writeBoolean(msg.ok);
			out.writeBoolean(msg.adding);
		}

		@Override
		public ChangeMembershipMessage deserialize(ByteBuf in) throws IOException {
			Host nReplica = Host.serializer.deserialize(in);
			int instance = in.readInt();

			int opSeq = in.readInt();
			int processId = in.readInt();
			Tag sequenceNumber = new Tag(opSeq, processId);

			boolean ok = in.readBoolean();
			boolean add = in.readBoolean();
			return new ChangeMembershipMessage(nReplica, instance, sequenceNumber, ok, add);
		}
	};

}

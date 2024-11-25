package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ChangeMembershipOKMessage extends ProtoMessage {

	public final static short MSG_ID = 196;

	private final Host replica;
	private final int instance;
	private final boolean adding;

	public ChangeMembershipOKMessage(Host replica, int instance, boolean adding) {
		super(MSG_ID);
		this.replica = replica;
		this.instance = instance;
		this.adding = adding;
	}

	public Host getReplica() {
		return replica;
	}

	public int getInstance() {
		return instance;
	}

	public boolean isAdding() {
		return adding;
	}


	@Override
	public String toString() {
		return "ChangeMembershipOKMessage{" +
				"Replica=" + replica +
				", instance=" + instance +
				'}';
	}

	public static ISerializer<ChangeMembershipOKMessage> serializer = new ISerializer<ChangeMembershipOKMessage>() {
		@Override
		public void serialize(ChangeMembershipOKMessage msg, ByteBuf out) throws IOException {
			Host.serializer.serialize(msg.replica, out);
			out.writeInt(msg.instance);
			out.writeBoolean(msg.adding);
		}

		@Override
		public ChangeMembershipOKMessage deserialize(ByteBuf in) throws IOException {
			Host nReplica = Host.serializer.deserialize(in);
			int instance = in.readInt();
			boolean add = in.readBoolean();
			return new ChangeMembershipOKMessage(nReplica, instance, add);
		}
	};

}

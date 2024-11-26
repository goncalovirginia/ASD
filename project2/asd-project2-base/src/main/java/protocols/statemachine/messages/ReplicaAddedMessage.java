package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ReplicaAddedMessage extends ProtoMessage {

	public final static short MSG_ID = 126;

	private final byte[] state;
	private final int instance;
	private final List<Host> membership;

	public ReplicaAddedMessage(int instance, byte[] state, List<Host> membership) {
		super(MSG_ID);
		this.instance = instance;
		this.state = state;
		this.membership = membership;
	}

	public int getInstance() {
		return instance;
	}

	public byte[] getState() {
		return state;
	}

	public List<Host> getMembership() {
		return membership;
	}

	@Override
	public String toString() {
		return "ReplicaAddedMessage{" +
				"instance=" + instance +
				'}';
	}

	public static ISerializer<ReplicaAddedMessage> serializer = new ISerializer<ReplicaAddedMessage>() {
		@Override
		public void serialize(ReplicaAddedMessage msg, ByteBuf out) throws IOException {
			out.writeInt(msg.instance);

			out.writeInt(msg.state.length);
			out.writeBytes(msg.state);

			out.writeInt(msg.membership.size());
			for (Host host : msg.membership) {
				Host.serializer.serialize(host, out);
			}
		}

		@Override
		public ReplicaAddedMessage deserialize(ByteBuf in) throws IOException {
			int instance = in.readInt();

			byte[] st = new byte[in.readInt()];
			in.readBytes(st);

			int size = in.readInt();
			List<Host> members = new LinkedList<>();
			for (int i = 0; i < size; i++) {
				Host host = Host.serializer.deserialize(in);
				members.add(host);
			}

			return new ReplicaAddedMessage(instance, st, members);
		}
	};

}

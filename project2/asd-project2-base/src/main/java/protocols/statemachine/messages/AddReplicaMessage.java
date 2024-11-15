package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AddReplicaMessage extends ProtoMessage {

	public final static short MSG_ID = 125;

	private final Host newReplica;

	public AddReplicaMessage(Host newReplica) {
		super(MSG_ID);
		this.newReplica = newReplica;
	}

	public Host getNewReplica() {
		return newReplica;
	}


	@Override
	public String toString() {
		return "AddReplicaMessage{" +
				"newReplica=" + newReplica + '}';
	}

	public static ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
		@Override
		public void serialize(AddReplicaMessage msg, ByteBuf out) throws IOException {
			Host.serializer.serialize(msg.newReplica, out);
		}

		@Override
		public AddReplicaMessage deserialize(ByteBuf in) throws IOException {
			Host nReplica = Host.serializer.deserialize(in);
			return new AddReplicaMessage(nReplica);
		}
	};

}

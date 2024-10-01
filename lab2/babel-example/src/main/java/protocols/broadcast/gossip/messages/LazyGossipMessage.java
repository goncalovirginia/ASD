package protocols.broadcast.gossip.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class LazyGossipMessage extends ProtoMessage {

	public enum LazyGossipAction {
		PUSH, PULL
	}

	public static final short MSG_ID = 501;

	private final UUID mid;
	private final Host sender;
	private final short toDeliver;
	private final LazyGossipAction action;

	public LazyGossipMessage(UUID mid, Host sender, short toDeliver, LazyGossipAction action) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
		this.toDeliver = toDeliver;
		this.action = action;
	}

	@Override
	public String toString() {
		return "LazyGossipMessage{" +
				"mid=" + mid +
				'}';
	}

	public UUID getMid() {
		return mid;
	}

	public Host getSender() {
		return sender;
	}

	public short getToDeliver() {
		return toDeliver;
	}

	public LazyGossipAction getAction() {
		return action;
	}

	public static ISerializer<LazyGossipMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(LazyGossipMessage gossipMessage, ByteBuf out) throws IOException {
			out.writeLong(gossipMessage.mid.getMostSignificantBits());
			out.writeLong(gossipMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(gossipMessage.sender, out);
			out.writeShort(gossipMessage.toDeliver);

			String actionString = gossipMessage.action.toString();
			int actionLength = actionString.length();
			out.writeInt(actionLength);
			out.writeCharSequence(actionString, StandardCharsets.UTF_8);
		}

		@Override
		public LazyGossipMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);
			short toDeliver = in.readShort();
			int actionLength = in.readInt();
			LazyGossipAction action = LazyGossipAction.valueOf(in.readCharSequence(actionLength, StandardCharsets.UTF_8).toString());

			return new LazyGossipMessage(mid, sender, toDeliver, action);
		}
	};

}

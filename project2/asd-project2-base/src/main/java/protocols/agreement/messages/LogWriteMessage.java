package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class LogWriteMessage extends ProtoMessage {

    public final static short MSG_ID = 145;

    private final int instance;
    private final int highest_prepare;
    private final Map<Integer, Pair<UUID, byte[]>> acceptedMessages;
    private final Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages;
    private final Map<Integer, Pair<Host, Boolean>> addReplicaInstances;
    
    public LogWriteMessage(int instance, int sn, Map<Integer, Pair<UUID, byte[]>> acceptedMessages,
    Map<Integer, Pair<UUID, byte[]>> toBeDecidedMessages, Map<Integer, Pair<Host, Boolean>> addReplicaInstances) {
        super(MSG_ID);
        this.instance = instance;
        this.highest_prepare = sn;
        this.acceptedMessages = new TreeMap<>(acceptedMessages);
        this.toBeDecidedMessages = new TreeMap<>(toBeDecidedMessages);
        this.addReplicaInstances = new TreeMap<>(addReplicaInstances);
    }

    public int getInstance() {
        return instance;
    }

    public int getHighestPrepare() {
        return highest_prepare;
    }

    public Map<Integer, Pair<UUID, byte[]>> getToBeDecidedMessages() {
        return toBeDecidedMessages;
    }

    public Map<Integer, Pair<UUID, byte[]>> getAcceptedMessages() {
        return acceptedMessages;
    }

    public Map<Integer, Pair<Host, Boolean>> getAddReplicaInstances() {
        return addReplicaInstances;
    }

    @Override
    public String toString() {
        return "LogWriteMessage{" +
                ", instance=" + instance +
                ", highest_prepare=" + highest_prepare +
                ", acceptedMsgs=" + acceptedMessages +
                ", tbdMsgs=" + toBeDecidedMessages +
                '}';
    }

    public static ISerializer<LogWriteMessage> serializer = new ISerializer<LogWriteMessage>() {
        @Override
        public void serialize(LogWriteMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeInt(msg.highest_prepare);

            Map<Integer, Pair<UUID, byte[]>> acceptedMessages = msg.getAcceptedMessages();
            out.writeInt(acceptedMessages.size());
            for (Map.Entry<Integer, Pair<UUID, byte[]>> entry : acceptedMessages.entrySet()) {
                out.writeInt(entry.getKey());
                UUID uuid = entry.getValue().getLeft();
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());

                byte[] data = entry.getValue().getRight();
                out.writeInt(data.length);
                out.writeBytes(data);
            }
            

            Map<Integer, Pair<UUID, byte[]>> tbdMessages = msg.getToBeDecidedMessages();
            out.writeInt(tbdMessages.size());
            for (Map.Entry<Integer, Pair<UUID, byte[]>> entry : tbdMessages.entrySet()) {
                out.writeInt(entry.getKey());
                UUID uuid = entry.getValue().getLeft();
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());

                byte[] data = entry.getValue().getRight();
                out.writeInt(data.length);
                out.writeBytes(data);
            }

            Map<Integer, Pair<Host, Boolean>> replicas = msg.getAddReplicaInstances();
            out.writeInt(replicas.size());
            for (Map.Entry<Integer, Pair<Host, Boolean>> entry : replicas.entrySet()) {
                out.writeInt(entry.getKey());
                Host.serializer.serialize(entry.getValue().getLeft(), out);
                out.writeBoolean(entry.getValue().getRight());
            }
        }

        @Override
        public LogWriteMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            int sn = in.readInt();

            int accMessageSize = in.readInt();
            TreeMap<Integer, Pair<UUID, byte[]>> accMessages = new TreeMap<>();
            for (int i = 0; i < accMessageSize; i++) {
                int key = in.readInt();
                long mostSigBits = in.readLong();
                long leastSigBits = in.readLong();
                UUID uuid = new UUID(mostSigBits, leastSigBits);

                int dataLength = in.readInt();
                byte[] data = new byte[dataLength];
                in.readBytes(data);

                accMessages.put(key, Pair.of(uuid, data));
            }

            int tbdMessageSize = in.readInt();
            TreeMap<Integer, Pair<UUID, byte[]>> tbdMessages = new TreeMap<>();
            for (int i = 0; i < tbdMessageSize; i++) {
                int key = in.readInt();
                long mostSigBits = in.readLong();
                long leastSigBits = in.readLong();
                UUID uuid = new UUID(mostSigBits, leastSigBits);

                int dataLength = in.readInt();
                byte[] data = new byte[dataLength];
                in.readBytes(data);

                tbdMessages.put(key, Pair.of(uuid, data));
            }

            int replicaSize = in.readInt();
            TreeMap<Integer, Pair<Host, Boolean>> replicas = new TreeMap<>();
            for (int i = 0; i < replicaSize; i++) {
                int key = in.readInt();
                Host host = Host.serializer.deserialize(in);
                boolean flag = in.readBoolean();
                replicas.put(key, Pair.of(host, flag));
            }

            return new LogWriteMessage(instance, sn, new TreeMap<>(accMessages), new TreeMap<>(tbdMessages), new TreeMap<>(replicas));
        }
    };
}

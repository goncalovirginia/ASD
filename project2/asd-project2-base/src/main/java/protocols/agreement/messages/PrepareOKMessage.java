package protocols.agreement.messages;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

//ADD MESSAGES OF INSTANCES > CURR LEADER OPSEQ
public class PrepareOKMessage extends ProtoMessage {

    public final static short MSG_ID = 107;

    private final int seqNumber;
    private final int instance;
    private final List<Pair<UUID, byte[]>> prepareOKMessages;

    
    public PrepareOKMessage(int sn, int instance, List<Pair<UUID, byte[]>> msgs) {
        super(MSG_ID);
        this.seqNumber = sn;
        this.instance = instance;
        prepareOKMessages = msgs;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public int getInstance() {
        return instance;
    }
    
    public List<Pair<UUID, byte[]>> getPrepareOKMsgs() {
        return prepareOKMessages;
    }

    @Override
    public String toString() {
        return "PrepareOKMessage{" +
                "seqNumber=" + seqNumber +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<PrepareOKMessage>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) {
            out.writeInt(msg.seqNumber);
            out.writeInt(msg.instance);

            List<Pair<UUID, byte[]>> messages = msg.getPrepareOKMsgs();
            out.writeInt(messages.size());
            for (Pair<UUID, byte[]> pair : messages) {
                UUID uuid = pair.getLeft();
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());

                byte[] data = pair.getRight();
                out.writeInt(data.length);
                out.writeBytes(data);
            }
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) {
            int sn = in.readInt();
            int inst = in.readInt();

            int size = in.readInt();
            List<Pair<UUID, byte[]>> messages = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                long mostSigBits = in.readLong();
                long leastSigBits = in.readLong();
                UUID uuid = new UUID(mostSigBits, leastSigBits);

                int dataLength = in.readInt();
                byte[] data = new byte[dataLength];
                in.readBytes(data);

                messages.add(Pair.of(uuid, data));
            }

            return new PrepareOKMessage(sn, inst, messages);
        }
    };

}

package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class LogReadMessage extends ProtoMessage {

    public final static short MSG_ID = 138;

    private final int instance;

    public LogReadMessage(int instance) {
        super(MSG_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "LogReadMessage{" +
                "instance=" + instance +
                '}';
    }

    public static ISerializer<LogReadMessage> serializer = new ISerializer<LogReadMessage>() {
        @Override
        public void serialize(LogReadMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
        }

        @Override
        public LogReadMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();

            return new LogReadMessage(instance);
        }
    };
}

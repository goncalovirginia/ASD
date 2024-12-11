package protocols.abd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.abd.messages.*;
import protocols.abd.renotifications.ReadCompleteNotification;
import protocols.abd.renotifications.UpdateValueNotification;
import protocols.abd.renotifications.WriteCompleteNotification;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.abd.utils.QuorumReply;
import protocols.abd.utils.Tag;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ABD extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(ABD.class);

	public static final String PROTOCOL_NAME = "ABD";
	public static final short PROTOCOL_ID = 500;

	private final Host thisHost;
	private final int tcpChannelId;
	private int thisProcessId;
	private List<Host> membership;
	private int opSeq;
	private List<QuorumReply> quorumReplies;
	private final Map<String, Tag> tags; //key - (opSeq, processId)
	private final Map<String, byte[]> values; //key - value
	private final Map<String, UUID> operations; //key - operation
	private byte[] pending; //value pending to be written

	public ABD(Properties props) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		opSeq = 0;
		quorumReplies = new LinkedList<>();
		tags = new HashMap<>();
		values = new HashMap<>();
		operations = new HashMap<>();
		pending = null;

		String address = props.getProperty("address");
		String port = props.getProperty("p2p_port");

		logger.info("Listening on {}:{}", address, port);
		this.thisHost = new Host(InetAddress.getByName(address), Integer.parseInt(port));

		Properties tcpChannelProps = new Properties();
		tcpChannelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
		tcpChannelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
		tcpChannelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
		tcpChannelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
		tcpChannelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
		tcpChannelId = createChannel(TCPChannel.NAME, tcpChannelProps);

		/*-------------------- Register Message Serializers ------------------------------- */
		registerMessageSerializer(tcpChannelId, QuorumMessage.MSG_ID, QuorumMessage.serializer);
		registerMessageSerializer(tcpChannelId, ReadTagReplyMessage.MSG_ID, ReadTagReplyMessage.serializer);
		registerMessageSerializer(tcpChannelId, ReadReplyMessage.MSG_ID, ReadReplyMessage.serializer);
		registerMessageSerializer(tcpChannelId, WriteMessage.MSG_ID, WriteMessage.serializer);
		registerMessageSerializer(tcpChannelId, AckMessage.MSG_ID, AckMessage.serializer);

		/*-------------------- Register Message Handlers ------------------------------- */
		registerMessageHandler(tcpChannelId, QuorumMessage.MSG_ID, this::uponQuorumMessage, this::uponMsgFail);
		registerMessageHandler(tcpChannelId, ReadTagReplyMessage.MSG_ID, this::uponReadTagReplyMessage, this::uponMsgFail);
		registerMessageHandler(tcpChannelId, WriteMessage.MSG_ID, this::uponWriteMessage, this::uponMsgFail);
		registerMessageHandler(tcpChannelId, AckMessage.MSG_ID, this::uponAckMessage, this::uponMsgFail);
		registerMessageHandler(tcpChannelId, ReadReplyMessage.MSG_ID, this::uponReadReplyMessage, this::uponMsgFail);

		/*-------------------- Register Channel Events ------------------------------- */
		registerChannelEventHandler(tcpChannelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(tcpChannelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(tcpChannelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(tcpChannelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(tcpChannelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
		registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
	}

	@Override
	public void init(Properties props) throws UnknownHostException {
		//load initial membership
		String host = props.getProperty("initial_membership");
		String[] hosts = host.split(",");
		List<Host> initialMembership = new LinkedList<>();
		for (String s : hosts) {
			String[] ipAndPort = s.split(":");
			initialMembership.add(new Host(InetAddress.getByName(ipAndPort[0]), Integer.parseInt(ipAndPort[1])));
		}

		if (initialMembership.contains(thisHost)) {
			logger.info("Starting in ACTIVE as I am part of initial membership");
			membership = new LinkedList<>(initialMembership);
			membership.forEach(this::openConnection);
			thisProcessId = initialMembership.indexOf(thisHost);
		} else {
			logger.info("Starting in JOINING as I am not part of initial membership");
		}
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponWriteRequest(WriteRequest request, short sourceProto) {
		logger.debug("Received WRITE request: {}", request);

		opSeq++;
		quorumReplies = new LinkedList<>();
		pending = request.getData();

		String key = new String(request.getKey());
		operations.put(key, request.getOpId());
		tags.put(key, new Tag(opSeq, thisProcessId));

		membership.forEach(h -> sendMessage(new QuorumMessage(opSeq, key, false), h));
	}

	private void uponReadRequest(ReadRequest request, short sourceProto) {
		logger.debug("Received ReadRequest: {}", request);

		opSeq++;
		quorumReplies = new LinkedList<>();
		pending = null;

		String key = new String(request.getKey());
		operations.put(key, request.getOpId());
		tags.put(key, new Tag(opSeq, thisProcessId));

		membership.forEach(h -> sendMessage(new QuorumMessage(opSeq, key, true), h));
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponQuorumMessage(QuorumMessage msg, Host host, short sourceProto, int channelId) {
		logger.debug("Received ReadTagMessage: {}", msg);

		Tag tag = tags.computeIfAbsent(msg.getKey(), k -> new Tag(0, thisProcessId));

		if (msg.isRead()) {
			byte[] value = values.getOrDefault(msg.getKey(), new byte[0]);
			sendMessage(new ReadReplyMessage(msg.getOpSeq(), tag, msg.getKey(), value), host);
		} else
			sendMessage(new ReadTagReplyMessage(msg.getOpSeq(), tag, msg.getKey()), host);
	}

	private void uponReadReplyMessage(ReadReplyMessage msg, Host host, short sourceProto, int channelId) {
		logger.debug("Received ReadReplyMessage: {}", msg);

		if (opSeq != msg.getOpId()) return;

		if (pending == null) //after majority decision, no more adds, or it can mess up ACKs
			quorumReplies.add(new QuorumReply(msg.getTag(), msg.getValue()));

		if (quorumReplies.size() == (membership.size() / 2) + 1) {
			QuorumReply maxTagQuorumReply = maxTagQuorumReply(quorumReplies);
			Tag maxTag = maxTagQuorumReply.getTag();
			pending = maxTagQuorumReply.getValue();
			opSeq++;
			quorumReplies = new LinkedList<>();
			membership.forEach(h -> sendMessage(new WriteMessage(opSeq, msg.getKey(), maxTag, pending), h));
		}
	}

	private void uponReadTagReplyMessage(ReadTagReplyMessage msg, Host host, short sourceProto, int channelId) {
		logger.debug("Received ReadTagReplyMessage: {}", msg);

		if (opSeq != msg.getOpId()) return;
		
		if (pending != null) //after majority decision, no more adds, or it can mess up ACKs
			quorumReplies.add(new QuorumReply(msg.getTag(), null));

		if (quorumReplies.size() == (membership.size() / 2) + 1) {
			int maxTagOpSeq = maxTagQuorumReply(quorumReplies).getTag().getOpSeq();
			opSeq++;
			quorumReplies = new LinkedList<>();
			membership.forEach(h -> sendMessage(new WriteMessage(opSeq, msg.getKey(), new Tag(maxTagOpSeq + 1, thisProcessId), pending), h));
			pending = null;
		}
	}

	private void uponWriteMessage(WriteMessage msg, Host host, short sourceProto, int channelId) {
		logger.debug("Received WriteMessage: INSTANCE {} - MSG: {} ", opSeq, msg);

		Tag tag = tags.get(msg.getKey());

		if (msg.getTag().greaterThan(tag)) {
			tags.put(msg.getKey(), msg.getTag());
			values.put(msg.getKey(), msg.getValue());

			if (!host.equals(thisHost)) {
				logger.debug("Updated -> message: MSG: {} TAGMsg {} PrevTag {} ", msg, msg.getTag(), tag);
				triggerNotification(new UpdateValueNotification(msg.getOpId(), msg.getKey().getBytes(), msg.getValue()));
			}
		}

		sendMessage(new AckMessage(msg.getOpId(), msg.getKey()), host);
	}

	private void uponAckMessage(AckMessage msg, Host host, short sourceProto, int channelId) {
		if (opSeq != msg.getOpId()) return; //change ACK to send value

		quorumReplies.add(new QuorumReply(new Tag(msg.getOpId(), thisProcessId), pending));

		if (quorumReplies.size() != (membership.size() / 2) + 1) return;

		logger.debug("NEW {}: opSeq {} - key {} - opId {}", (pending == null) ? "WRITE" : "READ", msg.getOpId(), msg.getKey().getBytes(), operations.get(msg.getKey()));
		quorumReplies = new LinkedList<>();

		if (pending == null) {
			triggerNotification(new WriteCompleteNotification(opSeq, msg.getKey().getBytes(), values.get(msg.getKey()), operations.get(msg.getKey())));
		} else {
			triggerNotification(new ReadCompleteNotification(opSeq, msg.getKey().getBytes(), pending, operations.get(msg.getKey())));
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Procedures ---------------------------------------- */

	private QuorumReply maxTagQuorumReply(List<QuorumReply> quorumReplies) {
		QuorumReply maxTagQuorumReply = new QuorumReply(new Tag(0, 0), null);
		for (QuorumReply quorumReply : quorumReplies) {
			if (quorumReply.getTag().greaterThan(maxTagQuorumReply.getTag()))
				maxTagQuorumReply = quorumReply;
		}
		return maxTagQuorumReply;
	}

	/* --------------------------------- TCPChannel Events ---------------------------- */

	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		logger.info("Connection to {} is up", event.getNode());
	}

	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
	}

	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
		//Maybe we don't want to do this forever. At some point we assume he is no longer there.
		//Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
		if (membership.contains(event.getNode()))
			openConnection(event.getNode());
	}

	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.trace("Connection from {} is up", event.getNode());
	}

	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

}

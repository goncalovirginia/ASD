package protocols.point2point;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.chord.notifications.DHTInitializedNotification;
import protocols.dht.chord.notifications.PeerDownNotification;
import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.requests.LookupRequest;
import protocols.point2point.messages.HelperNodeMessage;
import protocols.point2point.messages.Point2PointAckMessage;
import protocols.point2point.messages.Point2PointMessage;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import protocols.point2point.timers.ResendMessagesTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.util.*;

public class Point2PointCommunicator extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(Point2PointCommunicator.class);

	public final static String PROTOCOL_NAME = "Point2PointCommunicator";
	public final static short PROTOCOL_ID = 400;

	private final Host thisHost;
	private final short DHT_PROTO_ID;

	private int tcpChannelId;

	private final List<Send> messagesPendingLookup; //queued messages awaiting lookup until the Chord protocol is initialized
	private final Map<UUID, Send> messagesPendingLookupReply; //messages with lookups awaiting reply/notification from the Chord protocol
	private final Set<UUID> receivedMessages; //received and delivered Point2Point messages (ID's)
	private final Map<Point2PointMessage, Host> point2PointMessagesPendingAck; //key: Point2Point messages sent to target and awaiting Ack | value: the message's corresponding backup helper node host, in case the message fails to send
	private final Map<Host, Set<HelperNodeMessage>> helperMessagesToSend; //HelperNodeMessages I received in order to act as a helper, if the original sender goes down
	private final Set<HelperNodeMessage> helperMessagesSending; //HelperNodeMessages I am currently sending, after the original sender went down
	private final Map<Host, Set<HelperNodeMessage>> myHelpersMessages; //HelperNodeMessages I sent to others, in order to be my helpers

	private boolean isDHTInitialized;

	public Point2PointCommunicator(Host thisHost, short DHT_Proto_ID) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.thisHost = thisHost;
		this.DHT_PROTO_ID = DHT_Proto_ID;

		this.messagesPendingLookup = new LinkedList<>();
		this.messagesPendingLookupReply = new HashMap<>();
		this.receivedMessages = new HashSet<>();
		this.point2PointMessagesPendingAck = new HashMap<>();
		this.helperMessagesToSend = new HashMap<>();
		this.helperMessagesSending = new HashSet<>();
		this.myHelpersMessages = new HashMap<>();
		this.isDHTInitialized = false;

		//register request handlers
		registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);

		//register reply handlers
		registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

		//register notification handlers
		subscribeNotification(TCPChannelCreatedNotification.NOTIFICATION_ID, this::uponChannelCreatedNotification);
		subscribeNotification(DHTInitializedNotification.NOTIFICATION_ID, this::uponDHTInitializedNotification);
		subscribeNotification(PeerDownNotification.NOTIFICATION_ID, this::uponPeerDownNotification);

		//register timer handlers
		registerTimerHandler(ResendMessagesTimer.TIMER_ID, this::resendMessagesTimer);
	}

	@Override
	public void init(Properties props) {
		setupPeriodicTimer(new ResendMessagesTimer(), 3000, 3000);
	}

	private void openConnectionAndSendMessage(ProtoMessage protoMessage, Host host) {
		openConnection(host);
		sendMessage(protoMessage, host);
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponSendRequest(Send request, short protoID) {
		logger.info("Received SendRequest: {}", request.getMessageID());

		if (!isDHTInitialized) {
			logger.info("DHT protocol is not initialized, queueing message: {}", request.getMessageID());
			messagesPendingLookup.add(request);
			return;
		}

		LookupRequest lookupRequest = new LookupRequest(request.getDestinationPeerID(), request.getMessageID());
		sendRequest(lookupRequest, DHT_PROTO_ID);
		messagesPendingLookupReply.put(request.getMessageID(), request);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPoint2PointMessage(Point2PointMessage point2PointMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received Point2PointMessage: {}", point2PointMessage.toString());

		if (receivedMessages.contains(point2PointMessage.getMid())) {
			openConnectionAndSendMessage(new Point2PointAckMessage(point2PointMessage), from);
			return;
		}

		triggerNotification(new Deliver(point2PointMessage));
		receivedMessages.add(point2PointMessage.getMid());
		openConnectionAndSendMessage(new Point2PointAckMessage(point2PointMessage), from);
	}

	private void uponHelperNodeMessage(HelperNodeMessage helperNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received HelperNodeMessage: storing message {} to host {}", helperNodeMessage.getMid(), helperNodeMessage.getDestination());

		helperMessagesToSend.putIfAbsent(from, new HashSet<>());
		helperMessagesToSend.get(from).add(helperNodeMessage);
	}

	private void uponPoint2PointAckMessage(Point2PointAckMessage point2PointAckMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received Point2PointAckMessage: {}", point2PointAckMessage.toString());

		point2PointMessagesPendingAck.remove(new Point2PointMessage(point2PointAckMessage));
		helperMessagesSending.remove(new HelperNodeMessage(point2PointAckMessage));
	}

	private void uponPoint2PointMessageFail(Point2PointMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);

		Host helperHost = point2PointMessagesPendingAck.get(msg);
		HelperNodeMessage helperNodeMessage = new HelperNodeMessage(msg);

		//if HelperNodeMessage was already sent to helper, don't send it again
		if (helperHost == null) return;
		if (!myHelpersMessages.containsKey(helperHost)) myHelpersMessages.put(helperHost, new HashSet<>());
		if (myHelpersMessages.get(helperHost).contains(helperNodeMessage)) return;

		if (!helperHost.equals(msg.getDestination()) && !helperHost.equals(thisHost)) {
			logger.error("Sending HelperNodeMessage to {}", helperHost);
			myHelpersMessages.get(helperHost).add(helperNodeMessage);
			openConnectionAndSendMessage(helperNodeMessage, helperHost);
		}
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponLookupReply(LookupReply reply, short protoID) {
		logger.info("Received Lookup Reply: {}", reply.toString());

		Send send = messagesPendingLookupReply.remove(reply.getMid());
		if (send == null) return;

		Iterator<Pair<BigInteger, Host>> it = reply.getPeersIterator();
		Pair<BigInteger, Host> helper = it.next();
		Pair<BigInteger, Host> target = it.next();

		Point2PointMessage point2PointMessage = new Point2PointMessage(send, thisHost, target.getRight());

		if (target.getRight().equals(thisHost)) {
			uponPoint2PointMessage(point2PointMessage, target.getRight(), PROTOCOL_ID, tcpChannelId);
			return;
		}

		point2PointMessagesPendingAck.put(point2PointMessage, helper.getRight());
		openConnectionAndSendMessage(point2PointMessage, target.getRight());
	}

	//Upon receiving the channelId from the DHT algorithm, register our own callbacks and serializers
	private void uponChannelCreatedNotification(TCPChannelCreatedNotification notification, short sourceProto) {
		logger.info("TCPChannelCreatedNotification: {}", notification.toString());

		tcpChannelId = notification.getChannelId();
		registerSharedChannel(tcpChannelId);

		//register message serializers
		registerMessageSerializer(tcpChannelId, Point2PointMessage.MSG_ID, Point2PointMessage.serializer);
		registerMessageSerializer(tcpChannelId, HelperNodeMessage.MSG_ID, HelperNodeMessage.serializer);
		registerMessageSerializer(tcpChannelId, Point2PointAckMessage.MSG_ID, Point2PointAckMessage.serializer);

		//register message handlers
		try {
			registerMessageHandler(tcpChannelId, Point2PointMessage.MSG_ID, this::uponPoint2PointMessage, this::uponPoint2PointMessageFail);
			registerMessageHandler(tcpChannelId, HelperNodeMessage.MSG_ID, this::uponHelperNodeMessage, this::uponMessageFail);
			registerMessageHandler(tcpChannelId, Point2PointAckMessage.MSG_ID, this::uponPoint2PointAckMessage, this::uponMessageFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: {}", e.getMessage());
			logger.error(e.getStackTrace());
			System.exit(1);
		}
	}

	private void uponDHTInitializedNotification(DHTInitializedNotification notification, short sourceProto) {
		logger.info("DHT initialization state updated: {}", notification.isInitialized());

		if (!(isDHTInitialized = notification.isInitialized())) return;

		for (Send pendingMessage : messagesPendingLookup) {
			uponSendRequest(pendingMessage, PROTOCOL_ID);
		}
		messagesPendingLookup.clear();
	}

	private void uponPeerDownNotification(PeerDownNotification notification, short sourceProto) {
		Set<HelperNodeMessage> helperNodeMessages = helperMessagesToSend.get(notification.getPeer());
		if (helperNodeMessages == null) return;

		logger.info("Helper messages activated for peer: {} \n {}", notification.getPeer(), helperNodeMessages);

		helperMessagesSending.addAll(helperNodeMessages);
		helperNodeMessages.clear();
	}

	/*--------------------------------- Timers ---------------------------------------- */

	private void resendMessagesTimer(ResendMessagesTimer timer, long timerId) {
		logger.info("helperTimer: {}", timerId);

		if (!isDHTInitialized) return;

		for (Point2PointMessage point2PointMessage : point2PointMessagesPendingAck.keySet()) {
			openConnectionAndSendMessage(point2PointMessage, point2PointMessage.getDestination());
		}
		for (HelperNodeMessage helperNodeMessage : helperMessagesSending) {
			openConnectionAndSendMessage(new Point2PointMessage(helperNodeMessage), helperNodeMessage.getDestination());
		}
	}

}

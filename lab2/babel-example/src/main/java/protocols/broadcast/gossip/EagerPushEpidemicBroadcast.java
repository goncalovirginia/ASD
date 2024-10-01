package protocols.broadcast.gossip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.gossip.messages.GossipMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class EagerPushEpidemicBroadcast extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(EagerPushEpidemicBroadcast.class);

	//Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "EagerPushEpidemic";
	public static final short PROTOCOL_ID = 400;

	private final Host myself;
	private final Set<Host> neighbours;
	private final Set<UUID> receivedMessages;
	private boolean channelReady;

	public EagerPushEpidemicBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		receivedMessages = new HashSet<>();
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {

	}

	/*--------------------------------- Requests ---------------------------------------- */
	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady) return; //Ideally we would buffer this message to transmit when the channel is ready :)

		//Create the message object.
		GossipMessage msg = new GossipMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

		//Call the same handler as when receiving a new FloodMessage (since the logic is the same)
		uponFloodMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */
	private void uponFloodMessage(GossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received {} from {}", msg, from);
		//If we already received it once, do nothing (or we would end up with a nasty infinite loop)
		if (!receivedMessages.add(msg.getMid())) return;

		//Deliver the message to the application (even if it came from it)
		triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

		//Send the message to ceil(ln(neighbours.size())) neighbours(who will then do the same)
		for (Host neighbour : neighboursShuffleLnSublist(from)) {
			sendMessage(msg, neighbour);
			logger.trace("Sent {} to {}", msg, neighbour);
		}
	}

	private List<Host> neighboursShuffleLnSublist(Host from) {
		List<Host> neighboursList = new ArrayList<>(neighbours);
		neighboursList.remove(from);
		Collections.shuffle(neighboursList);
		return neighboursList.subList(0, (int) Math.ceil(Math.log(neighboursList.size() + 1)));
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	//When the membership protocol notifies of a new neighbour, simply update my list of neighbours
	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.add(h);
			logger.info("New neighbour: {}", h);
		}
	}

	//When the membership protocol notifies of a leaving neighbour simply update my list of neighbours
	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.remove(h);
			logger.info("Neighbour down: {}", h);
		}
	}

	//Upon receiving the channelId from the membership, register our own callbacks and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int channelId = notification.getChannelId();
		// Allows this protocol to receive events from this channel.
		registerSharedChannel(channelId);
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponFloodMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
		//Now we can start sending messages
		channelReady = true;
	}

}

import re
import os

send_pattern = r'Sending:\s(-\w{64})'  
receive_pattern = r'Received\s(\w{64})\s->\s(-\w{64})'  

messages_sent = []
app_messages_received = []

base_directory = os.path.join(os.getcwd(), 'res_100_1kb_1/asd-{}')  
total_messages_sent = 0
total_messages_received = 0
total_bytes_sent = 0
total_bytes_received = 0


for i in range(1, 101):
    log_file_path = base_directory.format(i) + '/console.log'

    if not os.path.exists(log_file_path):
        print(f"No log file found at {log_file_path}")
        continue

    with open(log_file_path) as f:
        for line in f:
            if "Sending" in line:
                match = re.search(send_pattern, line)
                if match:
                    msg_id = match.group(1)
                    messages_sent.append({'id': msg_id})
                    total_messages_sent += 1
                    total_bytes_sent += 1024 

            elif "Received" in line and "(1024)" in line:
                match = re.search(receive_pattern, line)
                if match:
                    received_id = match.group(1)
                    ack_id = match.group(2)
                    app_messages_received.append({
                        'received_id': received_id,
                        'ack_id': ack_id
                    })
                    total_messages_received += 1
                    total_bytes_received += 1024  

print("total Redudancy across the network:")
redundancy = total_messages_received / total_messages_sent if total_messages_sent > 0 else 0

print("Messages Sent:")
for msg in messages_sent:
    print(msg)

print("\nMessages Received:")
for msg in app_messages_received:
    print(msg)

print("\n--------------------------------------------------------")

print("\nAcross the ENTIRE network:")
print(f"res_100_1kb_1 experiment");
print(f"\nTotal Messages Sent: {total_messages_sent}")
print(f"Total Messages Received from Application: {total_messages_received}")
print(f"Total Bytes Sent: {total_bytes_sent}")
print(f"Total Bytes Received: {total_bytes_received}")
print(f"Total Redundancy Ratio: {redundancy:.2f}")

print("\n--------------------------------------------------------")
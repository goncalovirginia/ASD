import re
import os
from datetime import datetime

lookup_request_pattern = r'\[(\d{2}:\d{2}:\d{2},\d{3})\].*Received LookupRequest:\s(\d+)'
lookup_reply_pattern = r'\[(\d{2}:\d{2}:\d{2},\d{3})\].*LookupReply for key:\s(\d+)'  

lookup_requests = {}

log_file_path = os.path.join(os.getcwd(), 'console.log')

if not os.path.exists(log_file_path):
    print(f"No log file found at {log_file_path}")
else:
    with open(log_file_path) as f:
        for line in f:
            request_match = re.search(lookup_request_pattern, line)
            if request_match:
                print(line)
                timestamp_str = request_match.group(1)  
                lookup_request_key = request_match.group(2) 
                lookup_requests[lookup_request_key] = datetime.strptime(timestamp_str, "%H:%M:%S,%f")  


            reply_match = re.search(lookup_reply_pattern, line)
            if reply_match:
                print(line)
                timestamp_str = reply_match.group(1)  
                reply_key = reply_match.group(2)  

                if reply_key in lookup_requests:
                    start_time = lookup_requests.pop(reply_key)  
                    end_time = datetime.strptime(timestamp_str, "%H:%M:%S,%f") 
                    elapsed_time = (end_time - start_time).total_seconds() * 1000 
                    print(f"LookupRequest -> Key: {reply_key} -> LookupReply: {elapsed_time:.3f} milliseconds")


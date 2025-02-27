from google.cloud import firestore
import logging
import re  
import time
from google.api_core.exceptions import DeadlineExceeded
def fetch_flows(self, shop_id, batch_size):
    flows_ref = self.db.collection("shops").document(shop_id).collection("klaviyo_flows")
    last_flow_doc = None
    flows = []

    while True:
        flow_query = flows_ref.limit(batch_size)
        if last_flow_doc:
            flow_query = flow_query.start_after(last_flow_doc)

        # Retry logic for fetching flows
        for attempt in range(3):
            try:
                fetched_flows = list(flow_query.stream())
                if not fetched_flows:  # Check if no more flows
                    return flows  # Return the flows collected so far
                flows.extend(fetched_flows)  # Append fetched flows
                last_flow_doc = fetched_flows[-1]  # Update last flow document for pagination
                break
            except DeadlineExceeded:
                print(f"Deadline exceeded while fetching flows for shop {shop_id}, retrying...")
                time.sleep(2)
        else:
            print(f"Failed to fetch flows for shop {shop_id} after 3 attempts.")
            return flows  # Return the flows collected so far

def extract_links(self, batch_size=100):  # Increase batch size
    print("Start Extract")
    result = []
    shops_ref = self.db.collection("shops")
    print("Create shop ref ")

    last_doc = None
    while True:
        # Fetch shops in batches
        query = shops_ref.limit(batch_size)
        if last_doc:
            query = query.start_after(last_doc)

        # Retry logic for fetching shops
        for attempt in range(3):  # Try 3 times
            try:
                shops = list(query.stream())  # Convert stream to list
                if not shops:  # Check if no more shops
                    print("No more shops to process.")
                    break  # Exit the outer loop
                print("Fetched shops batch.")
                break  # Exit loop if successful
            except DeadlineExceeded:
                print("Deadline exceeded while fetching shops, retrying...")
                time.sleep(2)  # Wait before retrying
        else:
            print("Failed to fetch shops after 3 attempts.")
            break  # Exit the outer loop if failed

        # Use ThreadPoolExecutor to fetch flows in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_shop = {executor.submit(self.fetch_flows, shop.id, batch_size): shop for shop in shops}
            
            for future in as_completed(future_to_shop):
                shop = future_to_shop[future]
                try:
                    flows = future.result()
                    # Process each flow
                    for flow in flows:
                        flow_data = flow.to_dict()
                        
                        # Extract links from attributes
                        attributes = flow_data.get("attributes", {})
                        links = self.extract_links_from_text(attributes)
                        
                        for link in links:
                            result.append((shop.id, "attribute", link))
                        
                        # Extract links from actions
                        actions_ref = self.db.collection("shops").document(shop.id).collection("klaviyo_flows").document(flow.id).collection("actions")
                        last_action_doc = None
                        while True:
                            action_query = actions_ref.limit(batch_size)
                            if last_action_doc:
                                action_query = action_query.start_after(last_action_doc)

                            # Retry logic for fetching actions
                            for attempt in range(3):
                                try:
                                    actions = list(action_query.stream())
                                    if not actions:  # Check if no more actions
                                        print(f"No more actions for flow {flow.id}.")
                                        break  # Exit the actions loop
                                    print(f"Fetched actions batch for flow {flow.id}.")
                                    break
                                except DeadlineExceeded:
                                    print(f"Deadline exceeded while fetching actions for flow {flow.id}, retrying...")
                                    time.sleep(2)
                            else:
                                print(f"Failed to fetch actions for flow {flow.id} after 3 attempts.")
                                break  # Exit the actions loop if failed

                            # Process each action
                            for action in actions:
                                action_data = action.to_dict()
                                links = self.extract_links_from_text(action_data)
                                for link in links:
                                    result.append((shop.id, "action", link))       
                            break
                        break

                except Exception as exc:
                    print(f"{shop.id} generated an exception: {exc}")

        #last_doc = shops[-1]  # Update last document for shops

    print("Finish extract")
    return result

def extract_links_from_text(self, data):
    links = []
    text = str(data)
    
    # Regex patterns for different link types
    link_pattern = r'https:\/\/router-link-pylfsebcoa-ew.a.run.app\/link\/v2\/(?P<shopid>[a-zA-Z0-9]+)\/.*'

    # Find all matches for images, redirection, and cart links
    links.extend(re.findall(link_pattern, text))

    return links

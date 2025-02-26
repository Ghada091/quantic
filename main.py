from google.cloud import firestore
import logging
import re  
import time
from google.api_core.exceptions import DeadlineExceeded

class FirestoreService:
    def __init__(self):
        # Initialize the Firestore client
        print("Start init")
        self.db = firestore.Client.from_service_account_json("khalilserviceaccount.json")   

    def extract_links(self, batch_size=10):
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
                    if not shops:  # Break the loop if no more shops
                        print("No more shops to process.")
                        return result
                    print("Fetched shops batch.")
                    break  # Exit loop if successful
                except DeadlineExceeded:
                    print("Deadline exceeded while fetching shops, retrying...")
                    time.sleep(2)  # Wait before retrying
            else:
                print("Failed to fetch shops after 3 attempts.")
                return result
            
            # Process each shop in the current batch
            for shop in shops:
                shop_id = shop.id
                print(f"Extract for shop {shop_id}")
                flows_ref = shops_ref.document(shop_id).collection("klaviyo_flows")

                # Fetch flows for the shop in batches
                last_flow_doc = None
                while True:
                    flow_query = flows_ref.limit(batch_size)
                    if last_flow_doc:
                        flow_query = flow_query.start_after(last_flow_doc)

                    # Retry logic for fetching flows
                    for attempt in range(3):
                        try:
                            flows = list(flow_query.stream())
                            if not flows:  # Break if no more flows
                                print(f"No more flows for shop {shop_id}.")
                                break
                            print(f"Fetched flows batch for shop {shop_id}.")
                            break
                        except DeadlineExceeded:
                            print(f"Deadline exceeded while fetching flows for shop {shop_id}, retrying...")
                            time.sleep(2)
                    else:
                        print(f"Failed to fetch flows for shop {shop_id} after 3 attempts.")
                        break

                    # Process each flow
                    for flow in flows:
                        flow_data = flow.to_dict()
                        
                        # Extract links from attributes
                        attributes = flow_data.get("attributes", {})
                        links = self.extract_links_from_text(attributes)
                        
                        for link in links:
                            print("Link: " + link)
                            result.append((shop_id, "attribute", link))
                        print(f"Result from links: {len(result)}")
                        
                        # Extract links from actions
                        actions_ref = flows_ref.document(flow.id).collection("actions")

                        # Fetch actions in batches
                        last_action_doc = None
                        while True:
                            action_query = actions_ref.limit(batch_size)
                            if last_action_doc:
                                action_query = action_query.start_after(last_action_doc)

                            # Retry logic for fetching actions
                            for attempt in range(3):
                                try:
                                    actions = list(action_query.stream())
                                    if not actions:  # Break if no more actions
                                        print(f"No more actions for flow {flow.id}.")
                                        break
                                    print(f"Fetched actions batch for flow {flow.id}.")
                                    break
                                except DeadlineExceeded:
                                    print(f"Deadline exceeded while fetching actions for flow {flow.id}, retrying...")
                                    time.sleep(2)
                            else:
                                print(f"Failed to fetch actions for flow {flow.id} after 3 attempts.")
                                break

                            # Process each action
                            for action in actions:
                                action_data = action.to_dict()
                                links = self.extract_links_from_text(action_data)
                                for link in links:
                                    result.append((shop_id, "action", link))
                            print(f"Result from actions: {len(result)}")

                    last_flow_doc = flows[-1]  # Update last flow document for pagination

                last_doc = shops[-1]  # Update last document for shops

        print("Finish extract")
        return result

    def extract_links_from_text(self, data):
        links = []
        text = str(data)
        print("Data: " + str(data))
        
        # Regex patterns for different link types
        link_pattern = r'https:\/\/router-link-pylfsebcoa-ew.a.run.app\/link\/v2\/(?P<shopid>[a-zA-Z0-9]+)\/.*'

        # Find all matches for images, redirection, and cart links
        links.extend(re.findall(link_pattern, text))

        return links

# Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Start main")
    firestore_service = FirestoreService()
    print("Finish init firestore service")
    print("Start extract links")
    links = firestore_service.extract_links()
    print("Finish link extracts")
    for shop_id, template, link in links:
        print(f"Shop ID: {shop_id}, Template: {template}, Link: {link}")

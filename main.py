from google.cloud import firestore
import logging
import re  
import time
import pandas as pd
from google.api_core.exceptions import DeadlineExceeded
from concurrent.futures import ThreadPoolExecutor, as_completed

class FirestoreService:
    def __init__(self):
        print("Start init")
        self.db = firestore.Client.from_service_account_json("khalilserviceaccount.json")   

    def fetch_flows(self, shop_id, batch_size):
        flows_ref = self.db.collection("shops").document(shop_id).collection("klaviyo_flows")
        return self.fetch_documents(flows_ref, batch_size)

    def fetch_documents(self, collection_ref, batch_size):
        last_doc = None
        documents = []

        while True:
            query = collection_ref.limit(batch_size)
            if last_doc:
                query = query.start_after(last_doc)

            for attempt in range(3):
                try:
                    fetched_docs = list(query.stream())
                    if not fetched_docs:
                        return documents
                    documents.extend(fetched_docs)
                    last_doc = fetched_docs[-1]
                    break
                except DeadlineExceeded:
                    print("Deadline exceeded while fetching documents, retrying...")
                    time.sleep(2)
            else:
                print("Failed to fetch documents after 3 attempts.")
                return documents

    def extract_links(self, batch_size=100):
        print("Start Extract")
        result = []
        shops_ref = self.db.collection("shops")
        print("Create shop ref ")

        last_doc = None
        while True:
            query = shops_ref.limit(batch_size)
            if last_doc:
                query = query.start_after(last_doc)

            for attempt in range(3):
                try:
                    shops = list(query.stream())
                    if not shops:
                        print("No more shops to process.")
                        return result
                    print("Fetched shops batch.")
                    break
                except DeadlineExceeded:
                    print("Deadline exceeded while fetching shops, retrying...")
                    time.sleep(2)
            else:
                print("Failed to fetch shops after 3 attempts.")
                return result

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_shop = {executor.submit(self.process_shop, shop.id, batch_size): shop for shop in shops}

                for future in as_completed(future_to_shop):
                    shop = future_to_shop[future]
                    try:
                        result.extend(future.result())
                    except Exception as exc:
                        print(f"{shop.id} generated an exception: {exc}")

            last_doc = shops[-1]

        print("Finish extract")
        return result

    def process_shop(self, shop_id, batch_size):
        flows = self.fetch_flows(shop_id, batch_size)
        result = []

        for flow in flows:
            flow_data = flow.to_dict()
            flow_id = flow.id
            
            # Extract links from flow attributes
            attributes = flow_data.get("attributes", {})
            links = self.extract_links_from_text(attributes)

            # Store links with shop_id and flow_id
            for link in links:
                result.append((shop_id, flow_id, None, link))  # Removed template_id

            actions_ref = self.db.collection("shops").document(shop_id).collection("klaviyo_flows").document(flow.id).collection("actions")
            actions = self.fetch_documents(actions_ref, batch_size)

            for action in actions:
                action_data = action.to_dict()
                links = self.extract_links_from_text(action_data)
                
                # Extract action_template_id from action data
                action_template_id = action_data.get('data', {}).get('message', {}).get('template', {}).get('templateID', "N/A")

                for link in links:
                    result.append((shop_id, flow_id, action_template_id, link))  # Removed template_id

        return result

    def extract_links_from_text(self, data):
        links = []
        text = str(data)
        
        link_pattern = r'https?:\/\/router-link[a-zA-Z0-9\-\.]+\/[a-zA-Z0-9\{\}\|\/\s\_]+'
        links.extend(re.findall(link_pattern, text))

        return links

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Start main")
    firestore_service = FirestoreService()
    print("Finish init firestore service")

    links = firestore_service.extract_links(batch_size=100)
    print("Finish link extracts")
    
    if links:
        df = pd.DataFrame(links, columns=['shop_id', 'flow_id', 'action_template_id', 'links'])  # Removed template_id
        df.to_excel('lflow_links.xlsx', index=False)
        print(f"Saved {len(links)} links to flow_links.xlsx")
    else:
        print("No links found to save")

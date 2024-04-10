from pymongo import MongoClient
import uuid

# MongoDB connection setup for MAG, DBLP, Matched data, and TheAdvisor databases
mag_client = MongoClient('mongodb://localhost:27017/')
dblp_client = MongoClient('mongodb://localhost:27017/')
matched_client = MongoClient('mongodb://localhost:27017/')
theadvisor_client = MongoClient('mongodb://localhost:27017/')

mag_db = mag_client['mag']
dblp_db = dblp_client['dblp']
matched_db = matched_client['mag_dblp']
theadvisor_db = theadvisor_client['theadvisor_database']

# Collection access
mag_collection = mag_db['papers']
dblp_collection = dblp_db['papers']
matched_collection = matched_db['match']
theadvisor_collection = theadvisor_db['theadvisor_papers']

# Clear TheAdvisor Paper collection before insertion
theadvisor_collection.drop()

# Initialize a list to hold batched TheAdvisor papers
theadvisor_papers_batch = []

def insert_batch(batch):
    if batch:
        theadvisor_collection.insert_many(batch)
        print(f"Inserted batch of {len(batch)} TheAdvisor papers.")

# Iterate through matched records
for matched_record in matched_collection.find():
    mag_record = mag_collection.find_one({"paper_id": matched_record["mag_id"]})
    dblp_record = dblp_collection.find_one({"paper_id": matched_record["best_candidate_paper_dblp_id"]})

    # Only proceed if both MAG and DBLP records are found for a match
    if mag_record and dblp_record:
        theadvisor_paper = {
            "_id": str(uuid.uuid4()),
            "mag_id": mag_record["paper_id"],
            "dblp_id": dblp_record["paper_id"],
            "title": mag_record.get("title", ""),
            "author": dblp_record.get("author", ""),
            "year": mag_record.get("year", ""),
            "doi": dblp_record.get("doi", ""),
            "url": dblp_record.get("url", ""),
            "matched_data_id": matched_record["_id"]
        }
        theadvisor_papers_batch.append(theadvisor_paper)

        # Insert in batches of 1000
        if len(theadvisor_papers_batch) >= 1000:
            insert_batch(theadvisor_papers_batch)
            theadvisor_papers_batch = []

# Insert any remaining TheAdvisor papers
insert_batch(theadvisor_papers_batch)
print("Data loading completed.")

from flask import Flask, jsonify
from pymongo import MongoClient
from bson import ObjectId
import json

app = Flask(__name__)

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['theadvisor_database']  # Change this to your "TheAdvisor" database name
collection = db['theadvisor_papers']  # Change this to your collection name for "TheAdvisor Papers"

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

app.json_encoder = CustomJSONEncoder

@app.route('/papers', methods=['GET'])
def get_papers():
    papers_list = list(collection.aggregate([{"$sample": {"size": 50}}]))
    # Manually ensure that ObjectId is converted to string for each document
    for paper in papers_list:
        paper['_id'] = str(paper['_id'])
    return jsonify(papers_list)

if __name__ == '__main__':
    app.run(debug=True)

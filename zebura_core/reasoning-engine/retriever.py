from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch()

def retrieve_similar_candidates(query, n):
    # Define the Elasticsearch query
    search_query = {
        "query": {
            "match": {
                "content": query
            }
        },
        "size": n
    }

    # Execute the search query
    response = es.search(index="your_index_name", body=search_query)

    # Extract the top N candidates from the response
    candidates = [hit["_source"] for hit in response["hits"]["hits"]]

    return candidates

# Example usage
query = "your_query"
top_n = 10
similar_candidates = retrieve_similar_candidates(query, top_n)
print(similar_candidates)
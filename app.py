from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from google.cloud import bigquery
import pandas as pd
import os
import requests
import db_dtypes

app = Flask(__name__)
CORS(app)

# Set up environment variables for Google Cloud authentication
current_dir = os.path.dirname(os.path.abspath(__file__))
service_account_path = os.path.join(current_dir, 'key.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path

# Function to execute a SQL query in BigQuery and return a DataFrame
def query_bigquery(sql_query):
    client = bigquery.Client.from_service_account_json(service_account_path)
    query_job = client.query(sql_query)
    results = query_job.result()
    return results.to_dataframe()

# Elasticsearch server configuration
URL_ENDPOINT = "https://0a27419d3cae40c1a8cac40b7abe1cad.us-central1.gcp.cloud.es.io:443"
API_KEY = "MGU3MzE0NEJQbmM4bDUyZEJVZjc6N1BjUHQwY0lRRTY0QkNMTlRJYVYzdw=="
INDEX_NAME = 'cloud_a2_elastic'

# Initialize Elasticsearch client
es_client = Elasticsearch(URL_ENDPOINT, api_key=API_KEY)

@app.route('/')
def index():
    # Initialize connection to Elasticsearch index
    client = Elasticsearch(URL_ENDPOINT, api_key=API_KEY)

    # Display connection information
    client.info()
    print(client.info())

    return "Running Flask App!"

# TMDB API configuration for fetching movie details
TMDB_API_KEY = '9d958fd1518ad9574721ea4322f72a7d'
TMDB_BASE_URL = 'https://api.themoviedb.org/3'

# Fetch movie details from TMDB API using the provided tmdb_id
def get_movie_details_from_tmdb(tmdb_id):
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

@app.route('/movie_details', methods=['GET'])
def movie_details():
    tmdb_id = request.args.get('tmdb_id')
    if tmdb_id:
        details = get_movie_details_from_tmdb(tmdb_id)
        if details:
            return jsonify(details)
        else:
            return jsonify({"error": "Failed to fetch details"}), 404
    return jsonify({"error": "TMDB ID is required test"}), 400

@app.route('/load_movies', methods=['GET'])
def load_movies():
    # Load movies from BigQuery and return them as a JSON response
    client = bigquery.Client.from_service_account_json(service_account_path)
    query = """
    SELECT * FROM `assignement-1-416515.movies_a2.Movies_a2`;
    """
    query_job = client.query(query)
    movies_df = query_job.to_dataframe()
    return movies_df.to_json()

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '')
    if not query:
        return jsonify([])

    # Search for movies in Elasticsearch and enhance results with additional details from TMDB
    response = es_client.search(index=INDEX_NAME, body={
        "query": {
            "match_phrase_prefix": {
                "title": {
                    "query": query,
                    "max_expansions": 10
                }
            }
        },
        "_source": ["title", "genres", "movieId"],
        "size": 5
    })

    enriched_results = []
    for hit in response['hits']['hits']:
        result = hit['_source']
        try:
            movie_id = int(result['movieId'])  # Ensure movieId is an integer
            bigquery_query = f"SELECT tmdbId FROM `assignement-1-416515.Links_a2.Links` WHERE movieId = {movie_id}"
            # Execute the query using the existing utility function
            query_result = query_bigquery(bigquery_query)
            tmdb_id = query_result['tmdbId'][0] if not query_result.empty else None

            if tmdb_id:
                tmdb_details = get_movie_details_from_tmdb(tmdb_id)
                result.update({
                    'poster_path': f"https://image.tmdb.org/t/p/w500{tmdb_details.get('poster_path', '')}" if tmdb_details and tmdb_details.get('poster_path') else None,
                    'overview': tmdb_details.get('overview', 'No description available.') if tmdb_details else 'No details available.',
                    'tmdb_url': f"https://www.themoviedb.org/movie/{tmdb_id}" if tmdb_details else None
                })
            else:
                result.update({
                    'poster_path': None,
                    'overview': 'TMDB details not found.',
                    'tmdb_url': None
                })
        except Exception as e:
            # Handle exceptions and update results accordingly
            result.update({
                'poster_path': None,
                'overview': f'Failed to fetch details: {str(e)}',
                'tmdb_url': None
            })
        enriched_results.append(result)
    return jsonify(enriched_results)

def fetch_recommendations(user_id):
    # Query to fetch movie recommendations based on user similarity via BigQuery
    client = bigquery.Client.from_service_account_json(service_account_path)

    query = f"""
    DECLARE target_user_id INT64;
    SET target_user_id = {user_id};

    -- Select user ratings, compute norms and dot products, calculate cosine similarities, and fetch recommendations
    WITH user_ratings AS (
      SELECT userId, movieId, rating_im AS rating
      FROM `assignement-1-416515.ratings_a2.Ratings_a2`
    ),
    user_norms AS (
      SELECT userId, SQRT(SUM(POW(rating, 2))) AS norm
      FROM user_ratings
      GROUP BY userId
    ),
    dot_product AS (
      SELECT 
        a.userId AS user1,
        b.userId AS user2,
        SUM(a.rating * b.rating) AS dot_product
      FROM user_ratings a
      JOIN user_ratings b ON a.movieId = b.movieId
      WHERE a.userId < b.userId  -- Avoid counting pairs twice
      GROUP BY a.userId, b.userId
    ),
    cosine_similarity AS (
      SELECT 
        user1,
        user2,
        dot_product / (ua.norm * ub.norm) AS similarity
      FROM dot_product dp
      JOIN user_norms ua ON dp.user1 = ua.userId
      JOIN user_norms ub ON dp.user2 = ub.userId
    ),
    ranked_similarity AS (
      SELECT
        user1 AS target_user_id,
        user2 AS similar_user_id,
        similarity,
        ROW_NUMBER() OVER (PARTITION BY user1 ORDER BY similarity DESC) AS similarity_rank
      FROM cosine_similarity
    ),
    similar_users AS (
      SELECT 
        ARRAY_AGG(similar_user_id) AS similar_user_ids
      FROM 
        ranked_similarity
      WHERE 
        target_user_id = target_user_id  -- Utilize the parameter defined
      AND 
        similarity_rank <= 10
    ),
    SimilarUsers AS (
      SELECT
        user_ratings.userId,
        user_ratings.movieId,
        user_ratings.rating
      FROM
        user_ratings
      JOIN
        similar_users
      ON
        user_ratings.userId IN UNNEST(similar_user_ids)
    )
    SELECT 
      movieId, 
      rating AS predicted_rating
    FROM 
      SimilarUsers
    """
    query_job = client.query(query)
    recommendations_df = query_job.to_dataframe()
    return recommendations_df

@app.route('/recommendations', methods=['GET'])
def get_recommendations():
    user_id = request.args.get('user_id')
    if user_id:
        try:
            recommendations_df = fetch_recommendations(user_id)
            recommendations_json = recommendations_df.head(5).to_json(orient='records')
            return recommendations_json
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "User ID is required"}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
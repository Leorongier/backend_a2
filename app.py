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


@app.route('/recommendations', methods=['POST'])
def get_recommendations():
    # Réception du corps JSON de la requête POST.
    data = request.get_json()
    preferred_movies = data.get('preferred_movies')

    # Vérification que la liste des films préférés est présente.
    if not preferred_movies:
        return jsonify({"error": "Preferred movies list is required"}), 400

    # Création de la requête SQL pour obtenir des recommandations.
    preferred_movies_list = ', '.join([str(movie_id) for movie_id in preferred_movies])
    recommendation_query = f"""
    SELECT 
        movieId, 
        predicted_rating_im_confidence  # Utilisation du bon nom de colonne
    FROM
        ML.RECOMMEND(MODEL `assignement-1-416515.ratings_a2.first-MF-model`,
        (SELECT movieId FROM UNNEST([{preferred_movies_list}]) AS movieId))
    WHERE predicted_rating_im_confidence > 0  # Utilisation du bon nom de colonne
    ORDER BY predicted_rating_im_confidence DESC  # Utilisation du bon nom de colonne
    LIMIT 5
    """

    # Exécution de la requête et conversion des résultats en JSON.
    client = bigquery.Client.from_service_account_json(service_account_path)
    try:
        recommendations_df = client.query(recommendation_query).to_dataframe()
        recommendations = recommendations_df.to_dict(orient='records')
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

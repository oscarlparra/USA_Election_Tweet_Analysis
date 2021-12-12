from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import plotly.express as px

spark = SparkSession.builder.appName(
    'Resultados_tweets').getOrCreate()


tweets = spark.read.csv('2016_US_election_tweets_100k.csv' , sep=',', inferSchema=True, header=True)

ids = spark.read.csv('candidates.csv' , sep=',', inferSchema=True, header=True)

li = ['OH', 'AZ', 'MO', 'TN', 'ID', 'MA', 'LA', 'CA', 'SC', 'MN', 'NJ', 'DC', 'OR', 'VA', 'RI', 'KY',
          'WY', 'NH', 'MI', 'NV', 'WI', 'CT', 'NE', 'MT', 'NC', 'VT', 'MD', 'DE', 'IL', 'ME', 'WA',
          'ND', 'MS', 'AL', 'IN', 'IA', 'NM', 'PA', 'SD', 'NY', 'TX', 'WV', 'GA', 'KS', 'FL', 'CO',
          'AK', 'AR', 'OK', 'UT', 'HI']

#df = spark.read.option('header', True).csv(tweets)
new_df = tweets.select('candidate_id', 'polarity', 'subjectivity', 'state', 'created_at').filter(tweets.state.isNotNull() & tweets.created_at.isNotNull()).filter(tweets.state.isin(li))

result = new_df.groupBy('state').count()
#result.show()
pandasDF = result.toPandas()
#print(pandasDF)

file = open('states.json')
data = json.load(file)


fig = px.choropleth_mapbox(pandasDF, geojson=data, featureidkey='properties.name', locations='state', color='count',
                           color_continuous_scale="mint",
                           range_color=(0, 350),
                           mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=1,
                           labels={'tweets':'amount'}
                          )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
#fig.show()


#ids_df = spark.read.option('header', True).csv(ids)
candidates_df = ids.join(new_df, ids.id == new_df.candidate_id)
#candidates_df.show()
positive_polarity = candidates_df.filter(candidates_df.polarity >= 0).groupBy('name', 'state').count().withColumnRenamed('count', 'votes')
polarity_results = positive_polarity.sort(positive_polarity.state.asc(), positive_polarity.votes.desc()).groupBy('state').agg(first('name').alias('name')).sort('state')
polarity_results.toPandas().to_csv("Positive_pol.csv")
#polarity_results.show()
pos_polarity_pd = polarity_results.toPandas()
#print(polarity_pd)
negative_polarity = candidates_df.filter(candidates_df.polarity < 0).groupBy('name', 'state').count().withColumnRenamed('count', 'votes')
neg_polarity_results = negative_polarity.sort(negative_polarity.state.asc(), negative_polarity.votes.desc()).groupBy('state').agg(first('name').alias('name')).sort('state')
neg_polarity_pd = neg_polarity_results.toPandas()
neg_polarity_pd.to_csv("Negative_pol.csv")

#Mapa polaridad positiva
fig = px.choropleth_mapbox(pos_polarity_pd, geojson=data, color="name",
                    locations="state", featureidkey="properties.name", hover_data=['name'],
                    mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=1,
                   )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

#Mapa polaridad negativa
fig = px.choropleth_mapbox(neg_polarity_pd, geojson=data, color="name",
                    locations="state", featureidkey="properties.name", hover_data=['name'],
                    mapbox_style="carto-positron",
                           zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                           opacity=1,
                   )
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()
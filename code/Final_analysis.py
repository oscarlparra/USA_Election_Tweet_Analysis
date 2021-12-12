from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import plotly.express as px

spark = SparkSession.builder.appName(
    'Final_analysis').getOrCreate()

pos = spark.read.csv("Positive_pol.csv", sep=',', inferSchema=True, header=True)
neg = spark.read.csv("Negative_pol.csv", sep=',', inferSchema=True, header=True)
real = spark.read.csv("resultados.csv", sep=',', inferSchema=True, header=True)

#Constrast real vs positve polarity

cond_pos = [real.state_postal == pos.state, real.candidato == pos.name]
pos_contrast = real.join(pos, cond_pos, "inner")
pos_contrast.toPandas().to_csv('Pos_cont.csv')

file = open('states.json')
data = json.load(file)


fig_pos = px.choropleth_mapbox(pos_contrast.toPandas(), geojson=data, color="name",
                  locations="state_postal", featureidkey="properties.name", hover_data=['name'],
                mapbox_style="carto-positron",
                          zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                          opacity=1,
               )

fig_pos.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig_pos.show()


#Contrast real vs negative polarity
cond_neg = [real.state_postal == neg.state, real.candidato == neg.name]
neg_contrast = real.join(neg, cond_neg, "inner")
neg_contrast.toPandas().to_csv('Neg_cont.csv')

fig_neg = px.choropleth_mapbox(neg_contrast.toPandas(), geojson=data, color="name",
                  locations="state_postal", featureidkey="properties.name", hover_data=['name'],
                mapbox_style="carto-positron",
                          zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                          opacity=1,
               )

fig_neg.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig_neg.show()




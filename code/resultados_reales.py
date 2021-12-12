from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import plotly.express as px

spark = SparkSession.builder.appName(
    'Resultados_elecciones_reales').getOrCreate()

resultados = spark.read.csv('2016-precinct-president.csv', sep=',',
                            inferSchema=True, header=True)

#Operaciones dataset
maximos = resultados.select('state_postal', 'county_name', 'candidate', 'votes').groupby('state_postal', 'candidate').sum('votes').sort('state_postal', desc('sum(votes)'))
ganadores = maximos.groupBy('state_postal').agg(first('candidate').alias('candidato')).sort('state_postal')
cond = [maximos.candidate == ganadores.candidato, maximos.state_postal == ganadores.state_postal]
result = ganadores.join(maximos, cond, 'leftouter').select(ganadores.state_postal, 'candidato', 'sum(votes)').sort(col('candidato').desc())

result.toPandas().to_csv('resultados.csv')

#Construyendo el mapa
file = open('states.json')
data = json.load(file)

fig = px.choropleth_mapbox(result.toPandas(), geojson=data, color="candidato",
                  locations="state_postal", featureidkey="properties.name", hover_data=['candidato','sum(votes)'],
                mapbox_style="carto-positron",
                          zoom=3, center = {"lat": 37.0902, "lon": -95.7129},
                          opacity=1,
               )

fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

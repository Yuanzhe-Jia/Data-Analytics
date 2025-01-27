# Databricks notebook source

#
# step 1: get event data from GCP
# import libraries

from pyspark.sql import functions as fn
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import types 
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
from datetime import datetime as dt, timedelta as td
import numpy as np
import pandas as pd
import json

# convert btypes to string
from pyspark.sql import SparkSession
spark.conf.set("spark.sql.parquet.binaryAsString", "true")

# pull raw data from gcp
dt_data = spark.read.parquet("gs://...parquet")
dt_data.createOrReplaceTempView("dt_data")

#
# step 2: extract sequence data in a row
# transfer columns to rows

# function for 2 event flow
def get_sequence_2(data):
  event_sequence_2 = []

  for i in range(data.shape[0]-1):
    if data.label[i] == "near_target":
      if data.label[i+1] == "near_target":
        event_sequence_2.append([data.event_name[i], data.event_name[i+1]])
  return np.array(event_sequence_2)


# function for 3 event flow
def get_sequence_3(data):
  event_sequence_3 = []

  for i in range(data.shape[0]-2):
    if data.label[i] == "near_target":
      if data.label[i+1] == "near_target":
        if data.label[i+2] == "near_target":
          event_sequence_3.append([data.event_name[i], data.event_name[i+1], data.event_name[i+2]])
  return np.array(event_sequence_3)


# function for 5 event flow
def get_sequence_5(data):
  event_sequence_5 = []

  for i in range(data.shape[0]-4):
    if data.label[i] == "near_target":
      if data.label[i+1] == "near_target":
        if data.label[i+2] == "near_target":
          if data.label[i+3] == "near_target":
            if data.label[i+4] == "near_target":
              event_sequence_5.append([data.event_name[i], data.event_name[i+1], data.event_name[i+2], data.event_name[i+3], data.event_name[i+4]])
  return np.array(event_sequence_5)


# functions to get nodes and links
def get_nodes(columns, data):
  nodes = []

  for i in range(columns):
    values = data.iloc[:,i].unique()
    for value in values:
      dic = {}
      dic["name"] = value
      nodes.append(dic)
  return nodes

def get_links(data):
  links = []

  for i in data.values:
    dic = {}
    dic["source"] = i[0]
    dic["target"] = i[1]
    dic["value"] = i[2]
    links.append(dic)
  return links

#
# step 3: data pre-processing by SQL
# set a target event
# define a filter
# define event/mapped event flow
# set how many sequence data before the target event

after_boot = spark.sql("""

with
dataset as (
  select
    clid,
    event_name,
    derived_tstamp
  from event_table
),

dateset_1 as (
  select 
    *,
    lag(event_name) over (partition by clid order by derived_tstamp) as former_event,

    if(event_name="boot", 1, 0) as is_target,
    lag(if(event_name="boot", 1, 0)) over (partition by clid order by derived_tstamp) as former_target
  from dataset
),

dateset_2 as (--remove continuous events
  select 
    *
  from dateset_1
  where 
    --concat(is_target, former_target) <> "11"
    event_name <> former_event
),

dateset_3 as (
  select
    *,
    if(lag(event_name, 1) over(partition by clid order by derived_tstamp)="boot", 1, 0) as is_1_near,
    if(lag(event_name, 2) over(partition by clid order by derived_tstamp)="boot", 1, 0) as is_2_near,
    if(lag(event_name, 3) over(partition by clid order by derived_tstamp)="boot", 1, 0) as is_3_near
    --if(lag(event_name, 4) over(partition by clid order by derived_tstamp)="boot", 1, 0) as is_4_near,
    --if(lag(event_name, 5) over(partition by clid order by derived_tstamp)="boot", 1, 0) as is_5_near
  from dateset_2
),

dateset_4 as (
  select
    *,
    --if((is_1_near = 1) or (is_2_near = 1) or (is_3_near = 1) or (is_4_near = 1) or (is_5_near = 1), 1, 0) as is_target_near
    if((is_1_near = 1) or (is_2_near = 1) or (is_3_near = 1), 1, 0) as is_target_near
  from dateset_3
),

dateset_5 as (
  select
    * except(former_target, is_target_near),
    case when is_target = 1 then "is_target" when is_target_near = 1 then "near_target" else null end as label
  from dateset_4
  where 
    is_target = 1 or is_target_near = 1
)

select * from dateset_5 order by clid, derived_tstamp
"""
)

after_boot = after_boot.toPandas()
after_boot.head(10)

#
# step 4: make sankey chart
# transform the raw data into sequences

df = pd.DataFrame(get_sequence_3(after_boot), columns=["col2", "col3", "col4"])
df["col1"] = np.full(get_sequence_3(after_boot).shape[0], "boot")
# df.col1 = [x+"_1" for x in df.col1]
df.col2 = [x+"_2" for x in df.col2]
df.col3 = [x+"_3" for x in df.col3]
df.col4 = [x+"_4" for x in df.col4]
print(df.shape)
df.head(10)

# prepare datasets for the sankey chart

# col1 - col2
df12 = df.groupby(["col1","col2"])["col3"].count()
df12 = pd.DataFrame(df12).reset_index()
df12.columns = ['col1', 'col2', 'count12']
#print(df12.shape)
df12.head(10)

# col2 - col3
df23 = df.groupby(["col2","col3"])["col4"].count()
df23 = pd.DataFrame(df23).reset_index()
df23.columns = ['col2', 'col3', 'count23']
#print(df23.shape)
df23.head(10)

# col3 - col4
df34 = df.groupby(["col3","col4"])["col1"].count()
df34 = pd.DataFrame(df34).reset_index()
df34.columns = ['col3', 'col4', 'count34']
#print(df34.shape)
df34.head(10)

# col1 - col4
df12.columns = ["source", "target", "value"]
df23.columns = ["source", "target", "value"]
df34.columns = ["source", "target", "value"]
df14 = pd.concat([df12, df23, df34])
df14.head(10)

!pip install pyecharts
from pyecharts.charts import Sankey
from pyecharts import options as opts

# get nodes and links for all layers
nodes14 = get_nodes(4, df)
links14 = get_links(df14)

# create a sankey chart
pic = (
    Sankey(init_opts = opts.InitOpts(width = "1600px",
                                     height = "900px"))
    .add("",
         nodes = nodes14,
         links = links14,
         linestyle_opt = opts.LineStyleOpts(opacity = 0.3, curve = 0.5, color = "source"),
         label_opts = opts.LabelOpts(position = "right"),
         node_gap = 40,
    )
    #.set_global_opts(title_opts = opts.TitleOpts(title = ""))
)

# save the chart
pic.render("sankey.html")
sankey = open('/databricks/driver/sankey.html', 'r').read()
displayHTML(sankey)

get_sequence_3(after_boot)
pic.render("sankey.html")
sankey = open('/databricks/driver/sankey.html', 'r').read()
displayHTML(sankey)

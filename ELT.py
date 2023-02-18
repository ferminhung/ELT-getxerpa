import json
import csv
import requests
from datetime import datetime 
from datetime import date
import numpy as np
import pandas as pd
import operator
import os
from json import loads

df = pd. read_csv ('MCD.US.csv')

df7d = df.tail(7)
precio_max = df7d['High'].max()
precio_min = df7d['Low'].min()

print (precio_max, precio_min)

import sys
import os
import json

x = os.listdir('views_valid5')
print(len(x))

with open('valid_paths_5.json', "r") as f:
    model_paths = json.load(f)

print("json len ", len(model_paths))
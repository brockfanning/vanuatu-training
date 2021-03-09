import json
import os
import csv

# Rename the data files to start with "indicator_".
"""
for filename in os.listdir('data'):
    if filename.endswith(".csv") and not filename.startswith('indicator_'):
        path_from = os.path.join('data', filename)
        path_to = os.path.join('data', 'indicator_' + filename)
        os.rename(path_from, path_to)
"""

for filename in os.listdir('meta'):
    if not filename.endswith('.json'):
        continue
    with open(os.path.join('meta', filename), 'r') as stream:
        meta = json.load(stream)
    del meta['ID']
    del meta['DataID']
    del meta['indicator_number']
    del meta['goal_number']
    del meta['goal_name']
    del meta['target_number']
    del meta['target_name']
    del meta['indicator_name']
    del meta['UnitofMeasure']
    del meta['national_geographical_coverage']
    del meta['indicator']
    del meta['target_id']
    del meta['sdg_goal']
    del meta['reporting_status']
    del meta['indicator_sort_order']
    meta['data_non_statistical'] = False

    destination = os.path.join('meta', filename.replace('.json', '.csv'))
    with open(destination, 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in meta.items():
            writer.writerow([key, value])

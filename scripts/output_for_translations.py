from sdg.inputs import InputBase
from mysql.connector import connect
import os

def to_translation_key(value):
    value = value.replace(' ', '')
    value = value.replace('.', '-')
    return value.strip()

def to_goal_id(value):
    value = value.replace('Economy', 'ECO')
    value = value.replace('Environment', 'ENV')
    value = value.replace('Enviornment', 'ENV')
    value = value.replace('Society', 'SOC')
    return to_translation_key(value)

mydb = connect(user=os.environ.get('DBUSER'), password=os.environ.get('DBPASS'),
            host=os.environ.get('DBHOST'), database=os.environ.get('DBNAME'))
cursor = mydb.cursor(dictionary=True)

# Get the policy objective info.
"""
sql = 'SELECT NSDPPolicyObjID, NSDPPolicyObjName FROM nsdppolicyobjective'
cursor.execute(sql)
for row in cursor.fetchall():
    key = to_translation_key(row['NSDPPolicyObjID'])
    print(key + ': ' + row['NSDPPolicyObjName'])
"""

"""
sql = 'SELECT * FROM nsdpgoal'
cursor.execute(sql)
for row in cursor.fetchall():
    goal_id = to_goal_id(row['NSDPGoalID'])
    goal_name = row['NSDPGoalID']
    goal_description = row['NSDPGoalDescription']
    goal_theme = row['NSDPGoalTheme']
    print(goal_id + '-short: ' + goal_name)
    print(goal_id + '-title: ' + goal_description)
"""

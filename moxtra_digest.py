import psycopg2
import psycopg2.extras
import sqlite3
import os.path
from pathlib import Path
import re
import datetime
import requests
import json

CONTENT_TYPE_MESSAGES = 1
CONTENT_TYPE_TASKS = 2
LENGTH_EXCERPT = 100
START_DATE = '2020-11-22'
START_DATE = '2020-11-15'
DEBUG = False

dbname = None
configfile="/etc/openproject/conf.d/00_addon_postgres"
if os.path.isfile(configfile):
  f = open(configfile, "r")
  # export DATABASE_URL="postgres://openproject:topsecret@127.0.0.1:45432/openproject"
  config=f.read()
  dbuser=re.search(r"\/\/(.*?):", config).group(1)
  dbpwd=re.search(r":([^:]*)@", config).group(1)
  dbport=re.search(r":([0-9]+)\/", config).group(1)
  dbname=re.search(r"\/([^/]*?)\"", config).group(1)
  dbhost=re.search(r"@(.*):", config).group(1)
else:
  configfile=("%s/openproject/config/database.yml" % (Path.home(),))
  if os.path.isfile(configfile):
    f = open(configfile, "r")
    for line in f:
      dbport = 5432
      # host: localhost
      if "host: " in line:
        dbhost = re.search(r": (.*)", line).group(1)
      if "database: " in line:
        dbname = re.search(r": (.*)", line).group(1)
      if "username: " in line:
        dbuser = re.search(r": (.*)", line).group(1)
      if "password: " in line:
        dbpwd = re.search(r": (.*)", line).group(1).replace('"', '')

if not dbname:
  print("could not find the config file")
  exit(-1)

configfile=("%s/moxtra_notifications.yml" % (Path.home(),))
if os.path.isfile(configfile):
  f = open(configfile, "r")
  for line in f:
    if line.strip().startswith("#"):
        continue
    if "webhook_url" in line:
        webhook_url = re.search(r": (.*)", line).group(1)
    if "project_slug" in line:
        project_slug = re.search(r": (.*)", line).group(1)
    if "frequency" in line:
        frequency = re.search(r": (.*)", line).group(1)

# Connect to your postgres DB
params = {'dbname': dbname, 'user': dbuser, 'password': dbpwd, 'port': dbport, 'host': dbhost}
conn = psycopg2.connect(**params)
cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

# Connect to the sqlite database
sq3 = sqlite3.connect('notifications_moxtra.sqlite3')
sq3.execute("""
CREATE TABLE IF NOT EXISTS Notified (
id INTEGER PRIMARY KEY AUTOINCREMENT,
user_id INTEGER,
project_id INTEGER,
content_type INTEGER,
content_id INTEGER,
t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

# get the project_id
sqlProject = """
select projects.id
from projects
where identifier = %s"""

# get all posts per project within the past 2 weeks
sqlForumMessages = """
select messages.id, forum_id, parent_id, forums.name as forum_name, subject, firstname, lastname, login, messages.created_on, content, '' as url
from messages, users, forums
where messages.author_id = users.id and forums.id = messages.forum_id and forums.project_id = %s
and messages.created_on >= %s
order by created_on asc"""

# get all created or updated tasks per project within the past 2 weeks
sqlUpdatedTasks = """
select w.id, p.identifier as projectslug, w.subject, w.description, w.updated_at, '' as url
from work_packages as w, projects as p
where w.project_id = p.id
and w.project_id = %s
and w.updated_at >= %s
order by updated_at asc"""

# verify if user has already been notified abou this content
sqlCheckNotification = """
select *
from Notified
where user_id = ? and project_id = ? and content_id = ? and content_type = ?"""

sqlAddNotification = """
insert into Notified(user_id, project_id, content_id, content_type)
values(?,?,?,?)"""


def processUser(frequency):
  now = datetime.datetime.now()

  # frequency: each hour
  if frequency == 0:
    return True

  # frequency: every 3 hours, send at 7 am, 10 am, 1 pm, 4 pm, 7 pm, 10 pm, 1 am, 4 am
  if frequency == 1:
    if now.hour not in [7,10,13,16,19,22,1,4]:
      return False

  # frequency: daily, send at 7 am
  if frequency == 2:
    if now.hour != 7:
      return False

  # frequency: weekly, send at 7 pm on Saturday
  if frequency == 3:
    if now.hour != 19 and now.weekday != 5:
      return False

  # frequency: never
  if frequency == 4:
    return False

  return True

def alreadyNotified(userId, projectId, contentId, contentType):
  cursor = sq3.cursor()
  cursor.execute(sqlCheckNotification, (userId, projectId, contentId, contentType,))
  row = cursor.fetchone()
  if row is None:
    return False
  return True

def storeNotified(userId, projectId, contentId, contentType):
  sq3.execute(sqlAddNotification, (userId, projectId, contentId, contentType,))

def storeAllNotified(user_id, project_id, messages, tasks):
  for post in messages:
    storeNotified(user_id, project_id, post['id'], CONTENT_TYPE_MESSAGES)
  for task in tasks:
    storeNotified(user_id, project_id, task['id'], CONTENT_TYPE_TASKS)
  sq3.commit()

def sendNotification(sender, msg):
  S = requests.Session()
  DATA = {sender: msg}
  R = S.post(url=webhook_url, data=DATA)
  print(R)

def sendNotifications(messages, tasks):
  if (len(messages) == 0) and (len(tasks) == 0):
    return

  try:

    for post in messages:
        msg = ("[%s] %s\n%s wrote: %s\n%s\n%s" %
            (post['forum_name'], post['created_on'].strftime('%Y-%m-%d %H:%M'),post['login'],post['subject'],post['content'],post['url']))
        if DEBUG:
            print(msg)
        else:
            sendNotification('OpenProject Forum', msg)
    for task in tasks:
        msg = ("%s\n%s\n%s" % (task['subject'], task['description'], task['url']))
        if DEBUG:
            print(msg)
        else:
            sendNotification('OpenProject Tasks', msg)

  except Exception as e:
    print(e)

  if DEBUG:
    # don't store in sqlite database
    return False

  return True

# get the settings for SMTP and the URL
def getSettings():
  sqlSettings = """
    select name, value
    from settings
    where name in ('protocol', 'host_name')"""
  cur.execute(sqlSettings)
  rows = cur.fetchall()
  settings = {}
  settings['pageurl'] = ''
  for row in rows:
    if row['name'] == 'host_name':
       settings['pageurl'] += row['value']
    if row['name'] == 'protocol':
       settings['pageurl'] = row['value'] + "://" + settings['pageurl']
  return settings

settings = getSettings()
cur.execute(sqlProject, (project_slug,))
rows = cur.fetchall()
if not rows:
  # fail if no CustomField exists
  print('We cannot find the project %s' % (project_slug,))
  exit(-1)
project_id = rows[0]['id']
user_id = -1

# should we process the moxtra notification?
if DEBUG or processUser(int(frequency)):

    # get all new messages
    messages = []
    cur.execute(sqlForumMessages, (project_id,START_DATE,))
    posts = cur.fetchall()
    for p in posts:
      if not alreadyNotified(user_id, project_id, p['id'], CONTENT_TYPE_MESSAGES):
        if p['parent_id'] is None:
          p['parent_id'] = p['id']
        if len(p['content']) > LENGTH_EXCERPT:
          p['content'] = p['content'][0:LENGTH_EXCERPT].strip() + "[...]"
        p['url'] = ("%s/topics/%s?r=%s#message-%s" % (settings['pageurl'], p['parent_id'], p['id'], p['id']))
        messages.append(p)

    # get all new or updated tasks
    tasks = []
    cur.execute(sqlUpdatedTasks, (project_id,START_DATE,))
    items = cur.fetchall()
    for p in items:
      if not alreadyNotified(user_id, project_id, p['id'], CONTENT_TYPE_TASKS):
        if len(p['description']) > LENGTH_EXCERPT:
          p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
        p['url'] = ("%s/projects/%s/work_packages/%s/activity" % (settings['pageurl'], p['projectslug'], p['id']))
        tasks.append(p)

    if sendNotifications(messages, tasks):
      storeAllNotified(user_id, project_id, messages, tasks)




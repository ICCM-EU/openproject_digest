import psycopg2
import psycopg2.extras
import sqlite3
import os.path
import sys
from pathlib import Path
import re
import datetime
import requests
import json
from pytz import timezone
import pytz
from webdav3.client import Client
import tempfile
# pdfkit requires wkhtmltopdf to be installed: apt-get install wkhtmltopdf
import pdfkit
import time


CONTENT_TYPE_MESSAGES = 1
CONTENT_TYPE_TASKS = 2
CONTENT_TYPE_MEETING_AGENDA = 3
CONTENT_TYPE_MEETING_MINUTES = 4
LENGTH_EXCERPT = 100000
START_DATE = '2021-11-01'
DEBUG = False
localtz = timezone('Europe/Amsterdam')

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

configparameter = "nextcloud_notifications.yml"
if len(sys.argv) > 1:
    configparameter = sys.argv[1]
configfile=("%s/%s" % (os.path.dirname(os.path.realpath(__file__)),configparameter))
if os.path.isfile(configfile):
  f = open(configfile, "r")
  for line in f:
    if line.strip().startswith("#"):
        continue
    if "project_slug" in line:
        project_slug = re.search(r": (.*)", line).group(1)
    if "nc_url" in line:
        nc_url = re.search(r": (.*)", line).group(1)
    if "nc_channel" in line:
        nc_channel = re.search(r": (.*)", line).group(1)
    if "nc_user" in line:
        nc_user = re.search(r": (.*)", line).group(1)
    if "nc_pwd" in line:
        nc_pwd = re.search(r": (.*)", line).group(1)
    if "frequency" in line:
        frequency = re.search(r": (.*)", line).group(1)

# Connect to your postgres DB
params = {'dbname': dbname, 'user': dbuser, 'password': dbpwd, 'port': dbport, 'host': dbhost}
conn = psycopg2.connect(**params)
cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)

# Connect to the sqlite database
sq3 = sqlite3.connect('notifications_nextcloud.sqlite3')
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
select messages.id, forum_id, parent_id, forums.name as forum_name, subject, firstname, lastname, login, messages.created_at, content, '' as url
from messages, users, forums
where messages.author_id = users.id and forums.id = messages.forum_id and forums.project_id = %s
and messages.created_at >= %s
order by created_at asc"""

# get all created or updated tasks per project within the past 2 weeks
sqlUpdatedTasks = """
select w.id, p.identifier as projectslug, w.subject, w.description, w.updated_at, '' as url
from work_packages as w, projects as p
where w.project_id = p.id
and w.project_id = %s
and w.updated_at >= %s
order by updated_at asc"""

# get all created or updated meetings per project within the past 2 weeks
sqlUpdatedMeetings = """
select mc.id, m.id as meeting_id, p.identifier as projectslug, m.title as subject, m.start_time, mc.type, mc.text as description, mc.updated_at, '' as url
from meetings as m, meeting_contents as mc, projects as p
where m.project_id = p.id
and m.project_id = %s
and mc.meeting_id = m.id
and mc.updated_at >= %s
order by mc.updated_at asc"""

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

def storeAllNotified(user_id, project_id, messages, tasks, meetings):
  for post in messages:
    storeNotified(user_id, project_id, post['id'], CONTENT_TYPE_MESSAGES)
  for task in tasks:
    storeNotified(user_id, project_id, task['id'], CONTENT_TYPE_TASKS)
  for meeting in meetings:
    if meeting['type'] == 'MeetingMinutes':
      storeNotified(user_id, project_id, meeting['id'], CONTENT_TYPE_MEETING_MINUTES)
    if meeting['type'] == 'MeetingAgenda':
      storeNotified(user_id, project_id, meeting['id'], CONTENT_TYPE_MEETING_AGENDA)
  sq3.commit()

def shareAttachment(msg):

  # upload the file
  # see https://pypi.org/project/webdavclient3/
  options = {
    'webdav_hostname': f"https://cloud.iccm-europe.org/remote.php/dav/files/{nc_user}",
    'webdav_login':    nc_user,
    'webdav_password': nc_pwd
  }
  filename = f"html_{time.time()}.pdf"
  client = Client(options)

  client.mkdir('/Talk')

  fp = tempfile.NamedTemporaryFile(mode="w+", delete=False)
  fp.close()
  pdfkit.from_string(msg, fp.name)
  client.upload_sync(remote_path=f"/Talk/{filename}", local_path=fp.name)
  os.unlink(fp.name)

  # if 404, is the app enabled? php occ app:enable files_sharing
  S = requests.Session()
  data = {
          "shareType": 10,
          "shareWith": nc_channel,
          "path": f"/Talk/{filename}",
          #"referenceId": "TODO",
          #"talkMetaData": {"messageType": "comment"}
          }
  # see https://nextcloud-talk.readthedocs.io/en/latest/chat/#share-a-file-to-the-chat
  url = f"{nc_url}/ocs/v2.php/apps/files_sharing/api/v1/shares"
  print(url)
  payload = json.dumps(data)
  headers = {'content-type': 'application/json', 'OCS-APIRequest': 'true'}
  R = S.post(url, data=payload, headers=headers, auth=(nc_user, nc_pwd))
  print(R)
  if R.status_code < 200 or R.status_code >=300:
      raise Exception("problem sharing the file")

def sendNotification(sender, msg):
  S = requests.Session()
  data = {
        "token": nc_channel,
        "message": "*"+sender+"*\n"+msg,
        "actorDisplayName": "OpenProject Bot",
        "actorType": "",
        "actorId": "",
        "timestamp": 0,
        "messageParameters": []
    }
  # see https://nextcloud-talk.readthedocs.io/en/latest/chat/#sending-a-new-chat-message
  url = "{}/ocs/v2.php/apps/spreed/api/v1/chat/{}".format(nc_url, nc_channel)
  print(url)
  payload = json.dumps(data)
  headers = {'content-type': 'application/json', 'OCS-APIRequest': 'true'}
  R = S.post(url, data=payload, headers=headers, auth=(nc_user, nc_pwd))
  print(R)

def sendNotifications(messages, tasks, meetings):
  if (len(messages) == 0) and (len(tasks) == 0) and (len(meetings) == 0):
    return

  try:

    for post in messages:
        created_at = pytz.utc.localize(post['created_at'], is_dst=None).astimezone(localtz)
        msg = ("[%s] %s\n%s wrote: %s\n%s\n%s" %
            (post['forum_name'], created_at.strftime('%Y-%m-%d %H:%M'),post['login'],post['subject'],post['content'],post['url']))
        msg = msg.replace("&quot;", '"').replace("&#39;", "'")
        if DEBUG:
            print(msg)
        else:
            if "<table" in msg or "<tbody" in msg:
                sendNotification('OpenProject Forum', msg[0:msg.index('<t')])
                shareAttachment(msg)
            else:
                sendNotification('OpenProject Forum', msg)
    for task in tasks:
        msg = ("%s\n%s\n%s" % (task['subject'], task['description'], task['url']))
        if DEBUG:
            print(msg)
        else:
            sendNotification('OpenProject Tasks', msg)
    for meeting in meetings:
        msg = ("%s\n%s UTC\n%s\n%s" % (meeting['subject'], meeting['start_time'], meeting['description'], meeting['url']))
        if DEBUG:
            print(msg)
        else:
            sendNotification("OpenProject %s" % (meeting['type'],), msg)

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
        if p['description'] is not None and len(p['description']) > LENGTH_EXCERPT:
          p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
        p['url'] = ("%s/projects/%s/work_packages/%s/activity" % (settings['pageurl'], p['projectslug'], p['id']))
        tasks.append(p)

    # get all new or updated meetings
    meetings = []
    cur.execute(sqlUpdatedMeetings, (project_id,START_DATE,))
    items = cur.fetchall()
    for p in items:
      if not p['description']:
          # do not send out notifications for empty items
          continue
      if p['type'] == 'MeetingAgenda':
        if not alreadyNotified(user_id, project_id, p['id'], CONTENT_TYPE_MEETING_AGENDA):
          if len(p['description']) > LENGTH_EXCERPT:
            p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
          p['url'] = ("%s/meetings/%s/agenda" % (settings['pageurl'], p['meeting_id']))
          meetings.append(p)
      if p['type'] == 'MeetingMinutes':
        if not alreadyNotified(user_id, project_id, p['id'], CONTENT_TYPE_MEETING_MINUTES):
          if len(p['description']) > LENGTH_EXCERPT:
            p['description'] = p['description'][0:LENGTH_EXCERPT].strip() + " [...]"
          p['url'] = ("%s/meetings/%s/minutes" % (settings['pageurl'], p['meeting_id']))
          meetings.append(p)

    if sendNotifications(messages, tasks, meetings):
      storeAllNotified(user_id, project_id, messages, tasks, meetings)




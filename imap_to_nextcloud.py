
"""
This script will read messages from an IMAP mailbox,
and post the content of the messages to a Nextcloud Talk room.
We use this for posting messages from Wordpress Wordfence to a Talk room where the administrators are listening.

TODO: name of sqlite database to config yml
      add sample config yml
      all strings to config yml: "IMAP Bot"; "Wordpress Website"
"""

import sqlite3
import os.path
import sys
from pathlib import Path
import re
import datetime
import requests
import json
import imaplib
import email
from email.header import decode_header
from webdav3.client import Client
import tempfile
# pdfkit requires wkhtmltopdf to be installed: apt-get install wkhtmltopdf
import pdfkit
import time

DEBUG = False
MAX_LENGTH_CHAT_MESSAGE = 1000

configparameter = "imap_to_nextcloud.yml"
if len(sys.argv) > 1:
    configparameter = sys.argv[1]
configfile=("%s/%s" % (os.path.dirname(os.path.realpath(__file__)),configparameter))
if os.path.isfile(configfile):
  f = open(configfile, "r")
  for line in f:
    if line.strip().startswith("#"):
        continue
    if "nc_url" in line:
        nc_url = re.search(r": (.*)", line).group(1)
    if "nc_channel" in line:
        nc_channel = re.search(r": (.*)", line).group(1)
    if "nc_user" in line:
        nc_user = re.search(r": (.*)", line).group(1)
    if "nc_pwd" in line:
        nc_pwd = re.search(r": (.*)", line).group(1)
    if "imap_host" in line:
        imap_host = re.search(r": (.*)", line).group(1)
    if "imap_user" in line:
        imap_user = re.search(r": (.*)", line).group(1)
    if "imap_pwd" in line:
        imap_pwd = re.search(r": (.*)", line).group(1)

# Connect to the sqlite database
sq3 = sqlite3.connect('notifications_imap.sqlite3')
sq3.execute("""
CREATE TABLE IF NOT EXISTS Notified (
id INTEGER PRIMARY KEY AUTOINCREMENT,
message_id VARCHAR(255),
t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

# verify if nextcloud has already been notified about this content
sqlCheckNotification = """
select *
from Notified
where message_id = ?"""

sqlAddNotification = """
insert into Notified(message_id)
values(?)"""


def alreadyNotified(messageId):
  cursor = sq3.cursor()
  cursor.execute(sqlCheckNotification, (messageId,))
  row = cursor.fetchone()
  if row is None:
    return False
  return True

def storeNotified(messageId):
  sq3.execute(sqlAddNotification, (messageId,))

def storeAllNotified(messages):
  for post in messages:
    storeNotified(post['id'])
  sq3.commit()

def sendNotification(sender, msg):
  S = requests.Session()
  data = {
        "token": nc_channel,
        "message": "*"+sender+"*\n"+msg,
        "actorDisplayName": "IMAP Bot",
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
  if R.status_code < 200 or R.status_code >=300:
      raise Exception("problem posting the message")

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
  print(data)
  payload = json.dumps(data)
  headers = {'content-type': 'application/json', 'OCS-APIRequest': 'true'}
  R = S.post(url, data=payload, headers=headers, auth=(nc_user, nc_pwd))
  print(R)
  if R.status_code < 200 or R.status_code >=300:
      raise Exception("problem sharing the file")

def sendNotifications(messages):
  if (len(messages) == 0):
    return

  try:

    for post in messages:

        if alreadyNotified(post['id']):
            continue
        msg = ("Sender: %s\nDate: %s\nSubject: %s\n%s" %
            (post['from'], post['date'], post['subject'], post['text']))
        msg = msg.replace("&quot;", '"').replace("&#39;", "'")
        if DEBUG:
            print(msg)
        else:
            if len(msg) > MAX_LENGTH_CHAT_MESSAGE:
                msg = msg[:MAX_LENGTH_CHAT_MESSAGE] + "\n[...]"
            sendNotification('Wordpress Website', msg)
            if post['html']:
                shareAttachment(post['html'])

  except Exception as e:
    print(e)

  if DEBUG:
    # don't store in sqlite database
    return False

  return True


def main():
    # get all new messages
    messages = []

    imap = imaplib.IMAP4_SSL(imap_host)
    imap.login(imap_user, imap_pwd)

    status, imapmessages = imap.select("INBOX")
    total_number_messages = int(imapmessages[0])
    max_messages = 10
    if total_number_messages < max_messages:
        max_messages = total_number_messages

    for i in range(total_number_messages, total_number_messages - max_messages, -1):
      # fetch the email message by ID
      res, msg = imap.fetch(str(i), "(RFC822)")
      for response in msg:
        if isinstance(response, tuple):

            post = {}
            # parse a bytes email into a message object
            msg = email.message_from_bytes(response[1])
            # decode the email subject
            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                # if it's a bytes, decode to str
                subject = subject.decode(encoding)
            # decode email sender
            From, encoding = decode_header(msg.get("From"))[0]
            if isinstance(From, bytes):
                From = From.decode(encoding)

            post['id'] = msg["Message-Id"]
            post['date'] = msg["Date"]
            post['subject'] = subject
            post['from'] = From

            post['text'] = ""
            post['html'] = ""
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))
                    try:
                        body = part.get_payload(decode=True).decode()
                    except:
                        pass
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        post['text'] = body
            else:
                # extract content type of email
                content_type = msg.get_content_type()
                # get the email body
                body = msg.get_payload(decode=True).decode()
                if content_type == "text/plain":
                    post['text'] = body
            if content_type == "text/html":
                post['html'] = body

            messages.append(post)

    imap.close()
    imap.logout()

    if sendNotifications(messages):
      storeAllNotified(messages)


main()

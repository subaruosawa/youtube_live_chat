import base64
import json
from google.cloud import pubsub_v1
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import datetime
import time

pClient = pubsub_v1.PublisherClient()
bClient = bigquery.Client()
job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField("authorId","STRING"),bigquery.SchemaField("timestampUsec","DATETIME")])
session = requests.Session()
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
today = datetime.date.today()
iso = today.isoformat()

def publishing(topic,content,kaunt):
  topic_path = pClient.topic_path("project2434",topic)
  if not type(content) == list:
    data = str(content)
  elif type(content) == list:
    string = [str(cntnt) for cntnt in content]
    data = "`".join(string)
  enc = data.encode("utf-8")
  pClient.publish(topic_path,enc,count=kaunt)

def loading(datasetName,tableName,content,jobConfig):
  if content:
    tableId = "project2434."+datasetName+"."+tableName
    bClient.load_table_from_json(content,tableId,job_config=jobConfig)

def hours9(timestampUsec):
  nine = int(timestampUsec)+32400000000
  dt = datetime.datetime.fromtimestamp(nine/1000000)
  iso = dt.isoformat()
  return iso

def joining(num,mssg):
  jnt = "`".join([num]+mssg)
  return jnt


def hello_pubsub(event, context):
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  message = pubsub_message.split("`")
  half = int(len(message)/2)
  videoIds = message[:half]
  videoId = videoIds[0]
  continuations = message[half:]
  continuation = continuations[0]

  count = int(event["attributes"]["count"])
  count+=1
  count = str(count)

  authorId = []
  timestampUsec = []
  error = False
  tt = time.time()
  while continuation and time.time()-tt < 510:
    target_url = "https://www.youtube.com/live_chat_replay?continuation="+continuation
    html = session.get(target_url, headers=headers)
    soup = BeautifulSoup(html.text,"html.parser")
    time.sleep(2)

    ytInitialData = None
    for script in soup.find_all('script'):
      script_text = str(script)
      if 'window["ytInitialData"] =' in script_text:
        if script_text[26] == "{" and script_text[-2] == "}":
          ytInitialData = json.loads(script_text[26:-1])
          loading("error","twentysix",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)
        elif script_text[65] == "{" and script_text[-11] == "}":
          ytInitialData = json.loads(script_text[65:-10])

    if not ytInitialData:
      error = True
      loading("error","none",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)
      continuation = None
      break
    elif ytInitialData:
      authorid = []
      timestampusec = []
      if "actions" in ytInitialData["continuationContents"]["liveChatContinuation"]:
        for i in ytInitialData["continuationContents"]["liveChatContinuation"]["actions"]:
          if "addChatItemAction" in i["replayChatItemAction"]["actions"][0]:
            if not "liveChatViewerEngagementMessageRenderer" in i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
              if "liveChatTextMessageRenderer" in i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                authorid.append(i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatTextMessageRenderer"]["authorExternalChannelId"])
                timestampusec.append(hours9(i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatTextMessageRenderer"]["timestampUsec"]))
              elif "liveChatPaidMessageRenderer" in i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]:
                authorid.append(i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatPaidMessageRenderer"]["authorExternalChannelId"])
                timestampusec.append(hours9(i["replayChatItemAction"]["actions"][0]["addChatItemAction"]["item"]["liveChatPaidMessageRenderer"]["timestampUsec"]))
        if len(authorid) == len(timestampusec):
          authorId+=authorid
          timestampUsec+=timestampusec
        else:
          error = True
          loading("error","nomatch",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)
          continuation = None
          break

      if "liveChatReplayContinuationData" in ytInitialData["continuationContents"]["liveChatContinuation"]["continuations"][0]:
        continuation = ytInitialData["continuationContents"]["liveChatContinuation"]["continuations"][0]["liveChatReplayContinuationData"]["continuation"]
      else:
        continuation = None

  if not error:
    aId_tsU = [{"authorId":authorId[n],"timestampUsec":timestampUsec[n]} for n in range(len(authorId))]
    loading("chat",videoId,aId_tsU,job_config)

    finish = False
    if len(videoIds) > 1:
      if continuation:
        message = videoIds+[continuation]+continuations[1:]
      else:
        message = videoIds[1:]+continuations[1:]
    else:
      if continuation:
        message = [videoId]+[continuation]
      else:
        finish = True
  
    if not finish:
      loading("pubsub",iso,[{"pubsub":joining(count,message)}],None)
      publishing("chat",message,count)

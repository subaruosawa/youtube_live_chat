import base64
import json
from apiclient.discovery import build
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import datetime
import time

youtube = build("youtube","v3",developerKey = "AIzaSyCtzF8YJWJK4wiHT5-2_eDpQFFtM9qha3g")
pClient = pubsub_v1.PublisherClient()
bClient = bigquery.Client()
sClient = storage.Client()
bucket = sClient.bucket("project2434")
session = requests.Session()
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
schema = [bigquery.SchemaField("authorId","STRING"),bigquery.SchemaField("timestampUsec","DATETIME")]
job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("videoId","STRING"),bigquery.SchemaField("start","DATETIME"),bigquery.SchemaField("end","DATETIME")])
with open("/workspace/channelIds.json") as f:
  channelIds = json.load(f)


def ytVideos(videoId):
  response = youtube.videos().list(
    part = "snippet,liveStreamingDetails",
    id = videoId,
    ).execute()
  time.sleep(2)
  return response

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

def creating(datasetName,tableName):
  tableId = "project2434."+datasetName+"."+tableName
  table = bigquery.Table(tableId,schema=schema)
  bClient.create_table(table)

def downloading(blobName):
  blob = bucket.blob(blobName+".json")
  string = blob.download_as_string()
  dec = string.decode("utf-8")
  content = dec.split("`")
  return content

def uploading(blobName,content):
  blob = bucket.blob(blobName+".json")
  if not type(content) == list:
    connect = str(content)
  elif type(content) == list:
    string = [str(cntnt) for cntnt in content]
    connect = "`".join(string)
  blob.upload_from_string(connect)

def hours9(content):
  head = content[:19]
  fiso = datetime.datetime.fromisoformat(head)
  nine = fiso + datetime.timedelta(hours = 9)
  iso = nine.isoformat()
  return iso


def hello_pubsub(event, context):
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  #message = pubsub_message.split("`")
  arrivals = []
  continuations = []
  for chid in channelIds:
    target_url = "https://www.youtube.com/feeds/videos.xml?channel_id="+chid
    req = requests.get(target_url)
    soup = BeautifulSoup(req.text,"xml")
    time.sleep(2)

    elems = soup.find_all("yt:videoId")
    ytvideoIds = [elem.contents[0] for elem in elems]
    elems = soup.find_all("published")
    published = elems[1].contents[0]
    head = published[:19]
    published = datetime.datetime.fromisoformat(head)

    now = datetime.datetime.now()
    last = now - datetime.timedelta(days = 1)
    if last <= published:
      feeds = downloading("feeds/"+chid)
      news = [ytid for ytid in ytvideoIds if not ytid in feeds]
    else:
      news = []

    infos = []
    apis = []
    plus = []
    for new in news:
      resource = ytVideos(new)
      if not "liveStreamingDetails" in resource["items"][0]:
        plus.append(new)
        publishedAt = hours9(resource["items"][0]["snippet"]["publishedAt"])
        infos.append({"videoId":new,"start":publishedAt,"end":None})
      elif "liveStreamingDetails" in resource["items"][0]:
        if "actualEndTime" in resource["items"][0]["liveStreamingDetails"]:
          apis.append(new)
          actualStartTime = hours9(resource["items"][0]["liveStreamingDetails"]["actualStartTime"])
          actualEndTime = hours9(resource["items"][0]["liveStreamingDetails"]["actualEndTime"])
          infos.append({"videoId":new,"start":actualStartTime,"end":actualEndTime})

    nobar = []
    for api in apis:
      target_url = "https://www.youtube.com/watch?v="+api
      html = session.get(target_url, headers=headers)
      soup = BeautifulSoup(html.text,"html.parser")
      time.sleep(2)
    
      ytInitialData = None
      for script in soup.find_all('script'):
        script_text = str(script)
        if 'var ytInitialData =' in script_text:
          if script_text[59] == "{" and script_text[-11] == "}":
            ytInitialData = json.loads(script_text[59:-10])

      if not ytInitialData:
        loading("error","arrival",[{"videoId":api,"ytInitialData":str(soup.find_all())}],None)
      elif ytInitialData:
        if not "conversationBar" in ytInitialData["contents"]["twoColumnWatchNextResults"]:
          nobar.append(api)    
        elif "conversationBar" in ytInitialData["contents"]["twoColumnWatchNextResults"]:
          plus.append(api)
          if "liveChatRenderer" in ytInitialData["contents"]["twoColumnWatchNextResults"]["conversationBar"]:
            continuations.append(ytInitialData["contents"]["twoColumnWatchNextResults"]["conversationBar"]["liveChatRenderer"]["header"]["liveChatHeaderRenderer"]["viewSelector"]["sortFilterSubMenuRenderer"]["subMenuItems"][0]["continuation"]["reloadContinuationData"]["continuation"])
            arrivals.append(api)
            creating("chat",api)

    if plus:
      keep = 15 - len(plus)
      mix = plus + feeds[:keep]
      uploading("feeds/"+chid,mix)

    videoIds = [info for info in infos if not info["videoId"] in nobar]
    if videoIds:
      loading("videoId",chid,videoIds,job_config)


  if arrivals:
    uploading("pubsub",arrivals+continuations)
    publishing("chat",arrivals+continuations,"0")

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

#各クライアントライブラリの準備
youtube = build("youtube","v3",developerKey = "DEVELOPERKEY")
pClient = pubsub_v1.PublisherClient()
bClient = bigquery.Client()
sClient = storage.Client()
bucket = sClient.bucket("PROJECT")
#webページ取得の準備
session = requests.Session()
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
#BigQueryのカラムを準備
schema = [bigquery.SchemaField("authorId","STRING"),bigquery.SchemaField("timestampUsec","DATETIME")]
job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("videoId","STRING"),bigquery.SchemaField("start","DATETIME"),bigquery.SchemaField("end","DATETIME")])
#取得するchannelIdのリストを他ディレクトリから引っ張ってくる
with open("/workspace/channelIds.json") as f:
  channelIds = json.load(f)


#YouTubeAPIで動画情報を取得する関数
def ytVideos(videoId):
  response = youtube.videos().list(
    part = "snippet,liveStreamingDetails",
    id = videoId,
    ).execute()
  time.sleep(2)
  return response

#pubsubにメッセージを送る関数
def publishing(topic,content,kaunt):
  topic_path = pClient.topic_path("PROJECT",topic)
  if not type(content) == list:
    data = str(content)
  elif type(content) == list:
    string = [str(cntnt) for cntnt in content]
    data = "`".join(string)
  enc = data.encode("utf-8")
  pClient.publish(topic_path,enc,count=kaunt)

#BigQueryに格納する関数
def loading(datasetName,tableName,content,jobConfig):
  if content:
    tableId = "PROJECT."+datasetName+"."+tableName
    bClient.load_table_from_json(content,tableId,job_config=jobConfig)

#BigQueryに新しくテーブルを作る関数
def creating(datasetName,tableName):
  tableId = "PROJECT."+datasetName+"."+tableName
  table = bigquery.Table(tableId,schema=schema)
  bClient.create_table(table)

#storageからダウンロードする関数
def downloading(blobName):
  blob = bucket.blob(blobName+".json")
  string = blob.download_as_string()
  dec = string.decode("utf-8")
  content = dec.split("`")
  return content

#storageへアップロードする関数
def uploading(blobName,content):
  blob = bucket.blob(blobName+".json")
  if not type(content) == list:
    connect = str(content)
  elif type(content) == list:
    string = [str(cntnt) for cntnt in content]
    connect = "`".join(string)
  blob.upload_from_string(connect)

#日本時刻になおす関数
def hours9(content):
  head = content[:19]
  fiso = datetime.datetime.fromisoformat(head)
  nine = fiso + datetime.timedelta(hours = 9)
  iso = nine.isoformat()
  return iso


#本文
def hello_pubsub(event, context):
  arrivals = []#新着動画のIDを格納するリスト
  continuations = []#チャットを読み込むためのキーを格納するリスト
  for chid in channelIds:
    target_url = "https://www.youtube.com/feeds/videos.xml?channel_id="+chid#YouTubeチャンネルの数値情報が簡潔にまとめられているサイト
    req = requests.get(target_url)#アクセス
    soup = BeautifulSoup(req.text,"xml")#解析
    time.sleep(2)#マナー

    #ページには直近15件の動画情報がまとめられており、それらのvideoIdを抽出する
    elems = soup.find_all("yt:videoId")
    ytvideoIds = [elem.contents[0] for elem in elems]
    #直近の動画がいつアップロードされたか調べ、それを日本時間になおす
    elems = soup.find_all("published")
    published = elems[1].contents[0]
    head = published[:19]
    published = datetime.datetime.fromisoformat(head)

    #直近動画がアップロードされてから1日以上経過しているか調べる
    now = datetime.datetime.now()
    last = now - datetime.timedelta(days = 1)
    if last <= published:
      #経過していれば、過去に取得したvideoIdリストと見比べ、新着のものだけ抽出する
      feeds = downloading("feeds/"+chid)
      news = [ytid for ytid in ytvideoIds if not ytid in feeds]
    else:
      #経過していなければ、次の工程には流さない
      news = []

    infos = []#動画のvideoId、開始時間、終了時間
    apis = []#APIで調べ、通過させるものを格納するリスト
    plus = []#次回から扱わないように、今回アーカイブに追加するものを格納するリスト
    for new in news:
      resource = ytVideos(new)#videoIdからAPIを叩き、動画情報を引っ張ってくる
      if not "liveStreamingDetails" in resource["items"][0]:#もし配信ではなく動画であれば、
        plus.append(new)#アーカイブに追加する
        publishedAt = hours9(resource["items"][0]["snippet"]["publishedAt"])#動画の投稿時間
        infos.append({"videoId":new,"start":publishedAt,"end":None})#を格納する
      elif "liveStreamingDetails" in resource["items"][0]:#もし配信であれば
        if "actualEndTime" in resource["items"][0]["liveStreamingDetails"]:#配信が終了していれば
          apis.append(new)#次の工程に流す
          actualStartTime = hours9(resource["items"][0]["liveStreamingDetails"]["actualStartTime"])#配信開始時間
          actualEndTime = hours9(resource["items"][0]["liveStreamingDetails"]["actualEndTime"])#配信終了時間
          infos.append({"videoId":new,"start":actualStartTime,"end":actualEndTime})#を格納する

    nobar = []#配信アーカイブは視聴できるものの、チャット機能が追いついていないものを格納するリスト
    for api in apis:
      target_url = "https://www.youtube.com/watch?v="+api#YouTubeの動画ページ
      html = session.get(target_url, headers=headers)#アクセス
      soup = BeautifulSoup(html.text,"html.parser")#解析
      time.sleep(2)#マナー
    
      #webページからチャット取得に必要なキーのありかを探す
      ytInitialData = None
      for script in soup.find_all('script'):
        script_text = str(script)
        if 'var ytInitialData =' in script_text:
          if script_text[59] == "{" and script_text[-11] == "}":
            ytInitialData = json.loads(script_text[59:-10])

      if not ytInitialData:#もしキーのありかがなければ
        loading("error","arrival",[{"videoId":api,"ytInitialData":str(soup.find_all())}],None)#ログ的な役割でBigQueryにエラーデータを送る
      elif ytInitialData:#もしキーのありかがあれば
        if not "conversationBar" in ytInitialData["contents"]["twoColumnWatchNextResults"]:#もしチャット欄がなければ、→配信直後でチャット欄が追いついてない場合
          nobar.append(api)#格納
        elif "conversationBar" in ytInitialData["contents"]["twoColumnWatchNextResults"]:#もしチャット欄があれば
          plus.append(api)#アーカイブに追加する
          if "liveChatRenderer" in ytInitialData["contents"]["twoColumnWatchNextResults"]["conversationBar"]:#もしチャット機能が使用可能であれば
            continuations.append(ytInitialData["contents"]["twoColumnWatchNextResults"]["conversationBar"]["liveChatRenderer"]["header"]["liveChatHeaderRenderer"]["viewSelector"]["sortFilterSubMenuRenderer"]["subMenuItems"][0]["continuation"]["reloadContinuationData"]["continuation"])#キーをリストに追加
            arrivals.append(api)#新着動画のvideoIdを追加
            creating("chat",api)#BigQueryに新着動画のテーブルを作成

    if plus:#もしアーカイブに追加するものがあれば、合計15件になるように新着を追加してstorageに上書き保存する
      keep = 15 - len(plus)
      mix = plus + feeds[:keep]
      uploading("feeds/"+chid,mix)

    videoIds = [info for info in infos if not info["videoId"] in nobar]#配信直後でチャット機能が追いついていない動画を除いたリストを作成
    if videoIds:
      loading("videoId",chid,videoIds,job_config)#BigQueryに動画情報のレコードを格納


  if arrivals:#もし新着動画があれば、
    uploading("pubsub",arrivals+continuations)#保険として新着動画とキーを合わせたものをストレージに送っておく
    publishing("chat",arrivals+continuations,"0")#次の工程へメッセージ(新着動画とキーを合わせたもの)を飛ばす。0は何回目かわかるように

import base64
import json
from google.cloud import pubsub_v1
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import datetime
import time

#各クライアントライブラリの準備
pClient = pubsub_v1.PublisherClient()
bClient = bigquery.Client()
#BigQueryのカラムを準備
job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField("authorId","STRING"),bigquery.SchemaField("timestampUsec","DATETIME")])
#webページ取得の準備
session = requests.Session()
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
#時刻系を準備
today = datetime.date.today()
iso = today.isoformat()

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

#日本時刻になおす関数
def hours9(timestampUsec):
  nine = int(timestampUsec)+32400000000
  dt = datetime.datetime.fromtimestamp(nine/1000000)
  iso = dt.isoformat()
  return iso

#リストを文字列になおす関数
def joining(num,mssg):
  jnt = "`".join([num]+mssg)
  return jnt


#本文
def hello_pubsub(event, context):
  #新着動画とキーを合わせたものを受け取り、それぞれのリストを作る。また、先頭一つ目を抽出しておく
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  message = pubsub_message.split("`")
  half = int(len(message)/2)
  videoIds = message[:half]
  videoId = videoIds[0]
  continuations = message[half:]
  continuation = continuations[0]

  #カウントを一つ足しておく
  count = int(event["attributes"]["count"])
  count+=1
  count = str(count)

  authorId = []#チャット投稿者Id
  timestampUsec = []#チャット投稿時間
  error = False
  tt = time.time()#クローリング開始前に時刻をおさえておく
  while continuation and time.time()-tt < 510:#もしキーがあり、かつ510秒以内であれば(functionsの制限時間は500秒のため)
    target_url = "https://www.youtube.com/live_chat_replay?continuation="+continuation#ライブチャットリプレイという特殊なページにキーを打ち込む
    html = session.get(target_url, headers=headers)#アクセス
    soup = BeautifulSoup(html.text,"html.parser")#解析
    time.sleep(2)#マナー

    #webページからチャット取得に必要なキーのありかを探す。また、そこにはチャット一覧も含まれている
    ytInitialData = None
    for script in soup.find_all('script'):
      script_text = str(script)
      if 'window["ytInitialData"] =' in script_text:
        if script_text[26] == "{" and script_text[-2] == "}":
          ytInitialData = json.loads(script_text[26:-1])
          loading("error","twentysix",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)
        elif script_text[65] == "{" and script_text[-11] == "}":
          ytInitialData = json.loads(script_text[65:-10])

    if not ytInitialData:#もしキーのありかがなければ
      error = True#エラーを発見
      loading("error","none",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)#ログ的な役割でBigQueryにエラーデータを送る
      continuation = None#クローリングしないようにキーを消す
      break
    elif ytInitialData:#もしキーのありかがあれば
      authorid = []#今回のアクセスで取得するチャット投稿者Id
      timestampusec = []#今回のアクセスで取得するチャット投稿時間
      #随時チャットを取得
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
        if len(authorid) == len(timestampusec):#もし投稿者Idの数と投稿時間の数が一致していれば、大元のリストに追加する
          authorId+=authorid
          timestampUsec+=timestampusec
        else:#もし投稿者Idの数と投稿時間の数が一致していなければ
          error = True#エラーを発見
          loading("error","nomatch",[{"videoId":videoId,"continuation":continuation,"ytInitialData":str(soup.find_all())}],None)#ログ的な役割でBigQueryにエラーデータを送る
          continuation = None#クローリングしないようにキーを消す
          break

      if "liveChatReplayContinuationData" in ytInitialData["continuationContents"]["liveChatContinuation"]["continuations"][0]:#もし今回でチャットを最後まで取得しきれなかった場合
        continuation = ytInitialData["continuationContents"]["liveChatContinuation"]["continuations"][0]["liveChatReplayContinuationData"]["continuation"]#次に回す用のキー
      else:
        continuation = None#クローリングしないようにキーを消す

  #クローリングが終了して、、
  if not error:#エラーが発見されなければ、
    aId_tsU = [{"authorId":authorId[n],"timestampUsec":timestampUsec[n]} for n in range(len(authorId))]#投稿者Idと投稿時間をペアにしたレコードを作成
    loading("chat",videoId,aId_tsU,job_config)#BigQueryに格納

    #一、未処理のvideoIdが残っているか。二、未取得のチャットが残っているか。で条件分岐させ、それらに応じた「再度このfunctionsに投げるメッセージ」を作成する
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
  
    if not finish:#もし、再度このfunctionを回す必要があるなら
      loading("pubsub",iso,[{"pubsub":joining(count,message)}],None)#保険としてメッセージ内容をBigQueryに格納しておく
      publishing("chat",message,count)#再度このfunctionsにメッセージをおくる

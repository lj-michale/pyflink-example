##!/bin/sh

echo $1
echo $2

HOST="https://azkaban.jpushoa.com"
PROJECT=$1
USER_NAME=$2
PASSWORD=$3
TARGET_DIR="zip/"
ZIP_NAME=$TARGET_DIR$1.zip

if [[ ! -d $TARGET_DIR ]]; then
	mkdir -p $TARGET_DIR
fi

## 已经存在则先删除
if [[ -f $ZIP_NAME ]]; then
	rm -rf $ZIP_NAME
fi

## 压缩 job
zip -r $ZIP_NAME project

echo '-------------zip over---------------'

## 申请 session_id
azkaban_resp=`curl -s -k -X POST --data "action=login&username=${USER_NAME}&password=${PASSWORD}" ${HOST}`
echo $azkaban_resp
session_id=`echo $azkaban_resp | python -c 'import json,sys; data = json.load(sys.stdin); print (data["session.id"] if "session.id" in data else "error");'`

## 是否获取 session_id 失败
if [[ "error" = "$session_id" ]]; then
	echo "登录azkaban失败"
	exit 1
fi
echo "登录azkaban成功"

## 上传 job
echo ${ZIP_NAME}
azkaban_resp=`curl -s -k -H "Content-Type: multipart/mixed" -X POST --form "session.id="${session_id} --form "ajax=upload" --form "file=@${ZIP_NAME};type=application/zip" --form "project="${PROJECT} ${HOST}/manager`
echo $azkaban_resp | python -c 'import json,sys; data = json.load(sys.stdin); print (data.get("error") if "error" in data else "upload azkaban success!");'


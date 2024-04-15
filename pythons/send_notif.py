import requests

HUYNX7 = 217856628
ZALOBOT = 210965174
ERROR_NOTIF = 417139620

errors = {}

def sendGroupMsg(userId, groupId, msgContent, mentions=""):
    try:
        url = "http://10.30.58.19:8080/iapi/groupmsg/text"
        params = {
            "user_id": userId,
            "group_id": groupId,
            "tag_uids": mentions,
            "msg": msgContent,
        }
        requests.post(url, data=params)
    except Exception as e:
        return e

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: send_notif.py <userId> <msgContent> <mentions>")
        sys.exit(1)

    userId = sys.argv[1]
    msgContent = sys.argv[2]
    mentions = sys.argv[3] if len(sys.argv) > 3 else ""
    sendGroupMsg(userId, ERROR_NOTIF, msgContent, mentions)


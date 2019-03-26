import json
import http.client
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import logging
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    token = getBearerToken()
    allyCodeList = getListOfPlayers(token)
    notificationsList = updatePlayerCharacters(token, allyCodeList)
    sendUpdates(notificationsList)
    return {
        'statusCode': 200,
        'body': json.dumps('Scan Complete!')
    }

def getBearerToken():
    url = 'https://api.swgoh.help/auth/signin'

    postfields = {"username": os.environ['apiusername'],"password":  os.environ['apipassword'],"grant_type": "password","client_id": "abc","client_secret": "123"}

    request = Request(url, data=urlencode(postfields).encode())
    response = urlopen(request).read().decode()

    responseAsJson = json.loads(response)
    
    return responseAsJson['access_token']

def getListOfPlayers(token):
    url = 'https://api.swgoh.help/swgoh/guilds'

    postfields = {"allycodes": os.environ['query_allycode']}

    request = Request(url, data=urlencode(postfields).encode(), headers={"Authorization": "Bearer %s" %token})
    response = urlopen(request).read().decode()

    responseAsJson = json.loads(response)
    
    allyCodeList = []
    
    for guild in responseAsJson:
        roster = guild['roster']
        for player in roster:
            allyCodeList.append(player['allyCode'])
    
    return allyCodeList
    
def updatePlayerCharacters(token, allyCodeList):
    notifications = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['character_table'])
    brokenUpList = chunks(allyCodeList, 10)
    for sublist in brokenUpList:
        getRostersFromAllyCodeList(token, sublist, notifications, table)

    return notifications
    
def getRostersFromAllyCodeList(token, allyCodeList, notifications, table):
    url = 'https://api.swgoh.help/swgoh/roster'

    project = {"name": 1, "allyCode":1, "starLevel": 1, "gearLevel": 1, "zetas": 1, "player": 1}

    data = {}
    data['allycodes'] = allyCodeList
    data['project'] = project
    json_data = json.dumps(data).encode('utf8')
    
    request = Request(url, data=json_data, headers={"Authorization": "Bearer %s" %token, "Content-Type": "application/json"})
    response = urlopen(request).read().decode()

    responseAsJson = json.loads(response)

    for player in responseAsJson:
        dbDictionary = {}
        for character, stats in player.items():
            dbDictionary = loadDBItemsForAllyCode(table, str(stats[0]['allyCode']))
            break
        for character, stats in player.items():
            processPlayerCharacter(character, stats, table, dbDictionary, notifications)

def loadDBItemsForAllyCode(table, allyCode):
    dbItems = table.query(
        KeyConditionExpression=Key('allycode').eq(allyCode)
    )
    itemDict = {}
    for item in dbItems['Items']:
        itemDict[item['character']] = item
        
    return itemDict
        
def processPlayerCharacter(character, stats, table, dbDictionary, notifications):
    allyCode = str(stats[0]["allyCode"])
    gearLevel = (stats[0]["gearLevel"])
    starLevel = int(stats[0]["starLevel"])
    zetaCount = len(stats[0]["zetas"])
    playerName = str(stats[0]["player"])
    
    logger.info("Process " + playerName + "," + character)

    if (character in dbDictionary):
        #logger.info("Exists. Check stats")
        compareExistingChar(dbDictionary[character], playerName, allyCode, gearLevel, starLevel, zetaCount, table, notifications)
    else:
        #logger.info("New")
        writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount)
        notifications.append(playerName + " has unlocked " + getNiceCharacterName(character) + " at " + str(starLevel) + "* with Gear Level " + str(gearLevel) + " and " + str(zetaCount) + " zetas")
        
def compareExistingChar(dbItem, playerName, allyCode, gearLevel, starLevel, zetaCount, table, notifications):
    somethingHasChanged = False
    character = dbItem["character"]
    
    if (gearLevel != dbItem['gearLevel']):
        somethingHasChanged = True
        notifications.append(playerName + " has upgraded " + getNiceCharacterName(character) + " to Gear Level " + str(gearLevel))
    if (starLevel != dbItem['starLevel']):
        somethingHasChanged = True
        notifications.append(playerName + " has upgraded " + getNiceCharacterName(character) + " to " + str(starLevel) + " stars")
    if (zetaCount != dbItem['zetaCount']): 
        somethingHasChanged = True
        notifications.append(playerName + " has added a zeta to " + getNiceCharacterName(character))
    if (somethingHasChanged):
        writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount)

def writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount):
    table.put_item(
        Item={
            'allycode': allyCode,
            'character' : character,
            'starLevel' : starLevel,
            'gearLevel': gearLevel,
            'zetaCount': zetaCount 
        }
    )
    
def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i+size]

def sendUpdates(notificationsList):
    brokenUpList = chunks(notificationsList, 50)

    for sublist in brokenUpList:
        delimiter = "\n"
        sendToDiscord(delimiter.join(sublist))

def sendToDiscord(message):
    webhookUrl = os.environ['discordhook']
    formdata = "------:::BOUNDARY:::\r\nContent-Disposition: form-data; name=\"content\"\r\n\r\n" + message + "\r\n------:::BOUNDARY:::--"
  
    # get the connection and make the request
    connection = http.client.HTTPSConnection("discordapp.com")
    connection.request("POST", webhookUrl, formdata, {
        'content-type': "multipart/form-data; boundary=----:::BOUNDARY:::",
        'cache-control': "no-cache",
        })
  
    # get the response
    response = connection.getresponse()
    result = response.read()
  
    # return back to the calling function with the result
    return result.decode("utf-8")
    
def getNiceCharacterName(character):
    return str(character)

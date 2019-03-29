import json
import http.client
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import logging
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    token = getBearerToken()
    notificationsListByGuild = processGuilds(token)
    sendUpdates(notificationsListByGuild)
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

def processGuilds(token):
    allyCodeSeeds = []
    allyCodeSeeds.append("code1")
    allyCodeSeeds.append("code2")
    
    notificationsListByGuild = {}
    for allyCodeSeed in allyCodeSeeds:
        processGuild(token, allyCodeSeed, notificationsListByGuild)
        
    return notificationsListByGuild
        
def processGuild(token, allyCodeSeed, notificationsListByGuild):
    guildJson = getGuildFromAPI(token, allyCodeSeed)
    guildName = "NoGuild"
    for guild in guildJson:
        guildName = guild['name']
        break
     
    logger.info("Process guild " + guildName)   
    allyCodeList = getListOfPlayers(guildJson)
    notificationsListByGuild[guildName] = updatePlayerCharacters(token, allyCodeList)
    return

def getGuildFromAPI(token, allyCode):
    url = 'https://api.swgoh.help/swgoh/guilds'

    postfields = {"allycodes": allyCode}

    request = Request(url, data=urlencode(postfields).encode(), headers={"Authorization": "Bearer %s" %token})
    response = urlopen(request).read().decode()

    responseAsJson = json.loads(response)
    return responseAsJson

def getListOfPlayers(guildJson):
   
    allyCodeList = []
    
    for guild in guildJson:
        roster = guild['roster']
        for player in roster:
            allyCodeList.append(player['allyCode'])
    
    return allyCodeList
    
def updatePlayerCharacters(token, allyCodeList):
    notifications = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['character_table'])
    brokenUpList = chunks(allyCodeList, 1)
    for sublist in brokenUpList:
        getRostersFromAllyCodeList(token, sublist, notifications, table)

    logger.info("Notifications count " + str(len(notifications)))
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
        newSignup = False
        
        for character, stats in player.items():
            logger.info("Process Player " + str(stats[0]["player"]))
            dbDictionary = loadDBItemsForAllyCode(table, str(stats[0]['allyCode']))
            if not dbDictionary:
                logger.info("dictionary is empty")
                newSignup = True;
                notifications.append(str(stats[0]["player"]) + " has joined the guild")
            break
        for character, stats in player.items():
            if (newSignup):
                logger.info("New Player")
            processPlayerCharacter(character, stats, table, dbDictionary, newSignup, notifications)

def loadDBItemsForAllyCode(table, allyCode):
    logger.info("Check DB for records for " + str(allyCode))
    dbItems = table.query(
        KeyConditionExpression=Key('allycode').eq(allyCode)
    )
    itemDict = {}
    for item in dbItems['Items']:
        itemDict[item['character']] = item
        
    return itemDict
        

def processPlayerCharacter(character, stats, table, dbDictionary, newSignup, notifications):
    allyCode = str(stats[0]["allyCode"])
    gearLevel = (stats[0]["gearLevel"])
    starLevel = int(stats[0]["starLevel"])
    zetaCount = len(stats[0]["zetas"])
    playerName = str(stats[0]["player"])
    
    #logger.info("Process " + playerName + "," + allyCode + "," + character)

    if (character in dbDictionary):
        compareExistingChar(dbDictionary[character], playerName, allyCode, gearLevel, starLevel, zetaCount, table, notifications)
    else:
        logger.info(character + " is new for " + playerName)
        writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount)
        if not newSignup:
            notifications.append(playerName + " has unlocked " + getNiceCharacterName(character) + " at " + str(starLevel) + "* with Gear Level " + str(gearLevel) + " and " + str(zetaCount) + " zetas")
        
def compareExistingChar(dbItem, playerName, allyCode, gearLevel, starLevel, zetaCount, table, notifications):
    somethingHasChanged = False
    character = dbItem["character"]
    
    if (gearLevel != dbItem['gearLevel']):
        somethingHasChanged = True
        if (gearLevel > 10):
            notifications.append(playerName + " has upgraded " + getNiceCharacterName(character) + " to Gear Level " + str(gearLevel))
    if (starLevel != dbItem['starLevel']):
        somethingHasChanged = True
        if (starLevel > 6):
            notifications.append(playerName + " has upgraded " + getNiceCharacterName(character) + " to " + str(starLevel) + " stars")
    if (zetaCount != dbItem['zetaCount']): 
        somethingHasChanged = True
        notifications.append(playerName + " has added a zeta to " + getNiceCharacterName(character))
    if (somethingHasChanged):
        logger.info(character + " has changed for " + playerName)
        writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount)

def writeCharToDB(table, allyCode, character, starLevel, gearLevel, zetaCount):
    logger.info("Write " + allyCode + " " + character + " " + str(starLevel) + "*/GL" + str(gearLevel) + "/" + str(zetaCount))
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

def sendUpdates(notificationsListByGuild):
    for guildName in notificationsListByGuild:
        notificationsListForGuild = notificationsListByGuild[guildName]
        if notificationsListByGuild[guildName]:
            notificationsListForGuild.insert(0, "Updates for " + guildName)
        
        brokenUpList = chunks(notificationsListForGuild, 20)

        for sublist in brokenUpList:
            logger.info("Send sublist of " + str(len(sublist)) + " items")
            delimiter = "\n"
            sendToDiscord(delimiter.join(sublist))
            time.sleep(2)

def sendToDiscord(message):
    webhookUrl = os.environ['discordhook']
    formdata = "------:::BOUNDARY:::\r\nContent-Disposition: form-data; name=\"content\"\r\n\r\n" + message + "\r\n------:::BOUNDARY:::--"
  
    logger.info("Broadcast to Discord" + formdata)
  
    # get the connection and make the request
    connection = http.client.HTTPSConnection("discordapp.com")
    connection.request("POST", webhookUrl, formdata, {
        'content-type': "multipart/form-data; boundary=----:::BOUNDARY:::",
        'cache-control': "no-cache",
        })
  
    # get the response
    response = connection.getresponse()
    result = response.read()
    logger.info(result)
  
    # return back to the calling function with the result
    return result.decode("utf-8")
    
def getNiceCharacterName(character):
    if character in niceNames:
        return niceNames[character]
        
    return str(character)
    
niceNames = {}
niceNames['ADMIRALACKBAR'] = 'Admiral Ackbar'
niceNames['AAYLASECURA'] = 'Aayla Secura'
niceNames['ADMINISTRATORLANDO'] = 'Lando Calrissian'
niceNames['AHSOKATANO'] = 'Ahsoka Tano'
niceNames['AMILYNHOLDO'] = 'Amilyn Holdo'
niceNames['ARC170CLONESERGEANT'] = 'Clone Sergeant\'s ARC-170'
niceNames['ARC170REX'] = 'Rex\'s ARC-170'
niceNames['ASAJVENTRESS'] = 'Assaj Ventress'
niceNames['AURRA_SING'] = 'Aurra Sing'
niceNames['B1BATTLEDROIDV2'] = 'B1 Battledroid'
niceNames['B2SUPERBATTLEDROID'] = 'B2 Super Battledroid'
niceNames['BARRISSOFFEE'] = 'Barriss Offee'
niceNames['BASTILASHAN'] = 'Bastila Shan'
niceNames['BASTILASHANDARK'] = 'Bastila Shan (Fallen)'
niceNames['BB8'] = 'BB-8'
niceNames['BIGGSDARKLIGHTER'] = 'Biggs Darklighter'
niceNames['BISTAN'] = 'Bistan'
niceNames['BLADEOFDORIN'] = 'Plo Koon\'s Starfighter'
niceNames['BOBAFETT'] = 'Boba Fett'
niceNames['BODHIROOK'] = 'Bodhi Rook'
niceNames['BOSSK'] = 'Bossk'
niceNames['C3POLEGENDARY'] = 'C-3PO'
niceNames['CADBANE'] = 'Cad Bane'
niceNames['CANDEROUSORDO'] = 'Canderous Ordo'
niceNames['CAPITALCHIMAERA'] = 'Chimaera'
niceNames['CAPITALJEDICRUISER'] = 'Endurance'
niceNames['CAPITALMONCALAMARICRUISER'] = 'Home One'
niceNames['CAPITALSTARDESTROYER'] = 'Executrix'
niceNames['CARTHONASI'] = 'Carth Onasi'
niceNames['CASSIANANDOR'] = 'Cassian Andor'
niceNames['CC2224'] = 'Cody'
niceNames['CHEWBACCALEGENDARY'] = 'Chewbacca'
niceNames['CHIEFCHIRPA'] = 'Chief Chirpa'
niceNames['CHIRRUTIMWE'] = 'Chirrut Imwe'
niceNames['CHOPPERS3'] = 'Chopper'
niceNames['CLONESERGEANTPHASEI'] = 'Clone Sergeant'
niceNames['CLONEWARSCHEWBACCA'] = 'Clone Wars Chewbacca'
niceNames['COLONELSTARCK'] = 'Colonel Starck'
niceNames['COMMANDERLUKESKYWALKER'] = 'Commander Luke Skywalker'
niceNames['COMMANDSHUTTLE'] = 'Kylo Ren\'s Command Shuttle'
niceNames['CORUSCANTUNDERWORLDPOLICE'] = 'Coruscant Underworld Police'
niceNames['COUNTDOOKU'] = 'Count Dooku'
niceNames['CT210408'] = 'Echo'
niceNames['CT5555'] = 'Fives'
niceNames['CT7567'] = 'Rex'
niceNames['DAKA'] = 'Old Daka'
niceNames['DARTHNIHILUS'] = 'Darth Nihilus'
niceNames['DARTHSIDIOUS'] = 'Darth Sidious'
niceNames['DARTHSION'] = 'Darth Sion'
niceNames['DATHCHA'] = 'Datcha'
niceNames['DEATHTROOPER'] = 'Deathtrooper'
niceNames['DENGAR'] = 'Dengar'
niceNames['DIRECTORKRENNIC'] = 'Director Krennic'
niceNames['DROIDEKA'] = 'Droideka'
niceNames['EBONHAWK'] = 'Ebon Hawk'
niceNames['EETHKOTH'] = 'Eeth Koth'
niceNames['EMBO'] = 'Embo'
niceNames['EMPERORPALPATINE'] = 'Emperor Palpatine'
niceNames['EMPERORSSHUTTLE'] = 'Emperor Palpatine\'s Shuttle'
niceNames['ENFYSNEST'] = 'Enfys Nest'
niceNames['EWOKELDER'] = 'Ewok Elder'
niceNames['EWOKSCOUT'] = 'Ewok Scout'
niceNames['EZRABRIDGERS3'] = 'Ezra Bridger'
niceNames['FINN'] = 'Finn'
niceNames['FIRSTORDEREXECUTIONER'] = 'First Order Executioner'
niceNames['FIRSTORDEROFFICERMALE'] = 'First Order Officer'
niceNames['FIRSTORDERSPECIALFORCESPILOT'] = 'First Order Special Forces Tie Pilot'
niceNames['FIRSTORDERTIEPILOT'] = 'First Order Tie Pilot'
niceNames['FIRSTORDERTROOPER'] = 'First Order Stormtrooper'
niceNames['FULCRUMAHSOKA'] = 'Ahsoka Tano (Fulcrum)'
niceNames['GARSAXON'] = 'Gar Saxon'
niceNames['GAUNTLETSTARFIGHTER'] = 'Gauntle Starfighter'
niceNames['GENERALKENOBI'] = 'General Kenobi'
niceNames['GEONOSIANSOLDIER'] = 'Geonosian Soldier'
niceNames['GEONOSIANSPY'] = 'Geonosion Spy'
niceNames['GEONOSIANSTARFIGHTER1'] = 'Geonosian Starfighter?'
niceNames['GEONOSIANSTARFIGHTER2'] = 'Geonosian Starfighter?'
niceNames['GEONOSIANSTARFIGHTER3'] = 'Geonosian Starfighter?'
niceNames['GHOST'] = 'Ghosst'
niceNames['GRANDADMIRALTHRAWN'] = 'Grand Admiral Thrawn'
niceNames['GRANDMASTERYODA'] = 'Grand Master Yoda'
niceNames['GRANDMOFFTARKIN'] = 'Grand Moff Tarkin'
niceNames['GREEDO'] = 'Greedo'
niceNames['GRIEVOUS'] = 'General Grievous'
niceNames['HANSOLO'] = 'Han Solo'
niceNames['HERASYNDULLAS3'] = 'Hera Syndulla'
niceNames['HERMITYODA'] = 'Hermit Yoda'
niceNames['HK47'] = 'HK-47'
niceNames['HOTHLEIA'] = 'Rebel Officer Leia Organa'
niceNames['HOTHREBELSCOUT'] = 'Hoth Rebel Scout'
niceNames['HOTHREBELSOLDIER'] = 'Hoth Rebel Soldier'
niceNames['HOUNDSTOOTH'] = 'Hound\'s Tooth'
niceNames['HUMANTHUG'] = 'Mob Enforce'
niceNames['IG2000'] = 'IG-2000'
niceNames['IG86SENTINELDROID'] = 'IG-86 Sentinel Droid'
niceNames['IG88'] = 'IG-88'
niceNames['IMAGUNDI'] = 'Ima Gun-Di'
niceNames['IMPERIALPROBEDROID'] = 'Imperial Probe Droid'
niceNames['IMPERIALSUPERCOMMANDO'] = 'Imperial Supercommando'
niceNames['JANGOFETT'] = 'Jango Fett'
niceNames['JAWA'] = 'Jawa'
niceNames['JAWASCAVENGER'] = 'Jawa Scavanger'
niceNames['JEDIKNIGHTCONSULAR'] = 'Jedi Consular'
niceNames['JEDIKNIGHTGUARDIAN'] = 'Jedi Guardian'
niceNames['JEDIKNIGHTREVAN'] = 'Jedi Knight Revan'
niceNames['JEDISTARFIGHTERAHSOKATANO'] = 'Ahsoka Tano\'s Starfighter'
niceNames['JEDISTARFIGHTERCONSULAR'] = 'Jedi Consular\'s Starfighter'
niceNames['JOLEEBINDO'] = 'Jolee Bindo'
niceNames['JUHANI'] = 'Juhani'
niceNames['JYNERSO'] = 'Jyn Erso'
niceNames['K2SO'] = 'K-2SO'
niceNames['KANANJARRUSS3'] = 'Kanan Jarrus'
niceNames['KYLOREN'] = 'Kylo Ren'
niceNames['KYLORENUNMASKED'] = 'Kylo Ren (Unmasked)'
niceNames['L3_37'] = 'L3-37'
niceNames['LOBOT'] = 'Lobot'
niceNames['LOGRAY'] = 'Logray'
niceNames['LUKESKYWALKER'] = 'Luke Skywalker (Farmboy)'
niceNames['LUMINARAUNDULI'] = 'Luminara Unduli'
niceNames['MACEWINDU'] = 'Mace Windu'
niceNames['MAGMATROOPER'] = 'Magmatrooper'
niceNames['MAGNAGUARD'] = 'Magnaguard'
niceNames['MAUL'] = 'Darth Maul'
niceNames['MILLENNIUMFALCON'] = 'Han\'s Millenium Falcon'
niceNames['MILLENNIUMFALCONEP7'] = 'Rey\'s Millenium Falcon'
niceNames['MILLENNIUMFALCONPRISTINE'] = 'Lando\'s Millenium Falcon'
niceNames['MISSIONVAO'] = 'Mission Vao'
niceNames['MOTHERTALZIN'] = 'Mother Talzin'
niceNames['NIGHTSISTERACOLYTE'] = 'Nightsister Acolyte'
niceNames['NIGHTSISTERINITIATE'] = 'Nightsister Initiate'
niceNames['NIGHTSISTERSPIRIT'] = 'Nightsister Spirit'
niceNames['NIGHTSISTERZOMBIE'] = 'Nightsister Zombie'
niceNames['NUTEGUNRAY'] = 'Nute Gunray'
niceNames['OLDBENKENOBI'] = 'Old Ben Kenobi'
niceNames['PAO'] = 'Pao'
niceNames['PAPLOO'] = 'Paploo'
niceNames['PHANTOM2'] = 'Phantom 2'
niceNames['PHASMA'] = 'Captain Phasma'
niceNames['PLOKOON'] = 'Plo Koon'
niceNames['POE'] = 'Poe Dameron'
niceNames['POGGLETHELESSER'] = 'Poggle the Lesser'
niceNames['PRINCESSLEIA'] = 'Prince Leia'
niceNames['QIRA'] = 'Qira'
niceNames['QUIGONJINN'] = 'Qui Gon Jinn'
niceNames['R2D2_LEGENDARY'] = 'R2D2'
niceNames['RANGETROOPER'] = 'Rangetrooper'
niceNames['RESISTANCEPILOT'] = 'Resistance Pilot'
niceNames['RESISTANCETROOPER'] = 'Resitance Trooper'
niceNames['REY'] = 'Rey (Scavenger)'
niceNames['REYJEDITRAINING'] = 'Rey (Jedi Training)'
niceNames['ROSETICO'] = 'Rose Tico'
niceNames['ROYALGUARD'] = 'Royal Guard'
niceNames['SABINEWRENS3'] = 'Sabine Wren'
niceNames['SAVAGEOPRESS'] = 'Savage Opress'
niceNames['SCARIFREBEL'] = 'Scarif Rebel Pathfinder'
niceNames['SHORETROOPER'] = 'Shoretrooper'
niceNames['SITHASSASSIN'] = 'Sith Assassin'
niceNames['SITHBOMBER'] = 'Sith Bomber'
niceNames['SITHFIGHTER'] = 'Sith Fighter'
niceNames['SITHINFILTRATOR'] = 'Scimitar'
niceNames['SITHMARAUDER'] = 'Sith Marauder'
niceNames['SITHTROOPER'] = 'Sith Trooper'
niceNames['SLAVE1'] = 'Slave 1'
niceNames['SMUGGLERCHEWBACCA'] = 'Veteran Smuggler Chewbacca'
niceNames['SMUGGLERHAN'] = 'Veteran Smuggler Han'
niceNames['SNOWTROOPER'] = 'Snowtrooper'
niceNames['STORMTROOPER'] = 'Stormtrooper'
niceNames['STORMTROOPERHAN'] = 'Stormtrooper Han'
niceNames['SUNFAC'] = 'Sun Fac'
niceNames['T3_M4'] = 'T3-M4'
niceNames['TALIA'] = 'Talia'
niceNames['TEEBO'] = 'Teebo'
niceNames['TIEADVANCED'] = 'Vader\'s Tie Advanced'
niceNames['TIEFIGHTERFIRSTORDER'] = 'First Order Tie Fighter'
niceNames['TIEFIGHTERFOSF'] = 'First Order Special Forces Tie Fighter'
niceNames['TIEFIGHTERIMPERIAL'] = 'Tie Fighter'
niceNames['TIEFIGHTERPILOT'] = 'Tie Fighter Pilot'
niceNames['TIEREAPER'] = 'Tie Reaper'
niceNames['TIESILENCER'] = 'Tie Silencer'
niceNames['TUSKENRAIDER'] = 'Tusken Raider'
niceNames['TUSKENSHAMAN'] = 'Tusken Shaman'
niceNames['UGNAUGHT'] = 'Ugnaught'
niceNames['UMBARANSTARFIGHTER'] = 'Umbaran Starfighter'
niceNames['URORRURRR'] = 'Urorrurrr'
niceNames['UWINGROGUEONE'] = 'Cassian\'s U-wing'
niceNames['UWINGSCARIF'] = 'Bistan\'s U-wing'
niceNames['VADER'] = 'Darth Vader'
niceNames['VEERS'] = 'General Veers'
niceNames['VISASMARR'] = 'Visas marr'
niceNames['WAMPA'] = 'Wampa'
niceNames['WEDGEANTILLES'] = 'Wedge Antilles'
niceNames['WICKET'] = 'Wicket'
niceNames['XANADUBLOOD'] = 'Xanadu Blood'
niceNames['XWINGBLACKONE'] = 'Poe Dameron\s X-wing'
niceNames['XWINGRED2'] = 'Wedge Antilles\' X-wing'
niceNames['XWINGRED3'] = 'Biggs Darklighter\'s X-wing'
niceNames['XWINGRESISTANCE'] = 'Resistance X-wing'
niceNames['YOUNGCHEWBACCA'] = 'Vandor Chewbacca'
niceNames['YOUNGHAN'] = 'Young Han Solo'
niceNames['YOUNGLANDO'] = 'Young Lando Calrissien'
niceNames['ZAALBAR'] = 'Zaalbar'
niceNames['ZAMWESELL'] = 'Zam Wesell'
niceNames['ZEBS3'] = 'Zeb'

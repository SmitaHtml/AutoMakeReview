import re
import requests
from sys import getsizeof
from concurrent.futures import ThreadPoolExecutor

geocoder_api = "http://moria.middleearth.eltoro.com:8080/geocoder/graphql"

# any special characters
notAlphanums = re.compile(r'[^A-Za-z0-9@\- ]')

# any non-nums
notNums = re.compile(r'[^0-9]')

mapped_values = {
    ".": "",
    "'": "",
    "street": "st",
    "str": "st",
    "road": "rd",
    "court": "ct",
    "lane": "ln",
    "avenue": "ave",
    "highway": "hwy",
    "circle": "cir",
    "place": "pl",
    "terrace": "ter",
    "trail": "trl",
    "trailer": "trlr",
    "square": "sq",
    "parkway": "pwky",
    "trace": "trce",
    "cove": "cv",
    "grove": "grv",
    "point": "pt",
    "springs": "spgs",
    "spring": "spg",
    "heights": "hts",
    "ridge": "rdg",
    "corner": "cor",
    "expressway": "expy",
    "alley": "aly",
    "annex": "anx",
    "apartment": "apt",
    "arcade": "arc",
    "basement": "bsmt",
    "bayou": "byu",
    "beach": "bch",
    "bend": "bnd",
    "bluff": "blf",
    "bottom": "btm",
    "boulevard": "blvd",
    "branch": "br",
    "bridge": "brg",
    "brook": "brk",
    "building": "bldg",
    "burg": "bg",
    "bypass": "byp",
    "camp": "cp",
    "canyon": "cyn",
    "cape": "cpe",
    "causeway": "cswy",
    "center": "ctr",
    "cliff": "clfs",
    "cliffs": "clfs",
    "club": "clb",
    "crossing": "xing",
    "creek": "crk",
    "dale": "dl",
    "dam": "dm",
    "department": "dept",
    "divide": "dv",
    "drive": "dr",
    "estate": "est",
    "extension": "ext",
    "falls": "fls",
    "ferry": "fry",
    "field": "fld",
    "fields": "flds",
    "flat": "flt",
    "floor": "fl",
    "ford": "frd",
    "forest": "frst",
    "forge": "frg",
    "fork": "frk",
    "fort": "ft",
    "forks": "frks",
    "freeway": "fwy",
    "front": "frnt",
    "garden": "gdn",
    "gardens": "gdns",
    "gateway": "gtwy",
    "glen": "gln",
    "green": "grn",
    "hanger": "hngr",
    "harbor": "hbr",
    "haven": "hvn",
    "hill": "hl",
    "hills": "hls",
    "hollow": "holw",
    "inlet": "inlt",
    "island": "is",
    "junction": "jct",
    "key": "ky",
    "knoll": "knl",
    "knolls": "knls",
    "lake": "lk",
    "landing": "lndg",
    "lower": "lowr",
    "manor": "mnr",
    "meadow": "mdw",
    "meadows": "mdws",
    "mill": "ml",
    "mills": "mls",
    "mission": "msn",
    "mount": "mt",
    "mountain": "mtn",
    "penthouse": "ph",
    "plain": "pln",
    "plaza": "plz",
    "port": "prt",
    "ranch": "rnch",
    "river": "riv",
    "shoal": "shl",
    "shore": "shr",
    "space": "spc",
    "station": "sta",
    "suite": "ste",
    "summit": "smt",
    "turnpike": "tpke",
    "union": "un",
    "valley": "vly",
    "village": "vlg",
    "vista": "vis",
    "north": "n",
    "northeast": "ne",
    "northwest": "nw",
    "south": "s",
    "southeast": "se",
    "southwest": "sw",
    "east": "e",
    "west": "w",
}


# takes in address and cleans then returns cleaned address
def cleanAddress(address):
    # remove all non-alphanumerics to match append file
    stripped_adr = re.sub(notAlphanums, '', address)

    # make a list of the words used in the address, substituting
    # any abbreviations found in the mappedValues
    adrList = stripped_adr.lower().split()
    newAdr = [mapped_values.get(aw, aw) for aw in adrList]

    # join the list back together with spaces so it's readable again
    return ' '.join(newAdr)


# takes in line and columnDict returns string of address + zip
# adds i to line index to handle any columns that may be added after events json is made
# as an example, for the append file, the true/false column is added after events is made
def joinClean(address, zipCode):
   if len(zipCode) > 2:
       zipCode = re.sub(notNums, '', zipCode)
       joined_address = ','.join([cleanAddress(address), str(zipCode)])
       return joined_address


def makeChunks(adrList):
    chunkSize = 2000
    chunkList = []
    chunk = []
    for adr in adrList:
        chunk.append(adr)
        if getsizeof(chunk) > chunkSize:
            chunkList.append(str(chunk).replace("'", '"'))
            chunk = []
    chunkList.append(str(chunk).replace("'", '"'))
    return chunkList


def review(adrList):
    def batchGetEtHash(adrList):
        response = geocodeAddress(adrList)
        data = response.get("data")
        targetList = data.get("geocodeAddress")
        etHash = [t.get("etHash") if t is not None else "Hasherr" for t in targetList]
        return etHash

    # review geometry, handling geocoder requests concurrently
    resultList = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        chunkList = makeChunks(adrList)
        validBoolList = [executor.submit(batchGetEtHash, chunk) for chunk in chunkList]
        for threadResult in validBoolList:
            resultList.extend(threadResult.result())
    adrDict = dict(zip(adrList, resultList))
    return adrDict


def geocodeAddress(adrListString):
    # status = "not sent"
    query = (
        "{ geocodeAddress(address: "
        + adrListString
        + ', bestOnly: true, refId: "analyticsml:scraping") { etHash }}'
    )
    while True:
        response = requests.post(geocoder_api, json={"query": query})
        #print("EtHash request status code:", response.status_code)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 400:
            print("Something's screwing up geocoder")
            print("----------------------------------------------------------")
            print(response.content)
            print("----------------------------------------------------------")


def reverseGeocode(lat, lng):
    query = (
        "{ reverseGeocode(latitude: "
        + str(lat)
        + ", longitude: "
        + str(lng)
        + ', bestOnly:true, refId: "analyticsmlscrapers") {addressLine city state zipcode}}'
    )
    response = requests.post(geocoder_api, json={"query": query})
    if response.status_code == 200:
        return response.json()
    else:
        print(response.content)

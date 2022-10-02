import csv
import sys
from pathlib import Path
from xml.etree import ElementTree
import logging as lg
from zipfile import ZipFile


parent = str(Path(__file__).parent)

global downloaded
downloaded = False

import requests as rq

xml = ElementTree.parse(parent + "/task-git.xml")

for x in xml.findall("result"):
    docs = x.findall("doc")
    if (item := docs[0] if docs else None) is not None:
        for y in [
            x
            for x in item.findall("str")
            if (f := x.attrib.get("name")) is not None and f == "download_link"
        ]:
            if (f := y.text) is not None:
                get_header = rq.get(f)
                with open(parent + "/dfile.zip", "wb") as zfile:
                    if get_header.status_code == 200:
                        zfile.write(get_header.content)
                        downloaded = True


if not downloaded:
    sys.exit(1)

with ZipFile(parent + "/dfile.zip", "r") as zobj:
    zobj.extractall()

dltns = ElementTree.parse("DLTINS_20210117_01of01.xml")

root = dltns.getroot()

Figat = root.findall(".//{*}FinInstrmGnlAttrbts")
issr = dltns.findall(".//{*}Issr")

csv_rows = []
csv_columns = [
    "FinInstrmGnlAttrbts.Id",
    "FinInstrmGnlAttrbts.FullNm",
    "FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.NtnlCcy",
    "Issr"
]

for idy, figa in enumerate(Figat):
    tags = [
        tags.text
        for y in ["Id", "FullNm", "ClssfctnTp", "CmmtyDerivInd", "NtnlCcy"]
        if (tags := figa.find("./{*}" + y)) is not None
    ]
    tags.append(issr[idy].text)
    if len(tags) < 5:
        sys.exit(1)
    csv_rows.append(tags)



with open("DLTNS_20210117_01of01.csv", "w") as resde:
    writer = csv.writer(resde)
    writer.writerow(csv_columns)
    writer.writerows(csv_rows)


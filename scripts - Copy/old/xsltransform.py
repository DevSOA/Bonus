from lxml import etree
import os


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logs_path = os.path.join(ROOT_DIR, "Logs")
os.makedirs(logs_path, exist_ok=True)
data_path = os.path.join(ROOT_DIR, "Data")
os.makedirs(data_path, exist_ok=True)
config_path = os.path.join(ROOT_DIR, "Data")
os.makedirs(config_path, exist_ok=True)


def transform():
    xmlPath=f"{data_path}\\2022_bonusfile.xml"
    xslPath = f"{data_path}\\nt1.xsl"
    xslRoot = etree.fromstring(open(xslPath).read())
    transform = etree.XSLT(xslRoot)
    xmldata=open(xmlPath,"rb").read()
    #print(xmldata)
    xmlRoot = etree.fromstring(xmldata)
    transRoot = transform(xmlRoot)
    print("Almost there")
    with open(f"{data_path}\\newxml.xml","wb") as newxml:
        newxml.write(etree.tostring(transRoot))
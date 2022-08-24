import pip
import os

def install(package):
    #if hasattr(pip, 'main'):
    #    pip.main(['install', package,"--upgrade"])
    #else:
    #    pip._internal.main(['install', package,"--upgrade"])
    if pkgutil.find_loader(package):
        print(f"{package} found")
        pass
    else:
        print(f"Installing {package}")
        os.system(f"python -m pip install {package} --upgrade --user")

def call_installer():
    requirements =["lxml","xml","xmltodic","io","pdfdocument","dateutil","pip","os","boto3","sys","json","requests","base64","random","csv","pandas","logzero","openpyxl","calendar","datetime","operator","time","zipfile","ctypes","math","multiprocessing","operator"]
    for i in requirements:
        install(i)
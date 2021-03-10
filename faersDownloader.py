# -*- coding: utf-8 -*-
"""
Created on 2019/04/01

@author: Jing Li, backm
"""

import os
import re
import lxml
import time
import shutil
import warnings
import requests
from tqdm import tqdm
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import repeat

# this script will find target in this list pages.
target_page = ["https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"]

# local directory to save files.
source_dir = r"./FAERSsrc/"
data_dir = r"./FAERSdata/"
DELETE_UNWANTED_FILES = True

# ignore warnings
warnings.filterwarnings('ignore')

# Set to true if want parallelization
PARALLEL = True
THREAD_COUNT = None  # Change this at will


def downloadFiles_PLL(faers_file, source_dir, data_dir, file_num, total_files):
    """
    download faers data files with parallelization.
    :param faers_files: dict faers_files = {"name":"url"}
    :param source_dir: FAERSsrc
    :param data_dir: FAERSdata
    :return: none
    """

    if os.path.isfile(source_dir + faers_file.split("/")[-1]):
        print("\nFAERS file " + faers_file.split("/")
              [-1].split(".")[0] + " exists, skipping.", flush=True)
    else:
        try:
            print("\nDownload {0}/{1}: {2} \t {3}".format(str(file_num+1), total_files,
                                                        faers_file.split("/")[-1].split(".")[0], datetime.now().strftime('%Y-%m-%d %H:%M:%S')), flush=True)
            downloader(faers_file, source_dir, data_dir)
            print("\nDownload " + faers_file + " success!\t" +
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'), flush=True)
        except Exception as e:
            print("\nDownload " + faers_file.split("/")
                  [-1].split(".")[0] + " failed! Error: " + str(e), flush=True)
# =============================================================================
#         print("Sleep 30 seconds before starting download a new file.\n", flush=True)
#         time.sleep(30)
# =============================================================================


# =============================================================================
#     print("Download " + faers_file + "\t" +
#           datetime.now().strftime('%Y-%m-%d %H:%M:%S'), flush=True)
#     r = requests.get(faers_file, timeout=200)
#     z = ZipFile(BytesIO(r.content))
#     z.extractall(source_dir)
#     r.close()
# =============================================================================


def downloadFiles(faers_files, source_dir, data_dir):
    """
    download faers data files.
    :param faers_files: dict faers_files = {"name":"url"}
    :param source_dir: FAERSsrc
    :param data_dir: FAERSdata
    :return: none
    """
    for i, file_name in enumerate(faers_files):
        if os.path.isfile(source_dir + faers_files[file_name].split("/")[-1]):
            print("FAERS file "+file_name+" exists, skipping.", flush=True)
        else:
            print("Download {0}/{1}: {2} \t {3}".format(str(i+1), str(len(faers_files)),
                                                        file_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S')), flush=True)
            try:
                downloader(faers_files[file_name], source_dir, data_dir)
            except Exception as e:
                print("Download " + file_name + " failed! Error: " + str(e), flush=True)
# =============================================================================
#             print("Sleep 30 seconds before starting download a new file.\n")
#             time.sleep(30)
# =============================================================================


def downloader(url: str, source_dir: str = r"./", data_dir: str = r"./", fname: str = "", extract=True, bar_pos=0):
    if fname == "":
        fname = url.split("/")[-1]

    resp = requests.get(url,  timeout=200, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(source_dir + fname, 'wb') as file, tqdm(
            desc=fname,
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            position=bar_pos
    ) as bar:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
    if extract:
        print("\nExtracting "+fname+"...", flush=True)
        z = ZipFile(source_dir + fname)
        z.extractall(data_dir)
        print("\nExtracted "+fname+".", flush=True)
    resp.close()


def deleteUnwantedFiles(path):
    """
    delete unwanted files.
    :param path: FAERSsrc
    :return: none
    """
    print("Delete unwanted files.\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    for parent, dirnames, filenames in os.walk(path):
        for fn in filenames:
            # FDA Adverse Event Reporting System (FAERS) began 2012Q4.
            # keep data from 2012Q4 and after.
            if fn[4:8] < "12Q4":
                print("Delete " + fn)
                os.remove(parent +"/"+ fn)
            elif fn.lower().endswith('.pdf') or fn.lower().endswith('.doc'):
                print("Delete " + fn)
                os.remove(parent +"/"+ fn)
            elif fn.upper().startswith(("RPSR", "INDI", "THER", "SIZE", "STAT", "OUTC")):
                print("Delete " + fn)
                os.remove(parent +"/"+ fn)
    print("Delete unwanted files done!\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def copyFiles(source_dir, data_dir):
    """
    Copy files from FAERSsrc to FAERSdata.
    :param source_dir: FAERSsrc
    :param data_dir: FAERSdata
    :return: none
    """
    print("Copy files from " + source_dir + " to " + data_dir + ".    ", end="")
    RootDir = os.getcwd() + '/' + source_dir
    TargetFolder = os.getcwd() + '/' + data_dir
    for root, dirs, files in os.walk((os.path.normpath(RootDir)), topdown=False):
        for name in files:
            if name.lower().endswith('.txt'):
                SourceFolder = os.path.join(root, name)
                shutil.move(SourceFolder, TargetFolder)
    print("Done! ")


def getFilesUrl():
    """
    find all web urls in target page.
    :return: dict files = {"name":"url"}
    """
    print("Get web urls.\t")
    files = {}
    for page_url in target_page:
        try:
            request = urlopen(page_url)
            page_bs = BeautifulSoup(request, "lxml")
            request.close()
        except:
            request = urlopen(page_url)
            page_bs = BeautifulSoup(request)
        for url in page_bs.find_all("a"):
            a_string = str(url)
            if "ASCII" in a_string.upper():
                t_url = url.get('href')
                files[str(url.get('href'))[-16:-4]] = t_url

    # save urls to FaersFilesWebUrls.txt
    save_path = os.getcwd() + "/FaersFilesWebUrls.txt"
    if (os.path.exists(save_path)):
        os.remove(save_path)
    with open(save_path, 'a+') as f:
        for k in files.keys():
            f.write(k + ":" + files[k] + "\n")

    print("Done!")
    return files

def main():
    # creating the source directory if not exits.
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    # get faers data file's url and download them.
    faers_files = getFilesUrl()
    if PARALLEL:
        # =============================================================================
        #         with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        #             tqdm(executor.map(downloadFiles_PLL, faers_files.values(),
        #                               repeat(source_dir), repeat(data_dir)), total=len(faers_files))
        # # =============================================================================
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            executor.map(downloadFiles_PLL, faers_files.values(),
                         repeat(source_dir), repeat(data_dir), list(range(0, len(faers_files))), repeat(len(faers_files)))
    else:
        downloadFiles(faers_files, source_dir, data_dir)

    # delete and copy files to FAERSdata.
    if DELETE_UNWANTED_FILES:
        deleteUnwantedFiles(source_dir)
    copyFiles(source_dir, data_dir)


if __name__ == '__main__':
    main()

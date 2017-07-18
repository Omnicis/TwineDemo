import zipfile

zf = zipfile.PyZipFile("relib.zip", mode="w")
zf.writepy("src/main/python/twinelib/relib")

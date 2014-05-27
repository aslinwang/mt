#!/usr/bin/python
# coding: utf-8
import hashlib
import json
"""
js增量更新算法-增量文件生成
@param {string} oldFileContent 旧文件文本内容
@param {string} newFileContent 新文件文本内容
@param {integer} chunkSize 分块大小
"""
def makeIncDataFile(oldFileContent, newFileContent, chunkSize):
    resultFile = {}
    #是否变更
    resultFile['modify'] = True
    resultFile['chunkSize'] = chunkSize
    strDataArray = []

    #计算新旧两个文件，如果相同则说明文件没有改动，则直接返回空数组
    if getMd5(oldFileContent) == getMd5(newFileContent) :
        resultFile['modify'] = False
        resultFile['data'] = strDataArray
        return resultFile

    oldCheckSums = oldFileCheckSum(oldFileContent, chunkSize)
    diffArr = searchChunk(newFileContent, oldCheckSums, chunkSize)
    arrayData = ""
    lastitem = None
    matchCount = 0
    i = 0
    size = len(diffArr)

    #生成json，同时合并连续命中的块，压缩数据
    for item in diffArr:
        if item['isMatch']:
            if lastitem is None or not ("isMatch" in lastitem) or lastitem["isMatch"] == False:#todo maybe wrong
                arrayData = "[" + str(item['data']) + ","
                matchCount = 1
            elif "isMatch" in lastitem and lastitem["isMatch"] and (lastitem['data'] + 1 == item['data']):
                matchCount+=1
            elif "isMatch" in lastitem and lastitem["isMatch"] and (lastitem['data'] + 1 != item['data']):
                arrayData += str(matchCount) + "]"
                strDataArray.append(json.loads(arrayData))
                arrayData = "[" + str(item['data']) + ","
                matchCount = 1
            if i == size - 1:
                arrayData += str(matchCount) + "]"
                strDataArray.append(json.loads(arrayData))
                arrayData = ""
        else:
            if matchCount > 0:
                arrayData += str(matchCount) + "]"
                strDataArray.append(json.loads(arrayData))
                arrayData = ""
                matchCount = 0
            strDataArray.append(item['data'])
        lastitem = item
        i+=1
    resultFile['data'] = strDataArray
    resultFile = json.dumps(resultFile)
    return resultFile

"""
生成老文件的md5，块号信息
"""
def oldFileCheckSum(fileContent, chunkSize):
    txt = fileContent
    checkSums = {}
    curIdx = 0
    length = len(txt)
    chunkNo = 0
    while curIdx < length:
        chunk = txt[curIdx:curIdx+chunkSize]
        chunkMd5 = getMd5(chunk)
        if chunkMd5 in checkSums : #用dict解决冲突
            numArr = checkSums[chunkMd5]
        else :
            numArr = []
        numArr.append(chunkNo)
        checkSums[chunkMd5] = numArr
        curIdx = curIdx + chunkSize
        chunkNo+=1
    return checkSums

"""
用新文件在老文件的checksum dictionary中滚动查找，生成增量更新文件的map
"""
def searchChunk(strInput, checkSums, chunkSize):
    incDataArray = []
    outBuffer = "" 
    currentIndex = 0
    tLen = len(strInput)
    lastmatchNo = 0
    while currentIndex <= tLen:
        endIndex = currentIndex + chunkSize
        if endIndex > tLen:
            endIndex = tLen
        buffer = strInput[currentIndex:endIndex]
        chunkMd5 = getMd5(buffer)
        matchTrunkIndex = checkMatchIndex(chunkMd5, checkSums, lastmatchNo)
        if endIndex > tLen - 1:#最后一个
            if len(outBuffer) > 0 and outBuffer != "":
                doExactNewData(incDataArray, outBuffer)
                outBuffer = ""
            if len(buffer) > 0 and buffer != "":
                doExactNewData(incDataArray, buffer)
            currentIndex = currentIndex + chunkSize
        elif matchTrunkIndex >= 0:
            if len(outBuffer) > 0 and outBuffer != "":
                doExactNewData(incDataArray, outBuffer)
                outBuffer = ""
            doExactMatch(incDataArray, matchTrunkIndex)
            currentIndex = currentIndex + chunkSize
        else:
            outBuffer = outBuffer + strInput[currentIndex:currentIndex+1]
            currentIndex+=1
        if matchTrunkIndex >= 0:
            lastmatchNo = matchTrunkIndex
    return incDataArray

def checkMatchIndex(chunkMd5, checkSums, lastmatchNo):
    if chunkMd5 in checkSums:
        return getMatchNo(checkSums[chunkMd5], lastmatchNo)
    else:
        return -1

"""
如果是新数据，则将数据放入到最终队列中
"""
def doExactNewData(incDataArr, data):
    incDataArr.append({'isMatch':False, 'data':data.decode('gb2312').encode('utf-8')})

"""
如果是老数据，则将匹配块号放在最终队列中
"""
def doExactMatch(incDataArr, chunkNo):
    incDataArr.append({'isMatch':True, 'data':chunkNo})

"""
从一个匹配的块号序列里面获取离上一个匹配的块号最近的块号
有利于压缩数据
"""
def getMatchNo(numArr, lastmatchNo):
    if len(numArr) == 1:
        return numArr[0]
    else:
        lastNo = numArr[0]
        reNo = 0
        for key in numArr:
            curNo = key
            if curNo >= lastmatchNo and lastNo <= lastmatchNo:
                return ((lastmatchNo - lastNo) >= (curNo - lastmatchNo) and [curNo] or [lastNo])[0]
            elif curNo >= lastmatchNo and lastNo >= lastmatchNo:
                return lastNo
            elif curNo <= lastmatchNo and lastNo <= lastmatchNo:
                reNo = curNo
            else:
                reNo = curNo
            lastNo = curNo
        return reNo

def getMd5(c):
    s = c
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()
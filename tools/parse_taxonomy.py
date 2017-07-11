#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# to redirect to a file set PYTHONIOENCODING=UTF-8 
from lxml import html
import requests
import re

#page = requests.get('http://codelists.wpc-edi.com/mambo_taxonomy_2.asp')
#tree = html.fromstring(page.text)
page = open("raw.html").read()
tree = html.fromstring(page)

l1 = tree.xpath('//div/div/ul/li') 
l2=[i.xpath('following-sibling::*[1]/li') for i in l1]
l3=[[i.xpath('following-sibling::*[1]/li') for i in l2[j]] for j in range(len(l2))]
l4=[[[i.xpath('following-sibling::*[1]/li') for i in l3[j][k]] for k in range(len(l3[j]))] for j in range(len(l3))]

for i in range(len(l1)):
    l1t = l1[i].text
    for j in range(len(l2[i])):
        l2t = l2[i][j].text
        for k in range(len(l3[i][j])):
            l3t = re.sub(r'.-.$', '', l3[i][j][k].text)
            l3ba = l3[i][j][k].xpath('b/text()')
            l3b = l3ba[0] if len(l3ba)>0 else ""
            if len(l3ba)>0: 
                print l3b + "|||" + l3t + "|" + l2t + "|" + l1t 
            for o in l4[i][j][k]:
                l4t = re.sub(r'.-.$', '', o.text)
                l4ba = o.xpath('b/text()')
                if len(l4ba)>0:
                    print l4ba[0] + "|" + l4t + "|" + l3b + "|" + l3t + "|" + l2t + "|" + l1t 


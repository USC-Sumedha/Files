import sys
import requests
import re
import csv
import codecs
import pickle
import datetime
from bs4 import BeautifulSoup
from string import ascii_uppercase

# Global varibles

# to store category and subcategory information and link
allLinksObjList = list() 
# to store combination of category and app ID unique number
appIDSet = set()
# to store last url crawled to resume on system exit
last_crawling_url = '' 
startTime = datetime.datetime.now()

# App csv file variables
out = open("iTunesAppData.csv", 'a') 
header = ['AppID','Category','SubCategory','AppTitle','AppLink']
writer = csv.DictWriter(out, delimiter=',', lineterminator='\n', fieldnames=header)
writer.writeheader()

#used to convert the allLinksObjList from list of inner obj to list of lists
# because we cannot pickle inner objects. 
categoryList_currState = list()


class Crawler():

############################################ Method : Used to check if the time limit since exec has reached  ######################################################

    def checkTime(self,first_call):
        if (datetime.datetime.now() - startTime).total_seconds()/60 > 30:
            print('I am in over time')
            self.dumpPickle()
        elif first_call:
            print('I am in first call')
            with open('pickle_currStateFile.pickle', 'rb') as in_file:
                print('I am in file open')
                mainList = pickle.loads(in_file.read())
                if len(mainList) > 0:
                    self.loadPickle(mainList)
        else:
            pass

        
################################ Method : Load last session is program reached execution time previously  else start from beginning################################################        

    def loadPickle(self,mainList):
        global allLinksObjList
        global last_crawling_url
        global appIDSet
        if len(mainList) == 3:
            categoryLinksObject = list(mainList[0])
            last_crawling_url = str(mainList[1])
            appIDSet = set(mainList[2])
            print('load main :'+categoryLinksObject[0])
            if last_crawling_url == '' and appIDSet == '' and len(categoryLinksObject) == 0:
                print('Finished crawling or error in getting category list. System will now exit')
                sys.exit()
            elif len(categoryLinksObject) > 0:
                for obj in categoryLinksObject:
                    wr = self.innerClass(obj[0],obj[1], obj[2], Boolean.valueOf(obj[3]))
                allLinksObjList.append(wr)
            print('***appIDSet',appIDSet)
            print('crawling url',last_crawling_url)
            
        else:
            print('Pickle state load error')        


#################################### Method : Save session to pickle file and exit or continue based on user input ######################################################            

    def dumpPickle(self):
        global categoryList_currState
        global startTime
        pickleList_dump = list()
        for obj in allLinksObjList:
            categoryList_currState.append([obj.href,obj.categoryName,obj.subCategoryName,obj.visited])
        pickleList_dump = [categoryList_currState,last_crawling_url,appIDSet]

        pickle.dump(pickleList_dump,open('pickle_currStateFile.pickle','wb'))
    
        print('System exiting')
        print('press y to continue')
        crawl = input('Would you like to continue crawling')
        if crawl == 'y':
            startTime = datetime.datetime.now()
            Crawler('https://itunes.apple.com/us/genre/ios/id36?mt=8')
        else:
            sys.exit()
            
########################################################### Method : To write data to CSV File ######################################################################        

    def writeCSV(self,row):
        global writer
        writer.writerow({'AppID': row.appID, 'Category': row.catName, 'SubCategory': row.subCatName,'AppTitle': row.title,'AppLink': row.link})

########################################################## Method :  To return Beautiful Soup bject #################################################################
    def getSoup(self,url):
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text,'html.parser')
        return soup
    
########################################################## Method :  Returns matched Regex patterns #################################################################
    def patternMatcher(self,regStr):
        regex = r""+regStr+""
        flags = 0
        return re.compile(regex, flags)


########################################### Method :  Takes each main URL and recursively adds A-Z* url patterns to get Apps ##########################################        

    def urlModifier(self,eachLink):
        self.checkTime(False)
        global last_crawling_url
        letter = 'A'
        # resume from last crawled URL
        if last_crawling_url != '':
            self.getApps(last_crawling_url,eachLink)
            reg = "&letter=(.)&?"
            letter = chr(ord(self.patternMatcher(reg).findall(last_crawling_url)[0]) + 1)
                
        regex = "^https:\/\/itunes.apple\.com\/us\/genre\/.*letter=["+letter+"-Z *]$"
        try:
            if eachLink.visited is not True:
                #print('****'+eachLink.href)
                soup = self.getSoup(eachLink.href)
                for anchor in soup.findAll('a',href =self.patternMatcher(regex)):
                    #print('####'+anchor.get('href'))
                    last_crawling_url = anchor.get('href')
                    self.getApps(anchor.get('href'),eachLink)

                # remove all visited categories/subcategories
                eachLink.visited = True
                allLinksObjList.pop()
                
        except Exception as e:
            print('Some error in urlModifier '+str(e))
            

################################## Method : Crawles Apps in all pages for each category and each alphabet and write to CSV file ################################################

    def getApps(self,url,contentObj):
        self.checkTime(False)
        global appIDSet
        global last_crawling_url
        next = ""
        soup = self.getSoup(url)
        last_crawling_url = url
        appRegex = "^https:\/\/itunes.apple\.com\/us\/app\/.*"
        try:
            for app in soup.findAll('a', href =self.patternMatcher(appRegex)):
                appLink = app.get('href')
                regex = "\/id(.*?)[\?\/]"
                appID = self.patternMatcher(regex).findall(appLink)[0]
                regex = "\/app\/(.*)\/id"
                ################# create unique ID to avoid duplication on system exit #################
                tempID = appID+contentObj.categoryName+contentObj.subCategoryName
                if tempID not in appIDSet:
                    appIDSet.add(tempID)
                    try:
                        appTitle = str(app.contents[0])
                        wr1 = self.addtoTableClass(appLink,appTitle,contentObj.categoryName,contentObj.subCategoryName,appID)
                        self.writeCSV(wr1)
                    except:
                        appTitle = str(self.patternMatcher(regex).findall(appLink)[0])
                        wr1 = self.addtoTableClass(appLink,appTitle,contentObj.categoryName,contentObj.subCategoryName,appID)
                        self.writeCSV(wr1)
        except:
            print('Error in appleApps function')
            
        ############# get next URL page to crawl #######################    
        nextHTML = soup.find('a',{'class':'paginate-more'})
        if (nextHTML != None):
            next = nextHTML.get('href')
            if(next != "" or next != None):
               self.getApps(next,contentObj)


############################################## Method : sub Method of getCategories. Used to get sub-categories if exists ############################################
               
    def recursiveSubCategoryLinks(self,link,catName):
        global allLinksObjList
        if len(link.parent.contents) > 1:
            for child in link.parent.contents:
                if child.nextSibling != None:
                    for subCat in child.nextSibling.contents:
                        wr = self.innerClass(subCat.a.get('href'),catName,catName+'-'+subCat.a.contents[0],False)
                        allLinksObjList.append(wr)
        return 0


############################################## Method : gets all the cetegories and sub-categories of apps stored in iTunes ##########################################

    def getcategoryLinks(self,url):
        global allLinksObjList
        page = 1
        
        try:
            soup = self.getSoup(url)
            for link in soup.findAll('a',{'class':'top-level-genre'}):
                href = link.get('href')
                category = link.contents[0]
                if len(link.parent.contents) > 1:
                    subCat_href = self.recursiveSubCategoryLinks(link,category)
                elif len(link.parent.contents) == 1:
                    wr = self.innerClass(href,category,'',False)
                    allLinksObjList.append(wr)
        except:
            print('error while getting category list')


    ###################################### Inner class : Object to hold category/ sub category data and links visited for recursive crawling #####################

    class innerClass:
        href= ""
        categoryName = ""
        subCategoryName = ""
        visited = False
        def __init__(self,href,categoryName,subCategoryName,visited):
            self.href = href
            self.categoryName = categoryName
            self.subCategoryName = subCategoryName
            self.visited = visited

            
    ####################################### Inner class : Used to store App details to be used to write to CSV File ##################################

    class addtoTableClass:
        link = ""
        title = ""
        catName = ""
        subCatName = ""
        appID = ""
        def __init__(self,link,title,catName,subCatName,appID):
            self.link = link.strip()
            self.catName = catName.strip()
            self.subCatName = subCatName.strip()
            self.appID = appID.strip()
            self.title = title.strip()

################################ Constructor : loads last session from loadPickle or calls getCategories function and recursively Crawles them ########################           

    def __init__(self,url):
        sys.setrecursionlimit(3500)
        global out
        i = 0
        try:
            self.checkTime(True)
        except:
            self.getcategoryLinks(url)
        tempList = allLinksObjList

        try:
            if len(tempList) > 0:
                for obj in tempList:
                    i += 1
                    self.urlModifier(obj)
            print('Finished Crawling')
        except Exception as e:
            print('System error from init. This session is not saved \n',e)
            
        out.close() 


############################################################### Run the cralwer for the first time  ################################################################
        
Crawler('https://itunes.apple.com/us/genre/ios/id36?mt=8')

############################################################################    EOF    ###########################################################################

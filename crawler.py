from variables import POWERS, MODULO
import requests
from bs4 import BeautifulSoup
import re
import unicodedata
import time
from concurrent.futures import ThreadPoolExecutor
import threading

class Crawler:
    def __init__(self, sub_link, specialization, session, *args) -> None:
        root_link = "https://www.olx.ro/d/piese-auto/"
        self.reCodes = [x for x in args]
        self.regexCodes = []
        self.regexCodes.append('(bmw)|(bemve)|(bmve)|(bmv)')
        self.regexCodes.append('(vw)|(volkswagen)|(volsvagen)|(vozagan)|(volskvagen)|(volskwagen)|(volksvagen)')
        self.regexCodes.append('(audi)|(audy)')
        self.regexCodes.append('(mercedes)[ ,.\/-]*(benz)')
        self.length = len(self.reCodes)
        self.pagesFound = [0 for i in range(self.length)]
        self.brandFound = [0 for i in range(5)]
        self.new = 0
        self.old = 0
        self.pagesFound.append(0)
        self.root_link = root_link + sub_link + specialization
        self.session = requests.Session()
        self.page = 1
        self.Hashes = set()

    def getPagesFound(self) ->list:
        return self.pagesFound
    
    def getBrandFound(self) ->list:
        return self.brandFound

    def getNewOld(self) ->tuple:
        return (self.new, self.old)

    def CreateHash(self, link: str) ->int:
        hashCode = 0
        i = 0
        for chr in link:
            if chr != '/':
                hashCode += (ord(chr) * POWERS[i]) % MODULO
                hashCode = hashCode % MODULO
                i += 1
        return hashCode

    def RegexSearch(self, text_variable):
        i = 0
        pattern_found = 0
        for pattern in self.reCodes:
            result = re.search(pattern, text_variable, re.IGNORECASE)
            if result:
                self.pagesFound[i] += 1
                pattern_found = 1
            i += 1
        if pattern_found == 0:
            self.pagesFound[self.length] += 1
        i = 0
        pattern_found = 0
        for pattern in self.regexCodes:
            result = re.search(pattern, text_variable, re.IGNORECASE)
            if result:
                self.pagesFound[i] += 1
                pattern_found = 1
            i += 1
        if pattern_found == 0:
            self.brandFound[4] += 1
        result = re.search('(noua?)', text_variable, re.IGNORECASE)
        if result:
            self.new += 1
        else:
            self.old += 1

    def ProcessLink(self, advertisment):
        olxPrefix = 'https://www.olx.ro'
        title = advertisment.find('h6', class_="css-v3vynn-Text eu5v0x0").text
        anchor = advertisment.find('a', class_="css-1bbgabe", href=True)
        anchor = anchor['href']
        if anchor[0] == '/':
            anchor = olxPrefix + anchor
            hash_code = self.CreateHash(anchor)
            if hash_code in self.Hashes:
                return
            else:
                self.Hashes.add(hash_code)
            advertisment_page = self.session.get(anchor).text
            advertisment_soup = BeautifulSoup(advertisment_page, 'html.parser')
            advertisment_description = advertisment_soup.find('div', class_='css-g5mtbi-Text').text                
        else:
            hash_code = self.CreateHash(anchor)
            if hash_code in self.Hashes:
                return
            else:
                self.Hashes.add(hash_code)
            advertisment_page = self.session.get(anchor).text
            advertisment_soup = BeautifulSoup(advertisment_page, 'html.parser')
            advertisment_description = advertisment_soup.find('div', class_='offer-description__description').text
        title = unicodedata.normalize('NFD', title)
        title = title.encode('ascii', 'ignore')
        title = title.decode('utf-8')
        advertisment_description = unicodedata.normalize('NFD', advertisment_description)
        advertisment_description = advertisment_description.encode('ascii', 'ignore')
        advertisment_description = advertisment_description.decode('utf-8')
        advertisment_description = title + advertisment_description
        self.RegexSearch(advertisment_description)


    def StartCrawler(self):
        pageNumberText = '?page='
        link = self.root_link + pageNumberText + str(self.page)
        while True:
            # print(link)
            try:
                pageText = self.session.get(link).text
                soup = BeautifulSoup(pageText, 'html.parser')
                advertisments = soup.find_all("div", class_="css-19ucd76")
                with ThreadPoolExecutor(max_workers=45) as executor:
                    executor.map(self.ProcessLink, advertisments)
            except:
                break
            self.page += 1
            link = self.root_link + pageNumberText + str(self.page)




if __name__ == '__main__':
    session = requests.Session()
    Tires = Crawler("roti-jante-anvelope/", "anvelope/", session, "215[ .,\/-]*?65[ .,/\/-]*?[Rr]?[ .,\/-]*17", "235[ .,\/-]*?55[ .,/\/-]*?[Rr]?[ .,\/-]*19", "245[ .,\/-]*?45[ .,/\/-]*?[Rr]?[ .,\/-]*17")
    Wheels = Crawler("roti-jante-anvelope/", "jante-si-roti/", session, "[Rr]?[ .\/,-]*?16", "[Rr]?[ .\/,-]*?17", "[Rr]?[ .\/,-]*?18", "[Rr]?[ .\/,-]*?19", "[Rr]?[ .\/,-]*?20", "[Rr]?[ .\/,-]*?21")
    Body = Crawler("caroserie-interior/", "caroserie-oglinzi-si-faruri/", session, "(far(uri)?)?", "(aripa?i?e?)", "(bar[ai])[ ,.\/-]*?(spate)", "(bar[ai]*)[ ,.\/-]*?(fata)", "(ha[iye]*on)", "(oglin((zi)|(d)))")
    Interior_Parts = Crawler("caroserie-interior/", "interior/", session, "(nav[iy]*gat[iy]*e)", "(nuca)[ .,\/-]*(de)?[ .,\/-]*(schimbator)[ .,\/-]*(de)?[ .,\/-]*(vitez[ea])", "(hus[ae])", "(volan)", "(boxa)")
    Electronics = Crawler("mecanica-electrica/", "audio-si-electronice/", session, "(senzori?)[ ]*(parcare)", "((calculator)|(computer))[ ]*(de)?[ ]*(bord)", "(camera)[ ]*(video)?((spate)|(auto)|(360)|(marsalier)|(masalier)|(marsarier))", "(ceas(uri)?)[ ]*(de)?[ ]*(bord)")
    Brakes = Crawler("mecanica-electrica/", "frane/", session, "(\b)(((discuri) |(disc))(.*)fran[ae])(\b)", "(\b)(etrier[ie]?)(\b)", "(\b)(pompa)(.*)fran[ae](\b)", "(\b)((butuci?)(.*)fran[ae])(\b)", "(\b)placu(t)|[ae][ ]*(de)?[ ]*fran[ae](\b)")
    Engine = Crawler("mecanica-electrica/", "motor-racire-si-evacuare/", session, "(turbina)", "(\b)((injector)|(injectoare))(\b)", "(\b)volant[ae](\b)", "(\b)((ambreiaje?)|((ambreaj(e)|(uri))(\b)")
    Car = Crawler("vehicule-pentru-dezmembrare/", "", session)
    
    thread1 = threading.Thread(target=Tires.StartCrawler)
    thread2 = threading.Thread(target=Wheels.StartCrawler)
    thread3 = threading.Thread(target=Body.StartCrawler)
    thread4 = threading.Thread(target=Interior_Parts.StartCrawler)
    thread5 = threading.Thread(target=Electronics.StartCrawler)
    thread6 = threading.Thread(target=Brakes.StartCrawler)
    thread7 = threading.Thread(target=Engine.StartCrawler)
    thread8 = threading.Thread(target=Car.StartCrawler)

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()

    print("Filtre:")
    print("\tAnvelope:")
    print(f"\t\t215/65/R17 {Tires.pagesFound[0]}")
    print(f"\t\t235/65/R19 {Tires.pagesFound[1]}")
    print(f"\t\t245/45/R17 {Tires.pagesFound[2]}")
    print(f"\t\tAltele {Tires.pagesFound[3]}")
    print("\tJante:")
    print(f"\t\tR16 {Wheels.pagesFound[0]}")
    print(f"\t\tR17 {Wheels.pagesFound[1]}")
    print(f"\t\tR18 {Wheels.pagesFound[2]}")
    print(f"\t\tR19 {Wheels.pagesFound[3]}")
    print(f"\t\tR20 {Wheels.pagesFound[4]}")
    print(f"\t\tR21 {Wheels.pagesFound[5]}")
    print(f"\t\tAltele {Wheels.pagesFound[6]}")
    print("\tCaroserie:")
    print(f"\t\tFaruri {Body.pagesFound[0]}")
    print(f"\t\tAripa {Body.pagesFound[1]}")
    print(f"\t\tBara spate {Body.pagesFound[2]}")
    print(f"\t\tBara fata {Body.pagesFound[3]}")
    print(f"\t\tHaion {Body.pagesFound[4]}")
    print(f"\t\tOglinzi {Body.pagesFound[5]}")
    print(f"\t\tAltele {Body.pagesFound[6]}")
    print("\tPiese interior:")
    print(f"\t\tNavigatie {Interior_Parts.pagesFound[0]}")
    print(f"\t\tNuca schimbator de viteze {Interior_Parts.pagesFound[1]}")
    print(f"\t\tHusa auto {Interior_Parts.pagesFound[2]}")
    print(f"\t\tVolan {Interior_Parts.pagesFound[3]}")
    print(f"\t\tBoxe audio {Interior_Parts.pagesFound[4]}")
    print(f"\t\tAltele {Interior_Parts.pagesFound[5]}")
    print("\tElectronice:")
    print(f"\t\tSenzor parcare {Electronics.pagesFound[0]}")
    print(f"\t\tCalculator bord {Electronics.pagesFound[1]}")
    print(f"\t\tCamera auto {Electronics.pagesFound[2]}")
    print(f"\t\tCeasuri bord {Electronics.pagesFound[3]}")
    print(f"\t\tAltele {Electronics.pagesFound[4]}")
    print("\tMarca masina:")
    print(f"\t\tBMW {Tires.brandFound[0] + Wheels.brandFound[0] + Body.brandFound[0] + Interior_Parts.brandFound[0] + Electronics.brandFound[0] + Brakes.brandFound[0] + Engine.brandFound[0]}")
    print(f"\t\tVolkswagen {Tires.brandFound[1] + Wheels.brandFound[1] + Body.brandFound[1] + Interior_Parts.brandFound[1] + Electronics.brandFound[1] + Brakes.brandFound[1] + Engine.brandFound[1]}")
    print(f"\t\tAudi {Tires.brandFound[2] + Wheels.brandFound[2] + Body.brandFound[2] + Interior_Parts.brandFound[2] + Electronics.brandFound[2] + Brakes.brandFound[2] + Engine.brandFound[2]}")
    print(f"\t\tMercedes-Benz {Tires.brandFound[3] + Wheels.brandFound[3] + Body.brandFound[3] + Interior_Parts.brandFound[3] + Electronics.brandFound[3] + Brakes.brandFound[3] + Engine.brandFound[3]}")
    print(f"\t\tAltele {Tires.brandFound[4] + Wheels.brandFound[4] + Body.brandFound[4] + Interior_Parts.brandFound[4] + Electronics.brandFound[4] + Brakes.brandFound[4] + Engine.brandFound[4]}")
    print("\tStare piesa:")
    print(f"\t\tNoua {Tires.new + Wheels.new + Body.new + Interior_Parts.new + Electronics.new + Brakes.new + Engine.new}")
    print(f"\t\tUzate {Tires.old + Wheels.old + Body.old + Interior_Parts.old + Electronics.old + Brakes.old + Engine.old}")
    print("\tFrane")
    print(f"\t\tDiscuri {Brakes.pagesFound[0]}")
    print(f"\t\tEtriere {Brakes.pagesFound[1]}")
    print(f"\t\tPompa frana {Brakes.pagesFound[2]}")
    print(f"\t\tButuc {Brakes.pagesFound[3]}")
    print(f"\t\tPlacute frana {Brakes.pagesFound[4]}")
    print(f"\t\tAltele{Brakes.pagesFound[5]}")
    print("\tPiese motor/transmisie:")
    print(f"\t\tTurbina {Engine.pagesFound[0]}")
    print(f"\t\tInjectoare {Engine.pagesFound[1]}")
    print(f"\t\tVolanta {Engine.pagesFound[2]}")
    print(f"\t\tAmbreiaj {Engine.pagesFound[3]}")
    print(f"\t\tAltele {Engine.pagesFound[4]}")
    print("\tMarca masina pentru dezmembrat:")
    print(f"\t\tBMW {Car.brandFound[0]}")
    print(f"\t\tVolkswagen {Car.brandFound[1]}")
    print(f"\t\tAudi {Car.brandFound[2]}")
    print(f"\t\tMercedes-Benz {Car.brandFound[3]}")
    print(f"\t\tAltele {Car.brandFound[4]}")

    

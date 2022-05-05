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
        self.root_link = root_link + sub_link + specialization # https://www.olx.ro/d/piese-auto/   +   roti-jante-anvelope   +   anvelope/
        self.session = session
        self.page = 1
        self.Hashes = set()

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
        print(self.reCodes)
        while True:
            print(link)
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
        print(self.pagesFound)
        print(self.brandFound)
        print(self.new, end=' ')
        print(self.old)





if __name__ == '__main__':
    session = requests.Session()
    Tires = Crawler("roti-jante-anvelope/", "anvelope/", session, "215[ .,\/-]*?65[ .,/\/-]*?[Rr]?[ .,\/-]*17", "235[ .,\/-]*?55[ .,/\/-]*?[Rr]?[ .,\/-]*19", "245[ .,\/-]*?45[ .,/\/-]*?[Rr]?[ .,\/-]*17")
    Wheels = Crawler("roti-jante-anvelope/", "jante-si-roti/", session, "[Rr]?[ .\/,-]*?16", "[Rr]?[ .\/,-]*?17", "[Rr]?[ .\/,-]*?18", "[Rr]?[ .\/,-]*?19", "[Rr]?[ .\/,-]*?20", "[Rr]?[ .\/,-]*?21")
    Body = Crawler("caroserie-interior/", "caroserie-oglinzi-si-faruri/", session, "(far(uri)?)?", "(aripa?i?e?)", "(bar[ai])[ ,.\/-]*?(spate)", "(bar[ai]*)[ ,.\/-]*?(fata)", "(ha[iye]*on)", "(oglin((zi)|(d)))")
    Interior_Parts = Crawler("caroserie-interior/", "interior/", session, "(nav[iy]*gat[iy]*e)", "(nuca)[ .,\/-]*(de)?[ .,\/-]*(schimbator)[ .,\/-]*(de)?[ .,\/-]*(vitez[ea])", "(hus[ae])", "(volan)", "(box[aă])")
    Electronics = Crawler("mecanica-electrica/", "audio-si-electronice/", session, "(senzori?)[ ]*(parcare)", "((calculator)|(computer))[ ]*(de)?[ ]*(bord)", "(camer[aă])[ ]*(video)?((spate)|(auto)|(360)|(marsalier)|(masalier)|(marsarier))", "(ceas(uri)?)[ ]*(de)?[ ]*(bord)")
    Brakes = Crawler("mecanica-electrica/", "frane/", session, "(\b)(((discuri) |(disc))(.*)fr[âa]n[aăe])(\b)", "(\b)(etrier[ie]?)(\b)", "(\b)(pompa)(.*)fran[ae](\b)", "(\b)((butuc[i]?)(.*)fran[ae])(\b)", "(\b)placu[t][ae][ ]*(de)?[ ]*fr[a]n[ae](\b)")
    Engine = Crawler("mecanica-electrica/", "motor-racire-si-evacuare/", session, "(turbin[aă])", "(\b)((injector)|(injectoare))(\b)", "(\b)volant[ae](\b)", "(\b)((ambreiaje?)|((ambreaj(e)|(uri))(\b)")
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



    # text = 'mareă'
    # text = unicodedata.normalize('NFD', text)
    # text = text.encode('ascii', 'ignore')
    # text = text.decode("utf-8")
    # print(text)


    # patternn = '215[ .,\/-]*?65[ .,/\/-]*?[Rr]?[ .,\/-]*17'
    # test_string = 'pula mea 215 65 R17 roata'
    # result = re.search(patternn, test_string)
    # print(result)
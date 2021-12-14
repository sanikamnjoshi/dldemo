# internal imports
from core4.queue.job import CoreJob
from core4.queue.helper.functool import enqueue
#from driverlicense.jobs.process import ProcessFiles

# external imports
import requests
from bs4 import BeautifulSoup
import re
from gridfs import GridFS

# global definitions
url = "https://www.agof.de/service-downloads/downloadcenter/download-daily-digital-facts/"

class ScrapeFacts(CoreJob):
    author = "eha"
    #schedule = "*/30 * * * * "

    def execute(self, test=False, *args, **kwargs):
        """
        :param test: control, if test don't write data to mongoDB
        :param args:
        :param kwargs:
        :return:
        """
        self.target = self.config.dldemo.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        self.download(test)

    def download(self, test):
        """
        This function retrieves the urls from agof website,
        and saves the urls and corresponding files in the
        database
        :param test:
        :return:
        """
        # get agof website's content
        rv = requests.get(url)
        body = rv.content.decode("utf-8")
        
        # scrape the fetched content
        soup = BeautifulSoup(body, "html.parser")
        tables_list = soup.find_all("tr")
        
        # extract desired links from the scraped content
        links = [item for item in tables_list if "Angebote Ranking" in item.text]
        links_list = [item for item in links if "xlsx" in item.text]

        xls = []
        for i in links_list:
            xls.append(re.findall("href=[\"\'](.+?)[\"\']", str(i))[0])
    
        self.logger.info("found [%d] xlsx files", len(xls))

        download = 0
        for link in xls:
            # check if file already exists in the database
            doc = self.gfs.find_one({"filename": link})
            
            # if not, save the file to mongoDB
            if doc is None:
                self.logger.info("download [%s]", link)
                rv = requests.get(link)
                if not test:
                    self.gfs.put(rv.content, filename=link)
            download += 1
            self.progress(download / len(xls))
        self.logger.info("successfully retrieved [%d] of [%d] files",
                         download, len(xls))

if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ScrapeFacts, test=False)

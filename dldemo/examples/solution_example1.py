# internal imports
from core4.queue.job import CoreJob
from core4.queue.helper.functool import enqueue
from gridfs import GridFS
# external imports
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd

class Process_Data(CoreJob):
    author = "pst, fmoh"

    # schedule = "*/30 * * * * "

    def execute(self, test=False, *args, **kwargs):
        """
        :param test: flag; if test don't write data to mongoDB
        :return:
        """
        # retrieve xls from mongoDB
        self.target = self.config.dldemo.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        xls = self.gfs.list()
        self.process(test, xls[0])
        self.postprocess(test)

    def process(self, test, df):
        """
        This function creates a dataframe out of the downloaded data
        and saves it in a final format
        :param test: test: flag; if test don't write data to mongoDB
        """
        assert df.iloc[0, 0] == "Analyse"
        analyse = df.iloc[0, 1]
        assert df.iloc[1, 0] == "Grundgesamtheit"
        grundgesamtheit = df.iloc[1, 1]
        assert df.iloc[2, 0] == "Zeitraum"
        zeitraum = df.iloc[2, 1]
        assert df.iloc[3, 0] == "Vorfilter"
        vorfilter = df.iloc[3, 1]
        vorfilter_fallzahl = df.iloc[4, 1]
        assert df.iloc[5, 0] == "Zielgruppe"
        zielgruppe = df.iloc[5, 1]
        zielgruppe_fallzahl = df.iloc[6, 1]
        ln = 7
        while df.iloc[ln, 0] != "Basis":
            ln += 1
            if ln > 1000:
                raise RuntimeError("failed to identify start of data")
        d = df.iloc[ln:].copy()
        cols = list(df.iloc[ln - 1])
        cols[0] = "Titel"
        d.columns = ["" if pd.isnull(c)
                     else c.replace("\n", " ").replace(".", "") for c in cols]
        if "" in d.columns:
            d.drop([""], axis=1, inplace=True)
        d["Analyse"] = analyse
        d["Grundgesamtheit"] = grundgesamtheit
        d["Zeitraum"] = zeitraum
        d["Vorfilter"] = vorfilter
        d["Zielgruppe"] = zielgruppe
        return (d)



    def postprocess(self, test):
        """
        This function creates a dataframe out of the downloaded data
        and saves it in a final format
        :param test: test: flag; if test don't write data to mongoDB
        """
        # definieren client,database, access to database
        # client = pymongo.MongoClient
        # outside core4 job
        #db = client['']
        #fs = GridFS(db)
        #xls = fs.list()
        # inside core4 job
        # self.target = self.config.dldemo.collection.data
        # self.gfs = GridFS(self.target.connection[self.target.database])
        xls = self.gfs.list()

        # create a list to store the data generated after processing excel files
        fin_df = list()

        # process several excels from 'xls' using the 'process()' function we defined above
        # we process range of len(xls)
        # we start 'i' from 1 as we have already added processed data from xls[0] to the list 'fin_df'
        for i in range(0, len(xls)):
            # get the last version of the file form the database
            fh = self.gfs.get_last_version(xls)
            body = BytesIO(fh.read())
            body.seek(0)
            # create a dataframe from the retrieved data
            df = pd.read_excel(body, header=None)
            df_processed = self.process(test, df)
            fin_df.append(df_processed)

        # saving 'fin_df' as a dataframe
        fin_df = pd.concat(fin_df)

        # save into MongoDB again??

if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(Process_Data, test=False)
# internal imports
from core4.queue.job import CoreJob

# external imports
import pandas as pd
from gridfs import GridFS
from io import BytesIO
import pymongo


class ProcessFacts(CoreJob):
    author = "osc"
    max_parallel = 10

    def execute(self, test=False, *args, **kwargs):
        """
        :param test: flag; if test don't write data to mongoDB
        :return:
        """

        # get data
        self.target = self.config.dldemo.collection.data
        self.data_processed = self.config.dldemo.collection.data_processed
        self.gfs = GridFS(self.target.connection[self.target.database])
        file_list = self.gfs.list()

        # process data and save to mongodb
        for file in file_list:
            print(file)

            df = self.process(test, file)
            self.analyse(df)
            dic = df.to_dict('records')
            # client = pymongo.MongoClient('localhost', 27017, username="core", password="654321")
            # coll = client['dldemo']['data_processed']
            if test == True:
                pass
            else:
                self.target.insert_many(dic)
        # self.target = self.config.dldemo
        # coll = self.target['data_processed']
        # coll.insert_many(dic)


    def process(self, test, file):
        """
        add description here
        :param test: test: flag; if test don't write data to mongoDB
        """

        # get the last version of the file from the database
        f = self.gfs.get_last_version(file)
        body = BytesIO(f.read())
        body.seek(0)

        # create a dataframe from the retrieved data
        df = pd.read_excel(body, header=None)

        # identify where the data starts
        ln = 7
        while df.iloc[ln, 0] != "Basis":
            ln += 1
            if ln > 1000:
                raise RuntimeError("failed to identify start of data")

        # save metadata in separate variables
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

        # save the subset of 'df' containing the main data in a separate dataframe
        df2 = df.iloc[ln:].copy()

        # extract column names from 'df' and save them as a list 'cols'
        cols = list(df.iloc[ln - 1])
        cols[0] = "Titel"

        # name the columns of df using the list 'cols'
        df2.columns = ["" if pd.isnull(c)
                          else c.replace("\n", " ").replace(".", "") for c in cols]
        if "" in df2.columns:
            df2.drop([""], axis=1, inplace=True)

        # add columns containing the metadata
        df2["Analyse"] = analyse
        df2["Grundgesamtheit"] = grundgesamtheit
        df2["Zeitraum"] = zeitraum
        df2["Vorfilter"] = vorfilter
        df2["Zielgruppe"] = zielgruppe
        if test == True:
            return df2
        else:
            dd = df2.to_dict("records")

        # # return processed data
        # df3 = list()
        # df3.append(df2)
        # df3 = pd.concat(df3)
        return df2

    def analyse(self, df):
        pass


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ProcessFacts, test=False)
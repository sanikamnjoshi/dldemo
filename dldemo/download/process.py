from core4.queue.job import CoreJob
from gridfs import GridFS
import pandas as pd
from io import BytesIO
import concurrent.futures
from core4.queue.helper.functool import enqueue
import datetime


class ProcessFiles(CoreJob):
    author = "eha"
    max_parallel = 10

    def execute(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """
        # define database
        self.target = self.config.dldemo.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        file_list = self.gfs.list()
        self.process(file_list[0])



    def process(self, filename):
        """
        This function processes the excel files and
        saves them to the database
        :param filename:
        :return:
        """

        # get the last version of the file form the database
        fh = self.gfs.get_last_version(filename)
        body = BytesIO(fh.read())
        body.seek(0)
        # create a dataframe from the retrieved data
        df = pd.read_excel(body, header=None)
        # do some processing
        print(df.shape)



if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ProcessFiles)
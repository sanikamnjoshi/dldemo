from core4.queue.job import CoreJob
from gridfs import GridFS
import pandas as pd
from io import BytesIO
import concurrent.futures
from core4.queue.helper.functool import enqueue
import datetime

MONAT = {
    "Januar": "01",
    "Februar": "02",
    "März": "03",
    "April": "04",
    "Mai": "05",
    "Juni": "06",
    "Juli": "07",
    "August": "08",
    "September": "09",
    "Oktober": "10",
    "November": "11",
    "Dezember": "12"
}

class ProcessFiles(CoreJob):
    author = "mra"
    max_parallel = 10

    def execute(self, *args, **kwargs):
        """

        :param test: control, if test don't write data to mongoDB
        :param threaded: boolean, use multiple threads or not
        :param concurrent: boolean, launch job multiple times in parallel
        :param scope: files to be processed
        :param chunk_size: size of files chunk to be passed to each job
        if concurrent is true
        :param args:
        :param kwargs:
        :return:
        """
        # define database
        self.target = self.config.dldemo.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        # list of exsisting files in the database
        files = self.gfs.list()
        self.extract(files)


    def extract(self, files):
        """
        This function calls the process function as single
        or multi-threaded
        :param files:
        :param threaded: boolean,use multiple threads or not
        :return:
        """
        for f in files:
            self.process(f)
        n = self.target.count_documents({})
        self.logger.info("[%d] records with [%d] files", n, len(files))

    def process(self, filename):
        """
        This function processes the excel files and
        saves them to the database
        :param filename:
        :return:
        """
        basename = filename.split("/")[-1]
        # set the job source to the filename
        self.set_source(basename)
        self.logger.info("extract [%s]", basename)
        # get the last version of the file form the database
        fh = self.gfs.get_last_version(filename)
        body = BytesIO(fh.read())
        body.seek(0)
        # create a dataframe from the retrieved data
        df = pd.read_excel(body, header=None)

        # process the first lines of the dataframe to create
        # the extra variables
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
                     else c.replace("\n", " ").replace(".", "").replace("\s+"," ") for c in cols]
        if "" in d.columns:
            d.drop([""], axis=1, inplace=True)
        #  add the extra variables to the dataframe
        d["Analyse"] = analyse
        d["Grundgesamtheit"] = grundgesamtheit
        d["Zeitraum"] = zeitraum
        d["Vorfilter"] = vorfilter
        d["Zielgruppe"] = zielgruppe

        monat = d.Zeitraum.apply(
            lambda s: s.replace("Letzter Monat (", "").replace(")",
                                                               "").split())
        d["Monat"] = [
            datetime.datetime.strptime("01." + MONAT[m[0]] + "." + m[1],
                                       "%d.%m.%Y") for m in monat]
        d["val"] = d["Kontakte Mio"].apply(pd.to_numeric,
                                                     errors='coerce')
        d['Date'] = d.Monat.apply(lambda x: x.date().isoformat())

        doc = d.to_dict("records")
        n = 0
        # delete any previous version of the file in the database
        deleted = self.target.delete_many({"_src": basename})
        if deleted.deleted_count > 0:
            self.logger.info("reset [%d] records for [%s]", deleted.deleted_count,
                             basename)
        # insert the processed file in the database
        # each row of the dataframe is inserted as a record
        for rec in doc:
            nrec = {}
            for k, v in rec.items():
                if not pd.isnull(v):
                    nrec[k] = v
            self.target.insert_one(nrec)
            n += 1
        self.logger.info("inserted [%d] records for [%s]", n, basename)


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ProcessFiles)

import json
import pandas as pd
import numpy as np
import datetime

from core4.api.v1.application import CoreApiContainer
from core4.api.v1.request.main import CoreRequestHandler

from dldemo.examples.functions_example import compute_sum

# Handler

class SumHandler(CoreRequestHandler):
    '''
    Sum Demo
    '''
    author = "mmo"
    title = "Sum Demo"

    def get(self):

        a = self.get_argument('a', as_type=int)
        b = self.get_argument('b', as_type=int)
        res = compute_sum(a, b)

        self.reply(res)


class TableHandler(CoreRequestHandler):
    """
    Table Demo
    """
    author = "mmo"
    title = "Agof Table Example"

    async def get(self):

        # Access to the processed data
        collection = self.config.dldemo.collection.data # self.target
        cursor = collection.find()
        data = await cursor.to_list(length = 10) # length = None - all documents in the database
        # data = self.config.dldemo.collection.data.find().to_list(length = 10) # single line version

        df = pd.DataFrame(data)
        df = df[['Titel', 'Zeitraum', 'Kontakte Mio']]

        self.reply(df)


# Container

class MyServer(CoreApiContainer):
    root = "/dldemo/examples"
    rules = [
        ("/sum", SumHandler),
        ("/table", TableHandler),
    ]


# Start the container

if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve

    serve(MyServer)
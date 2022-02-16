import json
import pandas as pd
import numpy as np
import datetime

from core4.api.v1.application import CoreApiContainer
from core4.api.v1.request.main import CoreRequestHandler

from bokeh.plotting import figure
from bokeh.embed import json_item, components
from bokeh.resources import CDN
from bokeh.models import ColumnDataSource

from dldemo.examples.functions_example import compute_sum

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

class SumHandler(CoreRequestHandler):
    '''
    Sum Demo
    '''

    author = "mmo"
    title = "Sum Example"

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
        # data = await self.config.dldemo.collection.data.find().to_list(length = 10) # single line version

        # Data to pandas df
        df = pd.DataFrame(data)

        self.reply(df)


class TableFilterHandler(CoreRequestHandler):
    """
    Table Filter Demo
    """
    author = "mmo"
    title = "Agof Table with filter Example"

    async def get(self):

        # Filter
        query = self.get_argument("query", as_type=dict, default={})

        # Access to the processed data
        data = await self.config.dldemo.collection.data.find(query).to_list(length = 1000)

        # Data to pandas df
        df = pd.DataFrame(data)

        del df['_id']
        del df['_job_id']

        await self.render("template/xls-d.html", df=df.to_html(), query=json.dumps(query))


class ChartHandler(CoreRequestHandler):
    """
    Chart Demo
    """
    author = "mmo"
    title = "Agof Chart Example"

    async def get(self):

        if self.wants_json():

            # Access to the processed data
            data = await self.config.dldemo.collection.data.find().to_list(length = None)

            # Data to pandas df and preparation of the data
            df = pd.DataFrame(data)

            monat = df.Zeitraum.apply(lambda s: s.replace("Letzter Monat (", "").replace(")", "").split())
            df["Monat"] = [datetime.datetime.strptime("01." + MONAT[m[0]] + "." + m[1], "%d.%m.%Y") for m in monat]
            df["val"] = df["Kontakte Mio"].apply(pd.to_numeric, errors='coerce')
            df['Date'] = df.Monat.apply(lambda x: x.date().isoformat())

            df = df.replace(np.nan, 0)
            g = df.groupby(["Date"]).val.sum()  # aggregation by date

            df_agg = pd.DataFrame(g)
            df_agg = df_agg.reset_index()
            df_agg['Date'] = pd.to_datetime(df_agg['Date'])
            df_agg = df_agg[df_agg['Date'] >= '2020']

            ## Bokeh plot
            source = ColumnDataSource(df_agg)

            # generating the plot
            p = figure(title="Contacts", x_axis_type="datetime",
                       plot_height=600, plot_width=800)
            p.line(x = 'Date', y = 'val',
                   line_width=2,
                   source = source)
            self.reply(json_item(p, "myplot"))
        else:
            await self.render("template/base3.html", rsc=CDN.render(), mytitle='Demo Chart')


class MyServer(CoreApiContainer):
    root = "/dldemo/examples"
    rules = [
        ("/sum", SumHandler),
        ("/table", TableHandler),
        ("/table_filter", TableFilterHandler),
        ("/chart", ChartHandler),
    ]

if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve

    serve(MyServer)
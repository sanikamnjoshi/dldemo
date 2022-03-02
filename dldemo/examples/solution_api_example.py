import json
import pandas as pd
import numpy as np
import datetime

from core4.api.v1.application import CoreApiContainer
from core4.api.v1.request.main import CoreRequestHandler

from bokeh.plotting import figure
from bokeh.embed import json_item, components
from bokeh.resources import CDN
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Plot, VBar, LinearAxis

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


class ChartHandler(CoreRequestHandler):
    """
    Chart Demo
    """
    author = "osc"
    title = "Agof Chart Example"

    async def get(self):

        if self.wants_json():

            # Access to the processed data
            data = await self.config.dldemo.collection.data.find().to_list(length=None)

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

            # Monthly aggregated data
            df_Monat = df.groupby(["Monat"]).val.sum()
            df_Monat = pd.DataFrame(df_Monat)
            df_Monat['Monat'] = df_Monat.index
            df_Monat.reset_index(drop=True, inplace=True)

            ## Bokeh plot1
            source = ColumnDataSource(df_agg)

            p = figure(title="Contacts", x_axis_type="datetime",
                       plot_height=600, plot_width=800)
            p.line(x='Date', y='val',
                   line_width=2,
                   source=source)

            ## Bokeh plot2
            source2=ColumnDataSource(dict(x=df_Monat.Monat, top=df_Monat.val))
            plot = Plot(
                title=None, width=300, height=300,
                min_border=0, toolbar_location=None)

            glyph = VBar(x="x", top="top", bottom=0, width=0.5, fill_color="#b3de69")
            plot.add_glyph(source2, glyph)

            yaxis = LinearAxis()
            plot.add_layout(yaxis, 'left')

            # self.reply(json_item(p, "myplot"))
            # self.reply(json_item(plot, "myplot"))
            self.reply(json_item(column(p, plot), "myplot"))
        else:
            await self.render("template/base3.html", rsc=CDN.render(), mytitle='Demo Chart')


class MyServer(CoreApiContainer):
    root = "/dldemo/examples"
    rules = [
         ("/chart", ChartHandler),
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve
    serve(MyServer)
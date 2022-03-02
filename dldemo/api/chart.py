
import pandas as pd
import numpy as np
import datetime

from core4.api.v1.request.main import CoreRequestHandler

from bokeh.plotting import figure
from bokeh.embed import json_item, components
from bokeh.resources import CDN
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.layouts import gridplot
from bokeh.transform import dodge



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

            hover_tool = HoverTool(
                tooltips=[
                    ('Contacts', '@val{0,0.000}'),
                ],
                # formatters={
                #     '@val': 'numeral',
                # },
                # display a tooltip whenever the cursor is vertically in line with a glyph
                mode='vline'
            )
            p = figure(title="Contacts", x_axis_type="datetime",
                       plot_height=600, plot_width=800, tools = '')
            p.line(x = 'Date', y = 'val',
                   line_width=2,
                   source = source)
            p.tools.append(hover_tool)



            self.reply(json_item(p, "myplot"))
        else:
            await self.render("templates/base3.html", rsc=CDN.render(), mytitle='Demo Chart')



class MultiChartHandler(CoreRequestHandler):
    """
    Multiple Charts Demo
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

            ### Plot 1
            g = df.groupby(["Date"]).val.sum()  # aggregation by date
            df_agg = pd.DataFrame(g)
            df_agg = df_agg.reset_index()
            df_agg = df_agg[df_agg['Date'] >= '2020']
            dates = df_agg['Date'].to_list()

            # Bokeh plot
            # generating the plot
            source = ColumnDataSource(df_agg)

            p1 = figure(title="Contacts",
                        x_range=dates,
                        plot_height=600, plot_width=800)
            p1.vbar(x = 'Date', top = 'val',
                    width = 0.8,
                    source = source)
            p1.yaxis.axis_label = "Contacts"
            p1.xaxis.major_label_orientation = 1

            ### Plot 2
            df_new = df[df.Medientyp != 0]
            df_new = df_new[df_new['Date'] >= '2020']
            g2 = df_new.groupby(["Medientyp"]).val.sum()
            df_agg2 = pd.DataFrame(g2)
            df_agg2 = df_agg2.reset_index()

            source = ColumnDataSource(df_agg2)
            medientyp = df_agg2['Medientyp'].to_list()

            p2 = figure(title="Medientyp",
                        x_range=medientyp,
                        plot_height=600, plot_width=800)
            p2.vbar(x = 'Medientyp', top = 'val',
                    width=0.5,
                    source = source)
            p2.yaxis.axis_label = "Contacts"


            ### Plot 3

            df_new = df[df.Medientyp != 0]
            # Monthly contacts for each media group
            g3 = df_new.groupby(["Date", "Medientyp"]).val.sum().unstack()
            df_agg3 = pd.DataFrame(g3)
            df_agg3 = df_agg3.reset_index()
            df_agg3 = df_agg3[df_agg3['Date'] >= '2020']

            medientypen = df_agg2['Medientyp'].to_list()
            dates = df_agg3['Date'].to_list()

            source = ColumnDataSource(df_agg3)

            p3 = figure(title="Contacts and Medientypen",
                        x_range=dates,
                        plot_height=600, plot_width=800)
            p3.vbar(x=dodge('Date', -0.25, range=p3.x_range), top='Digitales Gesamtangebot', width=0.2, source=source,
                   color="#c9d9d3", legend_label="Digitales Gesamtangebot")

            p3.vbar(x=dodge('Date', 0.0, range=p3.x_range), top='Mobiles Gesamtangebot', width=0.2, source=source,
                   color="#718dbf", legend_label="Mobiles Gesamtangebot")

            p3.vbar(x=dodge('Date', 0.25, range=p3.x_range), top='Website Angebot', width=0.2, source=source,
                   color="#e84d60", legend_label="Website Angebot")

            p3.legend.location = "top_left"
            p3.legend.orientation = "horizontal"
            p3.xaxis.major_label_orientation = 1
            p3.y_range.start = 0
            p3.y_range.end = 48000
            p3.yaxis.axis_label = "Contacts"

            ### Multiple Charts in a gridplot

            chart_list = [p1, p2, p3]
            grid_plot = gridplot(chart_list, ncols=3, plot_width=250, plot_height=250,
                                 sizing_mode = "stretch_both")

            self.reply(json_item(grid_plot, "myplot"))

        else:
            await self.render("templates/base4.html", rsc=CDN.render(), mytitle='Demo Chart')



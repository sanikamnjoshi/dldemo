from core4.api.v1.application import CoreApiContainer
from dldemo.api.chart import MultiChartHandler, ChartHandler
from dldemo.api.table import TableFilterHandler




class MyServer(CoreApiContainer):
    root = "/dldemo/api"
    rules = [
        ("/chart_multiple", MultiChartHandler),
        ("/chart", ChartHandler),
        ("/table", TableFilterHandler)
    ]

if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve

    serve(MyServer)
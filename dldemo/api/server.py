from core4.api.v1.application import CoreApiContainer
from dldemo.api.chart import MultiChartHandler, ChartHandler
from dldemo.api.table import TableFilterHandler
from dldemo.api.perm_demo.api_demo import ApiDemo




class MyServer(CoreApiContainer):
    root = "/dldemo/api"
    rules = [
        ("/chart_multiple", MultiChartHandler),
        ("/chart", ChartHandler),
        ("/table", TableFilterHandler),
        ("/perm_demo", ApiDemo)
    ]

if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve

    serve(MyServer)
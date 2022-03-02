from core4.api.v1.request.main import CoreRequestHandler
import json
import pandas as pd

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

        await self.render("templates/xls-d.html", df=df, query=json.dumps(query))

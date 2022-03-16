from core4.api.v1.request.main import CoreRequestHandler
import pandas as pd

class ApiDemo(CoreRequestHandler):
    """
    Demo API permissions
    """
    author = "mkr"
    title = "example handler"
    protected1 = "app://dldemo/protected1"
    protected2 = "app://dldemo/protected2"

    async def get(self):
        """
        Only users with the api://.../r have access
        Modify return values according to app://dldemo/* permissions
        """
        doc = self.config.dldemo.collection.perm_demo.find({})
        df = pd.DataFrame(await doc.to_list(length=None))
        perms = await self.user.casc_perm()
        admin = await self.user.is_admin()

        if admin or (self.protected1 in perms and self.protected2 in perms):
            pass
        elif self.protected1 in perms:
            del df["protected2"]
        elif self.protected2 in perms:
            del df["protected1"]
        else:
            del df["protected1"]
            del df["protected2"]

        return await self.reply(df)

    async def post(self):
        """
        Only users with the api://.../c have access
        """
        return await self.reply("This is the POST method used to create new resources")

    async def put(self):
        """
        Only users with the api://.../u have access
        """
        return await self.reply("This is the PUT method used to modify existing resources ")

    async def delete(self):
        """
        Only users with the api://.../d have access
        """
        return await self.reply("This is the delete method to delete resources")

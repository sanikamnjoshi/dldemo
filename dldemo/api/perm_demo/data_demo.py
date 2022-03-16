from core4.queue.job import CoreJob
import pandas as pd
import numpy as np


class GenDemoData(CoreJob):
    """
    create demo data
    """
    author = "mkr"

    def execute(self):
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 5)), columns=[*list("ABC"), "protected1", "protected2"])
        self.set_source("demo_data")
        self.config.dldemo.collection.perm_demo.insert_many(df.to_dict(orient="records"))


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(GenDemoData)

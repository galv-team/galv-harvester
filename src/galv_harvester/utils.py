import json

import numpy as np


class NpEncoder(json.JSONEncoder):
    # https://stackoverflow.com/a/57915246
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

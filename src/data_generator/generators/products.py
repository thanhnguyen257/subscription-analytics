# generators/products.py

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator


class ProductsGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("products")
        self.id_alloc = IDAllocator("products")

    def _generate(self):

        products = [
            ("StreamFlix", "Streaming"),
            ("BingeNow", "Streaming"),
            ("LearnPro", "E-Learning"),
            ("SkillBoost", "E-Learning"),
            ("CloudSync", "Cloud Tools"),
            ("DataFlow", "Cloud Tools"),
            ("DesignSuite", "Creative Suite"),
            ("TaskMaster", "Productivity"),
        ]

        n = len(products)

        product_ids = np.array(list(self.id_alloc.next_batch(n)))

        base_date = datetime(2022, 1, 1)

        # vectorized random offsets
        offsets = np.random.randint(0, 365, n)

        created_at = pd.to_datetime([
            base_date + timedelta(days=int(d))
            for d in offsets
        ])

        names = [p[0] for p in products]
        categories = [p[1] for p in products]

        df = pd.DataFrame({
            "product_id": product_ids,
            "product_name": names,
            "category": categories,
            "created_at": created_at
        })

        return df
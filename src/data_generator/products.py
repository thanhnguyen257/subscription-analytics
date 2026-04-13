import pandas as pd
import random
from datetime import datetime, timedelta

def generate_products():
    products = [
        (1, "StreamFlix", "Streaming"),
        (2, "BingeNow", "Streaming"),
        (3, "LearnPro", "E-Learning"),
        (4, "SkillBoost", "E-Learning"),
        (5, "CloudSync", "Cloud Tools"),
        (6, "DataFlow", "Cloud Tools"),
        (7, "DesignSuite", "Creative Suite"),
        (8, "TaskMaster", "Productivity"),
    ]

    base_date = datetime(2022, 1, 1)

    rows = []
    for pid, name, category in products:
        dt = base_date + timedelta(days=random.randint(0, 365))
        rows.append((pid, name, category, dt.replace(tzinfo=None)))

    df = pd.DataFrame(rows, columns=[
        "product_id", "product_name", "category", "created_at"
    ])

    return df
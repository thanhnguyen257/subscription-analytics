import numpy as np
import random
from faker import Faker

N_USERS = 50000
N_SUBSCRIPTIONS = 250000
N_CHANGES = 25000
N_PAYMENTS = 350000
N_LICENSES = 11000
N_ALLOCATIONS = 55000

np.random.seed(2001)
random.seed(2001)
Faker.seed(2001)
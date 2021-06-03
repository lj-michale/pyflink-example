# -*- coding: utf-8 -*-#

# -------------------------------------------------------------------------------
# Name:         model
# Description:
# Author:       orange
# Date:         2021/6/3
# -------------------------------------------------------------------------------

import pickle
import pandas as pd
from sklearn.tree import DecisionTreeClassifier

X = pd.Series([0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.3, 0.3, 0.3]).values.reshape(-1, 1)
y = pd.Series([0, 0, 0, 1, 1, 1, 2, 2, 2])

clf = DecisionTreeClassifier()
clf.fit(X, y)

with open('model.pickle', 'wb') as f:
    pickle.dump(clf, f)
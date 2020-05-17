import pandas as pd
import numpy as np
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from .data_preparator import DataPreparator


class NYCCCCIRandomForest:

     def fit(self):
        rforest = RandomForestClassifier(
            bootstrap=True, 
            n_estimators=5000, 
            class_weight="balanced",
            max_depth=5, 
            criterion='gini'
        )
        data_prep = DataPreparator()
        X_train, y_train, X_test, y_test = data_prep.split_train_test()
        rforest.fit(X_train.values, y_train.values.ravel())
        return rforest
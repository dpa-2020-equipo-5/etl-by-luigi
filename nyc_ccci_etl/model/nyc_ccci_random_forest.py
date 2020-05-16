import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

from sklearn.model_selection import train_test_split

class NYCCCCIRandomForest:
    # def __init__(self, _x_train, _y_train, _x_test, _y_test):
    #     self.X_train = _x_train
    #     self.Y_train = _y_train
    #     self.X_test = _x_test
    #     self.Y_test = _y_test
    def __init__(self, _x, _y):
        self.X = _x
        self.y = _y

    def fit(self):
        #X_train, X_test, y_train, y_test = train_test_split(self.X, self.y)

        rforest = RandomForestClassifier()

        hyper_param_grid = {
            'n_estimators': [100,1000,5000], 
            'max_depth': [1,5,10,20], 
            'criterion':['gini'],
            'class_weight':["balanced"],
            'bootstrap':[True],
        }
        print("Setting grid params")

        grid_search = GridSearchCV(
            rforest, 
            hyper_param_grid, 
            scoring = 'f1',
            cv = 10, 
            n_jobs = -1,
            verbose = 3)
        print("Beginging grid search")
        grid_search.fit(self.X, self.y.ravel())

        print("="*100)
        print("best_params: ")
        print(grid_search.best_params_)

        print("="*100)
        print("best_score: ")
        print(grid_search.best_score_)

        
        cv_results = pd.DataFrame(grid_search.cv_results_)
        print("="*100)
        print("cv_results: ")
        print(cv_results.head())

        results = cv_results.sort_values(by='rank_test_score', ascending=True).head()
        print("="*100)
        print(results.iloc[0,:].params)

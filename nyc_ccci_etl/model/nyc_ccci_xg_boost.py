import pandas as pd
from xgboost import XGBClassifier
from sqlalchemy import create_engine
from sklearn import metrics
#from sklearn.model_selection import GridSearchCV
import multiprocessing


class NYCCCCIXGBoost:
    def __init__(self, _x_train, _y_train, _x_test, _y_test):
        self.X_train = _x_train
        self.Y_train = _y_train
        self.X_test = _x_test
        self.Y_test = _y_test

    def fit(self):
        xgb_class = XGBClassifier(n_estimators=500, max_depth=3, booster='gbtree', min_child_weight=6, gamma=0, eta=0.5, subsample=1, learning_rate=0.2, objective='binary:logistic', n_jobs=1, eval_metric=['auc', 'aucpr', 'error'], nthread=multiprocessing.cpu_count())
        xgb_class.fit(self.X_train, self.Y_train, early_stopping_rounds=10, eval_set=[(self.X_test, self.Y_test)])

        self.Y_pred = pd.DataFrame(xgb_class.predict(self.X_test))

        print("Precision:",metrics.precision_score(self.Y_test, self.Y_pred, average='macro'))
        print("Recall:",metrics.recall_score(self.Y_test, self.Y_pred, average='macro'))

        feature_importance_frame = pd.DataFrame()
        feature_importance_frame['features'] = list(self.X_train.keys())
        feature_importance_frame['importance'] = list(xgb_class.feature_importances_)
        feature_importance_frame = feature_importance_frame.sort_values(
                'importance', ascending=False)
        print(feature_importance_frame)

        print("best score: {0}, best iteration: {1}, best ntree limit {2}".format(xgb_class.best_score, xgb_class.best_iteration, xgb_class.best_ntree_limit))

        return xgb_class

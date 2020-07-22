from sklearn.datasets import load_diabetes
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import os
import pickle

X, y = load_diabetes(return_X_y=True)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.1, random_state=13)

params = {'n_estimators': 500,
          'max_depth': 4,
          'min_samples_split': 5,
          'learning_rate': 0.01,
          'loss': 'ls'}

model = GradientBoostingRegressor(**params)
model.fit(X_train, y_train)

rmse = mean_squared_error(y_test, model.predict(X_test),squared=False)
print("The root mean squared error (RMSE) on test set: {:.4f}".format(rmse))

# save the model
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path+'/model.pkl','wb') as output:
    pickle.dump(model, output)
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import make_pipeline
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC



OUTPUT_TEMPLATE = (
    'Model score: {score:.3g}\n'
)

def main():


    monthly_data_labelled = pd.read_csv(sys.argv[1])
    X = monthly_data_labelled.loc[:,'tmax-01':'snwd-12'].values
    y = monthly_data_labelled['city'].values

    monthly_data_unlabelled = pd.read_csv(sys.argv[2])

    # TODO: create some models
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    model = make_pipeline(
    	StandardScaler(),
    	SVC(kernel='linear', C=0.1)
    )


    # train each model and output image of predictions
    model.fit(X_train, y_train)
        
    #predict unknown cities    
    X_unknown = monthly_data_unlabelled.loc[:,'tmax-01':'snwd-12'].values
    predictions = model.predict(X_unknown)
    pd.Series(predictions).to_csv(sys.argv[3], index=False)

    #output score of model
    print(OUTPUT_TEMPLATE.format(score=model.score(X_test, y_test) ))


if __name__ == '__main__':
    main()


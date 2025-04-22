# Task 8: Building a Simple Prediction Model
## Now that we've verified the quality of our data, let's build a simple model to predict 
## the total number of bike rentals based on other parameters (excluding 'casual' and 'registered').


import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline


def train_and_evaluate_model(df: pd.DataFrame) -> None:
    df = df[['season', 'year', 'mnth', 'hour', 'holiday', 'weekday', 'workingday','weather', 'temp', 'humidity', 'windspeed', 'total']]

    # Prepare features and target
    X = df.drop('total', axis=1)  # All columns except 'total'
    y = df['total']  # Target variable

    # Identify categorical and numerical columns
    categorical_cols = ['season', 'weekday', 'weather']
    numeric_cols = ['year', 'mnth', 'hour', 'holiday', 'workingday', 'temp', 'humidity', 'windspeed']

    # Create preprocessing pipeline
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols),
            ('num', 'passthrough', numeric_cols)
        ]
    )

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create and train a RandomForest model
    model = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
    ])

    print("Training the model...")
    model.fit(X_train, y_train)

    # Make predictions
    y_pred = model.predict(X_test)

    # Evaluate the model
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"Model Evaluation:")
    print(f"Mean Absolute Error: {mae:.2f} bike rentals")
    print(f"R² Score: {r2:.4f}")

    # Feature importance
    feature_names = (
        [f"{col}_{cat}" for col in categorical_cols for cat in sorted(df[col].unique())] +
        numeric_cols
    )
    try:
        importances = model.named_steps['regressor'].feature_importances_
        indices = np.argsort(importances)[-10:]  # Top 10 features
        
        print("\nTop 10 Most Important Features:")
        for i in indices[::-1]:
            if i < len(feature_names):
                print(f"{feature_names[i]}: {importances[i]:.4f}")
    except:
        print("Couldn't extract feature importances due to preprocessing structure")
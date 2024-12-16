import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import json

# Load the data
input_file = '/Users/ll/TU-Berlin/MastersThesis/anotherdemoapplication/load-generator/output_json_files/Ann_Mcmahon_17750.json'

# Parse the JSON file
with open(input_file, 'r') as f:
    data = json.load(f)

# Convert the JSON to a pandas DataFrame
df = pd.DataFrame(data)

# Select relevant columns for prediction
# Assuming 'resp' (respiratory rate) is the target variable
features = ['oxpercent', 'oxflow', 'bps', 'bpd', 'pulse', 'temp']
df = df[features + ['resp']]

# Convert all columns to numeric, handling missing values
df = df.apply(pd.to_numeric, errors='coerce')
df = df.dropna()  # Drop rows with missing values

# Split the data into features (X) and target (y)
X = df[features]
y = df['resp']

# Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a simple linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Predict future data points (for example, using the last row of the dataset as input)
future_data_point = X.iloc[-1].values.reshape(1, -1)  # Last data point
future_prediction = model.predict(future_data_point)

print("Future Data Point Prediction:", future_prediction[0])
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import couchdb

# Function to pause the script and wait for user input
def wait_for_user_input(prompt):
    input(prompt)  # Waits for user input, proceed when Enter is pressed

# Initialize a Spark session
spark = SparkSession.builder.appName("DiabetesDataAnalysis").getOrCreate()

# Load the dataset
csv_file_path = "./diabetes.csv"
diabetes_df = spark.read.option("header", "true").csv(csv_file_path, inferSchema=True)

# Show a summary
diabetes_df.printSchema()
diabetes_df.show(5)

# Connect to CouchDB
couch = couchdb.Server('http://admin:couchdb@161.35.199.201:5984/')
dbname = 'diabetes_analysis'

# Ensure the database exists
if dbname in couch:
    db = couch[dbname]
else:
    db = couch.create(dbname)

# Convert DataFrame to JSON format and store data in CouchDB
diabetes_json = diabetes_df.toJSON().collect()

for idx, record in enumerate(diabetes_json):
    doc_id = f"patient_{idx}"  # Generating document ID
    db.save({"_id": doc_id, "data": record})

print("Data stored in CouchDB. Check your database now.")
wait_for_user_input("Press Enter to proceed to the update step...")

# Create a new row
new_row = {
    "Pregnancies": 10,
    "Glucose": 10,
    "BloodPressure": 10,
    "SkinThickness": 10,
    "Insulin": 10,
    "BMI": 10,
    "DiabetesPedigreeFunction": 10,
    "Age": 10,
    "Outcome": 10
}

# Save the new row to the CouchDB database with ID "bigdata"
db.save({"_id": "bigdata", "data": json.dumps(new_row)})

print("New row added to the database with ID 'bigdata'.")
wait_for_user_input("Press Enter to proceed to the update step...")


# UPDATE operation - Delete the "Age" column and add 5 to "BloodPressure"
for doc_id in db:
    fetched_doc = db[doc_id]
    data = json.loads(fetched_doc['data'])  # Parse data into a dictionary
    # Delete the "Age" column
    del data['Age']
    # Add 5 to the "BloodPressure"
    data['BloodPressure'] = int(data.get('BloodPressure')) + 5
    fetched_doc['data'] = json.dumps(data)  # Convert data back to JSON string
    db.save(fetched_doc)

print("Documents updated with 'Age' column deleted and 'BloodPressure' increased by 5. Check your CouchDB database now.")

wait_for_user_input("Press Enter to proceed to the delete step...")

# DELETE operation - Delete all documents from the database
for doc_id in db:
    db.delete(db[doc_id])

print("All documents deleted from the database. Check your CouchDB database now.")

if (spark.getActiveSession()):
    print('yes')
else:
    print('no')

# Clean up
spark.stop()

if (spark.getActiveSession()):
    print('yes')
else:
    print('no')
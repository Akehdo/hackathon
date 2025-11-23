from fastapi import FastAPI, UploadFile, File
import pandas as pd
from io import BytesIO
import joblib

app = FastAPI()

@app.post("/process/")
async def upload_csv(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    print("reading file1")
    content1 = await file1.read()
    df1 = pd.read_csv(BytesIO(content1), sep=';', encoding='cp1251')
        
    print("reading file2")
    content2 = await file2.read()
    df2 = pd.read_csv(BytesIO(content2), sep=';', encoding='cp1251')
    
    transactions = df1
    patterns = df2
    
    if df2.shape[1] < df1.shape[1]:
        patterns, transactions = transactions, patterns
        
        
    #clean data
    
    #merge

    #result = run_model(df1, df2)



#model = joblib.load("model.pkl")

#def run_model(df1, df2):
    #features = pd.concat([df1, df2], axis=1)
    #prediction = model.predict(features)
    #3return prediction.tolist()
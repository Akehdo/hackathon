from fastapi import FastAPI, UploadFile, File
import pandas as pd
from io import BytesIO
import joblib
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from helpers import validate_transaction_data,validate_patterns_data, identify_separator, merge_transaction_pattern_data, preprocess_merged_data
app = FastAPI()

PRED_THRESHOLD = 0.3

model = joblib.load("model.pkl")

@app.post("/process")
async def upload_csv(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    print("reading file1")
    content1 = await file1.read()
    sep1 = identify_separator(BytesIO(content1))
    df1 = pd.read_csv(BytesIO(content1), sep=sep1, encoding='cp1251')
        
    print("reading file2")
    content2 = await file2.read()
    sep2 = identify_separator(BytesIO(content2))
    df2 = pd.read_csv(BytesIO(content2), sep=sep2, encoding='cp1251')
    
    
    if df2.shape[1] < df1.shape[1]:
        df1, df2 = df2, df1
        
    # validate data
    transactions = validate_transaction_data(df1)
    patterns = validate_patterns_data(df2)
    if transactions.get("status") == "error":
        return {"error": transactions["message"]}
    if patterns.get("status") == "error":
        return {"error": patterns["message"]}
    
    #merge
    merged_df = merge_transaction_pattern_data(transactions=df1, patterns=df2)
    preprocessed_df = preprocess_merged_data(merged_df)

    temp = preprocessed_df.copy()
    temp.rename(columns={'target': "expected_target"}, inplace=True)
    
    preprocessed_df.drop(columns=['cst_dim_id', 'transdate', 'transdatetime', 'docno', 'target'], inplace=True, errors='ignore')

    try: 
        predictions = model.predict_proba(preprocessed_df)[:, 1]
        temp['target'] = (predictions > PRED_THRESHOLD).astype(int) 
        result = temp.to_dict(orient='records')
        
        # Calculate metrics of target vs expected_target
        metrics = {}
        if 'expected_target' in temp.columns:
            metrics = {
                "accuracy": accuracy_score(temp['expected_target'], temp['target']),
                "precision": precision_score(temp['expected_target'], temp['target'], zero_division=0),
                "recall": recall_score(temp['expected_target'], temp['target'], zero_division=0),
                "f1_score": f1_score(temp['expected_target'], temp['target'], zero_division=0)
            }
        
        return {"predictions": result, "metrics": metrics}
    except Exception as e:
        return {"error": str(e)}


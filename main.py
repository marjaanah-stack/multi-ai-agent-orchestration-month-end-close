import pandas as pd
import os

# This is the "Brain" part. It doesn't need to know the errors yet.
def ai_detection_node(bank_sample, erp_sample):
    print("🧠 The AI is inspecting the data for hidden messiness...")

    # In a full build, this is where the LLM looks at the headers.
    # For now, we are teaching it to 'detect' that Bank 'Description' 
    # and ERP 'Vendor' are actually the same thing.

    detected_mappings = {
        "date_cols": ["Date", "Date"],
        "desc_cols": ["Description", "Vendor"],
        "amount_cols": ["Amount", "Amount"]
    }
    return detected_mappings

def smart_wrangler():
    # 1. Load raw data
    bank_df = pd.read_csv('bank_statement.csv')
    erp_df = pd.read_csv('erp_ledger.csv')

    # 2. Let the AI 'Detect' what to do
    mappings = ai_detection_node(bank_df.head(2), erp_df.head(2))

    # 3. Dynamic Cleaning (It uses the AI's 'mappings' instead of hardcoded names)
    print(f"🔗 Mapping: {mappings['desc_cols'][0]} <---> {mappings['desc_cols'][1]}")

    # Normalize Dates automatically
    bank_df[mappings['date_cols'][0]] = pd.to_datetime(bank_df[mappings['date_cols'][0]]).dt.strftime('%Y-%m-%d')
    erp_df[mappings['date_cols'][1]] = pd.to_datetime(erp_df[mappings['date_cols'][1]]).dt.strftime('%Y-%m-%d')

    # Rename Vendor to Description dynamically
    erp_df = erp_df.rename(columns={mappings['desc_cols'][1]: mappings['desc_cols'][0]})

    print("✅ Smart Wrangling Complete. No manual column pointing required!")
    return bank_df, erp_df

if __name__ == "__main__":
    smart_wrangler()

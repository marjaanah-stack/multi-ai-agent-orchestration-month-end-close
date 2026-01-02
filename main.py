import os
import pandas as pd
from typing import TypedDict, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg_pool import ConnectionPool

# 1. Define the "Memory" structure
class AgentState(TypedDict):
    bank_data: list
    erp_data: list
    matches: list
    unmatched_count: int

# 2. The Logic for our Matchmaker Node
def matchmaker_node(state: AgentState):
    print("🤖 Matchmaker is processing...")
    bank_df = pd.DataFrame(state['bank_data'])
    erp_df = pd.DataFrame(state['erp_data'])

    current_matches = []
    unmatched = 0

    for _, row in bank_df.iterrows():
        # Simple logic: If Amount matches exactly, we call it a win for now
        # (In the next step, we'll re-add the LLM brain here)
        match = erp_df[erp_df['Amount'] == row['Amount']]

        if not match.empty:
            current_matches.append({"bank": row['Description'], "status": "MATCHED"})
        else:
            current_matches.append({"bank": row['Description'], "status": "UNMATCHED"})
            unmatched += 1

    return {"matches": current_matches, "unmatched_count": unmatched}

# 3. Setup the Graph with Database Memory
DB_URI = os.environ.get("DATABASE_URL")

# We use a "Connection Pool" so the database doesn't get overwhelmed
pool = ConnectionPool(conninfo=DB_URI, max_size=20)
checkpointer = PostgresSaver(pool)

# 4. Build the Workflow
workflow = StateGraph(AgentState)
workflow.add_node("matchmaker", matchmaker_node)
workflow.set_entry_point("matchmaker")

# Logic: If there are unmatched items, go to a "Human Review" node (which we'll build next)
workflow.add_edge("matchmaker", END)

# Compile the graph with memory!
app = workflow.compile(checkpointer=checkpointer)

print("✅ Stateful Graph is ready with Supabase memory.")
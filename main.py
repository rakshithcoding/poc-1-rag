import os
import json
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from langchain.prompts import PromptTemplate

# Load environment variables from .env file
load_dotenv()

# Import your project modules
# It's important to load env vars before these imports
import database
import rag_chain

# --- App Lifecycle (Startup & Shutdown) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This code runs on startup
    print("--- Server is starting up ---")
    if database.cluster is None:
        print("FATAL: Database connection failed. The server may not operate correctly.")
    if rag_chain.llm is None or rag_chain.retriever is None:
        print("FATAL: RAG chain components failed to initialize. The server may not operate correctly.")
    print("--- Startup complete ---")
    
    yield # The application runs while the server is alive
    
    # This code runs on shutdown
    print("--- Server is shutting down ---")


# --- FastAPI App Initialization ---
app = FastAPI(
    title="AI Conversational Agent API",
    description="API for the RAG-based conversational agent to query a Couchbase database.",
    version="1.0.0",
    lifespan=lifespan  # Use the new lifespan event handler
)

# --- CORS Configuration ---
# Allows the frontend to communicate with this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend's domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models for API Contracts ---
class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    report: str
    generated_n1ql: str
    result: str


# --- API Endpoint ---
@app.post("/generate-report", response_model=QueryResponse)
async def generate_report(request: QueryRequest):
    """
    The main endpoint to generate a report from a natural language query.
    This endpoint will try up to 3 times to generate a valid, executable query.
    """
    user_query = request.query
    print(f"\nReceived new query: {user_query}")

    max_retries = 3
    last_error = None
    query_result = None
    generated_n1ql = ""

    # --- Chain for fixing a failed query ---
    fix_n1ql_template = """
    The user asked the following question: "{question}"
    I generated this N1QL query:
    ---
    {failed_query}
    ---
    But it failed with this error:
    ---
    {error_message}
    ---
    Please correct the N1QL query to fix the error.
    You must follow these rules:
    1. Only return the corrected N1QL query.
    2. Do not add any explanation, introductory text, or markdown formatting.
    3. Ensure all date functions like NOW_STR() are used correctly for Couchbase N1QL.
    """
    FIX_N1QL_PROMPT = PromptTemplate.from_template(fix_n1ql_template)
    fix_n1ql_chain = FIX_N1QL_PROMPT | rag_chain.llm | rag_chain.StrOutputParser()

    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        try:
            # Step 1: Generate N1QL query
            if attempt == 0:
                # First attempt: use the standard RAG chain
                print("Generating initial N1QL query...")
                n1ql_generation_chain = rag_chain.get_rag_chain()
                generated_n1ql = n1ql_generation_chain.invoke(user_query)
            else:
                # Subsequent attempts: use the self-correction chain
                print("Attempting to self-correct previous query...")
                generated_n1ql = fix_n1ql_chain.invoke({
                    "question": user_query,
                    "failed_query": generated_n1ql, # Use the previously failed query
                    "error_message": last_error
                })

            if not generated_n1ql or not isinstance(generated_n1ql, str):
                last_error = "LLM failed to generate a valid N1QL string."
                continue # Go to the next attempt

            # Step 2: Execute the generated N1QL query
            query_result = database.execute_n1ql_query(generated_n1ql)
            print("Query executed successfully!")
            break  # If successful, exit the loop

        except Exception as e:
            last_error = str(e)
            print(f"Attempt {attempt + 1} failed: {last_error}")
            # If this is the last attempt, the loop will end
            if attempt == max_retries - 1:
                print("All retry attempts failed.")

    # After the loop, check if we have a successful result
    if query_result is None:
        raise HTTPException(
            status_code=500,
            detail="Unable to get data at this time. The AI agent could not generate a valid query after multiple attempts."
        )

    # If successful, proceed with summarization
    try:
        result_str = json.dumps(query_result, indent=2)

        # Step 3: Generate a human-readable summary
        summary_chain = rag_chain.get_summary_chain()
        report = summary_chain.invoke({
            "question": user_query,
            "query_result": result_str
        })

        print(f"Generated Report: {report}")

        return QueryResponse(
            report=report,
            generated_n1ql=generated_n1ql,
            result=result_str
        )
    except Exception as e:
        print(f"An unexpected error occurred during summarization: {e}")
        raise HTTPException(status_code=500, detail=str(e))

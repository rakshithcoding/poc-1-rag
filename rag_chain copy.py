import os
from langchain_google_genai import GoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser

from knowledge import schema_knowledge

# --- Configuration ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- LLM and Embeddings Initialization ---
try:
    llm = GoogleGenerativeAI(model="gemini-2.5-flash-lite", google_api_key=GOOGLE_API_KEY)
    embeddings = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004", google_api_key=GOOGLE_API_KEY)
    print("LLM and Embeddings models initialized successfully.")
except Exception as e:
    print(f"Error initializing Google AI models: {e}")
    llm = None
    embeddings = None

# --- Vector Store Setup ---
try:
    vector_store = Chroma.from_texts(
        texts=schema_knowledge,
        embedding=embeddings
    )
    retriever = vector_store.as_retriever(search_kwargs={"k": 2}) # Retrieve top 2 most relevant documents
    print("Vector store created and retriever is ready.")
except Exception as e:
    print(f"Error creating vector store: {e}")
    retriever = None

# --- Prompt Templates ---

# 1. N1QL Generation Prompt
n1ql_prompt_template = """
You are a Couchbase N1QL expert. Your task is to write a N1QL query based on a user's question and the provided database schema context.

You must follow these rules:
1.  Generate only the N1QL query. Do not add any explanations, introductory text, or markdown formatting like ```n1ql.
2.  The query must be for the `sales_poc` bucket.
3.  When querying a collection, you MUST use the full keyspace path which is `sales_poc`._default.COLLECTION_NAME.
    - For the `customers` collection, the full path is `sales_poc`._default.customers
    - For the `sales` collection, the full path is `sales_poc`._default.sales
4.  The `customer_id` in the `sales` collection links to the document ID in the `customers` collection. The join condition is `s.customer_id = META(c).id`.
5.  **IMPORTANT DATE RULE**: To get the current date, you MUST use `NOW_STR("1111-11-11")`. For date calculations, use functions like `DATE_ADD_STR()`. Do NOT use functions like `CURDATE()` or `NOW()`. The current date for this query is August 17, 2025.

**Schema Context:**
{context}

**User's Question:**
{question}

**N1QL Query:**
"""
N1QL_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template=n1ql_prompt_template,
)

# 2. Result Summarization Prompt
summary_prompt_template = """
You are an AI assistant for a business intelligence dashboard.
Your task is to provide a concise, easy-to-understand summary of the data returned from a database query.
The user's original question was: "{question}"
The data from the database is:
{query_result}

Based on this data, provide a clear, natural language summary.
If the data is empty or contains an error message, state that you couldn't find an answer.
Do not mention N1QL or the database. Just present the answer to the user.

**Summary:**
"""
SUMMARY_PROMPT = PromptTemplate(
    input_variables=["question", "query_result"],
    template=summary_prompt_template,
)


# --- LangChain Chains ---

def get_rag_chain():
    """
    Builds and returns the primary RAG chain for generating N1QL.
    """
    if not retriever or not llm:
        raise ConnectionError("RAG chain dependencies (retriever or llm) are not initialized.")

    return (
        {"context": retriever, "question": RunnablePassthrough()}
        | N1QL_PROMPT
        | llm
        | StrOutputParser()
    )

def get_summary_chain():
    """
    Builds and returns the chain for summarizing query results.
    """
    if not llm:
        raise ConnectionError("Summarization chain dependency (llm) is not initialized.")
        
    return SUMMARY_PROMPT | llm | StrOutputParser()

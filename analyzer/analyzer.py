import os
from dotenv import load_dotenv
load_dotenv()
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
import pandas as pd

# Asks the LLM your question
def ask(question):
    df = pd.read_csv('data/processed/metrics.csv')

    openai_api_key = os.getenv("OPENAI_API_KEY")
    llm = ChatOpenAI(model_name="gpt-4o", temperature=0.7, openai_api_key=openai_api_key)

    # Prompt for the LLM
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"The following is data from metric.csv:\n{df}\n\n You are a helpful assistant that answers questions based on the provided data. You provide structured insights and are polite."),
        MessagesPlaceholder(variable_name="history"),
        ("human", "{input}")
    ])

    # Set up chat memory for the convo
    def memory_factory():
        return InMemoryChatMessageHistory()

    conversation = RunnableWithMessageHistory(
        prompt | llm,
        memory_factory,
        input_messages_key="input",
        history_messages_key="history"
    )

    # Asks the question and gets a response
    response = conversation.invoke({"input": question}, config={"configurable": {"session_id": "default"}})
    return response.content if hasattr(response, 'content') else response
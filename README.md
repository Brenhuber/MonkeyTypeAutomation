<h1 align="center">⌨️ MonkeyType Analysis Chatbot</h1>
<p align="center"><em>Analyze and chat with your typing performance data</em></p>

---

### 🚀 Overview

**Monkeytype Chatbot** is an interactive app that lets you explore your typing performance using natural language.  
It combines **PySpark pipelines**, **LangChain with OpenAI models**, and **Taipy GUI** to clean data, generate insights, and provide a chatbot interface for answering questions about your typing history.

---

### ✨ Features

- Automated **data cleaning** of raw Monkeytype CSVs with PySpark  
- **ETL orchestration** using Prefect to validate, deduplicate, and update metrics  
- **Chatbot interface** powered by LangChain + OpenAI to answer natural language questions about your performance  
- **Memory-enabled conversations** for contextual chat history  
- Clean UI built with **Taipy GUI** for seamless interaction  
- **Status tracking** for every cleaning and update run  

---

### 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Processing-orange?logo=apachespark)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-blueviolet?logo=prefect)
![LangChain](https://img.shields.io/badge/LangChain-LLM%20Framework-green?logo=chainlink)
![OpenAI](https://img.shields.io/badge/OpenAI-LLM-black?logo=openai)
![Taipy](https://img.shields.io/badge/Taipy-Interactive%20UI-red?logo=python)

---

### ⚙️ Setup Instructions

```bash
# 1. Clone the repository
git clone https://github.com/Brenhuber/MonkeytypeChatbot.git
cd MonkeytypeChatbot

# 2. Install dependencies
pip install -r requirements.txt
# Or with conda:
# conda install -c conda-forge pyspark pandas prefect taipy openai langchain

# 3. Set your OpenAI API Key
export OPENAI_API_KEY="your_api_key_here"
# On Windows (PowerShell):
# $env:OPENAI_API_KEY="your_api_key_here"

# 4. Run the Prefect pipeline (optional, to clean/update data)
python orchestration/check_and_clean.py

# 5. Run the chatbot app
python app.py


from datetime import datetime
import pandas as pd
from taipy.gui import Gui, Markdown
from analyzer.analyzer import ask
from pipelines.clean_metrics import run_cleaner  

upload_path = None
chat_history = []
user_input = ""
data_preview = pd.DataFrame()

def on_chat(state):
    if state.user_input.strip():
        q = state.user_input.strip()
        state.chat_history.append(("You", q))
        response = ask(q)  
        state.chat_history.append(("Bot", response))
        state.user_input = ""

page = Markdown("""
# 🐒 Monkeytype Data Chatbot

---

## 💬 Chat with Your Data
Ask natural language questions about your typing performance, accuracy, speed trends, and more.

<|{user_input}|input|label=Type your question here...|on_action=on_chat|>  
<|Ask|button|on_action=on_chat|>

**Chat History:**  
<|{chat_history}|chat|>
""")

if __name__ == "__main__":
    gui = Gui(page)
    gui.run(title="Monkeytype Chatbot", port=5000)
